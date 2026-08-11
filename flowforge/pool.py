"""Connection pooling.

A pool exists to stop paying for the handshake. For Redis that is a TCP round
trip per command if you reconnect; for HTTPS it is TCP plus a TLS negotiation,
which dwarfs the request itself on a short call. Both matter here because a
wide workflow wave issues its calls all at once.

The single-connection-behind-a-lock design this replaces was worse than it
looked: it did not just cost handshakes, it *serialised* every caller, so a
50-node wave talking to Redis ran its commands one after another no matter how
much concurrency the scheduler had arranged.

Shape of the pool: bounded, lazily grown, with connections health-checked on
checkout and discarded rather than returned when a call leaves them in an
unknown state. Idle connections above ``min_size`` are reaped once they pass
``max_idle_s``, because the other end closes them anyway and a silently dead
socket is the classic pool bug.
"""

from __future__ import annotations

import asyncio
import json
import ssl
from dataclasses import dataclass, field
from time import monotonic
from typing import Any, AsyncIterator, Awaitable, Callable, Mapping
from urllib.parse import urlsplit

from .errors import FlowForgeError


class PoolError(FlowForgeError):
    """The pool could not hand out a usable connection."""


@dataclass
class PooledConnection:
    """A live socket pair plus the bookkeeping the pool reaps it by."""

    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    created_at: float = field(default_factory=monotonic)
    idle_since: float = field(default_factory=monotonic)
    uses: int = 0

    @property
    def closed(self) -> bool:
        return self.writer.is_closing() or self.reader.at_eof()

    async def close(self) -> None:
        if not self.writer.is_closing():
            self.writer.close()
            try:
                await self.writer.wait_closed()
            except (ConnectionError, OSError):
                pass


class ConnectionPool:
    """Bounded pool of connections to one endpoint.

    ``acquire`` blocks when every connection is checked out and the pool is at
    ``max_size`` — back-pressure rather than an unbounded fan-out that the
    server would refuse anyway.
    """

    def __init__(
        self,
        connect: Callable[[], Awaitable[PooledConnection]],
        max_size: int = 10,
        min_size: int = 0,
        max_idle_s: float = 60.0,
        max_uses: int = 0,
    ) -> None:
        if max_size < 1:
            raise ValueError("max_size must be >= 1")
        if min_size < 0 or min_size > max_size:
            raise ValueError("min_size must be between 0 and max_size")
        self._connect = connect
        self.max_size = max_size
        self.min_size = min_size
        self.max_idle_s = max_idle_s
        self.max_uses = max_uses
        self._idle: list[PooledConnection] = []
        self._in_use = 0
        self._closed = False
        self._capacity = asyncio.Semaphore(max_size)
        self._lock = asyncio.Lock()
        self.stats = {"created": 0, "reused": 0, "discarded": 0}

    @property
    def size(self) -> int:
        return len(self._idle) + self._in_use

    @property
    def idle(self) -> int:
        return len(self._idle)

    def _expired(self, connection: PooledConnection) -> bool:
        if connection.closed:
            return True
        if self.max_uses and connection.uses >= self.max_uses:
            return True
        idle_for = monotonic() - connection.idle_since
        return len(self._idle) > self.min_size and idle_for > self.max_idle_s

    async def acquire(self) -> PooledConnection:
        if self._closed:
            raise PoolError("pool is closed")
        await self._capacity.acquire()
        try:
            async with self._lock:
                while self._idle:
                    candidate = self._idle.pop()
                    # Health check on checkout: the far end may have hung up
                    # while this one sat idle, and handing out a dead socket
                    # turns into a confusing error at the call site.
                    if self._expired(candidate):
                        self.stats["discarded"] += 1
                        await candidate.close()
                        continue
                    self._in_use += 1
                    self.stats["reused"] += 1
                    candidate.uses += 1
                    return candidate
                self._in_use += 1
            connection = await self._connect()
            connection.uses = 1
            self.stats["created"] += 1
            return connection
        except BaseException:
            async with self._lock:
                self._in_use = max(0, self._in_use - 1)
            self._capacity.release()
            raise

    async def release(self, connection: PooledConnection, reuse: bool = True) -> None:
        async with self._lock:
            self._in_use = max(0, self._in_use - 1)
            if reuse and not self._closed and not connection.closed:
                connection.idle_since = monotonic()
                self._idle.append(connection)
            else:
                self.stats["discarded"] += 1
                await connection.close()
        self._capacity.release()

    async def close(self) -> None:
        self._closed = True
        async with self._lock:
            idle, self._idle = self._idle, []
        for connection in idle:
            await connection.close()


# ---------------------------------------------------------------------- HTTP


@dataclass
class HTTPResponse:
    status: int
    headers: dict[str, str]
    body: str


class HTTPConnectionPool:
    """Keep-alive HTTP/1.1 client with one pool per origin.

    Only what an MCP endpoint needs: POST a JSON body, read a JSON response,
    handle both ``Content-Length`` and ``Transfer-Encoding: chunked``. A response
    the parser cannot resynchronise on closes the connection instead of
    returning it to the pool — a half-read body poisons the next caller.
    """

    def __init__(self, max_size: int = 10, timeout_s: float = 30.0) -> None:
        self.max_size = max_size
        self.timeout_s = timeout_s
        self._pools: dict[tuple[str, str, int], ConnectionPool] = {}
        self._lock = asyncio.Lock()

    def _pool_for(self, scheme: str, host: str, port: int) -> ConnectionPool:
        key = (scheme, host, port)
        if key not in self._pools:
            async def connect() -> PooledConnection:
                context = ssl.create_default_context() if scheme == "https" else None
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(host, port, ssl=context),
                    timeout=self.timeout_s,
                )
                return PooledConnection(reader, writer)

            self._pools[key] = ConnectionPool(connect, max_size=self.max_size)
        return self._pools[key]

    async def request(
        self,
        method: str,
        url: str,
        body: bytes = b"",
        headers: Mapping[str, str] | None = None,
    ) -> tuple[int, dict[str, str], bytes]:
        """One request/response over a pooled connection, body as bytes.

        Bytes rather than text because this also fetches objects out of S3, and
        decoding a PNG as UTF-8 to hand it back is not a service anyone wants.
        """
        parts = urlsplit(url)
        scheme = parts.scheme or "http"
        host = parts.hostname or "127.0.0.1"
        port = parts.port or (443 if scheme == "https" else 80)
        target = parts.path or "/"
        if parts.query:
            target = f"{target}?{parts.query}"

        request_headers = {
            "Host": parts.netloc,
            "Content-Length": str(len(body)),
            "Connection": "keep-alive",
            **dict(headers or {}),
        }
        head = f"{method.upper()} {target} HTTP/1.1\r\n" + "".join(
            f"{k}: {v}\r\n" for k, v in request_headers.items()
        )

        async with self._lock:
            pool = self._pool_for(scheme, host, port)

        connection = await pool.acquire()
        reusable = True
        try:
            connection.writer.write(head.encode("latin-1") + b"\r\n" + body)
            await connection.writer.drain()
            status, response_headers = await asyncio.wait_for(
                _read_head(connection.reader), timeout=self.timeout_s
            )
            raw = await asyncio.wait_for(
                _read_body(connection.reader, response_headers), timeout=self.timeout_s
            )
            reusable = response_headers.get("connection", "").lower() != "close"
            return status, response_headers, raw
        except Exception:
            reusable = False
            raise
        finally:
            await pool.release(connection, reuse=reusable)

    async def post_json(
        self,
        url: str,
        payload: Mapping[str, Any],
        headers: Mapping[str, str] | None = None,
    ) -> tuple[int, str]:
        status, _, raw = await self.request(
            "POST",
            url,
            json.dumps(payload).encode("utf-8"),
            {
                "Content-Type": "application/json",
                "Accept": "application/json, text/event-stream",
                **dict(headers or {}),
            },
        )
        return status, raw.decode("utf-8", errors="replace")

    async def stream_body(
        self,
        url: str,
        payload: Mapping[str, Any],
        headers: Mapping[str, str] | None = None,
        timeout_s: float | None = None,
    ) -> AsyncIterator[bytes]:
        """POST and yield the response body as it arrives, transfer-decoding it.

        The counterpart to :meth:`post_json` for responses that are the point of
        the call rather than the end of it — a token stream is useless buffered.
        Yields raw bytes because the framing above HTTP differs per vendor (SSE
        lines for most, binary event frames for Bedrock); parsing that is the
        caller's job.

        The per-read timeout applies to each read, not to the whole stream: a
        model that thinks for a minute and then talks for two is normal, and a
        total deadline would kill it. Retries and an overall limit are the node's
        ``retries`` / ``timeout``, as everywhere else.
        """
        parts = urlsplit(url)
        scheme = parts.scheme or "http"
        host = parts.hostname or "127.0.0.1"
        port = parts.port or (443 if scheme == "https" else 80)
        target = parts.path or "/"
        if parts.query:
            target = f"{target}?{parts.query}"
        read_timeout = self.timeout_s if timeout_s is None else timeout_s

        body = json.dumps(payload).encode("utf-8")
        request_headers = {
            "Host": parts.netloc,
            "Content-Type": "application/json",
            "Accept": "text/event-stream, application/json",
            "Content-Length": str(len(body)),
            "Connection": "keep-alive",
            **dict(headers or {}),
        }
        head = f"POST {target} HTTP/1.1\r\n" + "".join(
            f"{k}: {v}\r\n" for k, v in request_headers.items()
        )

        async with self._lock:
            pool = self._pool_for(scheme, host, port)

        connection = await pool.acquire()
        # A stream the caller abandons leaves unread bytes in the socket, so the
        # connection only goes back to the pool when the body ended cleanly.
        finished = False
        try:
            connection.writer.write(head.encode("latin-1") + b"\r\n" + body)
            await connection.writer.drain()
            reader = connection.reader
            status, response_headers = await asyncio.wait_for(
                _read_head(reader), timeout=read_timeout
            )
            if status != 200:
                detail = await asyncio.wait_for(
                    _read_error_body(reader, response_headers), timeout=read_timeout
                )
                raise PoolError(f"HTTP {status} from {host}: {detail[:400]}")

            chunked = response_headers.get("transfer-encoding", "").lower() == "chunked"
            if chunked:
                while True:
                    size_line = await asyncio.wait_for(
                        reader.readline(), timeout=read_timeout
                    )
                    size = int(size_line.split(b";")[0].strip() or b"0", 16)
                    if size == 0:
                        await asyncio.wait_for(reader.readline(), timeout=read_timeout)
                        break
                    chunk = await asyncio.wait_for(
                        reader.readexactly(size), timeout=read_timeout
                    )
                    await asyncio.wait_for(reader.readexactly(2), timeout=read_timeout)
                    yield chunk
                finished = response_headers.get("connection", "").lower() != "close"
            else:
                remaining = int(response_headers.get("content-length", 0) or 0)
                if remaining:
                    while remaining > 0:
                        chunk = await asyncio.wait_for(
                            reader.read(min(65536, remaining)), timeout=read_timeout
                        )
                        if not chunk:
                            raise PoolError("connection closed mid-body")
                        remaining -= len(chunk)
                        yield chunk
                    finished = (
                        response_headers.get("connection", "").lower() != "close"
                    )
                else:
                    # No length and no chunking: the body ends at EOF, so the
                    # connection is spent by definition.
                    while True:
                        chunk = await asyncio.wait_for(
                            reader.read(65536), timeout=read_timeout
                        )
                        if not chunk:
                            break
                        yield chunk
        finally:
            await pool.release(connection, reuse=finished)

    async def close(self) -> None:
        for pool in list(self._pools.values()):
            await pool.close()
        self._pools.clear()


async def _read_head(reader: asyncio.StreamReader) -> tuple[int, dict[str, str]]:
    status_line = await reader.readline()
    if not status_line:
        raise PoolError("connection closed before a response arrived")
    parts = status_line.decode("latin-1").split(None, 2)
    if len(parts) < 2 or not parts[1].isdigit():
        raise PoolError(f"malformed status line: {status_line!r}")
    headers: dict[str, str] = {}
    while True:
        line = await reader.readline()
        if line in (b"\r\n", b"\n", b""):
            break
        name, _, value = line.decode("latin-1").partition(":")
        headers[name.strip().lower()] = value.strip()
    return int(parts[1]), headers


async def _read_error_body(
    reader: asyncio.StreamReader, headers: Mapping[str, str]
) -> str:
    """Best-effort body read for an error response, so the message says why."""
    try:
        if headers.get("transfer-encoding", "").lower() == "chunked":
            collected = []
            while True:
                size_line = await reader.readline()
                size = int(size_line.split(b";")[0].strip() or b"0", 16)
                if size == 0:
                    break
                collected.append(await reader.readexactly(size))
                await reader.readexactly(2)
            return b"".join(collected).decode("utf-8", errors="replace")
        length = int(headers.get("content-length", 0) or 0)
        raw = await reader.readexactly(length) if length else b""
        return raw.decode("utf-8", errors="replace")
    except (asyncio.IncompleteReadError, ConnectionError, ValueError, OSError):
        return "<no body>"


async def _read_body(
    reader: asyncio.StreamReader, headers: Mapping[str, str]
) -> bytes:
    """The body of a complete (non-streamed) response."""
    if headers.get("transfer-encoding", "").lower() == "chunked":
        chunks = []
        while True:
            size_line = await reader.readline()
            size = int(size_line.split(b";")[0].strip() or b"0", 16)
            if size == 0:
                await reader.readline()  # trailing CRLF after the last chunk
                break
            chunks.append(await reader.readexactly(size))
            await reader.readexactly(2)
        return b"".join(chunks)
    length = int(headers.get("content-length", 0) or 0)
    return await reader.readexactly(length) if length else b""


async def _read_http_response(reader: asyncio.StreamReader) -> HTTPResponse:
    status, headers = await _read_head(reader)
    body = await _read_body(reader, headers)
    return HTTPResponse(status, headers, body.decode("utf-8", errors="replace"))
