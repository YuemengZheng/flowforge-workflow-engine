"""Where paused runs live between requests.

Two implementations behind one small interface: an in-process store for tests
and single-process use, and a Redis-backed one for anything with more than one
worker. Both round-trip the checkpoint through JSON, so a test against the
memory store exercises the same serialisation the Redis store relies on.

The Redis client is ~80 lines of RESP over ``asyncio.open_connection`` rather
than a dependency — the protocol is small, and it keeps the engine importable
with nothing installed.
"""

from __future__ import annotations

import asyncio
from typing import Any, Protocol, runtime_checkable

from .checkpoint import Checkpoint
from .errors import FlowForgeError
from .pool import ConnectionPool, PooledConnection


@runtime_checkable
class RunStore(Protocol):
    """Persistence for paused runs, keyed by run id."""

    async def save(self, checkpoint: Checkpoint) -> None: ...
    async def load(self, run_id: str) -> Checkpoint | None: ...
    async def delete(self, run_id: str) -> bool: ...
    async def list_ids(self) -> list[str]: ...


class MemoryRunStore:
    """Dict-backed store. Fine for one process; loses everything on restart."""

    def __init__(self) -> None:
        self._items: dict[str, str] = {}

    async def save(self, checkpoint: Checkpoint) -> None:
        self._items[checkpoint.run_id] = checkpoint.to_json()

    async def load(self, run_id: str) -> Checkpoint | None:
        raw = self._items.get(run_id)
        return None if raw is None else Checkpoint.from_json(raw)

    async def delete(self, run_id: str) -> bool:
        return self._items.pop(run_id, None) is not None

    async def list_ids(self) -> list[str]:
        return sorted(self._items)

    def __len__(self) -> int:
        return len(self._items)


# ------------------------------------------------------------------ RESP


class RedisError(FlowForgeError):
    """The Redis server returned an error, or the connection broke."""


def encode_command(*args: Any) -> bytes:
    """Encode a command as a RESP array of bulk strings."""
    parts = [b"*%d\r\n" % len(args)]
    for arg in args:
        if isinstance(arg, bytes):
            raw = arg
        elif isinstance(arg, str):
            raw = arg.encode("utf-8")
        else:
            raw = str(arg).encode("utf-8")
        parts.append(b"$%d\r\n" % len(raw))
        parts.append(raw)
        parts.append(b"\r\n")
    return b"".join(parts)


async def read_reply(reader: asyncio.StreamReader) -> Any:
    """Read one RESP reply. Bulk strings come back as bytes, errors raise."""
    line = await reader.readline()
    if not line:
        raise RedisError("connection closed while awaiting a reply")
    tag, payload = line[:1], line[1:-2]
    if tag == b"+":
        return payload.decode("utf-8")
    if tag == b"-":
        raise RedisError(payload.decode("utf-8"))
    if tag == b":":
        return int(payload)
    if tag == b"$":
        length = int(payload)
        if length == -1:
            return None
        body = await reader.readexactly(length + 2)
        return body[:-2]
    if tag == b"*":
        count = int(payload)
        if count == -1:
            return None
        return [await read_reply(reader) for _ in range(count)]
    raise RedisError(f"unrecognised RESP reply type {tag!r}")


class RedisClient:
    """Pooled Redis client.

    Redis handles one command at a time per connection, so a single shared
    connection does not just cost reconnects — it *serialises* every caller.
    A workflow wave that fires 50 checkpoint writes at once would run them one
    after another no matter how much concurrency the scheduler arranged. One
    connection per in-flight command, bounded and reused, fixes that.

    ``AUTH`` and ``SELECT`` run on each connection as it is created, since they
    are per-connection state rather than per-client.
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 6379,
        db: int = 0,
        password: str | None = None,
        max_connections: int = 10,
    ) -> None:
        self.host = host
        self.port = port
        self.db = db
        self.password = password
        self._pool = ConnectionPool(self._open, max_size=max_connections)

    @property
    def stats(self) -> dict[str, int]:
        return dict(self._pool.stats)

    async def _open(self) -> PooledConnection:
        reader, writer = await asyncio.open_connection(self.host, self.port)
        connection = PooledConnection(reader, writer)
        # Sent directly rather than through execute(), which would try to take
        # a connection from the pool that this call is in the middle of filling.
        for command in (
            ("AUTH", self.password) if self.password else (),
            ("SELECT", self.db) if self.db else (),
        ):
            if not command:
                continue
            writer.write(encode_command(*command))
            await writer.drain()
            await read_reply(reader)
        return connection

    async def execute(self, *args: Any) -> Any:
        connection = await self._pool.acquire()
        reuse = True
        try:
            connection.writer.write(encode_command(*args))
            await connection.writer.drain()
            return await read_reply(connection.reader)
        except RedisError:
            # An error *reply* was read in full — the connection is still in
            # sync and perfectly reusable. Only transport faults poison it.
            raise
        except (ConnectionError, asyncio.IncompleteReadError, OSError) as exc:
            reuse = False
            raise RedisError(f"redis connection lost: {exc!r}") from exc
        except BaseException:
            reuse = False
            raise
        finally:
            await self._pool.release(connection, reuse=reuse)

    async def close(self) -> None:
        await self._pool.close()


class RedisRunStore:
    """Checkpoints in Redis, one key per run, with an optional TTL.

    ``ttl_s`` is a safety valve, not a policy: a run nobody ever answers should
    not sit in Redis forever. Resuming rewrites the key, so an active
    conversation keeps refreshing its own expiry.
    """

    def __init__(
        self,
        client: RedisClient | None = None,
        prefix: str = "flowforge:run:",
        ttl_s: int | None = 86_400,
        **connection: Any,
    ) -> None:
        self.client = client or RedisClient(**connection)
        self.prefix = prefix
        self.ttl_s = ttl_s

    def _key(self, run_id: str) -> str:
        return f"{self.prefix}{run_id}"

    async def save(self, checkpoint: Checkpoint) -> None:
        args: list[Any] = ["SET", self._key(checkpoint.run_id), checkpoint.to_json()]
        if self.ttl_s:
            args += ["EX", self.ttl_s]
        await self.client.execute(*args)

    async def load(self, run_id: str) -> Checkpoint | None:
        raw = await self.client.execute("GET", self._key(run_id))
        if raw is None:
            return None
        text = raw.decode("utf-8") if isinstance(raw, bytes) else str(raw)
        return Checkpoint.from_json(text)

    async def delete(self, run_id: str) -> bool:
        return bool(await self.client.execute("DEL", self._key(run_id)))

    async def list_ids(self) -> list[str]:
        """SCAN rather than KEYS — KEYS blocks the server on a large keyspace."""
        found: list[str] = []
        cursor: Any = 0
        while True:
            cursor, batch = await self.client.execute(
                "SCAN", cursor, "MATCH", f"{self.prefix}*", "COUNT", 100
            )
            for key in batch or []:
                name = key.decode("utf-8") if isinstance(key, bytes) else str(key)
                found.append(name[len(self.prefix) :])
            cursor = int(cursor)
            if cursor == 0:
                return sorted(found)

    async def ping(self) -> bool:
        return await self.client.execute("PING") == "PONG"

    async def close(self) -> None:
        await self.client.close()
