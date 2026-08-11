"""A minimal asyncio HTTP server that exposes workflows over SSE.

The dependency-free fallback. ``api.py`` is the FastAPI service layer and is what
the containers run; this file keeps the whole path — engine -> event queue ->
``text/event-stream`` -> client — working with nothing installed, which is worth
having both as a demonstration and for anywhere pip is not an option. It speaks
enough HTTP/1.1 for that and closes each connection when it is done.

What a run *means* is not decided here: that is ``service.WorkflowService``,
shared with the FastAPI surface so the two cannot drift.

    GET  /health                 liveness
    GET  /workflows              what is loaded
    POST /runs/<id>              run to completion, return JSON
    POST /runs/<id>/stream       run and stream events as SSE
"""

from __future__ import annotations

import asyncio
import json
from typing import Any, AsyncIterator, Mapping

from .service import ServiceError, WorkflowService

MAX_BODY_BYTES = 1 << 20


class HTTPError(ServiceError):
    """Kept as a name: this module's callers have always caught ``HTTPError``."""


STATUS_TEXT = {
    200: "OK",
    400: "Bad Request",
    404: "Not Found",
    405: "Method Not Allowed",
    413: "Payload Too Large",
    500: "Internal Server Error",
}


class WorkflowServer(WorkflowService):
    """Serves a directory of workflow JSON files over hand-rolled HTTP/1.1."""

    # ------------------------------------------------------------------ HTTP

    async def serve(self, host: str = "127.0.0.1", port: int = 8000) -> None:
        server = await asyncio.start_server(self._handle, host, port)
        addresses = ", ".join(str(s.getsockname()) for s in server.sockets or [])
        print(f"flowforge serving {len(self.workflows)} workflow(s) on {addresses}")
        async with server:
            await server.serve_forever()

    async def _handle(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            method, target, body = await self._read_request(reader)
            await self._route(method, target, body, writer)
        except ServiceError as exc:
            # The base class, not HTTPError: the shared service raises plain
            # ServiceErrors and they carry the status this should answer with.
            await self._send_json(writer, exc.status, {"error": str(exc)})
        except (ConnectionResetError, asyncio.IncompleteReadError):
            pass
        except Exception as exc:  # never let one bad request kill the server
            await self._send_json(writer, 500, {"error": f"{type(exc).__name__}: {exc}"})
        finally:
            writer.close()
            with_suppressed = getattr(writer, "wait_closed", None)
            if with_suppressed is not None:
                try:
                    await writer.wait_closed()
                except (ConnectionResetError, BrokenPipeError):
                    pass

    async def _read_request(
        self, reader: asyncio.StreamReader
    ) -> tuple[str, str, dict[str, Any]]:
        request_line = await reader.readline()
        if not request_line:
            raise asyncio.IncompleteReadError(b"", None)
        try:
            method, target, _ = request_line.decode("latin-1").split()
        except ValueError:
            raise HTTPError(400, "malformed request line") from None

        length = 0
        while True:
            line = await reader.readline()
            if line in (b"\r\n", b"\n", b""):
                break
            name, _, value = line.decode("latin-1").partition(":")
            if name.strip().lower() == "content-length":
                length = int(value.strip() or 0)

        if length > MAX_BODY_BYTES:
            raise HTTPError(413, "request body too large")
        raw = await reader.readexactly(length) if length else b""
        if not raw:
            return method, target, {}
        try:
            body = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise HTTPError(400, f"invalid JSON body: {exc}") from None
        if not isinstance(body, dict):
            raise HTTPError(400, "request body must be a JSON object")
        return method, target, body

    async def _route(
        self,
        method: str,
        target: str,
        body: dict[str, Any],
        writer: asyncio.StreamWriter,
    ) -> None:
        path = target.split("?", 1)[0].rstrip("/") or "/"
        parts = [segment for segment in path.split("/") if segment]

        if path == "/health":
            return await self._send_json(writer, 200, {"status": "ok"})
        if path == "/workflows":
            return await self._send_json(writer, 200, {"workflows": self.catalogue()})
        if parts[:1] == ["workflows"] and len(parts) == 2 and method == "GET":
            return await self._send_json(writer, 200, self.shape_of(parts[1]))
        if path == "/runs" and method == "GET":
            return await self._send_json(
                writer, 200, {"paused": await self.paused_ids()}
            )
        if parts and parts[0] == "runs" and len(parts) in (2, 3):
            if method != "POST":
                raise HTTPError(405, f"{method} not allowed on {path}")
            streaming = len(parts) == 3 and parts[2] == "stream"

            # /runs/<workflow>[/stream]  starts a run;
            # /runs/<run_id>/resume      answers a paused one.
            if len(parts) == 3 and parts[2] == "resume":
                return await self._resume(parts[1], body, writer, streaming=False)
            if len(parts) == 3 and parts[2] == "resume-stream":
                return await self._resume(parts[1], body, writer, streaming=True)
            if len(parts) == 2 or streaming:
                inputs = body.get("inputs", {})
                if streaming:
                    return await self._sse(self.stream(parts[1], inputs), writer)
                return await self._finish(await self.start(parts[1], inputs), writer)
        raise HTTPError(404, f"no route for {method} {path}")

    async def _resume(
        self,
        run_id: str,
        body: Mapping[str, Any],
        writer: asyncio.StreamWriter,
        streaming: bool,
    ) -> None:
        answers = body.get("answers", {})
        if streaming:
            return await self._sse(await self.resume_stream(run_id, answers), writer)
        return await self._finish(await self.resume(run_id, answers), writer)

    async def _finish(self, result: Any, writer: asyncio.StreamWriter) -> None:
        await self._send_json(writer, 200, self.payload_for(result))

    async def _sse(
        self, events: AsyncIterator[Any], writer: asyncio.StreamWriter
    ) -> None:
        writer.write(
            b"HTTP/1.1 200 OK\r\n"
            b"Content-Type: text/event-stream\r\n"
            b"Cache-Control: no-cache\r\n"
            b"Connection: close\r\n"
            b"X-Accel-Buffering: no\r\n\r\n"
        )
        await writer.drain()
        # drain() after every frame is the back-pressure link: a slow client
        # parks this coroutine, the bounded queue fills, and the producing node
        # suspends rather than buffering the whole run in memory. The store
        # bookkeeping happens inside sse_frames, shared with the FastAPI surface.
        async for frame in self.sse_frames(events):
            writer.write(frame.encode("utf-8"))
            await writer.drain()

    async def _send_json(
        self, writer: asyncio.StreamWriter, status: int, payload: Mapping[str, Any]
    ) -> None:
        raw = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
        writer.write(
            f"HTTP/1.1 {status} {STATUS_TEXT.get(status, 'Unknown')}\r\n"
            f"Content-Type: application/json\r\n"
            f"Content-Length: {len(raw)}\r\n"
            f"Connection: close\r\n\r\n".encode("latin-1")
            + raw
        )
        await writer.drain()
