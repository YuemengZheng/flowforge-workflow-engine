"""Connection pooling, and the HTTP MCP transport that rides on it.

The HTTP tests run against a real asyncio server that counts how many TCP
connections it accepted — which is the only way to actually demonstrate that
pooling happened, rather than asserting on the pool's own bookkeeping.
"""

import asyncio
import json
import unittest

from flowforge.mcp import HTTPTransport, MCPClient, MCPError
from flowforge.pool import ConnectionPool, HTTPConnectionPool, PoolError, PooledConnection


class FakeMCPHTTPServer:
    """Minimal keep-alive HTTP server speaking MCP, counting connections."""

    def __init__(self, chunked: bool = False, status: int = 200) -> None:
        self.connections = 0
        self.requests = 0
        self.chunked = chunked
        self.status = status
        self._server: asyncio.Server | None = None

    @property
    def port(self) -> int:
        assert self._server is not None
        return self._server.sockets[0].getsockname()[1]

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._handle, "127.0.0.1", 0)

    async def stop(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()

    def _result(self, message):
        method = message.get("method")
        if method == "initialize":
            return {"protocolVersion": "2025-06-18", "capabilities": {},
                    "serverInfo": {"name": "http-server", "version": "1"}}
        if method == "tools/list":
            return {"tools": [{"name": "ping", "description": "pong",
                               "inputSchema": {"type": "object", "properties": {}}}]}
        if method == "tools/call":
            return {"content": [{"type": "text", "text": "pong"}]}
        return {}

    async def _handle(self, reader, writer):
        self.connections += 1
        try:
            while True:
                request_line = await reader.readline()
                if not request_line:
                    return
                length = 0
                while True:
                    line = await reader.readline()
                    if line in (b"\r\n", b"\n", b""):
                        break
                    name, _, value = line.decode().partition(":")
                    if name.strip().lower() == "content-length":
                        length = int(value.strip())
                raw = await reader.readexactly(length) if length else b"{}"
                message = json.loads(raw)
                self.requests += 1

                if "id" not in message:
                    body = b""
                else:
                    body = json.dumps(
                        {"jsonrpc": "2.0", "id": message["id"],
                         "result": self._result(message)}
                    ).encode()

                if self.chunked:
                    frame = (
                        f"HTTP/1.1 {self.status} OK\r\nContent-Type: application/json\r\n"
                        "Transfer-Encoding: chunked\r\n\r\n"
                    ).encode()
                    frame += b"%x\r\n%s\r\n0\r\n\r\n" % (len(body), body) if body else b"0\r\n\r\n"
                else:
                    frame = (
                        f"HTTP/1.1 {self.status} OK\r\nContent-Type: application/json\r\n"
                        f"Content-Length: {len(body)}\r\n\r\n"
                    ).encode() + body
                writer.write(frame)
                await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionResetError):
            return
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except (ConnectionError, OSError):
                pass


class PoolMechanicsTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.opened = 0

        async def connect():
            self.opened += 1
            reader, writer = await asyncio.open_connection(self.server.host, self.server.port)
            return PooledConnection(reader, writer)

        self.server = _EchoTCPServer()
        await self.server.start()
        self.connect = connect

    async def asyncTearDown(self):
        await self.server.stop()

    async def test_a_released_connection_is_reused(self):
        pool = ConnectionPool(self.connect, max_size=4)
        first = await pool.acquire()
        await pool.release(first)
        second = await pool.acquire()

        self.assertIs(first, second)
        self.assertEqual(self.opened, 1)
        self.assertEqual(pool.stats["reused"], 1)
        await pool.close()

    async def test_concurrent_callers_get_distinct_connections(self):
        pool = ConnectionPool(self.connect, max_size=4)
        held = [await pool.acquire() for _ in range(3)]

        self.assertEqual(len({id(c) for c in held}), 3)
        self.assertEqual(pool.size, 3)
        for connection in held:
            await pool.release(connection)
        await pool.close()

    async def test_the_pool_is_bounded_and_applies_back_pressure(self):
        pool = ConnectionPool(self.connect, max_size=2)
        a = await pool.acquire()
        b = await pool.acquire()

        waiting = asyncio.create_task(pool.acquire())
        await asyncio.sleep(0)
        self.assertFalse(waiting.done())  # capacity is exhausted; the caller waits

        await pool.release(a)
        third = await asyncio.wait_for(waiting, timeout=1)
        self.assertIs(third, a)

        await pool.release(b)
        await pool.release(third)
        await pool.close()

    async def test_a_connection_returned_as_broken_is_discarded(self):
        pool = ConnectionPool(self.connect, max_size=4)
        connection = await pool.acquire()
        await pool.release(connection, reuse=False)

        self.assertEqual(pool.idle, 0)
        self.assertEqual(pool.stats["discarded"], 1)
        await pool.close()

    async def test_a_dead_connection_is_not_handed_out(self):
        pool = ConnectionPool(self.connect, max_size=4)
        connection = await pool.acquire()
        await pool.release(connection)

        await connection.close()  # the far end went away while it sat idle
        fresh = await pool.acquire()

        self.assertIsNot(fresh, connection)
        self.assertEqual(self.opened, 2)
        await pool.release(fresh)
        await pool.close()

    async def test_max_uses_recycles_a_connection(self):
        pool = ConnectionPool(self.connect, max_size=2, max_uses=2)
        first = await pool.acquire()
        await pool.release(first)
        second = await pool.acquire()  # second use
        await pool.release(second)
        third = await pool.acquire()  # would be the third; must be a new socket

        self.assertIsNot(third, first)
        await pool.release(third)
        await pool.close()

    async def test_using_a_closed_pool_is_refused(self):
        pool = ConnectionPool(self.connect)
        await pool.close()
        with self.assertRaises(PoolError):
            await pool.acquire()

    async def test_invalid_sizes_are_rejected(self):
        with self.assertRaises(ValueError):
            ConnectionPool(self.connect, max_size=0)
        with self.assertRaises(ValueError):
            ConnectionPool(self.connect, max_size=2, min_size=5)


class _EchoTCPServer:
    """A socket that accepts and holds connections, for pool mechanics tests."""

    host = "127.0.0.1"

    def __init__(self):
        self._server = None

    @property
    def port(self):
        return self._server.sockets[0].getsockname()[1]

    async def start(self):
        async def handle(reader, writer):
            try:
                while await reader.read(1024):
                    pass
            except ConnectionError:
                pass
            finally:
                writer.close()
                try:
                    await writer.wait_closed()
                except (ConnectionError, OSError):
                    pass

        self._server = await asyncio.start_server(handle, self.host, 0)

    async def stop(self):
        self._server.close()
        await self._server.wait_closed()


class HTTPPoolTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.server = FakeMCPHTTPServer()
        await self.server.start()
        self.pool = HTTPConnectionPool(max_size=4)
        self.url = f"http://127.0.0.1:{self.server.port}/mcp"

    async def asyncTearDown(self):
        await self.pool.close()
        await self.server.stop()

    async def test_sequential_requests_share_one_connection(self):
        for i in range(5):
            status, body = await self.pool.post_json(
                self.url, {"jsonrpc": "2.0", "id": i, "method": "initialize"}
            )
            self.assertEqual(status, 200)
            self.assertEqual(json.loads(body)["id"], i)

        self.assertEqual(self.server.requests, 5)
        self.assertEqual(self.server.connections, 1)  # the whole point

    async def test_concurrent_requests_open_at_most_max_size(self):
        await asyncio.gather(
            *(
                self.pool.post_json(
                    self.url, {"jsonrpc": "2.0", "id": i, "method": "initialize"}
                )
                for i in range(12)
            )
        )
        self.assertEqual(self.server.requests, 12)
        self.assertLessEqual(self.server.connections, 4)

    async def test_chunked_responses_are_decoded(self):
        server = FakeMCPHTTPServer(chunked=True)
        await server.start()
        pool = HTTPConnectionPool()
        try:
            status, body = await pool.post_json(
                f"http://127.0.0.1:{server.port}/mcp",
                {"jsonrpc": "2.0", "id": 1, "method": "initialize"},
            )
            self.assertEqual(status, 200)
            self.assertEqual(json.loads(body)["id"], 1)
        finally:
            await pool.close()
            await server.stop()


class HTTPTransportTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.server = FakeMCPHTTPServer()
        await self.server.start()
        self.client = MCPClient(
            "http", HTTPTransport(f"http://127.0.0.1:{self.server.port}/mcp")
        )

    async def asyncTearDown(self):
        await self.client.close()
        await self.server.stop()

    async def test_full_mcp_flow_over_http(self):
        tools = await self.client.list_tools()
        self.assertEqual([t.name for t in tools], ["ping"])

        result = await self.client.call_tool("ping", {})
        self.assertEqual(result["text"], "pong")

    async def test_the_whole_flow_reuses_one_connection(self):
        await self.client.list_tools()
        await self.client.call_tool("ping", {})
        self.assertEqual(self.server.connections, 1)

    async def test_an_http_error_is_reported(self):
        server = FakeMCPHTTPServer(status=500)
        await server.start()
        client = MCPClient("bad", HTTPTransport(f"http://127.0.0.1:{server.port}/mcp"))
        try:
            with self.assertRaises(MCPError) as ctx:
                await client.initialize()
            self.assertIn("HTTP 500", str(ctx.exception))
        finally:
            await client.close()
            await server.stop()


if __name__ == "__main__":
    unittest.main()
