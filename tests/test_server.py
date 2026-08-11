"""Exercises the HTTP surface over a real socket, not a mocked one."""

import asyncio
import json
import unittest
from pathlib import Path

from flowforge import Edge, Graph, NodeSpec
from flowforge.server import WorkflowServer

EXAMPLES = Path(__file__).resolve().parent.parent / "examples"


def demo_graph():
    return Graph(
        [
            NodeSpec("start", "start"),
            NodeSpec("say", "llm", {"prompt": "hello there friend of mine"}),
            NodeSpec("end", "end"),
        ],
        [Edge("start", "say"), Edge("say", "end")],
        graph_id="demo",
    )


class ServerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.server_app = WorkflowServer({"demo": demo_graph()})
        self._server = await asyncio.start_server(
            self.server_app._handle, "127.0.0.1", 0
        )
        self.port = self._server.sockets[0].getsockname()[1]

    async def asyncTearDown(self):
        self._server.close()
        await self._server.wait_closed()

    async def request(self, method, path, body=None):
        reader, writer = await asyncio.open_connection("127.0.0.1", self.port)
        raw = b"" if body is None else json.dumps(body).encode()
        writer.write(
            f"{method} {path} HTTP/1.1\r\nHost: localhost\r\n"
            f"Content-Length: {len(raw)}\r\n\r\n".encode()
            + raw
        )
        await writer.drain()
        payload = await reader.read()
        writer.close()
        await writer.wait_closed()
        head, _, rest = payload.partition(b"\r\n\r\n")
        status = int(head.split(b" ")[1])
        return status, head.decode(), rest.decode()

    async def test_health(self):
        status, _, body = await self.request("GET", "/health")
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body), {"status": "ok"})

    async def test_workflow_listing(self):
        status, _, body = await self.request("GET", "/workflows")
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body)["workflows"][0]["id"], "demo")

    async def test_run_returns_result_json(self):
        status, _, body = await self.request("POST", "/runs/demo", {"inputs": {}})
        payload = json.loads(body)

        self.assertEqual(status, 200)
        self.assertEqual(payload["status"], "completed")
        self.assertEqual(payload["stats"]["nodes_executed"], 3)
        self.assertIn("end", payload["outputs"])

    async def test_stream_returns_sse_frames(self):
        status, head, body = await self.request("POST", "/runs/demo/stream")

        self.assertEqual(status, 200)
        self.assertIn("text/event-stream", head)
        frames = [f for f in body.split("\n\n") if f.strip()]
        self.assertTrue(frames[0].startswith("id: 1\nevent: run.started"))
        self.assertIn("event: node.delta", body)
        self.assertTrue(frames[-1].startswith("id: "))
        self.assertIn("event: run.completed", frames[-1])

    async def test_unknown_workflow_is_404(self):
        status, _, body = await self.request("POST", "/runs/ghost")
        self.assertEqual(status, 404)
        self.assertIn("ghost", json.loads(body)["error"])

    async def test_wrong_method_is_405(self):
        status, _, _ = await self.request("GET", "/runs/demo")
        self.assertEqual(status, 405)

    async def test_bad_json_is_400(self):
        reader, writer = await asyncio.open_connection("127.0.0.1", self.port)
        writer.write(b"POST /runs/demo HTTP/1.1\r\nContent-Length: 5\r\n\r\n{oops")
        await writer.drain()
        payload = await reader.read()
        writer.close()
        await writer.wait_closed()
        self.assertIn(b"400", payload.split(b"\r\n")[0])

    async def test_unknown_route_is_404(self):
        status, _, _ = await self.request("GET", "/nope")
        self.assertEqual(status, 404)


class PauseResumeOverHTTPTests(unittest.IsolatedAsyncioTestCase):
    """The full loop a caller sees: run -> paused -> answer -> completed."""

    async def asyncSetUp(self):
        self.app = WorkflowServer(
            {"approval": Graph.from_file(EXAMPLES / "approval.json")}
        )
        self._server = await asyncio.start_server(self.app._handle, "127.0.0.1", 0)
        self.port = self._server.sockets[0].getsockname()[1]
        self.request = ServerTests.request.__get__(self)

    async def asyncTearDown(self):
        self._server.close()
        await self._server.wait_closed()

    async def test_json_pause_then_resume(self):
        status, _, body = await self.request("POST", "/runs/approval")
        paused = json.loads(body)

        self.assertEqual(status, 200)
        self.assertEqual(paused["status"], "paused")
        self.assertIn("ask", paused["awaiting"])

        # The pause outlived the request: a later call can look it up by id.
        _, _, listing = await self.request("GET", "/runs")
        self.assertEqual(json.loads(listing)["paused"], [paused["run"]])

        status, _, body = await self.request(
            "POST", f"/runs/{paused['run']}/resume", {"answers": {"ask": {"approved": True}}}
        )
        done = json.loads(body)
        self.assertEqual(status, 200)
        self.assertEqual(done["status"], "completed")
        self.assertEqual(done["run"], paused["run"])

        # Finished runs are cleared from the store.
        _, _, listing = await self.request("GET", "/runs")
        self.assertEqual(json.loads(listing)["paused"], [])

    async def test_streamed_pause_is_also_persisted(self):
        _, _, body = await self.request("POST", "/runs/approval/stream")
        self.assertIn("event: node.paused", body)
        self.assertIn("event: run.paused", body)

        _, _, listing = await self.request("GET", "/runs")
        run_id = json.loads(listing)["paused"][0]

        _, _, resumed = await self.request(
            "POST",
            f"/runs/{run_id}/resume-stream",
            {"answers": {"ask": {"approved": False}}},
        )
        self.assertIn("event: run.resumed", resumed)
        self.assertIn("event: run.completed", resumed)
        self.assertIn("node.skipped", resumed)

    async def test_resuming_an_unknown_run_is_404(self):
        status, _, _ = await self.request("POST", "/runs/nope/resume", {"answers": {}})
        self.assertEqual(status, 404)

    async def test_resuming_without_the_answer_is_400(self):
        _, _, body = await self.request("POST", "/runs/approval")
        run_id = json.loads(body)["run"]

        status, _, body = await self.request(
            "POST", f"/runs/{run_id}/resume", {"answers": {}}
        )
        self.assertEqual(status, 400)
        self.assertIn("ask", json.loads(body)["error"])


class DirectoryLoadingTests(unittest.TestCase):
    def test_examples_directory_loads_and_skips_invalid_fixtures(self):
        server = WorkflowServer.from_directory(EXAMPLES)
        self.assertEqual(
            sorted(server.workflows),
            ["approval", "batch_review", "diamond", "recovery", "research", "showcase", "triage"],
        )


if __name__ == "__main__":
    unittest.main()
