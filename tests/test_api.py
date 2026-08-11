"""The FastAPI surface, and its parity with the dependency-free fallback.

Skipped whole when the ``api`` extra is not installed — the engine must stay
usable with nothing on the system, so these tests cannot be a reason to install
FastAPI.

``ParityTests`` is the point of the file: the same request against both surfaces
has to come back the same, because they are two shells over one
``WorkflowService`` and a difference means one of them has grown its own opinion.
"""

import asyncio
import json
import unittest
from pathlib import Path

from flowforge import Graph
from flowforge.service import WorkflowService

try:
    import httpx

    from flowforge.api import FASTAPI_AVAILABLE, create_app, routes_for
except ImportError:  # pragma: no cover
    FASTAPI_AVAILABLE = False

EXAMPLES = Path(__file__).resolve().parent.parent / "examples"
requires_api = unittest.skipUnless(
    FASTAPI_AVAILABLE, "needs the api extra: pip install 'flowforge[api]'"
)


def service_for(*names: str) -> WorkflowService:
    return WorkflowService(
        {name: Graph.from_file(EXAMPLES / f"{name}.json") for name in names}
    )


class ClientMixin:
    def client_for(self, service: WorkflowService):
        transport = httpx.ASGITransport(app=create_app(service))
        return httpx.AsyncClient(transport=transport, base_url="http://flowforge")

    async def sse_text(self, client, url: str, payload=None) -> str:
        async with client.stream("POST", url, json=payload) as response:
            self.assertEqual(response.status_code, 200)
            self.assertIn("text/event-stream", response.headers["content-type"])
            return "".join([chunk async for chunk in response.aiter_text()])


@requires_api
class EndpointTests(ClientMixin, unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.service = service_for("diamond", "research", "approval")
        self.client = self.client_for(self.service)

    async def asyncTearDown(self):
        await self.client.aclose()

    async def test_health(self):
        response = await self.client.get("/health")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"status": "ok"})

    async def test_workflow_catalogue(self):
        response = await self.client.get("/workflows")
        listed = {item["id"] for item in response.json()["workflows"]}
        self.assertEqual(listed, {"diamond", "research", "approval"})

    async def test_run_returns_the_result(self):
        response = await self.client.post("/runs/diamond", json={"inputs": {"q": "hi"}})
        payload = response.json()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(payload["status"], "completed")
        self.assertIn("stats", payload)

    async def test_a_run_with_no_body_works(self):
        # The fallback server accepts a bodyless POST, so this one must too.
        response = await self.client.post("/runs/diamond")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["status"], "completed")

    async def test_unknown_workflow_is_404_with_an_error_envelope(self):
        response = await self.client.post("/runs/ghost")
        self.assertEqual(response.status_code, 404)
        self.assertIn("ghost", response.json()["error"])

    async def test_a_malformed_body_is_400_not_422(self):
        response = await self.client.post(
            "/runs/diamond",
            content=b'{"inputs": "not an object"}',
            headers={"content-type": "application/json"},
        )
        self.assertEqual(response.status_code, 400)
        self.assertIn("error", response.json())

    async def test_stream_emits_sse_frames(self):
        body = await self.sse_text(self.client, "/runs/research/stream")

        frames = [frame for frame in body.split("\n\n") if frame.strip()]
        self.assertTrue(frames[0].startswith("id: 1\nevent: run.started"))
        self.assertIn("event: node.delta", body)
        self.assertIn("event: run.completed", frames[-1])

    async def test_openapi_schema_documents_the_routes(self):
        schema = (await self.client.get("/openapi.json")).json()
        self.assertIn("/runs/{workflow_id}", schema["paths"])
        self.assertIn("post", schema["paths"]["/runs/{workflow_id}"])

    async def test_the_route_surface_is_what_the_fallback_serves(self):
        # FastAPI's own /docs and /openapi.json are excluded: they are not part
        # of this service's contract, and the fallback has no equivalent.
        self.assertEqual(
            routes_for(self.service),
            [
                ("GET", "/health"),
                ("GET", "/runs"),
                ("GET", "/workflows"),
                ("GET", "/workflows/{workflow_id}"),
                ("POST", "/runs/{run_id}/resume"),
                ("POST", "/runs/{run_id}/resume-stream"),
                ("POST", "/runs/{workflow_id}"),
                ("POST", "/runs/{workflow_id}/stream"),
            ],
        )

    async def test_the_docs_endpoints_exist_but_are_opt_in_to_this_check(self):
        self.assertIn(("GET", "/openapi.json"), routes_for(self.service, True))


@requires_api
class PauseResumeTests(ClientMixin, unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.service = service_for("approval")
        self.client = self.client_for(self.service)

    async def asyncTearDown(self):
        await self.client.aclose()

    async def test_pause_then_resume(self):
        paused = (await self.client.post("/runs/approval")).json()
        self.assertEqual(paused["status"], "paused")
        self.assertIn("ask", paused["awaiting"])

        listed = (await self.client.get("/runs")).json()["paused"]
        self.assertEqual(listed, [paused["run"]])

        done = (
            await self.client.post(
                f"/runs/{paused['run']}/resume",
                json={"answers": {"ask": {"approved": True}}},
            )
        ).json()
        self.assertEqual(done["status"], "completed")
        self.assertEqual(done["run"], paused["run"])
        self.assertEqual((await self.client.get("/runs")).json()["paused"], [])

    async def test_a_streamed_pause_is_persisted_too(self):
        body = await self.sse_text(self.client, "/runs/approval/stream")
        self.assertIn("event: run.paused", body)

        run_id = (await self.client.get("/runs")).json()["paused"][0]
        resumed = await self.sse_text(
            self.client,
            f"/runs/{run_id}/resume-stream",
            {"answers": {"ask": {"approved": False}}},
        )
        self.assertIn("event: run.resumed", resumed)
        self.assertIn("event: run.completed", resumed)

    async def test_resuming_an_unknown_run_is_404(self):
        response = await self.client.post("/runs/nope/resume", json={"answers": {}})
        self.assertEqual(response.status_code, 404)

    async def test_resuming_without_the_answer_is_400(self):
        paused = (await self.client.post("/runs/approval")).json()
        response = await self.client.post(
            f"/runs/{paused['run']}/resume", json={"answers": {}}
        )
        self.assertEqual(response.status_code, 400)
        self.assertIn("ask", response.json()["error"])


@requires_api
class ParityTests(unittest.IsolatedAsyncioTestCase):
    """Both surfaces, same request, same answer."""

    async def asyncSetUp(self):
        from flowforge.server import WorkflowServer

        graphs = {
            name: Graph.from_file(EXAMPLES / f"{name}.json")
            for name in ("diamond", "approval")
        }
        self.fallback = WorkflowServer(graphs)
        self._server = await asyncio.start_server(
            self.fallback._handle, "127.0.0.1", 0
        )
        self.port = self._server.sockets[0].getsockname()[1]

        transport = httpx.ASGITransport(app=create_app(WorkflowService(graphs)))
        self.client = httpx.AsyncClient(transport=transport, base_url="http://flowforge")

    async def asyncTearDown(self):
        await self.client.aclose()
        self._server.close()
        await self._server.wait_closed()

    async def raw(self, method: str, path: str, body=None):
        reader, writer = await asyncio.open_connection("127.0.0.1", self.port)
        raw = b"" if body is None else json.dumps(body).encode()
        writer.write(
            f"{method} {path} HTTP/1.1\r\nHost: localhost\r\n"
            f"Content-Type: application/json\r\n"
            f"Content-Length: {len(raw)}\r\n\r\n".encode()
            + raw
        )
        await writer.drain()
        payload = await reader.read()
        writer.close()
        await writer.wait_closed()
        head, _, rest = payload.partition(b"\r\n\r\n")
        return int(head.split(b" ")[1]), rest.decode()

    async def test_health_and_catalogue_match(self):
        for path in ("/health", "/workflows", "/runs"):
            with self.subTest(path=path):
                status, body = await self.raw("GET", path)
                response = await self.client.get(path)

                self.assertEqual(status, response.status_code)
                self.assertEqual(json.loads(body), response.json())

    async def test_a_completed_run_matches_apart_from_ids_and_timings(self):
        status, body = await self.raw("POST", "/runs/diamond", {"inputs": {"q": "x"}})
        response = await self.client.post("/runs/diamond", json={"inputs": {"q": "x"}})

        self.assertEqual(status, response.status_code)
        fallback, api = json.loads(body), response.json()
        self.assertEqual(fallback.keys(), api.keys())
        self.assertEqual(fallback["status"], api["status"])
        self.assertEqual(fallback["outputs"], api["outputs"])
        self.assertEqual(fallback["failures"], api["failures"])
        # Wall times differ run to run; the shape of the stats must not.
        self.assertEqual(fallback["stats"].keys(), api["stats"].keys())
        self.assertEqual(
            fallback["stats"]["nodes_executed"], api["stats"]["nodes_executed"]
        )

    async def test_error_envelopes_match(self):
        status, body = await self.raw("POST", "/runs/ghost")
        response = await self.client.post("/runs/ghost")

        self.assertEqual(status, 404)
        self.assertEqual(response.status_code, 404)
        self.assertEqual(json.loads(body).keys(), response.json().keys())
        self.assertIn("ghost", json.loads(body)["error"])
        self.assertIn("ghost", response.json()["error"])

    async def test_a_pause_looks_the_same_on_both(self):
        status, body = await self.raw("POST", "/runs/approval")
        response = await self.client.post("/runs/approval")

        fallback, api = json.loads(body), response.json()
        self.assertEqual(status, response.status_code)
        self.assertEqual(fallback["status"], api["status"], "both should pause")
        self.assertEqual(fallback["awaiting"], api["awaiting"])


if __name__ == "__main__":
    unittest.main()


class EngineReuseTests(unittest.IsolatedAsyncioTestCase):
    """One engine per workflow, reused — including under concurrency.

    An engine assigns nothing to ``self`` after construction, so reuse is safe;
    these tests are what makes that a checked property rather than a comment.
    """

    def setUp(self):
        self.service = service_for("diamond", "approval")

    def test_the_same_engine_comes_back_every_time(self):
        first = self.service.engine_for("diamond")
        self.assertIs(self.service.engine_for("diamond"), first)
        self.assertIsNot(self.service.engine_for("approval"), first)

    def test_an_unknown_workflow_is_still_a_404_and_is_not_cached(self):
        from flowforge.service import ServiceError

        for _ in range(2):
            with self.assertRaises(ServiceError) as raised:
                self.service.engine_for("ghost")
            self.assertEqual(raised.exception.status, 404)

    async def test_concurrent_runs_on_one_engine_do_not_mix(self):
        # Different inputs through the same engine at the same time: if any
        # per-run state lived on the instance, these would cross-contaminate.
        results = await asyncio.gather(
            *(self.service.start("diamond", {"q": f"run{i}"}) for i in range(12))
        )

        self.assertEqual(
            sorted(r.outputs["end"]["join"]["q"] for r in results),
            sorted(f"run{i}" for i in range(12)),
        )
        self.assertEqual(len({r.run_id for r in results}), 12)

    async def test_a_paused_run_and_a_fresh_run_share_the_engine_safely(self):
        paused = await self.service.start("approval")
        other = await self.service.start("approval")
        self.assertNotEqual(paused.run_id, other.run_id)

        done = await self.service.resume(paused.run_id, {"ask": {"approved": True}})
        self.assertEqual(done.run_id, paused.run_id)
        self.assertEqual(await self.service.paused_ids(), [other.run_id])


@requires_api
class WorkflowShapeTests(ClientMixin, unittest.IsolatedAsyncioTestCase):
    """The graph itself, for anything that draws it."""

    async def asyncSetUp(self):
        self.service = service_for("diamond", "triage")
        self.client = self.client_for(self.service)

    async def asyncTearDown(self):
        await self.client.aclose()

    async def test_nodes_edges_and_waves_come_back(self):
        shape = (await self.client.get("/workflows/diamond")).json()

        self.assertEqual(shape["id"], "diamond")
        self.assertTrue(all({"id", "type"} <= set(n) for n in shape["nodes"]))
        self.assertTrue(all({"source", "target"} <= set(e) for e in shape["edges"]))
        # Every edge endpoint must be a declared node, or a client would draw an
        # edge into nothing.
        ids = {n["id"] for n in shape["nodes"]}
        for edge in shape["edges"]:
            self.assertIn(edge["source"], ids)
            self.assertIn(edge["target"], ids)

    async def test_waves_cover_every_node_exactly_once(self):
        shape = (await self.client.get("/workflows/diamond")).json()
        flattened = [node for wave in shape["waves"] for node in wave]

        self.assertEqual(sorted(flattened), sorted(n["id"] for n in shape["nodes"]))
        self.assertEqual(len(flattened), len(set(flattened)))

    async def test_branch_labels_survive(self):
        # triage branches, so at least one edge must carry its label — otherwise
        # a drawn graph cannot show which way a decision went.
        shape = (await self.client.get("/workflows/triage")).json()
        self.assertTrue(any(e["branch"] for e in shape["edges"]))

    async def test_an_unknown_workflow_is_404(self):
        response = await self.client.get("/workflows/ghost")
        self.assertEqual(response.status_code, 404)
        self.assertIn("ghost", response.json()["error"])

    async def test_the_fallback_server_serves_the_same_shape(self):
        from flowforge.server import WorkflowServer

        graphs = {
            name: Graph.from_file(EXAMPLES / f"{name}.json")
            for name in ("diamond", "triage")
        }
        fallback = WorkflowServer(graphs)
        server = await asyncio.start_server(fallback._handle, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        try:
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            writer.write(b"GET /workflows/diamond HTTP/1.1\r\nHost: x\r\n\r\n")
            await writer.drain()
            raw = await reader.read()
            writer.close()
            await writer.wait_closed()
        finally:
            server.close()
            await server.wait_closed()

        body = json.loads(raw.partition(b"\r\n\r\n")[2])
        self.assertEqual(body, (await self.client.get("/workflows/diamond")).json())
