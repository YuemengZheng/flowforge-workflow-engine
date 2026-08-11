"""The queue worker: job parsing, the run loop, and results filed to storage.

The worker's own logic is tested against a stub client, because all it asks of
Redis is ``BRPOP``. The *queue* — that LPUSH/BRPOP really is an atomic FIFO and
two workers never take one job — is tested against a real server when
``FLOWFORGE_TEST_REDIS`` is set, since that is a claim about Redis, not about this
code, and a stub asserting it would be asserting my own assumption.
"""

import asyncio
import os
import unittest
from pathlib import Path

from flowforge import Graph, RedisClient, RunStatus
from flowforge.errors import FlowForgeError
from flowforge.service import WorkflowService
from flowforge.worker import DEFAULT_QUEUE, Job, Worker, enqueue

REAL_REDIS = os.environ.get("FLOWFORGE_TEST_REDIS")
EXAMPLES = Path(__file__).resolve().parent.parent / "examples"


def service_for(*names: str) -> WorkflowService:
    return WorkflowService(
        {name: Graph.from_file(EXAMPLES / f"{name}.json") for name in names}
    )


class StubClient:
    """Hands out queued replies to BRPOP, and records what it was asked."""

    def __init__(self, jobs: list[str]) -> None:
        self.jobs = list(jobs)
        self.commands: list[tuple] = []
        self.closed = False

    async def execute(self, *args):
        self.commands.append(args)
        if args[0] == "BRPOP":
            if not self.jobs:
                return None  # the blocking read timed out
            return [args[1], self.jobs.pop(0).encode()]
        if args[0] == "LPUSH":
            self.jobs.append(args[2])
            return len(self.jobs)
        return None

    async def close(self):
        self.closed = True


class RecordingArtifacts:
    def __init__(self):
        self.saved = []

    async def save_result(self, result):
        self.saved.append(result.run_id)
        return None

    async def close(self):
        pass


class JobTests(unittest.TestCase):
    def test_round_trips_through_json(self):
        job = Job("triage", {"severity": 9})
        restored = Job.from_json(job.to_json())

        self.assertEqual(restored.workflow, "triage")
        self.assertEqual(restored.inputs, {"severity": 9})

    def test_inputs_default_to_empty(self):
        self.assertEqual(Job.from_json('{"workflow": "x"}').inputs, {})

    def test_a_job_without_a_workflow_is_refused(self):
        for text in ('{"inputs": {}}', "[]", '{"workflow": ""}'):
            with self.subTest(text=text):
                with self.assertRaises(FlowForgeError):
                    Job.from_json(text)

    def test_malformed_json_is_refused(self):
        with self.assertRaises(FlowForgeError) as raised:
            Job.from_json("{nope")
        self.assertIn("not JSON", str(raised.exception))


class WorkerTests(unittest.IsolatedAsyncioTestCase):
    async def test_a_queued_job_runs(self):
        client = StubClient([Job("diamond", {"q": "hi"}).to_json()])
        worker = Worker(service_for("diamond"), client)

        result = await worker.run_once()

        self.assertIs(result.status, RunStatus.COMPLETED)
        self.assertEqual(result.outputs["end"]["join"]["q"], "hi")
        self.assertEqual(worker.completed, 1)

    async def test_an_empty_queue_returns_nothing(self):
        worker = Worker(service_for("diamond"), StubClient([]))
        self.assertIsNone(await worker.run_once())

    async def test_drain_runs_everything_queued_and_stops(self):
        jobs = [Job("diamond").to_json() for _ in range(3)]
        worker = Worker(service_for("diamond"), StubClient(jobs))

        self.assertEqual(await worker.drain(), 3)
        self.assertEqual(worker.completed, 3)

    async def test_results_are_filed_to_object_storage(self):
        artifacts = RecordingArtifacts()
        client = StubClient([Job("diamond").to_json()])
        worker = Worker(service_for("diamond"), client, artifacts=artifacts)

        result = await worker.run_once()
        self.assertEqual(artifacts.saved, [result.run_id])

    async def test_a_paused_run_is_not_filed_as_a_result(self):
        # It has no result yet; the checkpoint is in the store and the answer
        # will arrive through the API.
        artifacts = RecordingArtifacts()
        client = StubClient([Job("approval").to_json()])
        worker = Worker(service_for("approval"), client, artifacts=artifacts)

        result = await worker.run_once()

        self.assertIs(result.status, RunStatus.PAUSED)
        self.assertEqual(artifacts.saved, [])
        # ...but it is resumable, which is the point of sharing the store.
        self.assertEqual(await worker.service.paused_ids(), [result.run_id])

    async def test_a_bad_job_does_not_stop_the_loop(self):
        client = StubClient(["{nope", Job("diamond").to_json()])
        worker = Worker(service_for("diamond"), client)
        stop = asyncio.Event()

        async def until_done():
            while worker.completed < 1:
                await asyncio.sleep(0.005)
            stop.set()

        await asyncio.gather(worker.run_forever(stop), until_done())

        self.assertEqual(worker.completed, 1)
        self.assertEqual(worker.failed, 1)

    async def test_an_unknown_workflow_counts_as_a_failure_and_carries_on(self):
        client = StubClient([Job("ghost").to_json(), Job("diamond").to_json()])
        worker = Worker(service_for("diamond"), client)
        stop = asyncio.Event()

        async def until_done():
            while worker.completed < 1:
                await asyncio.sleep(0.005)
            stop.set()

        await asyncio.gather(worker.run_forever(stop), until_done())

        # The unknown workflow is that job's problem; the next job still ran.
        self.assertEqual(worker.failed, 1)
        self.assertEqual(worker.completed, 1)

    async def test_run_once_propagates_rather_than_swallowing(self):
        # drain() has no error policy on purpose — run_forever owns that, and a
        # batch caller should see the failure rather than a silent skip.
        worker = Worker(service_for("diamond"), StubClient([Job("ghost").to_json()]))
        with self.assertRaises(FlowForgeError):
            await worker.run_once()

    async def test_run_forever_stops_when_asked(self):
        worker = Worker(service_for("diamond"), StubClient([]), block_seconds=0)
        stop = asyncio.Event()
        stop.set()

        await asyncio.wait_for(worker.run_forever(stop), timeout=2)
        self.assertFalse(worker.running)

    async def test_brpop_is_given_the_queue_and_a_timeout(self):
        client = StubClient([])
        worker = Worker(service_for("diamond"), client, queue="q", block_seconds=3)
        await worker.take()

        self.assertEqual(client.commands[0], ("BRPOP", "q", 3))


@unittest.skipUnless(REAL_REDIS, "set FLOWFORGE_TEST_REDIS=host:port to run these")
class RealQueueTests(unittest.IsolatedAsyncioTestCase):
    """LPUSH/BRPOP against a real server — the FIFO and atomicity claims."""

    async def asyncSetUp(self):
        host, _, port = REAL_REDIS.partition(":")
        self.client = RedisClient(host=host, port=int(port or 6379))
        self.queue = "flowforge:test:jobs"
        await self.client.execute("DEL", self.queue)

    async def asyncTearDown(self):
        await self.client.execute("DEL", self.queue)
        await self.client.close()

    async def test_a_job_survives_the_round_trip(self):
        worker = Worker(service_for("diamond"), self.client, queue=self.queue)
        await enqueue(self.client, Job("diamond", {"q": "real"}), self.queue)

        result = await worker.run_once()
        self.assertEqual(result.outputs["end"]["join"]["q"], "real")

    async def test_the_queue_is_first_in_first_out(self):
        worker = Worker(service_for("triage"), self.client, queue=self.queue)
        for severity in (1, 2, 3):
            await enqueue(self.client, Job("triage", {"severity": severity}), self.queue)

        seen = []
        for _ in range(3):
            job = await worker.take()
            seen.append(job.inputs["severity"])

        self.assertEqual(seen, [1, 2, 3])

    async def test_two_workers_never_take_the_same_job(self):
        service = service_for("diamond")
        first = Worker(service, self.client, queue=self.queue, block_seconds=1)
        second = Worker(service, self.client, queue=self.queue, block_seconds=1)
        for index in range(6):
            await enqueue(self.client, Job("diamond", {"n": index}), self.queue)

        taken = await asyncio.gather(first.drain(), second.drain())

        self.assertEqual(sum(taken), 6)
        self.assertEqual(await self.client.execute("LLEN", self.queue), 0)

    async def test_the_default_queue_name_is_namespaced(self):
        self.assertTrue(DEFAULT_QUEUE.startswith("flowforge:"))


if __name__ == "__main__":
    unittest.main()


class HeartbeatTests(unittest.IsolatedAsyncioTestCase):
    """The worker's liveness signal, which is what its healthcheck probes."""

    async def test_the_loop_refreshes_a_key_with_a_ttl(self):
        client = StubClient([])
        worker = Worker(service_for("diamond"), client, block_seconds=0)
        await worker.beat()

        self.assertEqual(
            client.commands[-1],
            ("SET", "flowforge:worker:heartbeat", "1", "EX", 30),
        )

    async def test_the_ttl_outlives_one_blocking_read(self):
        # Otherwise the key lapses while the loop is legitimately waiting, and a
        # healthy idle worker would be reported unhealthy.
        worker = Worker(service_for("diamond"), StubClient([]), block_seconds=5)
        self.assertGreater(worker.heartbeat_ttl_s, worker.block_seconds)

    async def test_a_dead_redis_does_not_kill_the_loop(self):
        class Broken(StubClient):
            async def execute(self, *args):
                if args[0] == "SET":
                    raise OSError("redis is gone")
                return await super().execute(*args)

        worker = Worker(service_for("diamond"), Broken([]), block_seconds=0)
        await worker.beat()  # telemetry, not work: must not raise

    async def test_run_forever_beats_before_it_blocks(self):
        client = StubClient([])
        worker = Worker(service_for("diamond"), client, block_seconds=0)
        stop = asyncio.Event()
        stop.set()

        await asyncio.wait_for(worker.run_forever(stop), timeout=2)

        self.assertTrue(any(c[0] == "SET" for c in client.commands))
