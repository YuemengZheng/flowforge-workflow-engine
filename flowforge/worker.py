"""A worker that runs queued workflows instead of serving HTTP.

The API answers requests; the worker exists for the runs nobody is waiting on the
other end of — a batch, a webhook's follow-up work, anything where holding a
connection open for the duration is the wrong shape. Jobs arrive on a Redis list,
results go to object storage, and the API and the worker share the run store, so a
run the worker pauses can be answered through the API.

The queue is a Redis list with ``BRPOP``, which is the smallest thing that is
actually correct here: it blocks (no polling loop), and it is atomic, so two
workers never take the same job. What it deliberately does not have is
redelivery — a worker that dies mid-job loses that job's *dispatch*, though not
its progress if per-wave checkpointing is on. Redis Streams with consumer groups
would fix that, and the honest reason it is a list is that redelivery semantics
are a project of their own, not something to imply in passing.
"""

from __future__ import annotations

import asyncio
import json
import signal
from typing import Any, Mapping

from .artifacts import ArtifactStore
from .engine import RunResult, RunStatus
from .errors import FlowForgeError
from .service import WorkflowService
from .store import RedisClient

DEFAULT_QUEUE = "flowforge:jobs"


class Job:
    """One unit of queued work: which workflow, with what inputs."""

    __slots__ = ("workflow", "inputs", "run_id")

    def __init__(
        self, workflow: str, inputs: Mapping[str, Any] | None = None, run_id: str = ""
    ) -> None:
        self.workflow = workflow
        self.inputs = dict(inputs or {})
        self.run_id = run_id

    @classmethod
    def from_json(cls, text: str) -> "Job":
        try:
            data = json.loads(text)
        except json.JSONDecodeError as exc:
            raise FlowForgeError(f"job is not JSON: {exc}") from exc
        if not isinstance(data, dict) or not data.get("workflow"):
            raise FlowForgeError("job must be an object with a 'workflow' key")
        return cls(str(data["workflow"]), data.get("inputs"), str(data.get("run_id", "")))

    def to_json(self) -> str:
        payload: dict[str, Any] = {"workflow": self.workflow, "inputs": self.inputs}
        if self.run_id:
            payload["run_id"] = self.run_id
        return json.dumps(payload)

    def __repr__(self) -> str:
        return f"<Job {self.workflow!r} inputs={sorted(self.inputs)}>"


async def enqueue(client: RedisClient, job: Job, queue: str = DEFAULT_QUEUE) -> int:
    """Push a job. ``LPUSH`` with ``BRPOP`` makes the list a FIFO."""
    return int(await client.execute("LPUSH", queue, job.to_json()))


class Worker:
    """Takes jobs off a queue, runs them, files the results."""

    def __init__(
        self,
        service: WorkflowService,
        client: RedisClient,
        queue: str = DEFAULT_QUEUE,
        artifacts: ArtifactStore | None = None,
        block_seconds: int = 5,
        heartbeat_key: str = "flowforge:worker:heartbeat",
        heartbeat_ttl_s: int = 30,
    ) -> None:
        self.service = service
        self.client = client
        self.queue = queue
        self.artifacts = artifacts
        self.block_seconds = block_seconds
        # A consumer has no port, so "is it up" cannot be answered with an HTTP
        # probe. It can be answered with proof that the loop is still turning:
        # a key refreshed every iteration, with a TTL longer than one blocking
        # read. If the loop wedges, the key lapses and the healthcheck fails.
        self.heartbeat_key = heartbeat_key
        self.heartbeat_ttl_s = heartbeat_ttl_s
        self.running = False
        self.completed = 0
        self.failed = 0

    async def take(self) -> Job | None:
        """Block for the next job, or return ``None`` when the wait times out.

        The timeout is not impatience: it is what lets the loop notice it has
        been asked to stop without a second channel to poll.
        """
        reply = await self.client.execute("BRPOP", self.queue, self.block_seconds)
        if not reply:
            return None
        _, raw = reply
        text = raw.decode("utf-8") if isinstance(raw, bytes) else str(raw)
        return Job.from_json(text)

    async def handle(self, job: Job) -> RunResult:
        result = await self.service.start(job.workflow, job.inputs)
        if self.artifacts is not None and result.status is not RunStatus.PAUSED:
            # A paused run has no result yet; its checkpoint is already in the
            # store, and the API is where the answer will arrive.
            await self.artifacts.save_result(result)
        if result.status is RunStatus.COMPLETED:
            self.completed += 1
        elif result.status is RunStatus.FAILED:
            self.failed += 1
        return result

    async def run_once(self) -> RunResult | None:
        job = await self.take()
        if job is None:
            return None
        return await self.handle(job)

    async def beat(self) -> None:
        """Refresh the liveness key. Never fatal: telemetry, not work."""
        try:
            await self.client.execute(
                "SET", self.heartbeat_key, "1", "EX", self.heartbeat_ttl_s
            )
        except Exception:
            pass

    async def run_forever(self, stop: asyncio.Event | None = None) -> None:
        stop = stop or asyncio.Event()
        self.running = True
        print(f"worker: waiting on {self.queue}", flush=True)
        await self.beat()
        try:
            while not stop.is_set():
                await self.beat()
                try:
                    result = await self.run_once()
                except FlowForgeError as exc:
                    # A malformed job is that job's problem, not the worker's.
                    self.failed += 1
                    print(f"worker: bad job: {exc}", flush=True)
                    continue
                except asyncio.CancelledError:
                    raise
                except Exception as exc:  # keep the loop alive
                    self.failed += 1
                    print(f"worker: {type(exc).__name__}: {exc}", flush=True)
                    continue
                if result is None:
                    # An empty read. With a real BRPOP this already blocked for
                    # `block_seconds`, but yielding here keeps the loop
                    # cooperative for any client that answers immediately —
                    # otherwise nothing else on the event loop, including the
                    # signal handler that sets `stop`, ever gets to run.
                    await asyncio.sleep(0)
                    continue
                print(
                    f"worker: {result.run_id} {result.status.value} "
                    f"({result.stats.total_ms:.1f}ms)",
                    flush=True,
                )
        finally:
            self.running = False

    async def drain(self, limit: int = 1000) -> int:
        """Run everything currently queued, then return. For batch use and tests."""
        handled = 0
        while handled < limit:
            job = await self.take()
            if job is None:
                return handled
            await self.handle(job)
            handled += 1
        return handled

    async def close(self) -> None:
        await self.client.close()
        if self.artifacts is not None:
            await self.artifacts.close()
        await self.service.close()


def install_signal_handlers(stop: asyncio.Event) -> None:
    """SIGTERM/SIGINT set the stop event so the current job finishes first.

    Compose sends SIGTERM on ``down``; a worker that dies mid-run leaves a job
    half-done, and finishing the one in hand costs at most ``block_seconds``.
    """
    loop = asyncio.get_running_loop()
    for name in ("SIGTERM", "SIGINT"):
        try:
            loop.add_signal_handler(getattr(signal, name), stop.set)
        except (NotImplementedError, AttributeError):  # pragma: no cover
            pass
