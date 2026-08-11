"""What the HTTP surfaces have in common, with no HTTP in it.

There are two surfaces — FastAPI in ``api.py`` and the dependency-free
``asyncio`` server in ``server.py`` — and exactly one set of rules about what a
run means: which workflows exist, when a checkpoint is written, when it is
deleted, what a result looks like as JSON. Those rules live here so the two
cannot drift apart. A bug fixed in one is fixed in both, and the framework choice
stays a delivery detail.

The only thing this module knows about HTTP is that some failures carry a status
code, which is why :class:`ServiceError` has one.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, AsyncIterator, Mapping

from .checkpoint import Checkpoint, CheckpointError
from .engine import RunResult, RunStatus, WorkflowEngine
from .errors import FlowForgeError
from .events import Event
from .graph import Graph
from .store import MemoryRunStore, RunStore


class ServiceError(FlowForgeError):
    """A request that cannot be served, with the status it should become."""

    def __init__(self, status: int, message: str) -> None:
        self.status = status
        super().__init__(message)


class WorkflowService:
    """Runs, resumes and streams a fixed set of workflows."""

    def __init__(
        self,
        workflows: Mapping[str, Graph],
        max_concurrency: int | None = None,
        store: RunStore | None = None,
        event_sink: Any = None,
    ) -> None:
        self.workflows = dict(workflows)
        self.max_concurrency = max_concurrency
        # Handed to every engine this builds, so run events reach Kafka (or any
        # other sink) whether the caller streamed or not.
        self.event_sink = event_sink
        # Paused runs outlive the request that created them, so they need to
        # live somewhere. In-process by default; Redis or SQL when there is more
        # than one worker and the answer may arrive at a different one.
        self.store: RunStore = store or MemoryRunStore()
        self._engines: dict[str, WorkflowEngine] = {}

    @classmethod
    def from_directory(cls, directory: str | Path, **kwargs: Any) -> "WorkflowService":
        loaded: dict[str, Graph] = {}
        for path in sorted(Path(directory).glob("*.json")):
            if ".invalid." in path.name:  # fixtures that exist to fail validation
                continue
            graph = Graph.from_file(path)
            loaded[graph.id] = graph
        if not loaded:
            raise FlowForgeError(f"no workflow JSON files found in {directory}")
        return cls(loaded, **kwargs)

    # ----------------------------------------------------------------- lookup

    def engine_for(self, workflow_id: str) -> WorkflowEngine:
        """The engine for a workflow, built once and reused.

        An engine holds no per-run state — ``run()`` builds a fresh ``_RunState``
        every time and nothing is assigned to ``self`` after construction — so one
        instance serves every request, including concurrent ones. Constructing per
        request meant recompiling every node's config per request, which is 14% of
        the wall time of a 500-node run.
        """
        try:
            engine = self._engines[workflow_id]
        except KeyError:
            pass
        else:
            return engine

        try:
            graph = self.workflows[workflow_id]
        except KeyError:
            raise ServiceError(404, f"unknown workflow {workflow_id!r}") from None
        engine = WorkflowEngine(
            graph,
            max_concurrency=self.max_concurrency,
            event_sink=self.event_sink,
        )
        self._engines[workflow_id] = engine
        return engine

    def catalogue(self) -> list[dict[str, Any]]:
        return [
            {
                "id": graph.id,
                "nodes": len(graph),
                "edges": len(graph.edges),
                "max_width": graph.max_width,
            }
            for graph in self.workflows.values()
        ]

    def shape_of(self, workflow_id: str) -> dict[str, Any]:
        """The graph itself — nodes, edges and wave layout.

        ``catalogue()`` returns counts, which is all a list view needs. Anything
        that draws the graph needs the graph, and that is this: node ids and
        types, edges with their branch label, and the wave layout the scheduler
        would use, so a client can lay the DAG out left to right without
        reimplementing the topological sort.
        """
        try:
            graph = self.workflows[workflow_id]
        except KeyError:
            raise ServiceError(404, f"unknown workflow {workflow_id!r}") from None
        return {
            "id": graph.id,
            "nodes": [
                {"id": spec.id, "type": spec.type} for spec in graph.nodes.values()
            ],
            "edges": [
                {"source": edge.source, "target": edge.target, "branch": edge.branch}
                for edge in graph.edges
            ],
            "waves": [list(level) for level in graph.waves()],
            "roots": list(graph.roots),
            "leaves": list(graph.leaves),
            "max_width": graph.max_width,
        }

    async def paused_ids(self) -> list[str]:
        return await self.store.list_ids()

    async def checkpoint_for(self, run_id: str) -> Checkpoint:
        checkpoint = await self.store.load(run_id)
        if checkpoint is None:
            raise ServiceError(404, f"no paused run {run_id!r}")
        return checkpoint

    # -------------------------------------------------------------------- run

    async def start(
        self, workflow_id: str, inputs: Mapping[str, Any] | None = None
    ) -> RunResult:
        engine = self.engine_for(workflow_id)
        result = await engine.run(inputs or {})
        await self.remember(result)
        return result

    async def resume(
        self, run_id: str, answers: Mapping[str, Any] | None = None
    ) -> RunResult:
        checkpoint = await self.checkpoint_for(run_id)
        engine = self.engine_for(checkpoint.workflow_id)
        try:
            result = await engine.resume(checkpoint, answers or {})
        except CheckpointError as exc:
            raise ServiceError(400, str(exc)) from exc
        await self.remember(result)
        return result

    def stream(
        self, workflow_id: str, inputs: Mapping[str, Any] | None = None, maxsize: int = 64
    ) -> AsyncIterator[Event]:
        engine = self.engine_for(workflow_id)
        return engine.stream(inputs or {}, maxsize=maxsize)

    async def resume_stream(
        self, run_id: str, answers: Mapping[str, Any] | None = None, maxsize: int = 64
    ) -> AsyncIterator[Event]:
        checkpoint = await self.checkpoint_for(run_id)
        engine = self.engine_for(checkpoint.workflow_id)
        try:
            return engine.stream_resume(checkpoint, answers or {}, maxsize=maxsize)
        except CheckpointError as exc:
            raise ServiceError(400, str(exc)) from exc

    # ------------------------------------------------------------ bookkeeping

    async def remember(self, result: RunResult) -> None:
        """Persist a pause, and clear the checkpoint once the run is over."""
        if result.paused and result.checkpoint is not None:
            await self.store.save(result.checkpoint)
        elif result.status in (RunStatus.COMPLETED, RunStatus.FAILED):
            await self.store.delete(result.run_id)

    async def remember_event(self, event: Event) -> None:
        """The same bookkeeping, driven off a terminal stream event."""
        checkpoint = event.data.get("checkpoint")
        if checkpoint is not None:
            await self.store.save(Checkpoint.from_dict(checkpoint))
        else:
            await self.store.delete(event.run_id)

    async def sse_frames(self, events: AsyncIterator[Event]) -> AsyncIterator[str]:
        """SSE text for a run, doing the store bookkeeping on the way past.

        Both surfaces consume this, so a pause is persisted identically whether
        the client is talking to FastAPI or to the fallback server.
        """
        async for event in events:
            yield event.to_sse()
            if event.is_terminal:
                await self.remember_event(event)

    def payload_for(self, result: RunResult) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "run": result.run_id,
            "status": result.status.value,
            "stats": result.stats.as_dict(),
            "outputs": result.outputs,
            "failures": result.failures,
        }
        if result.paused:
            payload["awaiting"] = result.awaiting
        return payload

    async def close(self) -> None:
        for closeable in (self.store, self.event_sink):
            closer = getattr(closeable, "close", None)
            if closer is not None:
                await closer()
