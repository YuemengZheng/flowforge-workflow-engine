"""Kahn-scheduled workflow engine.

The scheduler keeps one counter per node — how many predecessors have not
settled yet — and a ready queue of nodes whose counter has reached zero. Every
node in the ready queue is dispatched in a single ``asyncio.gather``, so the
available parallelism is whatever the graph structure exposes, with no
per-workflow coordination code.

In-degree counts *settled* predecessors, not *completed* ones (see
``NodeStatus.is_settled``), which is the hook T2 needs for branch skipping.
"""

from __future__ import annotations

import asyncio
from collections import deque
from contextlib import suppress
from dataclasses import dataclass, field
from enum import Enum
from time import perf_counter
from typing import Any, AsyncIterator, Mapping
from uuid import uuid4

from .checkpoint import Checkpoint, CheckpointError, graph_fingerprint
from .errors import GraphError, NodePaused, UnknownNodeTypeError
from .events import (
    NODE_COMPLETED,
    NODE_FAILED,
    NODE_PAUSED,
    NODE_RETRY,
    NODE_SKIPPED,
    NODE_STARTED,
    RUN_COMPLETED,
    RUN_FAILED,
    RUN_PAUSED,
    RUN_RESUMED,
    RUN_STARTED,
    Event,
    EventStream,
)
from .graph import Graph
from .nodes import (
    Node,
    NodeContext,
    NodeRegistry,
    NodeStatus,
    registry as default_registry,
)
from .retry import ErrorStrategy
from .store import RunStore
from .variables import Template, VariablePool, compile_template


class RunStatus(str, Enum):
    COMPLETED = "completed"
    FAILED = "failed"
    PAUSED = "paused"


@dataclass
class _RunState:
    """Everything the scheduler needs to keep going — and to be rebuilt later.

    ``run()`` builds one from scratch and ``resume()`` builds one from a
    checkpoint; the driving loop cannot tell the difference.
    """

    run_id: str
    inputs: dict[str, Any]
    pool: VariablePool
    pending: dict[str, int]
    taken: dict[str, int]
    runs: dict[str, NodeRun]
    completed_outputs: dict[str, dict[str, Any]]
    ready: deque[str]
    waves: int = 0
    failures: list[str] = field(default_factory=list)
    awaiting: dict[str, dict[str, Any]] = field(default_factory=dict)


@dataclass
class NodeRun:
    """Per-node execution record."""

    node_id: str
    type: str
    status: NodeStatus = NodeStatus.PENDING
    wave: int | None = None
    outputs: dict[str, Any] = field(default_factory=dict)
    branch: str | None = None
    error: str | None = None
    duration_ms: float = 0.0
    attempts: int = 0
    recovered: bool = False


@dataclass
class RunStats:
    """Measured, not estimated — these are the numbers benchmarks report."""

    nodes_total: int = 0
    nodes_executed: int = 0
    nodes_skipped: int = 0
    nodes_recovered: int = 0
    waves: int = 0
    peak_concurrency: int = 0
    total_ms: float = 0.0
    node_ms: float = 0.0
    scheduler_ms: float = 0.0

    def as_dict(self) -> dict[str, Any]:
        return {
            "nodes_total": self.nodes_total,
            "nodes_executed": self.nodes_executed,
            "nodes_skipped": self.nodes_skipped,
            "nodes_recovered": self.nodes_recovered,
            "waves": self.waves,
            "peak_concurrency": self.peak_concurrency,
            "total_ms": round(self.total_ms, 3),
            "node_ms": round(self.node_ms, 3),
            "scheduler_ms": round(self.scheduler_ms, 3),
        }


@dataclass
class RunResult:
    run_id: str
    status: RunStatus
    nodes: dict[str, NodeRun]
    outputs: dict[str, dict[str, Any]]
    stats: RunStats
    failures: list[str] = field(default_factory=list)
    variables: dict[str, Any] = field(default_factory=dict)
    checkpoint: "Checkpoint | None" = None

    @property
    def ok(self) -> bool:
        return self.status is RunStatus.COMPLETED

    @property
    def paused(self) -> bool:
        return self.status is RunStatus.PAUSED

    @property
    def awaiting(self) -> dict[str, dict[str, Any]]:
        """Node id -> the question it is waiting on. Empty unless paused."""
        return dict(self.checkpoint.awaiting) if self.checkpoint else {}

    @property
    def skipped(self) -> list[str]:
        return sorted(
            node_id
            for node_id, record in self.nodes.items()
            if record.status is NodeStatus.SKIPPED
        )

    def outputs_of(self, node_id: str) -> dict[str, Any]:
        return self.nodes[node_id].outputs

    def status_of(self, node_id: str) -> NodeStatus:
        return self.nodes[node_id].status

    def branch_of(self, node_id: str) -> str | None:
        return self.nodes[node_id].branch


class WorkflowEngine:
    """Executes a :class:`Graph` with a ready-queue scheduler.

    Args:
        graph: validated DAG.
        node_registry: type -> implementation map; defaults to the global one.
        max_concurrency: cap on simultaneously running nodes; ``None`` = the
            graph's own width is the only limit.
    """

    def __init__(
        self,
        graph: Graph,
        node_registry: NodeRegistry | None = None,
        max_concurrency: int | None = None,
        store: RunStore | None = None,
        checkpoint_every_wave: bool = False,
        event_sink: Any = None,
    ) -> None:
        if max_concurrency is not None and max_concurrency < 1:
            raise ValueError("max_concurrency must be >= 1")
        if checkpoint_every_wave and store is None:
            raise ValueError("checkpoint_every_wave needs a store to write to")
        self.graph = graph
        self.registry = node_registry or default_registry
        self.max_concurrency = max_concurrency
        self.store = store
        # Durability knob. Off: a checkpoint is written only when the run pauses
        # for input, so a crash loses everything since the last pause. On: one
        # is written after every wave settles, so a crash costs at most the
        # wave that was in flight — at the price of a store round trip per wave.
        self.checkpoint_every_wave = checkpoint_every_wave
        # A second destination for every event — a Kafka topic, an audit log.
        # Applied even on the non-streaming path, where there is no consumer:
        # telemetry that only exists when somebody happens to be watching the
        # stream is not telemetry.
        self.event_sink = event_sink

        # Fail before any node runs rather than halfway through a workflow, and
        # compile each config while we are already walking them: the parse
        # depends on the config alone, so paying for it per attempt is waste.
        self._config_plans: dict[str, Template] = {}
        self._raw_config: dict[str, dict[str, Any]] = {}
        for spec in graph.nodes.values():
            if spec.type not in self.registry:
                raise UnknownNodeTypeError(
                    spec.id, spec.type, self.registry.known_types()
                )
            self._compile_config(spec)

    def _sink_stream(self, run_id: str) -> EventStream | None:
        """A stream that feeds only the sink, for runs with no consumer."""
        if self.event_sink is None:
            return None
        return EventStream(run_id=run_id, sink=self.event_sink, buffer=False)

    async def run(
        self,
        inputs: Mapping[str, Any] | None = None,
        run_id: str | None = None,
        events: EventStream | None = None,
    ) -> RunResult:
        run_id = run_id or uuid4().hex[:12]
        run_inputs = dict(inputs or {})
        events = events if events is not None else self._sink_stream(run_id)
        if events is not None and not events.run_id:
            events.run_id = run_id

        state = _RunState(
            run_id=run_id,
            inputs=run_inputs,
            pool=VariablePool(run_inputs),
            pending=self.graph.indegree,
            # Incoming edges that were actually taken. A node whose in-degree
            # hits zero with none taken is unreachable for this run.
            taken={node_id: 0 for node_id in self.graph.nodes},
            runs={
                node_id: NodeRun(node_id=node_id, type=spec.type)
                for node_id, spec in self.graph.nodes.items()
            },
            completed_outputs={},
            ready=deque(sorted(self.graph.roots)),
        )
        return await self._drive(state, events, RUN_STARTED)

    async def resume(
        self,
        checkpoint: Checkpoint,
        answers: Mapping[str, Mapping[str, Any]] | None = None,
        events: EventStream | None = None,
    ) -> RunResult:
        """Continue a paused run, supplying the answers it was waiting on.

        Completed nodes are not re-executed: the checkpoint restores the
        counters, so the scheduler picks up exactly where it stopped. A run can
        pause and resume any number of times.
        """
        checkpoint.verify_against(self.graph)
        answers = dict(answers or {})
        missing = [n for n in checkpoint.awaiting_nodes if n not in answers]
        if missing:
            raise CheckpointError(
                f"run {checkpoint.run_id!r} is waiting on "
                f"{', '.join(checkpoint.awaiting_nodes)}; no answer given for "
                f"{', '.join(missing)}"
            )
        events = events if events is not None else self._sink_stream(checkpoint.run_id)
        if events is not None and not events.run_id:
            events.run_id = checkpoint.run_id

        state = self._state_from_checkpoint(checkpoint)

        # The answers settle the paused nodes, which unblocks their successors.
        settling: deque[tuple[str, str | None, bool, bool]] = deque()
        for node_id in checkpoint.awaiting_nodes:
            record = state.runs[node_id]
            record.outputs = dict(answers[node_id] or {})
            record.status = NodeStatus.COMPLETED
            state.completed_outputs[node_id] = record.outputs
            state.pool.set_outputs(node_id, record.outputs)
            settling.append((node_id, None, False, False))
        state.awaiting.clear()
        unlocked = await self._drain_settles(state, settling, events)
        state.ready.extend(sorted(unlocked))

        return await self._drive(state, events, RUN_RESUMED)

    async def _drive(
        self,
        state: _RunState,
        events: EventStream | None,
        opening_event: str,
    ) -> RunResult:
        run_id = state.run_id
        run_inputs = state.inputs
        pool = state.pool
        pending = state.pending
        taken = state.taken
        runs = state.runs
        completed_outputs = state.completed_outputs
        ready = state.ready
        failures = state.failures

        semaphore = (
            asyncio.Semaphore(self.max_concurrency) if self.max_concurrency else None
        )
        live = 0
        peak = 0
        busy_s = 0.0
        started = perf_counter()

        async def execute(node_id: str, wave_no: int) -> NodeRun:
            nonlocal live, peak
            record = runs[node_id]
            record.wave = wave_no
            spec = self.graph.nodes[node_id]
            upstream: dict[str, dict[str, Any]] = {}
            for edge in self.graph.predecessors[node_id]:
                if edge.source in completed_outputs:
                    upstream[edge.source] = completed_outputs[edge.source]
            node = self.registry.create(spec)
            if semaphore is not None:
                await semaphore.acquire()
            node_started = perf_counter()
            live += 1
            peak = max(peak, live)
            record.status = NodeStatus.RUNNING

            async def emit(event_type: str, **data: Any) -> None:
                if events is not None:
                    await events.emit(event_type, node=node_id, **data)

            def build_context(attempt: int) -> NodeContext:
                # Templates are resolved here, once per attempt, against the
                # pool as it stands after the previous wave. Nodes only ever
                # see values.
                return NodeContext(
                    run_id=run_id,
                    spec=spec,
                    config=self._resolve_config(node_id, pool),
                    pool=pool,
                    run_inputs=run_inputs,
                    upstream=upstream,
                    attempt=attempt,
                    emitter=emit if events is not None else None,
                )

            async def on_retry(attempt: int, exc: BaseException) -> None:
                await emit(
                    NODE_RETRY, attempt=attempt, error=f"{type(exc).__name__}: {exc}"
                )

            await emit(NODE_STARTED, node_type=spec.type, wave=wave_no)
            try:
                ctx, outputs = await self._attempt(node, spec, build_context, record, on_retry)
                record.outputs = dict(outputs or {})
                record.branch = self._validated_branch(node, ctx, record.outputs)
                record.status = NodeStatus.COMPLETED
                record.error = None
            except NodePaused as pause:  # a question, not a failure
                record.status = NodeStatus.PAUSED
                record.error = None
                state.awaiting[node_id] = pause.as_dict()
            except Exception as exc:  # node failures are data, not crashes
                record.status = NodeStatus.FAILED
                record.error = f"{type(exc).__name__}: {exc}"
                self._apply_error_strategy(record, spec)
            finally:
                live -= 1
                record.duration_ms = (perf_counter() - node_started) * 1000
                if semaphore is not None:
                    semaphore.release()

            if record.status is NodeStatus.COMPLETED:
                await emit(
                    NODE_COMPLETED,
                    ms=round(record.duration_ms, 3),
                    attempts=record.attempts,
                    outputs=record.outputs,
                    branch=record.branch,
                    recovered=record.recovered,
                )
            elif record.status is NodeStatus.PAUSED:
                await emit(NODE_PAUSED, **state.awaiting[node_id])
            else:
                await emit(
                    NODE_FAILED,
                    ms=round(record.duration_ms, 3),
                    attempts=record.attempts,
                    error=record.error,
                    recovered=record.recovered,
                    branch=record.branch,
                )
            return record

        if events is not None:
            await events.emit(
                opening_event,
                workflow=self.graph.id,
                nodes=len(self.graph),
                from_wave=state.waves,
            )

        while ready and not failures:
            state.waves += 1
            wave = state.waves
            batch = list(ready)
            ready.clear()

            wave_started = perf_counter()
            results = await asyncio.gather(*(execute(node_id, wave) for node_id in batch))
            busy_s += perf_counter() - wave_started

            settling: deque[tuple[str, str | None, bool, bool]] = deque()
            for record in results:
                if record.status is NodeStatus.FAILED and not record.recovered:
                    failures.append(record.node_id)
                    continue
                if record.status is NodeStatus.PAUSED:
                    # Not settled: successors keep waiting until an answer lands.
                    continue
                completed_outputs[record.node_id] = record.outputs
                pool.set_outputs(record.node_id, record.outputs)
                settling.append(
                    (
                        record.node_id,
                        record.branch,
                        False,
                        record.status is NodeStatus.FAILED,
                    )
                )
            if failures:
                break

            ready.extend(sorted(await self._drain_settles(state, settling, events)))

            if self.checkpoint_every_wave and self.store is not None:
                # Written after the wave has settled, so what lands is a
                # consistent frontier: every node in it either finished or
                # never started. Nothing in flight is recorded as done.
                await self.store.save(self._checkpoint(state))

        total_s = perf_counter() - started
        executed = [
            record
            for record in runs.values()
            if record.status in (NodeStatus.COMPLETED, NodeStatus.FAILED)
        ]
        stats = RunStats(
            nodes_total=len(runs),
            nodes_executed=len(executed),
            nodes_skipped=sum(
                1 for record in runs.values() if record.status is NodeStatus.SKIPPED
            ),
            nodes_recovered=sum(1 for record in runs.values() if record.recovered),
            waves=state.waves,
            peak_concurrency=peak,
            total_ms=total_s * 1000,
            node_ms=sum(record.duration_ms for record in executed),
            scheduler_ms=(total_s - busy_s) * 1000,
        )
        if failures:
            status = RunStatus.FAILED
        elif state.awaiting:
            status = RunStatus.PAUSED
        else:
            status = RunStatus.COMPLETED

        result = RunResult(
            run_id=run_id,
            status=status,
            nodes=runs,
            outputs={
                node_id: runs[node_id].outputs
                for node_id in self.graph.leaves
                if runs[node_id].status is NodeStatus.COMPLETED
            },
            stats=stats,
            failures=failures,
            variables=pool.snapshot(),
            checkpoint=self._checkpoint(state) if status is RunStatus.PAUSED else None,
        )
        if self.store is not None:
            if status is RunStatus.PAUSED:
                await self.store.save(result.checkpoint)
            elif self.checkpoint_every_wave:
                # The run is over; the per-wave frontier is now dead weight.
                await self.store.delete(run_id)
        if events is not None:
            terminal = {
                RunStatus.COMPLETED: RUN_COMPLETED,
                RunStatus.FAILED: RUN_FAILED,
                RunStatus.PAUSED: RUN_PAUSED,
            }[status]
            payload: dict[str, Any] = {
                "status": status.value,
                "stats": stats.as_dict(),
                "outputs": result.outputs,
                "failures": failures,
            }
            if status is RunStatus.PAUSED and result.checkpoint is not None:
                # The checkpoint rides on the event so a streaming client has
                # everything it needs to resume without a second request.
                payload["awaiting"] = state.awaiting
                payload["checkpoint"] = result.checkpoint.as_dict()
            await events.emit(terminal, **payload)
        return result

    async def _drain_settles(
        self,
        state: _RunState,
        settling: deque[tuple[str, str | None, bool, bool]],
        events: EventStream | None,
    ) -> list[str]:
        """Count down successors until nothing new settles; return ready nodes."""
        unlocked: list[str] = []
        while settling:
            node_id, branch, was_skipped, error_routed = settling.popleft()
            for target in self._settle(
                node_id, branch, was_skipped, error_routed, state.pending, state.taken
            ):
                if state.taken[target]:
                    unlocked.append(target)
                else:
                    # Every incoming edge settled and none was taken: this node
                    # is on a branch that was not chosen. Skipping it still
                    # counts down its successors, so a join further downstream
                    # is not left waiting forever.
                    skipped = state.runs[target]
                    skipped.status = NodeStatus.SKIPPED
                    skipped.wave = state.waves
                    settling.append((target, None, True, False))
                    if events is not None:
                        await events.emit(
                            NODE_SKIPPED, node=target, wave=state.waves
                        )
        return unlocked

    def _checkpoint(self, state: _RunState) -> Checkpoint:
        return Checkpoint(
            run_id=state.run_id,
            workflow_id=self.graph.id,
            fingerprint=graph_fingerprint(self.graph),
            inputs=dict(state.inputs),
            pending=dict(state.pending),
            taken=dict(state.taken),
            nodes={
                node_id: {
                    "status": record.status.value,
                    "wave": record.wave,
                    "outputs": record.outputs,
                    "branch": record.branch,
                    "error": record.error,
                    "duration_ms": record.duration_ms,
                    "attempts": record.attempts,
                    "recovered": record.recovered,
                }
                for node_id, record in state.runs.items()
            },
            ready=list(state.ready),
            variables=state.pool.snapshot(),
            awaiting=dict(state.awaiting),
            waves=state.waves,
        )

    def _state_from_checkpoint(self, checkpoint: Checkpoint) -> _RunState:
        pool = VariablePool(checkpoint.inputs)
        for namespace, values in checkpoint.variables.items():
            if namespace != VariablePool.RUN_NAMESPACE:
                pool.set_outputs(namespace, values)

        runs: dict[str, NodeRun] = {}
        completed: dict[str, dict[str, Any]] = {}
        for node_id, spec in self.graph.nodes.items():
            saved = checkpoint.nodes.get(node_id, {})
            record = NodeRun(
                node_id=node_id,
                type=spec.type,
                status=NodeStatus(saved.get("status", NodeStatus.PENDING.value)),
                wave=saved.get("wave"),
                outputs=dict(saved.get("outputs") or {}),
                branch=saved.get("branch"),
                error=saved.get("error"),
                duration_ms=float(saved.get("duration_ms") or 0.0),
                attempts=int(saved.get("attempts") or 0),
                recovered=bool(saved.get("recovered") or False),
            )
            runs[node_id] = record
            if record.status is NodeStatus.COMPLETED:
                completed[node_id] = record.outputs

        return _RunState(
            run_id=checkpoint.run_id,
            inputs=dict(checkpoint.inputs),
            pool=pool,
            pending=dict(checkpoint.pending),
            taken=dict(checkpoint.taken),
            runs=runs,
            completed_outputs=completed,
            ready=deque(checkpoint.ready),
            waves=checkpoint.waves,
            awaiting=dict(checkpoint.awaiting),
        )

    def stream(
        self,
        inputs: Mapping[str, Any] | None = None,
        run_id: str | None = None,
        maxsize: int = 0,
    ) -> AsyncIterator[Event]:
        """Run the workflow and yield events as they happen.

        The run executes in its own task while this generator drains the queue,
        so a slow consumer applies back-pressure to the producer rather than
        buffering without bound (when ``maxsize`` is set). The final
        ``run.completed`` / ``run.failed`` event carries the stats and outputs.
        """
        # Returns the generator rather than delegating to it: an outer
        # `async for ... yield` would leave the inner generator to be closed by
        # GC, so abandoning the stream would not promptly cancel the run.
        return self._stream(
            lambda channel: self.run(inputs, run_id, events=channel),
            run_id or "",
            maxsize,
        )

    def stream_resume(
        self,
        checkpoint: Checkpoint,
        answers: Mapping[str, Mapping[str, Any]] | None = None,
        maxsize: int = 0,
    ) -> AsyncIterator[Event]:
        """Same as :meth:`stream`, continuing a paused run."""
        return self._stream(
            lambda channel: self.resume(checkpoint, answers, events=channel),
            checkpoint.run_id,
            maxsize,
        )

    async def _stream(
        self, runner: Any, run_id: str, maxsize: int
    ) -> AsyncIterator[Event]:
        # The sink goes on the consumer's stream too, so a streamed run publishes
        # exactly what a non-streamed one does.
        stream = EventStream(run_id=run_id, maxsize=maxsize, sink=self.event_sink)

        async def drive() -> None:
            try:
                await runner(stream)
            finally:
                await stream.close()

        task = asyncio.create_task(drive())
        try:
            async for event in stream:
                yield event
        finally:
            if not task.done():
                task.cancel()
            with suppress(asyncio.CancelledError):
                await task

    def run_sync(
        self,
        inputs: Mapping[str, Any] | None = None,
        run_id: str | None = None,
    ) -> RunResult:
        """Convenience wrapper for scripts and the CLI."""
        return asyncio.run(self.run(inputs, run_id))

    async def _attempt(
        self,
        node: Node,
        spec: Any,
        build_context: Any,
        record: NodeRun,
        on_retry: Any,
    ) -> tuple[NodeContext, Mapping[str, Any]]:
        """Run one node under its retry policy; re-raise the last failure.

        The policy is engine-level on purpose: every node type gets timeouts and
        retries without implementing them, and a node that wraps a provider with
        its own retry loop can turn that loop off and let this one count.
        """
        policy = spec.retry
        for attempt in range(1, policy.attempts + 1):
            record.attempts = attempt
            ctx = build_context(attempt)
            try:
                if policy.timeout_s is None:
                    outputs = await node.run(ctx)
                else:
                    outputs = await asyncio.wait_for(node.run(ctx), policy.timeout_s)
                return ctx, outputs
            except NodePaused:
                raise  # asking a question is not an error; never retry it
            except Exception as exc:
                if attempt >= policy.attempts:
                    raise
                await on_retry(attempt, exc)
                delay = policy.delay_before(attempt + 1)
                if delay:
                    await asyncio.sleep(delay)
        raise AssertionError("unreachable")  # pragma: no cover

    def _settle(
        self,
        node_id: str,
        branch: str | None,
        was_skipped: bool,
        error_routed: bool,
        pending: dict[str, int],
        taken: dict[str, int],
    ) -> list[str]:
        """Count down each successor edge; return targets that just hit zero.

        Called for every settled node, completed *and* skipped — that is the
        whole trick. In-degree counts predecessors whose fate is decided, not
        predecessors that produced output, so an untaken branch still unblocks
        the join it feeds into.
        """
        policy = self.graph.nodes[node_id].retry
        error_label = (
            policy.error_branch if policy.on_error is ErrorStrategy.BRANCH else None
        )
        unlocked = []
        for edge in self.graph.successors[node_id]:
            if was_skipped:
                edge_taken = False
            elif error_routed:
                # The failure edges and nothing else — the normal continuation
                # is exactly what the run is stepping around.
                edge_taken = edge.branch == branch
            elif branch is not None:
                # A decision: the chosen label, plus unlabelled edges, which are
                # the "whichever way this goes" ones (audit, logging).
                edge_taken = edge.branch is None or edge.branch == branch
            else:
                # No branch chosen. Everything downstream runs except the error
                # edge — a node that succeeded must not also take its failure
                # path just because the label exists.
                edge_taken = edge.branch is None or edge.branch != error_label
            if edge_taken:
                taken[edge.target] += 1
            pending[edge.target] -= 1
            if pending[edge.target] == 0:
                unlocked.append(edge.target)
        return unlocked

    def _compile_config(self, spec: Any) -> None:
        """Split a node's config into a compiled part and an untouched part.

        Keys the node declares raw are held aside: a nested workflow's
        ``{{inputs.item}}`` belongs to the sub-run's pool, not this one, and
        resolving it here would consume the reference against the wrong scope
        and fail with a confusing "no field 'item'".
        """
        raw_keys = self.registry.implementation(spec.type, spec.id).raw_config_keys
        if not raw_keys:
            self._config_plans[spec.id] = compile_template(spec.config)
            return
        self._config_plans[spec.id] = compile_template(
            {key: value for key, value in spec.config.items() if key not in raw_keys}
        )
        self._raw_config[spec.id] = {
            key: value for key, value in spec.config.items() if key in raw_keys
        }

    def _resolve_config(self, node_id: str, pool: VariablePool) -> Mapping[str, Any]:
        """Render the plan compiled in ``__init__`` against this run's pool.

        Read-only: a config with no templates in it renders as the spec's own
        mapping rather than a copy, which is most of why this is faster.
        """
        resolved = self._config_plans[node_id].render(pool)
        raw = self._raw_config.get(node_id)
        return {**resolved, **raw} if raw else resolved

    def _apply_error_strategy(self, record: NodeRun, spec: Any) -> None:
        """Decide whether a failed node stops the run or is absorbed.

        Sets ``record.recovered`` when the run may continue. A recovered node
        still reports ``status=failed`` and keeps its error — the run should not
        pretend nothing went wrong — but it settles like any other node, so the
        counters keep moving and downstream work happens.
        """
        policy = spec.retry
        if policy.on_error is ErrorStrategy.DEFAULT:
            record.status = NodeStatus.COMPLETED
            record.outputs = {**dict(policy.error_output), "error": record.error}
            record.recovered = True
            return
        if policy.on_error is ErrorStrategy.BRANCH:
            labels = self.graph.branch_labels(record.node_id)
            if policy.error_branch not in labels:
                record.error = (
                    f"{record.error} (on_error=branch, but node has no outgoing edge "
                    f"labelled {policy.error_branch!r}; labels: {sorted(labels)})"
                )
                return
            record.branch = policy.error_branch
            record.outputs = {"error": record.error}
            record.recovered = True

    def _validated_branch(
        self, node: Node, ctx: NodeContext, outputs: dict[str, Any]
    ) -> str | None:
        """Ask the node which branch it took, and check the graph offers it."""
        branch = node.select_branch(ctx, outputs)
        if branch is None:
            return None
        labels = self.graph.branch_labels(ctx.node_id)
        if branch not in labels:
            raise GraphError(
                f"node {ctx.node_id!r} selected branch {branch!r} but its outgoing "
                f"edges are labelled {sorted(labels) or '<none>'}"
            )
        return branch
