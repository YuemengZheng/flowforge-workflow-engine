"""Node contract and the registry the scheduler dispatches through.

The scheduler never imports a concrete node class: it asks the registry for an
instance and awaits ``run``. Adding a node type is a registration, not a change
to the engine.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Awaitable, Callable, ClassVar, Mapping, TypeVar

from .errors import FlowForgeError, NodePaused, UnknownNodeTypeError
from .events import NODE_DELTA
from .graph import NodeSpec
from .variables import VariablePool


class NodeStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"
    PAUSED = "paused"

    @property
    def is_settled(self) -> bool:
        """Settled = the node's fate is decided, so successors may count down.

        COMPLETED and SKIPPED both settle. This is the in-degree semantics that
        keeps a join node from waiting forever on a branch that was never taken.
        PAUSED is deliberately *not* settled — the node's fate is still open,
        so its successors must keep waiting until an answer arrives.
        """
        return self in (NodeStatus.COMPLETED, NodeStatus.SKIPPED)


@dataclass
class NodeContext:
    """Everything a node is allowed to read while it runs.

    ``config`` arrives already resolved against the pool, so a node never has
    to think about templates: by the time ``run`` is called, ``{{a.b}}`` in the
    workflow JSON is the actual upstream value. Treat it as **read-only**: a
    template-free config is shared with the graph's own spec rather than copied,
    so mutating it would edit the workflow definition.
    """

    run_id: str
    spec: NodeSpec
    config: Mapping[str, Any] = field(default_factory=dict)
    pool: VariablePool = field(default_factory=VariablePool)
    run_inputs: Mapping[str, Any] = field(default_factory=dict)
    upstream: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)
    attempt: int = 1
    emitter: "Callable[..., Awaitable[None]] | None" = None

    @property
    def node_id(self) -> str:
        return self.spec.id

    async def emit(self, event_type: str = NODE_DELTA, **data: Any) -> None:
        """Publish a partial result while still running.

        A no-op when nobody is listening, so a node streams the same way whether
        it was started by ``run()`` or by ``stream()``.
        """
        if self.emitter is not None:
            await self.emitter(event_type, **data)

    def merged_upstream(self) -> dict[str, Any]:
        """Flatten predecessor outputs into one dict, later predecessors win.

        Convenient for pass-through nodes; anything that cares which node a
        value came from should use ``{{node.field}}`` in its config instead.
        """
        merged: dict[str, Any] = {}
        for outputs in self.upstream.values():
            merged.update(outputs)
        return merged


class Node:
    """Base class. Subclasses implement ``run`` and return their outputs."""

    type: ClassVar[str] = ""

    #: Config keys the engine must hand over untouched. Everything else is
    #: resolved against the variable pool before ``run`` is called — but a node
    #: that carries a *template for someone else* (a nested workflow, a prompt
    #: to be filled in later) needs those templates to survive intact, or the
    #: parent's pool consumes references meant for a different scope.
    raw_config_keys: ClassVar[frozenset[str]] = frozenset()

    def __init__(self, spec: NodeSpec) -> None:
        self.spec = spec

    @property
    def id(self) -> str:
        return self.spec.id

    async def run(self, ctx: NodeContext) -> Mapping[str, Any]:  # pragma: no cover
        raise NotImplementedError

    def select_branch(
        self, ctx: NodeContext, outputs: Mapping[str, Any]
    ) -> str | None:
        """Which labelled outgoing edge to take, or ``None`` for all of them.

        Only branching nodes override this. The scheduler uses the answer to
        decide which successors are reachable and which get skipped.
        """
        return None

    def __repr__(self) -> str:
        return f"<{type(self).__name__} {self.spec.id!r}>"


NodeT = TypeVar("NodeT", bound=type[Node])


class NodeRegistry:
    """Maps a node ``type`` string to the class that implements it."""

    def __init__(self) -> None:
        self._types: dict[str, type[Node]] = {}

    def register(self, type_name: str) -> Callable[[NodeT], NodeT]:
        def decorator(cls: NodeT) -> NodeT:
            if type_name in self._types:
                raise ValueError(f"node type {type_name!r} already registered")
            cls.type = type_name
            self._types[type_name] = cls
            return cls

        return decorator

    def create(self, spec: NodeSpec) -> Node:
        return self.implementation(spec.type, spec.id)(spec)

    def implementation(self, type_name: str, node_id: str = "") -> type[Node]:
        """The class behind a type name, without instantiating it.

        The engine needs ``raw_config_keys`` before any node exists, to compile
        each config once at construction rather than once per attempt.
        """
        try:
            return self._types[type_name]
        except KeyError:
            raise UnknownNodeTypeError(node_id, type_name, list(self._types)) from None

    def known_types(self) -> list[str]:
        return sorted(self._types)

    def __contains__(self, type_name: object) -> bool:
        return type_name in self._types


registry = NodeRegistry()


@registry.register("start")
class StartNode(Node):
    """Entry point: hands the run inputs to the rest of the graph."""

    async def run(self, ctx: NodeContext) -> Mapping[str, Any]:
        return dict(ctx.run_inputs)


@registry.register("stub")
class StubNode(Node):
    """Placeholder work unit, used by tests and benchmarks.

    config:
        ``delay``  seconds to await (simulated I/O), default 0
        ``output`` dict merged into the result, default ``{}``
        ``fail``   truthy -> raise, to exercise failure handling
    """

    async def run(self, ctx: NodeContext) -> Mapping[str, Any]:
        delay = float(ctx.config.get("delay", 0) or 0)
        if delay > 0:
            await asyncio.sleep(delay)
        if ctx.config.get("fail"):
            raise RuntimeError(str(ctx.config.get("fail")))
        outputs = dict(ctx.merged_upstream())
        outputs.update(ctx.config.get("output", {}))
        outputs["node"] = ctx.node_id
        return outputs


@registry.register("end")
class EndNode(Node):
    """Terminal node: collects whatever reached it, namespaced per predecessor."""

    async def run(self, ctx: NodeContext) -> Mapping[str, Any]:
        return {source: dict(outputs) for source, outputs in ctx.upstream.items()}


@registry.register("await_input")
class AwaitInputNode(Node):
    """Stops the run and asks a question; resumes with whatever answer arrives.

    config::

        {"prompt": "Approve deploying {{build.sha}}?", "fields": ["approved"]}

    The engine checkpoints the run at this point. ``WorkflowEngine.resume``
    supplies this node's outputs directly, so the rest of the graph reads the
    answer with the usual ``{{node.field}}`` references.
    """

    async def run(self, ctx: NodeContext) -> Mapping[str, Any]:
        raise NodePaused(
            prompt=str(ctx.config.get("prompt", "")),
            fields=list(ctx.config.get("fields", [])),
        )


class ConditionError(FlowForgeError):
    """A condition in a decision node is malformed."""


def _as_number(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _compare(op: str, left: Any, right: Any) -> bool:
    """Ordered comparison, numeric when both sides look numeric."""
    left_num, right_num = _as_number(left), _as_number(right)
    if left_num is not None and right_num is not None:
        left, right = left_num, right_num
    try:
        return {
            "gt": left > right,
            "gte": left >= right,
            "lt": left < right,
            "lte": left <= right,
        }[op]
    except TypeError as exc:
        raise ConditionError(
            f"cannot compare {type(left).__name__} with {type(right).__name__} using {op!r}"
        ) from exc


_OPERATORS = {
    "eq": lambda left, right: left == right,
    "ne": lambda left, right: left != right,
    "gt": lambda left, right: _compare("gt", left, right),
    "gte": lambda left, right: _compare("gte", left, right),
    "lt": lambda left, right: _compare("lt", left, right),
    "lte": lambda left, right: _compare("lte", left, right),
    "contains": lambda left, right: right in (left or ()),
    "in": lambda left, right: left in (right or ()),
    "truthy": lambda left, right: bool(left),
    "empty": lambda left, right: not left,
}


def evaluate_condition(condition: Mapping[str, Any]) -> bool:
    """Evaluate one condition, or an ``all``/``any`` group of them.

    Values are already resolved — the pool substitutes ``{{a.b}}`` in the node
    config before ``run`` is called, so this only deals with plain data.
    """
    if not isinstance(condition, Mapping):
        raise ConditionError(f"condition must be an object, got {condition!r}")
    for group, combine in (("all", all), ("any", any)):
        if group in condition:
            members = condition[group]
            if not isinstance(members, list):
                raise ConditionError(f"{group!r} must be a list of conditions")
            return combine(evaluate_condition(member) for member in members)
    op = condition.get("op", "truthy")
    if op not in _OPERATORS:
        raise ConditionError(
            f"unknown operator {op!r}; known: {', '.join(sorted(_OPERATORS))}"
        )
    return bool(_OPERATORS[op](condition.get("left"), condition.get("right")))


@registry.register("decision")
class DecisionNode(Node):
    """Picks one labelled outgoing edge; the others are skipped.

    config::

        {
          "cases": [
            {"branch": "premium", "when": {"left": "{{user.tier}}", "op": "eq",
                                           "right": "gold"}}
          ],
          "default": "standard"
        }

    The first matching case wins. ``default`` is required so that every run
    settles a branch — a decision node that matches nothing would otherwise
    strand its whole downstream.
    """

    async def run(self, ctx: NodeContext) -> Mapping[str, Any]:
        cases = ctx.config.get("cases", [])
        if not isinstance(cases, list):
            raise ConditionError(f"node {ctx.node_id!r}: 'cases' must be a list")
        for index, case in enumerate(cases):
            if not isinstance(case, Mapping) or "branch" not in case:
                raise ConditionError(
                    f"node {ctx.node_id!r}: case {index} needs a 'branch'"
                )
            if evaluate_condition(case.get("when", {})):
                return {"branch": case["branch"], "matched_case": index}
        default = ctx.config.get("default")
        if default is None:
            raise ConditionError(
                f"node {ctx.node_id!r}: no case matched and no 'default' branch is set"
            )
        return {"branch": default, "matched_case": None}

    def select_branch(
        self, ctx: NodeContext, outputs: Mapping[str, Any]
    ) -> str | None:
        return outputs.get("branch")
