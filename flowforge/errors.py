"""Exception hierarchy for FlowForge."""

from __future__ import annotations


class FlowForgeError(Exception):
    """Base class for every error raised by the engine."""


class GraphError(FlowForgeError):
    """The workflow definition is structurally invalid."""


class CycleError(GraphError):
    """The graph contains at least one cycle, so no topological order exists."""

    def __init__(self, nodes: list[str]) -> None:
        self.nodes = sorted(nodes)
        super().__init__(
            "cycle detected, these nodes never reach in-degree 0: "
            + ", ".join(self.nodes)
        )


class UnknownNodeTypeError(GraphError):
    """A node declares a type that is not present in the registry."""

    def __init__(self, node_id: str, node_type: str, known: list[str]) -> None:
        self.node_id = node_id
        self.node_type = node_type
        super().__init__(
            f"node {node_id!r} has unknown type {node_type!r}; "
            f"registered types: {', '.join(sorted(known)) or '<none>'}"
        )


class NodePaused(FlowForgeError):
    """Raised by a node that needs an answer before the run can continue.

    Not a failure: the engine catches it, checkpoints the run, and waits. It is
    never retried — a node asking a question has not gone wrong.
    """

    def __init__(self, prompt: str = "", **details: object) -> None:
        self.prompt = prompt
        self.details = details
        super().__init__(prompt or "node paused awaiting input")

    def as_dict(self) -> dict[str, object]:
        return {"prompt": self.prompt, **self.details}


class NodeExecutionError(FlowForgeError):
    """A node raised while executing. Carries the node id for reporting."""

    def __init__(self, node_id: str, cause: BaseException) -> None:
        self.node_id = node_id
        self.cause = cause
        super().__init__(f"node {node_id!r} failed: {cause!r}")
