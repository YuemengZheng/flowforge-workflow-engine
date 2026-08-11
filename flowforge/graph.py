"""JSON workflow definition -> DAG, in-degree table, Kahn validation.

The graph layer is deliberately execution-free: it knows about node ids, edges
and dependency counts, but nothing about how a node runs. The engine consumes
the in-degree table produced here and drives it with a ready queue.
"""

from __future__ import annotations

import json
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Mapping

from .errors import CycleError, GraphError
from .retry import DEFAULT_RETRY, RetryPolicy


@dataclass(frozen=True)
class NodeSpec:
    """Static definition of one node, straight out of the JSON.

    ``retry`` comes from the node's own ``timeout`` / ``retries`` keys, which
    sit beside ``type`` rather than inside ``config`` — they are the engine's
    business, not the node implementation's.
    """

    id: str
    type: str
    config: Mapping[str, Any] = field(default_factory=dict)
    retry: RetryPolicy = DEFAULT_RETRY


@dataclass(frozen=True)
class Edge:
    """A directed dependency. ``branch`` is unused in T1 and carried for T2."""

    source: str
    target: str
    branch: str | None = None


class Graph:
    """An immutable, validated DAG.

    Validation happens in the constructor, so an existing ``Graph`` instance is
    always acyclic and internally consistent.
    """

    def __init__(
        self,
        nodes: Iterable[NodeSpec],
        edges: Iterable[Edge],
        graph_id: str = "workflow",
    ) -> None:
        self.id = graph_id

        self.nodes: dict[str, NodeSpec] = {}
        for spec in nodes:
            if spec.id in self.nodes:
                raise GraphError(f"duplicate node id {spec.id!r}")
            self.nodes[spec.id] = spec
        if not self.nodes:
            raise GraphError("workflow has no nodes")

        self.edges: list[Edge] = []
        self.successors: dict[str, list[Edge]] = {nid: [] for nid in self.nodes}
        self.predecessors: dict[str, list[Edge]] = {nid: [] for nid in self.nodes}

        seen: set[tuple[str, str, str | None]] = set()
        for edge in edges:
            for endpoint, role in ((edge.source, "source"), (edge.target, "target")):
                if endpoint not in self.nodes:
                    raise GraphError(
                        f"edge {edge.source!r} -> {edge.target!r} references "
                        f"unknown {role} node {endpoint!r}"
                    )
            if edge.source == edge.target:
                raise GraphError(f"self loop on node {edge.source!r}")
            key = (edge.source, edge.target, edge.branch)
            if key in seen:
                raise GraphError(
                    f"duplicate edge {edge.source!r} -> {edge.target!r}"
                    + (f" on branch {edge.branch!r}" if edge.branch else "")
                )
            seen.add(key)
            self.edges.append(edge)
            self.successors[edge.source].append(edge)
            self.predecessors[edge.target].append(edge)

        # Raises CycleError if the definition is not a DAG.
        self.topological_order()

    # ------------------------------------------------------------------ build

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "Graph":
        if not isinstance(data, Mapping):
            raise GraphError("workflow definition must be a JSON object")
        raw_nodes = data.get("nodes")
        if not isinstance(raw_nodes, list):
            raise GraphError("workflow definition needs a 'nodes' array")
        raw_edges = data.get("edges", [])
        if not isinstance(raw_edges, list):
            raise GraphError("'edges' must be an array")

        nodes = []
        for raw in raw_nodes:
            if not isinstance(raw, Mapping):
                raise GraphError(f"node entry must be an object, got {raw!r}")
            try:
                node_id = raw["id"]
                node_type = raw["type"]
            except KeyError as exc:
                raise GraphError(f"node entry {raw!r} is missing {exc.args[0]!r}") from exc
            config = raw.get("config", {})
            if not isinstance(config, Mapping):
                raise GraphError(f"node {node_id!r}: 'config' must be an object")
            try:
                retry = RetryPolicy.from_dict(raw)
            except (TypeError, ValueError) as exc:
                raise GraphError(f"node {node_id!r}: {exc}") from exc
            nodes.append(
                NodeSpec(
                    id=str(node_id),
                    type=str(node_type),
                    config=dict(config),
                    retry=retry,
                )
            )

        edges = []
        for raw in raw_edges:
            if not isinstance(raw, Mapping):
                raise GraphError(f"edge entry must be an object, got {raw!r}")
            try:
                source, target = raw["source"], raw["target"]
            except KeyError as exc:
                raise GraphError(f"edge entry {raw!r} is missing {exc.args[0]!r}") from exc
            branch = raw.get("branch")
            edges.append(
                Edge(
                    source=str(source),
                    target=str(target),
                    branch=None if branch is None else str(branch),
                )
            )

        return cls(nodes, edges, graph_id=str(data.get("id", "workflow")))

    @classmethod
    def from_json(cls, text: str) -> "Graph":
        try:
            data = json.loads(text)
        except json.JSONDecodeError as exc:
            raise GraphError(f"invalid JSON: {exc}") from exc
        return cls.from_dict(data)

    @classmethod
    def from_file(cls, path: str | Path) -> "Graph":
        return cls.from_json(Path(path).read_text(encoding="utf-8"))

    # ----------------------------------------------------------------- shape

    @property
    def indegree(self) -> dict[str, int]:
        """Dependency count per node — the engine's starting counter table."""
        return {nid: len(preds) for nid, preds in self.predecessors.items()}

    @property
    def roots(self) -> list[str]:
        return [nid for nid, preds in self.predecessors.items() if not preds]

    @property
    def leaves(self) -> list[str]:
        return [nid for nid, succs in self.successors.items() if not succs]

    def successor_ids(self, node_id: str) -> list[str]:
        return [edge.target for edge in self.successors[node_id]]

    def branch_labels(self, node_id: str) -> set[str]:
        """Labels on this node's outgoing edges. Empty = the node never branches."""
        return {
            edge.branch for edge in self.successors[node_id] if edge.branch is not None
        }

    def topological_order(self) -> list[str]:
        """Kahn's algorithm. Raises :class:`CycleError` if nodes remain stuck.

        This is the same counter mechanic the engine runs at execution time;
        keeping it here means a cycle is caught before any node executes.
        """
        pending = self.indegree
        ready = deque(sorted(nid for nid, deg in pending.items() if deg == 0))
        order: list[str] = []
        while ready:
            node_id = ready.popleft()
            order.append(node_id)
            for target in sorted(self.successor_ids(node_id)):
                pending[target] -= 1
                if pending[target] == 0:
                    ready.append(target)
        if len(order) < len(self.nodes):
            raise CycleError([nid for nid, deg in pending.items() if deg > 0])
        return order

    def waves(self) -> list[list[str]]:
        """Static level decomposition: ``waves()[i]`` can all run in parallel.

        Only an upper bound on what the engine actually batches (a node runs as
        soon as its own predecessors settle, not when its whole level does), but
        it is the useful number for sizing a benchmark's expected parallelism.
        """
        pending = self.indegree
        frontier = sorted(nid for nid, deg in pending.items() if deg == 0)
        levels: list[list[str]] = []
        settled = 0
        while frontier:
            levels.append(frontier)
            settled += len(frontier)
            nxt: list[str] = []
            for node_id in frontier:
                for target in self.successor_ids(node_id):
                    pending[target] -= 1
                    if pending[target] == 0:
                        nxt.append(target)
            frontier = sorted(nxt)
        if settled < len(self.nodes):
            raise CycleError([nid for nid, deg in pending.items() if deg > 0])
        return levels

    @property
    def max_width(self) -> int:
        """Widest static level — the peak parallelism the graph allows."""
        return max((len(level) for level in self.waves()), default=0)

    def __len__(self) -> int:
        return len(self.nodes)

    def __repr__(self) -> str:
        return (
            f"<Graph {self.id!r} nodes={len(self.nodes)} "
            f"edges={len(self.edges)} width={self.max_width}>"
        )
