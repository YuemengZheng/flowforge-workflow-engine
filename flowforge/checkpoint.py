"""Durable scheduler state: everything needed to continue a paused run.

A checkpoint is the scheduler's own bookkeeping — the in-degree counters, which
incoming edges were taken, each node's record, the ready queue, and the variable
pool — serialised to plain JSON. That is deliberately the *whole* state: resume
does not replay completed nodes, it reconstructs the counters and carries on
from the ready queue.

It is fingerprinted against the graph it came from, so a checkpoint can never be
applied to a workflow whose shape has changed underneath it.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from time import time
from typing import Any, Mapping

from .errors import FlowForgeError


class CheckpointError(FlowForgeError):
    """A checkpoint is malformed, or does not match the graph being resumed."""


def graph_fingerprint(graph: Any) -> str:
    """Stable hash of a graph's structure — ids, types, config and edges.

    Node ``config`` is included because a resumed run must not silently pick up
    a rewritten prompt or a changed branch condition halfway through.
    """
    payload = {
        "id": graph.id,
        "nodes": sorted(
            (spec.id, spec.type, json.dumps(spec.config, sort_keys=True, default=str))
            for spec in graph.nodes.values()
        ),
        "edges": sorted(
            (edge.source, edge.target, edge.branch or "") for edge in graph.edges
        ),
    }
    encoded = json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()[:16]


@dataclass
class Checkpoint:
    """A paused run, ready to be stored and picked up later."""

    run_id: str
    workflow_id: str
    fingerprint: str
    inputs: dict[str, Any] = field(default_factory=dict)
    pending: dict[str, int] = field(default_factory=dict)
    taken: dict[str, int] = field(default_factory=dict)
    nodes: dict[str, dict[str, Any]] = field(default_factory=dict)
    ready: list[str] = field(default_factory=list)
    variables: dict[str, Any] = field(default_factory=dict)
    awaiting: dict[str, dict[str, Any]] = field(default_factory=dict)
    waves: int = 0
    saved_at: float = field(default_factory=time)

    @property
    def awaiting_nodes(self) -> list[str]:
        """Nodes blocked on an answer — what a caller must supply to resume."""
        return sorted(self.awaiting)

    def as_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "workflow_id": self.workflow_id,
            "fingerprint": self.fingerprint,
            "inputs": self.inputs,
            "pending": self.pending,
            "taken": self.taken,
            "nodes": self.nodes,
            "ready": self.ready,
            "variables": self.variables,
            "awaiting": self.awaiting,
            "waves": self.waves,
            "saved_at": self.saved_at,
        }

    def to_json(self) -> str:
        return json.dumps(self.as_dict(), ensure_ascii=False, default=str)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "Checkpoint":
        try:
            return cls(
                run_id=str(data["run_id"]),
                workflow_id=str(data["workflow_id"]),
                fingerprint=str(data["fingerprint"]),
                inputs=dict(data.get("inputs") or {}),
                pending={str(k): int(v) for k, v in (data.get("pending") or {}).items()},
                taken={str(k): int(v) for k, v in (data.get("taken") or {}).items()},
                nodes=dict(data.get("nodes") or {}),
                ready=[str(n) for n in (data.get("ready") or [])],
                variables=dict(data.get("variables") or {}),
                awaiting=dict(data.get("awaiting") or {}),
                waves=int(data.get("waves") or 0),
                saved_at=float(data.get("saved_at") or 0.0),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise CheckpointError(f"malformed checkpoint: {exc}") from exc

    @classmethod
    def from_json(cls, text: str) -> "Checkpoint":
        try:
            data = json.loads(text)
        except json.JSONDecodeError as exc:
            raise CheckpointError(f"checkpoint is not valid JSON: {exc}") from exc
        if not isinstance(data, Mapping):
            raise CheckpointError("checkpoint must be a JSON object")
        return cls.from_dict(data)

    def verify_against(self, graph: Any) -> None:
        expected = graph_fingerprint(graph)
        if expected != self.fingerprint:
            raise CheckpointError(
                f"checkpoint {self.run_id!r} was taken from a different version of "
                f"workflow {self.workflow_id!r} (fingerprint {self.fingerprint} != "
                f"{expected}); the graph changed since it was saved"
            )
        unknown = set(self.pending) - set(graph.nodes)
        if unknown:
            raise CheckpointError(
                f"checkpoint references unknown nodes: {', '.join(sorted(unknown))}"
            )
