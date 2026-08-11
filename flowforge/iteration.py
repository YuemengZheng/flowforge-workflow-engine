"""Iterate a sub-workflow over a list.

Each item runs the nested graph in its **own** engine run, which means its own
variable pool, its own counters, its own node records. That is the execution
domain isolation: two iterations cannot see each other's variables, because
there is no shared pool to leak through — not a deep copy of one, but a
separate one.

Items run concurrently under a semaphore. The reference implementation walks
the batch with a sequential ``for`` loop and awaits each item in turn; here the
whole batch is dispatched at once and bounded by ``max_concurrency``, which is
the same trick the top-level scheduler uses on a ready set.
"""

from __future__ import annotations

import asyncio
from typing import Any, Mapping

from .errors import FlowForgeError, NodePaused
from .events import NODE_DELTA
from .graph import Graph
from .nodes import Node, NodeContext, registry

DEFAULT_ITEM_NAME = "item"


class IterationError(FlowForgeError):
    """The iterate node is misconfigured, or an iteration failed."""


@registry.register("iterate")
class IterateNode(Node):
    """Runs a nested workflow once per item and collects the results.

    config::

        {
          "items": "{{fetch.rows}}",          # resolved to a list
          "item_as": "row",                   # sub-run input name (default "item")
          "max_concurrency": 8,               # 0/absent = unbounded
          "collect": ["summary"],             # leaf fields to gather; default: all
          "on_item_error": "fail",            # or "skip"
          "workflow": { "nodes": [...], "edges": [...] }
        }

    Outputs ``results`` (one entry per item, in input order), ``count``, and
    ``failed`` (indexes that errored, when ``on_item_error`` is ``skip``).

    Order is preserved even though execution is not ordered: results are placed
    by index, not appended as they finish.
    """

    # The nested definition's templates belong to the sub-run's pool. Without
    # this the parent resolves {{inputs.item}} against its own inputs and the
    # whole thing fails before the first iteration starts.
    raw_config_keys = frozenset({"workflow"})

    async def run(self, ctx: NodeContext) -> Mapping[str, Any]:
        items = ctx.config.get("items")
        # One check, not three: a string, a dict and an int are all equally
        # wrong here, and str would otherwise iterate character by character.
        if not isinstance(items, (list, tuple)):
            raise IterationError(
                f"node {ctx.node_id!r}: 'items' must resolve to a list, got "
                f"{type(items).__name__}"
            )
        items = list(items)

        definition = ctx.config.get("workflow")
        if not isinstance(definition, Mapping):
            raise IterationError(
                f"node {ctx.node_id!r}: 'workflow' must be a nested graph object"
            )

        # Built once, shared by every iteration: a Graph is immutable, and the
        # per-run state lives in the engine call, not in the graph.
        from .engine import RunStatus, WorkflowEngine

        try:
            sub_graph = Graph.from_dict(definition)
        except FlowForgeError as exc:
            raise IterationError(f"node {ctx.node_id!r}: {exc}") from exc
        engine = WorkflowEngine(sub_graph)

        item_name = str(ctx.config.get("item_as", DEFAULT_ITEM_NAME))
        collect = ctx.config.get("collect")
        skip_failures = str(ctx.config.get("on_item_error", "fail")).lower() == "skip"
        limit = int(ctx.config.get("max_concurrency", 0) or 0)
        gate = asyncio.Semaphore(limit) if limit > 0 else None

        results: list[Any] = [None] * len(items)
        failed: list[int] = []
        done = 0

        async def run_one(index: int, item: Any) -> None:
            nonlocal done
            if gate is not None:
                await gate.acquire()
            try:
                run = await engine.run(
                    {item_name: item, "index": index, **dict(ctx.run_inputs)},
                    run_id=f"{ctx.run_id}:{ctx.node_id}:{index}",
                )
                if run.status is RunStatus.PAUSED:
                    raise IterationError(
                        f"item {index} paused; a nested workflow cannot ask for "
                        f"input — move the {list(run.awaiting)} node to the parent"
                    )
                if not run.ok:
                    detail = "; ".join(
                        f"{n}: {run.nodes[n].error}" for n in run.failures
                    )
                    raise IterationError(f"item {index} failed ({detail})")
                results[index] = _collect(run.outputs, collect)
            except Exception:
                failed.append(index)
                if not skip_failures:
                    raise
            finally:
                done += 1
                if gate is not None:
                    gate.release()
                await ctx.emit(
                    NODE_DELTA, index=index, done=done, total=len(items)
                )

        await asyncio.gather(*(run_one(i, item) for i, item in enumerate(items)))

        return {
            "results": [r for i, r in enumerate(results) if i not in set(failed)],
            "count": len(items) - len(failed),
            "failed": sorted(failed),
        }


def _collect(outputs: Mapping[str, Mapping[str, Any]], collect: Any) -> Any:
    """Flatten a sub-run's leaf outputs, optionally down to named fields."""
    flat: dict[str, Any] = {}
    for leaf_outputs in outputs.values():
        flat.update(leaf_outputs)
    if collect is None:
        return flat
    if isinstance(collect, str):
        return flat.get(collect)
    return {key: flat.get(key) for key in collect}
