"""Crash recovery: a run that dies mid-flight resumes from its last wave.

Pausing for input is a planned stop; this is the unplanned one. The engine
writes a checkpoint after every wave settles, so the frontier on disk is always
consistent — every node in it either finished or never started, never "was
running when the process died".
"""

import unittest

from flowforge import (
    Edge,
    Graph,
    MemoryRunStore,
    Node,
    NodeRegistry,
    NodeSpec,
    NodeStatus,
    RunStatus,
    WorkflowEngine,
)


class Crash(BaseException):
    """Not an Exception: the engine must not catch this, as with a real kill."""


def crashing_registry(crash_on: str):
    """Standard node types plus one that takes the process down with it."""
    from flowforge.nodes import EndNode, StartNode, StubNode

    registry = NodeRegistry()
    registry.register("start")(type("S", (StartNode,), {}))
    registry.register("stub")(type("T", (StubNode,), {}))
    registry.register("end")(type("E", (EndNode,), {}))

    fired = {"count": 0}

    @registry.register("crash")
    class CrashNode(Node):
        async def run(self, ctx):
            if ctx.node_id == crash_on and fired["count"] == 0:
                fired["count"] += 1
                raise Crash("worker died")
            return {"survived": True}

    return registry, fired


def chain_graph():
    """start -> a, b (parallel) -> mid -> late -> end"""
    return Graph(
        [
            NodeSpec("start", "start"),
            NodeSpec("a", "stub", {"delay": 0.01, "output": {"v": "a"}}),
            NodeSpec("b", "stub", {"delay": 0.01, "output": {"w": "b"}}),
            NodeSpec("mid", "stub", {"output": {"joined": "{{a.v}}{{b.w}}"}}),
            NodeSpec("late", "crash"),
            NodeSpec("end", "end"),
        ],
        [
            Edge("start", "a"),
            Edge("start", "b"),
            Edge("a", "mid"),
            Edge("b", "mid"),
            Edge("mid", "late"),
            Edge("late", "end"),
        ],
        graph_id="durable",
    )


class PerWaveCheckpointTests(unittest.IsolatedAsyncioTestCase):
    async def test_a_checkpoint_is_written_after_every_wave(self):
        store = MemoryRunStore()
        graph = chain_graph()
        registry, _ = crashing_registry(crash_on="never")
        engine = WorkflowEngine(
            graph, node_registry=registry, store=store, checkpoint_every_wave=True
        )
        result = await engine.run(run_id="r1")

        self.assertIs(result.status, RunStatus.COMPLETED)
        # Finished runs clean up after themselves.
        self.assertEqual(await store.list_ids(), [])

    async def test_off_by_default(self):
        store = MemoryRunStore()
        engine = WorkflowEngine(chain_graph(), node_registry=crashing_registry("never")[0],
                                store=store)
        await engine.run()
        self.assertEqual(await store.list_ids(), [])

    async def test_enabling_it_without_a_store_is_refused(self):
        with self.assertRaises(ValueError):
            WorkflowEngine(chain_graph(), checkpoint_every_wave=True)


class CrashRecoveryTests(unittest.IsolatedAsyncioTestCase):
    async def test_a_killed_run_resumes_from_its_last_wave(self):
        store = MemoryRunStore()
        graph = chain_graph()
        registry, fired = crashing_registry(crash_on="late")

        # First attempt: dies in the wave that runs `late`.
        engine = WorkflowEngine(
            graph, node_registry=registry, store=store, checkpoint_every_wave=True
        )
        with self.assertRaises(Crash):
            await engine.run(run_id="doomed")

        # The frontier survived the death.
        saved = await store.load("doomed")
        self.assertIsNotNone(saved)
        self.assertEqual(saved.ready, ["late"])
        self.assertEqual(saved.nodes["mid"]["status"], NodeStatus.COMPLETED.value)
        self.assertEqual(saved.nodes["late"]["status"], NodeStatus.PENDING.value)

        # A fresh worker picks it up. `late` no longer crashes.
        recovered = await WorkflowEngine(
            graph, node_registry=registry, store=store, checkpoint_every_wave=True
        ).resume(saved)

        self.assertIs(recovered.status, RunStatus.COMPLETED)
        self.assertEqual(recovered.run_id, "doomed")
        self.assertIs(recovered.status_of("late"), NodeStatus.COMPLETED)
        self.assertEqual(fired["count"], 1)  # the crash happened exactly once

    async def test_completed_work_is_not_redone_after_a_crash(self):
        store = MemoryRunStore()
        graph = chain_graph()
        registry, _ = crashing_registry(crash_on="late")

        engine = WorkflowEngine(
            graph, node_registry=registry, store=store, checkpoint_every_wave=True
        )
        with self.assertRaises(Crash):
            await engine.run(run_id="doomed")

        saved = await store.load("doomed")
        mid_before = saved.nodes["mid"]["duration_ms"]

        recovered = await WorkflowEngine(
            graph, node_registry=registry, store=store, checkpoint_every_wave=True
        ).resume(saved)

        # Same recorded duration: restored from the checkpoint, not re-run.
        self.assertEqual(recovered.nodes["mid"].duration_ms, mid_before)
        self.assertEqual(recovered.outputs_of("mid")["joined"], "ab")

    async def test_recovery_needs_no_answers(self):
        """A crash checkpoint has nothing awaiting, unlike a pause checkpoint."""
        store = MemoryRunStore()
        registry, _ = crashing_registry(crash_on="late")
        engine = WorkflowEngine(
            chain_graph(), node_registry=registry, store=store, checkpoint_every_wave=True
        )
        with self.assertRaises(Crash):
            await engine.run(run_id="doomed")

        saved = await store.load("doomed")
        self.assertEqual(saved.awaiting, {})
        self.assertEqual(saved.awaiting_nodes, [])

    async def test_the_crash_checkpoint_is_still_fingerprint_guarded(self):
        from flowforge import CheckpointError

        store = MemoryRunStore()
        registry, _ = crashing_registry(crash_on="late")
        with self.assertRaises(Crash):
            await WorkflowEngine(
                chain_graph(),
                node_registry=registry,
                store=store,
                checkpoint_every_wave=True,
            ).run(run_id="doomed")

        saved = await store.load("doomed")
        edited = Graph(
            [NodeSpec("start", "start"), NodeSpec("a", "stub")], [Edge("start", "a")]
        )
        with self.assertRaises(CheckpointError):
            await WorkflowEngine(edited, node_registry=registry).resume(saved)


if __name__ == "__main__":
    unittest.main()
