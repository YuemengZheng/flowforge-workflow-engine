"""Pause, persist, resume — and the guards that stop a bad resume."""

import unittest
from pathlib import Path

from flowforge import (
    Checkpoint,
    CheckpointError,
    Edge,
    Graph,
    MemoryRunStore,
    NodeSpec,
    NodeStatus,
    RunStatus,
    WorkflowEngine,
    graph_fingerprint,
)
from flowforge.events import NODE_PAUSED, RUN_PAUSED, RUN_RESUMED

EXAMPLES = Path(__file__).resolve().parent.parent / "examples"


def approval_graph():
    return Graph.from_file(EXAMPLES / "approval.json")


class FingerprintTests(unittest.TestCase):
    def test_same_graph_same_fingerprint(self):
        self.assertEqual(
            graph_fingerprint(approval_graph()), graph_fingerprint(approval_graph())
        )

    def test_changing_a_config_changes_the_fingerprint(self):
        original = Graph([NodeSpec("a", "stub", {"delay": 1})], [])
        edited = Graph([NodeSpec("a", "stub", {"delay": 2})], [])
        self.assertNotEqual(graph_fingerprint(original), graph_fingerprint(edited))

    def test_changing_an_edge_changes_the_fingerprint(self):
        one = Graph([NodeSpec("a", "stub"), NodeSpec("b", "stub")], [Edge("a", "b")])
        two = Graph([NodeSpec("a", "stub"), NodeSpec("b", "stub")], [])
        self.assertNotEqual(graph_fingerprint(one), graph_fingerprint(two))


class SerializationTests(unittest.TestCase):
    def test_round_trip_through_json(self):
        original = Checkpoint(
            run_id="r1",
            workflow_id="wf",
            fingerprint="abc",
            inputs={"x": 1},
            pending={"a": 0, "b": 1},
            taken={"a": 0, "b": 1},
            nodes={"a": {"status": "completed", "outputs": {"v": 2}}},
            ready=["b"],
            variables={"inputs": {"x": 1}, "a": {"v": 2}},
            awaiting={"ask": {"prompt": "ok?"}},
            waves=3,
        )
        restored = Checkpoint.from_json(original.to_json())

        self.assertEqual(restored.as_dict(), original.as_dict())
        self.assertEqual(restored.awaiting_nodes, ["ask"])

    def test_malformed_json_is_rejected(self):
        with self.assertRaises(CheckpointError):
            Checkpoint.from_json("{not json")

    def test_missing_field_is_rejected(self):
        with self.assertRaises(CheckpointError):
            Checkpoint.from_dict({"run_id": "r"})


class PauseResumeTests(unittest.IsolatedAsyncioTestCase):
    async def test_run_pauses_at_the_input_node(self):
        result = await WorkflowEngine(approval_graph()).run()

        self.assertIs(result.status, RunStatus.PAUSED)
        self.assertTrue(result.paused)
        self.assertFalse(result.ok)
        self.assertIs(result.status_of("build"), NodeStatus.COMPLETED)
        self.assertIs(result.status_of("ask"), NodeStatus.PAUSED)
        self.assertIs(result.status_of("gate"), NodeStatus.PENDING)
        self.assertEqual(list(result.awaiting), ["ask"])
        self.assertEqual(
            result.awaiting["ask"]["prompt"],
            "Deploy a1b2c3d to production? Tests are passing.",
        )

    async def test_resume_approved_takes_the_go_branch(self):
        engine = WorkflowEngine(approval_graph())
        paused = await engine.run()
        final = await engine.resume(paused.checkpoint, {"ask": {"approved": True}})

        self.assertIs(final.status, RunStatus.COMPLETED)
        self.assertEqual(final.branch_of("gate"), "go")
        self.assertIs(final.status_of("deploy"), NodeStatus.COMPLETED)
        self.assertIs(final.status_of("notify_hold"), NodeStatus.SKIPPED)
        self.assertEqual(
            final.outputs_of("report")["message"],
            "sha a1b2c3d -> go (deployed: a1b2c3d)",
        )

    async def test_resume_rejected_takes_the_halt_branch(self):
        engine = WorkflowEngine(approval_graph())
        paused = await engine.run()
        final = await engine.resume(paused.checkpoint, {"ask": {"approved": False}})

        self.assertIs(final.status, RunStatus.COMPLETED)
        self.assertEqual(final.branch_of("gate"), "halt")
        self.assertIs(final.status_of("deploy"), NodeStatus.SKIPPED)
        self.assertEqual(
            final.outputs_of("report")["message"], "sha a1b2c3d -> halt (deployed: none)"
        )

    async def test_completed_nodes_are_not_re_executed(self):
        engine = WorkflowEngine(approval_graph())
        paused = await engine.run()
        build_duration = paused.nodes["build"].duration_ms

        final = await engine.resume(paused.checkpoint, {"ask": {"approved": True}})

        # Same record, same measured time: it was restored, not run again.
        self.assertEqual(final.nodes["build"].duration_ms, build_duration)
        self.assertEqual(final.nodes["build"].wave, 2)
        self.assertGreater(final.stats.waves, paused.stats.waves)

    async def test_answer_is_readable_as_a_normal_variable(self):
        engine = WorkflowEngine(approval_graph())
        paused = await engine.run()
        final = await engine.resume(
            paused.checkpoint, {"ask": {"approved": True, "by": "zheng"}}
        )
        self.assertEqual(final.variables["ask"]["by"], "zheng")

    async def test_resume_survives_a_full_json_round_trip(self):
        engine = WorkflowEngine(approval_graph())
        paused = await engine.run()

        # What Redis would hand back: text, not the original object.
        revived = Checkpoint.from_json(paused.checkpoint.to_json())
        final = await WorkflowEngine(approval_graph()).resume(
            revived, {"ask": {"approved": True}}
        )

        self.assertIs(final.status, RunStatus.COMPLETED)
        self.assertEqual(final.run_id, paused.run_id)

    async def test_resume_without_the_answer_is_refused(self):
        engine = WorkflowEngine(approval_graph())
        paused = await engine.run()

        with self.assertRaises(CheckpointError) as ctx:
            await engine.resume(paused.checkpoint, {})
        self.assertIn("ask", str(ctx.exception))

    async def test_resume_against_a_changed_graph_is_refused(self):
        paused = await WorkflowEngine(approval_graph()).run()

        edited = Graph.from_dict(
            {
                **{"id": "approval"},
                "nodes": [
                    {"id": "start", "type": "start"},
                    {"id": "build", "type": "stub", "config": {"output": {"sha": "DIFFERENT"}}},
                    {"id": "ask", "type": "await_input", "config": {"prompt": "?"}},
                ],
                "edges": [
                    {"source": "start", "target": "build"},
                    {"source": "build", "target": "ask"},
                ],
            }
        )
        with self.assertRaises(CheckpointError) as ctx:
            await WorkflowEngine(edited).resume(
                paused.checkpoint, {"ask": {"approved": True}}
            )
        self.assertIn("changed since it was saved", str(ctx.exception))

    async def test_pause_is_never_retried(self):
        graph = Graph.from_dict(
            {
                "nodes": [
                    {
                        "id": "ask",
                        "type": "await_input",
                        "retries": 3,
                        "config": {"prompt": "hello?"},
                    }
                ]
            }
        )
        result = await WorkflowEngine(graph).run()

        self.assertIs(result.status, RunStatus.PAUSED)
        self.assertEqual(result.nodes["ask"].attempts, 1)

    async def test_siblings_finish_even_though_one_node_paused(self):
        graph = Graph(
            [
                NodeSpec("root", "stub"),
                NodeSpec("ask", "await_input", {"prompt": "?"}),
                NodeSpec("sibling", "stub", {"output": {"done": True}}),
            ],
            [Edge("root", "ask"), Edge("root", "sibling")],
        )
        result = await WorkflowEngine(graph).run()

        self.assertIs(result.status, RunStatus.PAUSED)
        self.assertIs(result.status_of("sibling"), NodeStatus.COMPLETED)

    async def test_two_pauses_in_one_workflow(self):
        graph = Graph(
            [
                NodeSpec("first", "await_input", {"prompt": "one?"}),
                NodeSpec("second", "await_input", {"prompt": "two?"}),
                NodeSpec("done", "stub", {"output": {"a": "{{first.v}}", "b": "{{second.v}}"}}),
            ],
            [Edge("first", "second"), Edge("second", "done")],
        )
        engine = WorkflowEngine(graph)

        first_pause = await engine.run()
        self.assertEqual(list(first_pause.awaiting), ["first"])

        second_pause = await engine.resume(first_pause.checkpoint, {"first": {"v": 1}})
        self.assertIs(second_pause.status, RunStatus.PAUSED)
        self.assertEqual(list(second_pause.awaiting), ["second"])

        final = await engine.resume(second_pause.checkpoint, {"second": {"v": 2}})
        self.assertIs(final.status, RunStatus.COMPLETED)
        # Both answers survived the two pauses and reached the last node.
        outputs = final.outputs_of("done")
        self.assertEqual((outputs["a"], outputs["b"]), (1, 2))


class PauseStreamingTests(unittest.IsolatedAsyncioTestCase):
    async def test_pause_is_visible_on_the_event_stream(self):
        engine = WorkflowEngine(approval_graph())
        events = [e async for e in engine.stream()]

        self.assertEqual(events[-1].type, RUN_PAUSED)
        self.assertTrue(events[-1].is_terminal)
        self.assertIn("ask", events[-1].data["awaiting"])
        paused = [e for e in events if e.type == NODE_PAUSED]
        self.assertEqual(paused[0].node_id, "ask")

    async def test_resume_opens_with_run_resumed(self):
        engine = WorkflowEngine(approval_graph())
        paused = await engine.run()
        events = [
            e
            async for e in engine.stream_resume(
                paused.checkpoint, {"ask": {"approved": True}}
            )
        ]

        self.assertEqual(events[0].type, RUN_RESUMED)
        self.assertEqual(events[0].data["from_wave"], paused.stats.waves)
        self.assertEqual(events[-1].data["status"], "completed")


class StoreIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_pause_store_load_resume(self):
        store = MemoryRunStore()
        engine = WorkflowEngine(approval_graph())

        paused = await engine.run()
        await store.save(paused.checkpoint)

        # A different process would only have the run id to go on.
        self.assertEqual(await store.list_ids(), [paused.run_id])
        loaded = await store.load(paused.run_id)
        final = await WorkflowEngine(approval_graph()).resume(
            loaded, {"ask": {"approved": True}}
        )

        self.assertIs(final.status, RunStatus.COMPLETED)
        await store.delete(paused.run_id)
        self.assertEqual(await store.list_ids(), [])

    async def test_loading_an_unknown_run_returns_none(self):
        self.assertIsNone(await MemoryRunStore().load("nope"))


if __name__ == "__main__":
    unittest.main()
