import unittest
from pathlib import Path

from flowforge import (
    Edge,
    Graph,
    Node,
    NodeContext,
    NodeRegistry,
    NodeSpec,
    NodeStatus,
    RunStatus,
    UnknownNodeTypeError,
    WorkflowEngine,
)

EXAMPLES = Path(__file__).resolve().parent.parent / "examples"


def stub_graph(nodes, edges, graph_id="test"):
    """nodes: {id: config}, edges: [(source, target)]."""
    return Graph(
        [NodeSpec(id=nid, type="stub", config=cfg) for nid, cfg in nodes.items()],
        [Edge(source=s, target=t) for s, t in edges],
        graph_id=graph_id,
    )


def fan_out(width, delay=0.05):
    """One root, ``width`` independent workers, one join. Peak width = width."""
    nodes = {"root": {}, "join": {}}
    edges = []
    for i in range(width):
        nodes[f"w{i}"] = {"delay": delay}
        edges += [("root", f"w{i}"), (f"w{i}", "join")]
    return stub_graph(nodes, edges, graph_id=f"fan{width}")


class ExecutionTests(unittest.IsolatedAsyncioTestCase):
    async def test_diamond_runs_every_node_in_dependency_order(self):
        graph = Graph.from_file(EXAMPLES / "diamond.json")
        result = await WorkflowEngine(graph).run()

        self.assertIs(result.status, RunStatus.COMPLETED)
        self.assertTrue(result.ok)
        for record in result.nodes.values():
            self.assertIs(record.status, NodeStatus.COMPLETED, record.node_id)
        self.assertEqual(result.nodes["start"].wave, 1)
        self.assertEqual(result.nodes["fetch_a"].wave, 2)
        self.assertEqual(result.nodes["join"].wave, 3)
        self.assertEqual(result.nodes["end"].wave, 4)
        self.assertEqual(result.stats.waves, 4)

    async def test_join_receives_every_predecessor_output(self):
        graph = Graph.from_file(EXAMPLES / "diamond.json")
        result = await WorkflowEngine(graph).run()

        end_outputs = result.outputs_of("end")
        self.assertEqual(list(end_outputs), ["join"])
        joined = result.outputs_of("join")
        self.assertEqual({joined["a"], joined["b"], joined["c"]}, {1, 2, 3})

    async def test_run_inputs_reach_downstream_nodes(self):
        graph = Graph(
            [NodeSpec("start", "start"), NodeSpec("tail", "stub")],
            [Edge("start", "tail")],
        )
        result = await WorkflowEngine(graph).run({"question": "hi"})
        self.assertEqual(result.outputs_of("tail")["question"], "hi")

    async def test_only_leaf_outputs_are_surfaced(self):
        graph = stub_graph(
            {"a": {}, "b": {}, "c": {}}, [("a", "b"), ("a", "c")], graph_id="two_leaves"
        )
        result = await WorkflowEngine(graph).run()
        self.assertEqual(sorted(result.outputs), ["b", "c"])


class ConcurrencyTests(unittest.IsolatedAsyncioTestCase):
    async def test_independent_nodes_run_at_the_same_time(self):
        # 4 x 100ms of sleeping. Serial would need >= 400ms.
        graph = fan_out(4, delay=0.1)
        result = await WorkflowEngine(graph).run()

        self.assertIs(result.status, RunStatus.COMPLETED)
        self.assertEqual(result.stats.peak_concurrency, 4)
        self.assertLess(result.stats.total_ms, 250)
        # Summed node time far exceeds wall time — that is the parallel win.
        self.assertGreater(result.stats.node_ms, 380)

    async def test_max_concurrency_caps_parallelism(self):
        graph = fan_out(4, delay=0.05)
        result = await WorkflowEngine(graph, max_concurrency=2).run()

        self.assertIs(result.status, RunStatus.COMPLETED)
        self.assertEqual(result.stats.peak_concurrency, 2)
        self.assertGreater(result.stats.total_ms, 90)  # two batches of 50ms

    async def test_max_concurrency_must_be_positive(self):
        with self.assertRaises(ValueError):
            WorkflowEngine(fan_out(2), max_concurrency=0)

    async def test_wide_graph_of_500_nodes(self):
        graph = fan_out(500, delay=0.01)
        result = await WorkflowEngine(graph).run()

        self.assertIs(result.status, RunStatus.COMPLETED)
        self.assertEqual(result.stats.nodes_executed, 502)
        self.assertEqual(result.stats.peak_concurrency, 500)

    async def test_chained_diamonds_do_not_blow_up(self):
        # k diamonds in series => 2^k distinct paths, but only 3k+1 nodes.
        # Kahn touches each node and edge once; path enumeration would not.
        k = 12
        nodes = {"n0": {}}
        edges = []
        for i in range(k):
            src, left, right, dst = f"n{i}", f"l{i}", f"r{i}", f"n{i + 1}"
            nodes.update({left: {"delay": 0.001}, right: {"delay": 0.001}, dst: {}})
            edges += [(src, left), (src, right), (left, dst), (right, dst)]
        result = await WorkflowEngine(stub_graph(nodes, edges)).run()

        self.assertIs(result.status, RunStatus.COMPLETED)
        self.assertEqual(result.stats.nodes_executed, 3 * k + 1)
        self.assertEqual(result.stats.waves, 2 * k + 1)  # linear in k, not 2**k
        self.assertEqual(result.stats.peak_concurrency, 2)


class FailureTests(unittest.IsolatedAsyncioTestCase):
    async def test_failing_node_stops_the_run_and_leaves_downstream_pending(self):
        graph = stub_graph(
            {"a": {}, "boom": {"fail": "upstream exploded"}, "c": {}},
            [("a", "boom"), ("boom", "c")],
        )
        result = await WorkflowEngine(graph).run()

        self.assertIs(result.status, RunStatus.FAILED)
        self.assertFalse(result.ok)
        self.assertEqual(result.failures, ["boom"])
        self.assertIs(result.status_of("boom"), NodeStatus.FAILED)
        self.assertIn("upstream exploded", result.nodes["boom"].error)
        self.assertIs(result.status_of("c"), NodeStatus.PENDING)
        self.assertEqual(result.outputs, {})

    async def test_siblings_in_the_same_wave_still_finish(self):
        graph = stub_graph(
            {"root": {}, "ok": {}, "boom": {"fail": "nope"}},
            [("root", "ok"), ("root", "boom")],
        )
        result = await WorkflowEngine(graph).run()

        self.assertIs(result.status, RunStatus.FAILED)
        self.assertIs(result.status_of("ok"), NodeStatus.COMPLETED)

    async def test_unknown_node_type_is_rejected_before_running(self):
        graph = Graph([NodeSpec("a", "does_not_exist")], [])
        with self.assertRaises(UnknownNodeTypeError):
            WorkflowEngine(graph)


class RegistryTests(unittest.IsolatedAsyncioTestCase):
    async def test_custom_node_type_needs_no_scheduler_change(self):
        custom = NodeRegistry()

        @custom.register("double")
        class DoubleNode(Node):
            async def run(self, ctx: NodeContext):
                value = ctx.merged_upstream().get("value", ctx.config.get("value", 0))
                return {"value": value * 2}

        graph = Graph(
            [
                NodeSpec("a", "double", {"value": 3}),
                NodeSpec("b", "double"),
            ],
            [Edge("a", "b")],
        )
        result = await WorkflowEngine(graph, node_registry=custom).run()

        self.assertEqual(custom.known_types(), ["double"])
        self.assertEqual(result.outputs_of("b"), {"value": 12})

    async def test_registering_a_type_twice_is_an_error(self):
        custom = NodeRegistry()

        @custom.register("x")
        class First(Node):
            async def run(self, ctx):
                return {}

        with self.assertRaises(ValueError):

            @custom.register("x")
            class Second(Node):
                async def run(self, ctx):
                    return {}


class SyncApiTests(unittest.TestCase):
    def test_run_sync_executes_the_workflow(self):
        result = WorkflowEngine(fan_out(3, delay=0.01)).run_sync()
        self.assertIs(result.status, RunStatus.COMPLETED)


if __name__ == "__main__":
    unittest.main()
