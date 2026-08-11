import unittest
from pathlib import Path

from flowforge import CycleError, Edge, Graph, GraphError, NodeSpec

EXAMPLES = Path(__file__).resolve().parent.parent / "examples"


def build(edges, node_ids=None):
    ids = node_ids or sorted({n for e in edges for n in e})
    return Graph(
        [NodeSpec(id=n, type="stub") for n in ids],
        [Edge(source=s, target=t) for s, t in edges],
    )


class ParsingTests(unittest.TestCase):
    def test_from_dict_builds_nodes_and_edges(self):
        graph = Graph.from_dict(
            {
                "id": "wf",
                "nodes": [
                    {"id": "a", "type": "start"},
                    {"id": "b", "type": "stub", "config": {"delay": 0.1}},
                ],
                "edges": [{"source": "a", "target": "b"}],
            }
        )
        self.assertEqual(graph.id, "wf")
        self.assertEqual(len(graph), 2)
        self.assertEqual(graph.nodes["b"].config["delay"], 0.1)
        self.assertEqual(graph.successor_ids("a"), ["b"])

    def test_edges_default_to_empty(self):
        graph = Graph.from_dict({"nodes": [{"id": "solo", "type": "start"}]})
        self.assertEqual(graph.roots, ["solo"])
        self.assertEqual(graph.leaves, ["solo"])

    def test_invalid_json_is_a_graph_error(self):
        with self.assertRaises(GraphError):
            Graph.from_json("{not json")

    def test_missing_node_field_is_reported(self):
        with self.assertRaises(GraphError) as ctx:
            Graph.from_dict({"nodes": [{"id": "a"}]})
        self.assertIn("type", str(ctx.exception))


class ValidationTests(unittest.TestCase):
    def test_empty_graph_rejected(self):
        with self.assertRaises(GraphError):
            Graph([], [])

    def test_duplicate_node_id_rejected(self):
        with self.assertRaises(GraphError):
            Graph([NodeSpec("a", "stub"), NodeSpec("a", "stub")], [])

    def test_edge_to_unknown_node_rejected(self):
        with self.assertRaises(GraphError) as ctx:
            Graph([NodeSpec("a", "stub")], [Edge("a", "ghost")])
        self.assertIn("ghost", str(ctx.exception))

    def test_self_loop_rejected(self):
        with self.assertRaises(GraphError):
            Graph([NodeSpec("a", "stub")], [Edge("a", "a")])

    def test_duplicate_edge_rejected(self):
        with self.assertRaises(GraphError):
            Graph(
                [NodeSpec("a", "stub"), NodeSpec("b", "stub")],
                [Edge("a", "b"), Edge("a", "b")],
            )

    def test_cycle_rejected_and_names_the_cycle(self):
        with self.assertRaises(CycleError) as ctx:
            build([("s", "a"), ("a", "b"), ("b", "c"), ("c", "a")])
        self.assertEqual(ctx.exception.nodes, ["a", "b", "c"])

    def test_two_node_cycle_rejected(self):
        with self.assertRaises(CycleError):
            build([("a", "b"), ("b", "a")])

    def test_cycle_example_file_rejected(self):
        with self.assertRaises(CycleError):
            Graph.from_file(EXAMPLES / "cycle.invalid.json")


class ShapeTests(unittest.TestCase):
    def setUp(self):
        # start -> a, b, c -> join -> end   (a classic fan-out / fan-in)
        self.graph = Graph.from_file(EXAMPLES / "diamond.json")

    def test_indegree_counts_predecessors(self):
        self.assertEqual(self.graph.indegree["start"], 0)
        self.assertEqual(self.graph.indegree["fetch_a"], 1)
        self.assertEqual(self.graph.indegree["join"], 3)

    def test_indegree_is_a_fresh_copy(self):
        first = self.graph.indegree
        first["join"] = 99
        self.assertEqual(self.graph.indegree["join"], 3)

    def test_roots_and_leaves(self):
        self.assertEqual(self.graph.roots, ["start"])
        self.assertEqual(self.graph.leaves, ["end"])

    def test_topological_order_respects_dependencies(self):
        order = self.graph.topological_order()
        self.assertEqual(len(order), len(self.graph))
        position = {node_id: i for i, node_id in enumerate(order)}
        for edge in self.graph.edges:
            self.assertLess(position[edge.source], position[edge.target])

    def test_waves_group_independent_nodes(self):
        self.assertEqual(
            self.graph.waves(),
            [["start"], ["fetch_a", "fetch_b", "fetch_c"], ["join"], ["end"]],
        )
        self.assertEqual(self.graph.max_width, 3)

    def test_waves_of_a_pure_chain(self):
        graph = build([("a", "b"), ("b", "c")])
        self.assertEqual(graph.waves(), [["a"], ["b"], ["c"]])
        self.assertEqual(graph.max_width, 1)

    def test_disconnected_components_share_a_wave(self):
        graph = build([("a", "b"), ("x", "y")])
        self.assertEqual(graph.waves(), [["a", "x"], ["b", "y"]])


if __name__ == "__main__":
    unittest.main()
