"""The branch-skip case: an untaken branch must not strand the join below it."""

import unittest
from pathlib import Path

from flowforge import (
    ConditionError,
    Edge,
    Graph,
    NodeSpec,
    NodeStatus,
    RunStatus,
    WorkflowEngine,
    evaluate_condition,
)

EXAMPLES = Path(__file__).resolve().parent.parent / "examples"


def branching_graph(join_type="stub"):
    """start -> route -{yes|no}-> (yes_side | no_side) -> join

    The join has in-degree 2 but only ever one live predecessor.
    """
    return Graph(
        [
            NodeSpec("start", "start"),
            NodeSpec(
                "route",
                "decision",
                {
                    "cases": [
                        {
                            "branch": "yes",
                            "when": {"left": "{{inputs.n}}", "op": "gt", "right": 10},
                        }
                    ],
                    "default": "no",
                },
            ),
            NodeSpec("yes_side", "stub", {"output": {"path": "yes"}}),
            NodeSpec("no_side", "stub", {"output": {"path": "no"}}),
            NodeSpec("join", join_type, {"output": {"joined": True}}),
        ],
        [
            Edge("start", "route"),
            Edge("route", "yes_side", branch="yes"),
            Edge("route", "no_side", branch="no"),
            Edge("yes_side", "join"),
            Edge("no_side", "join"),
        ],
        graph_id="branching",
    )


class BranchSkipTests(unittest.IsolatedAsyncioTestCase):
    async def test_untaken_branch_is_skipped_and_join_still_runs(self):
        result = await WorkflowEngine(branching_graph()).run({"n": 99})

        self.assertIs(result.status, RunStatus.COMPLETED)
        self.assertEqual(result.branch_of("route"), "yes")
        self.assertIs(result.status_of("yes_side"), NodeStatus.COMPLETED)
        self.assertIs(result.status_of("no_side"), NodeStatus.SKIPPED)
        self.assertIs(result.status_of("join"), NodeStatus.COMPLETED)
        self.assertEqual(result.skipped, ["no_side"])
        self.assertEqual(result.stats.nodes_skipped, 1)

    async def test_default_branch_taken_when_no_case_matches(self):
        result = await WorkflowEngine(branching_graph()).run({"n": 1})

        self.assertEqual(result.branch_of("route"), "no")
        self.assertIs(result.status_of("no_side"), NodeStatus.COMPLETED)
        self.assertIs(result.status_of("yes_side"), NodeStatus.SKIPPED)
        self.assertIs(result.status_of("join"), NodeStatus.COMPLETED)

    async def test_join_only_sees_the_live_predecessor(self):
        result = await WorkflowEngine(branching_graph()).run({"n": 99})
        self.assertEqual(result.outputs_of("join")["path"], "yes")

    async def test_skip_propagates_through_a_whole_subgraph(self):
        # route -no-> a -> b -> join ; the skip has to travel a and b before
        # the join's in-degree can reach zero.
        graph = Graph(
            [
                NodeSpec(
                    "route",
                    "decision",
                    {"cases": [{"branch": "yes", "when": {"left": True}}], "default": "no"},
                ),
                NodeSpec("live", "stub"),
                NodeSpec("a", "stub"),
                NodeSpec("b", "stub"),
                NodeSpec("join", "stub"),
            ],
            [
                Edge("route", "live", branch="yes"),
                Edge("route", "a", branch="no"),
                Edge("a", "b"),
                Edge("live", "join"),
                Edge("b", "join"),
            ],
        )
        result = await WorkflowEngine(graph).run()

        self.assertIs(result.status, RunStatus.COMPLETED)
        self.assertEqual(result.skipped, ["a", "b"])
        self.assertIs(result.status_of("join"), NodeStatus.COMPLETED)

    async def test_unlabelled_edge_is_always_taken(self):
        # An audit node hanging off the decision runs whichever branch wins.
        graph = Graph(
            [
                NodeSpec(
                    "route",
                    "decision",
                    {"cases": [{"branch": "yes", "when": {"left": True}}], "default": "no"},
                ),
                NodeSpec("yes_side", "stub"),
                NodeSpec("no_side", "stub"),
                NodeSpec("audit", "stub"),
            ],
            [
                Edge("route", "yes_side", branch="yes"),
                Edge("route", "no_side", branch="no"),
                Edge("route", "audit"),
            ],
        )
        result = await WorkflowEngine(graph).run()

        self.assertIs(result.status_of("audit"), NodeStatus.COMPLETED)
        self.assertIs(result.status_of("no_side"), NodeStatus.SKIPPED)

    async def test_selecting_a_branch_the_graph_does_not_offer_fails(self):
        graph = Graph(
            [
                NodeSpec(
                    "route",
                    "decision",
                    {"cases": [], "default": "typo"},
                ),
                NodeSpec("yes_side", "stub"),
            ],
            [Edge("route", "yes_side", branch="yes")],
        )
        result = await WorkflowEngine(graph).run()

        self.assertIs(result.status, RunStatus.FAILED)
        self.assertIn("typo", result.nodes["route"].error)

    async def test_no_match_and_no_default_fails_loudly(self):
        graph = Graph(
            [
                NodeSpec(
                    "route",
                    "decision",
                    {"cases": [{"branch": "yes", "when": {"left": False}}]},
                ),
                NodeSpec("yes_side", "stub"),
            ],
            [Edge("route", "yes_side", branch="yes")],
        )
        result = await WorkflowEngine(graph).run()

        self.assertIs(result.status, RunStatus.FAILED)
        self.assertIn("default", result.nodes["route"].error)


class ConditionTests(unittest.TestCase):
    def test_comparison_operators(self):
        self.assertTrue(evaluate_condition({"left": 5, "op": "gt", "right": 3}))
        self.assertTrue(evaluate_condition({"left": "5", "op": "gte", "right": 5}))
        self.assertFalse(evaluate_condition({"left": 2, "op": "lt", "right": 2}))
        self.assertTrue(evaluate_condition({"left": "a", "op": "eq", "right": "a"}))
        self.assertTrue(evaluate_condition({"left": [1, 2], "op": "contains", "right": 2}))
        self.assertTrue(evaluate_condition({"left": "b", "op": "in", "right": ["a", "b"]}))

    def test_truthiness_is_the_default_operator(self):
        self.assertTrue(evaluate_condition({"left": "text"}))
        self.assertFalse(evaluate_condition({"left": ""}))
        self.assertTrue(evaluate_condition({"left": [], "op": "empty"}))

    def test_groups(self):
        yes = {"left": 1, "op": "eq", "right": 1}
        no = {"left": 1, "op": "eq", "right": 2}
        self.assertTrue(evaluate_condition({"all": [yes, yes]}))
        self.assertFalse(evaluate_condition({"all": [yes, no]}))
        self.assertTrue(evaluate_condition({"any": [no, yes]}))
        self.assertFalse(evaluate_condition({"any": [no, no]}))

    def test_unknown_operator_is_reported(self):
        with self.assertRaises(ConditionError):
            evaluate_condition({"left": 1, "op": "approximately", "right": 1})

    def test_incomparable_types(self):
        with self.assertRaises(ConditionError):
            evaluate_condition({"left": {"a": 1}, "op": "gt", "right": 3})


class TriageExampleTests(unittest.IsolatedAsyncioTestCase):
    async def test_urgent_path(self):
        graph = Graph.from_file(EXAMPLES / "triage.json")
        result = await WorkflowEngine(graph).run({"severity": 9})

        self.assertIs(result.status, RunStatus.COMPLETED)
        self.assertEqual(result.branch_of("route"), "urgent")
        self.assertEqual(result.skipped, ["queue_ticket"])
        self.assertEqual(
            result.outputs_of("notify")["message"],
            "severity 9 routed to urgent, owner on-call-1",
        )

    async def test_normal_path_uses_the_fallback_for_the_skipped_node(self):
        graph = Graph.from_file(EXAMPLES / "triage.json")
        result = await WorkflowEngine(graph).run({"severity": 2})

        self.assertEqual(result.branch_of("route"), "normal")
        self.assertEqual(result.skipped, ["page_oncall"])
        self.assertEqual(
            result.outputs_of("notify")["message"],
            "severity 2 routed to normal, owner unassigned",
        )

    async def test_missing_input_falls_back_to_the_declared_default(self):
        graph = Graph.from_file(EXAMPLES / "triage.json")
        result = await WorkflowEngine(graph).run()

        self.assertIs(result.status, RunStatus.COMPLETED)
        self.assertEqual(result.outputs_of("classify")["severity"], 3)


if __name__ == "__main__":
    unittest.main()
