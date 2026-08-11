"""What happens after a node has failed for the last time.

Three strategies, matching the reference implementation's set: stop the run,
substitute an output, or route down a failure edge.
"""

import unittest

from flowforge import (
    Edge,
    ErrorStrategy,
    Graph,
    GraphError,
    NodeSpec,
    NodeStatus,
    RetryPolicy,
    RunStatus,
    WorkflowEngine,
)
from flowforge.events import NODE_COMPLETED, NODE_FAILED


def boom(node_id="boom", **policy):
    return NodeSpec(
        node_id, "stub", {"fail": "downstream is down"}, retry=RetryPolicy(**policy)
    )


class ParsingTests(unittest.TestCase):
    def test_default_strategy_is_fail(self):
        graph = Graph.from_dict({"nodes": [{"id": "a", "type": "stub"}]})
        self.assertIs(graph.nodes["a"].retry.on_error, ErrorStrategy.FAIL)

    def test_strategies_parse_from_json(self):
        graph = Graph.from_dict(
            {
                "nodes": [
                    {
                        "id": "a",
                        "type": "stub",
                        "on_error": "default",
                        "error_output": {"score": 0},
                    },
                    {"id": "b", "type": "stub", "on_error": "branch",
                     "error_branch": "oops"},
                ]
            }
        )
        self.assertIs(graph.nodes["a"].retry.on_error, ErrorStrategy.DEFAULT)
        self.assertEqual(graph.nodes["a"].retry.error_output, {"score": 0})
        self.assertEqual(graph.nodes["b"].retry.error_branch, "oops")

    def test_unknown_strategy_is_rejected(self):
        with self.assertRaises(GraphError) as ctx:
            Graph.from_dict({"nodes": [{"id": "a", "type": "stub", "on_error": "shrug"}]})
        self.assertIn("shrug", str(ctx.exception))

    def test_error_output_must_be_an_object(self):
        with self.assertRaises(GraphError):
            Graph.from_dict(
                {"nodes": [{"id": "a", "type": "stub", "error_output": [1, 2]}]}
            )


class FailStrategyTests(unittest.IsolatedAsyncioTestCase):
    async def test_failure_stops_the_run_by_default(self):
        graph = Graph([boom(), NodeSpec("after", "stub")], [Edge("boom", "after")])
        result = await WorkflowEngine(graph).run()

        self.assertIs(result.status, RunStatus.FAILED)
        self.assertIs(result.status_of("after"), NodeStatus.PENDING)
        self.assertFalse(result.nodes["boom"].recovered)


class DefaultStrategyTests(unittest.IsolatedAsyncioTestCase):
    async def test_substituted_output_lets_the_run_continue(self):
        graph = Graph(
            [
                boom(on_error=ErrorStrategy.DEFAULT, error_output={"score": 0}),
                NodeSpec("after", "stub", {"output": {"used": "{{boom.score}}"}}),
            ],
            [Edge("boom", "after")],
        )
        result = await WorkflowEngine(graph).run()

        self.assertIs(result.status, RunStatus.COMPLETED)
        self.assertIs(result.status_of("after"), NodeStatus.COMPLETED)
        self.assertEqual(result.outputs_of("after")["used"], 0)

    async def test_the_failure_is_still_reported_not_hidden(self):
        graph = Graph([boom(on_error=ErrorStrategy.DEFAULT, error_output={"score": 0})], [])
        result = await WorkflowEngine(graph).run()

        record = result.nodes["boom"]
        self.assertTrue(record.recovered)
        self.assertIn("downstream is down", record.outputs["error"])
        self.assertEqual(result.stats.nodes_recovered, 1)

    async def test_retries_are_spent_before_the_fallback(self):
        graph = Graph(
            [
                boom(
                    attempts=3,
                    backoff_s=0,
                    on_error=ErrorStrategy.DEFAULT,
                    error_output={"score": 0},
                )
            ],
            [],
        )
        result = await WorkflowEngine(graph).run()

        self.assertIs(result.status, RunStatus.COMPLETED)
        self.assertEqual(result.nodes["boom"].attempts, 3)

    async def test_completed_event_marks_it_recovered(self):
        graph = Graph([boom(on_error=ErrorStrategy.DEFAULT)], [])
        events = [e async for e in WorkflowEngine(graph).stream()]
        completed = [e for e in events if e.type == NODE_COMPLETED]
        self.assertTrue(completed[0].data["recovered"])


class BranchStrategyTests(unittest.IsolatedAsyncioTestCase):
    def graph(self, **policy):
        return Graph(
            [
                boom(on_error=ErrorStrategy.BRANCH, **policy),
                NodeSpec("happy", "stub"),
                NodeSpec("handler", "stub", {"output": {"saw": "{{boom.error}}"}}),
                NodeSpec("join", "stub"),
            ],
            [
                Edge("boom", "happy", branch="ok"),
                Edge("boom", "handler", branch="error"),
                Edge("happy", "join"),
                Edge("handler", "join"),
            ],
        )

    async def test_failure_routes_down_the_error_edge(self):
        result = await WorkflowEngine(self.graph()).run()

        self.assertIs(result.status, RunStatus.COMPLETED)
        self.assertIs(result.status_of("handler"), NodeStatus.COMPLETED)
        self.assertIs(result.status_of("happy"), NodeStatus.SKIPPED)
        self.assertIs(result.status_of("join"), NodeStatus.COMPLETED)

    async def test_the_node_still_reads_as_failed(self):
        result = await WorkflowEngine(self.graph()).run()
        record = result.nodes["boom"]

        self.assertIs(record.status, NodeStatus.FAILED)
        self.assertTrue(record.recovered)
        self.assertEqual(record.branch, "error")

    async def test_the_handler_can_read_the_error(self):
        result = await WorkflowEngine(self.graph()).run()
        self.assertIn("downstream is down", result.outputs_of("handler")["saw"])

    async def test_custom_error_branch_label(self):
        graph = Graph(
            [
                boom(on_error=ErrorStrategy.BRANCH, error_branch="oops"),
                NodeSpec("handler", "stub"),
            ],
            [Edge("boom", "handler", branch="oops")],
        )
        result = await WorkflowEngine(graph).run()
        self.assertIs(result.status, RunStatus.COMPLETED)

    async def test_missing_error_edge_falls_back_to_failing(self):
        graph = Graph(
            [boom(on_error=ErrorStrategy.BRANCH), NodeSpec("after", "stub")],
            [Edge("boom", "after")],  # unlabelled: there is no error branch
        )
        result = await WorkflowEngine(graph).run()

        self.assertIs(result.status, RunStatus.FAILED)
        self.assertIn("no outgoing edge labelled 'error'", result.nodes["boom"].error)

    async def test_a_node_that_succeeds_never_takes_its_error_edge(self):
        """Regression: having an error edge must not mean always taking it.

        The node completes, selects no branch, and the error path has to stay
        unvisited — otherwise every successful run also fires its own handler.
        """
        graph = Graph(
            [
                NodeSpec(
                    "work",
                    "stub",
                    {"output": {"v": 1}},
                    retry=RetryPolicy(on_error=ErrorStrategy.BRANCH),
                ),
                NodeSpec("happy", "stub"),
                NodeSpec("handler", "stub"),
            ],
            [Edge("work", "happy", branch="ok"), Edge("work", "handler", branch="error")],
        )
        result = await WorkflowEngine(graph).run()

        self.assertIs(result.status, RunStatus.COMPLETED)
        self.assertIs(result.status_of("happy"), NodeStatus.COMPLETED)
        self.assertIs(result.status_of("handler"), NodeStatus.SKIPPED)

    async def test_a_failure_does_not_also_take_the_normal_edges(self):
        """The mirror case: routing to the handler skips the normal continuation."""
        graph = Graph(
            [
                boom(on_error=ErrorStrategy.BRANCH),
                NodeSpec("normal", "stub"),
                NodeSpec("handler", "stub"),
            ],
            [Edge("boom", "normal"), Edge("boom", "handler", branch="error")],
        )
        result = await WorkflowEngine(graph).run()

        self.assertIs(result.status, RunStatus.COMPLETED)
        self.assertIs(result.status_of("handler"), NodeStatus.COMPLETED)
        self.assertIs(result.status_of("normal"), NodeStatus.SKIPPED)

    async def test_failed_event_carries_the_branch(self):
        events = [e async for e in WorkflowEngine(self.graph()).stream()]
        failed = [e for e in events if e.type == NODE_FAILED]

        self.assertEqual(failed[0].data["branch"], "error")
        self.assertTrue(failed[0].data["recovered"])


if __name__ == "__main__":
    unittest.main()
