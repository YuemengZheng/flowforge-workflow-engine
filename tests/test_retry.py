import asyncio
import unittest

from flowforge import (
    Edge,
    Graph,
    Node,
    NodeRegistry,
    NodeSpec,
    NodeStatus,
    RetryPolicy,
    RunStatus,
    WorkflowEngine,
)
from flowforge.events import NODE_RETRY


class PolicyTests(unittest.TestCase):
    def test_defaults_mean_one_try_no_timeout(self):
        policy = RetryPolicy()
        self.assertEqual(policy.attempts, 1)
        self.assertEqual(policy.retries, 0)
        self.assertIsNone(policy.timeout_s)
        self.assertTrue(policy.is_default)

    def test_backoff_is_exponential_and_capped(self):
        policy = RetryPolicy(attempts=9, backoff_s=1, backoff_multiplier=2, max_backoff_s=8)
        self.assertEqual(policy.delay_before(1), 0.0)
        self.assertEqual(policy.delay_before(2), 1)
        self.assertEqual(policy.delay_before(3), 2)
        self.assertEqual(policy.delay_before(4), 4)
        self.assertEqual(policy.delay_before(9), 8)  # capped

    def test_invalid_values_are_rejected(self):
        for kwargs in (
            {"attempts": 0},
            {"timeout_s": 0},
            {"backoff_s": -1},
            {"backoff_multiplier": 0.5},
        ):
            with self.subTest(**kwargs), self.assertRaises(ValueError):
                RetryPolicy(**kwargs)

    def test_parsed_from_node_json(self):
        graph = Graph.from_dict(
            {
                "nodes": [
                    {"id": "a", "type": "stub", "timeout": 2.5, "retries": 3},
                    {"id": "b", "type": "stub"},
                ]
            }
        )
        self.assertEqual(graph.nodes["a"].retry.attempts, 4)
        self.assertEqual(graph.nodes["a"].retry.timeout_s, 2.5)
        self.assertTrue(graph.nodes["b"].retry.is_default)

    def test_bad_retry_value_is_a_graph_error(self):
        from flowforge import GraphError

        with self.assertRaises(GraphError):
            Graph.from_dict({"nodes": [{"id": "a", "type": "stub", "timeout": -1}]})


def flaky_registry(failures_before_success, delay=0.0):
    """A registry with one node type that fails N times, then succeeds."""
    registry = NodeRegistry()
    state = {"calls": 0}

    @registry.register("flaky")
    class FlakyNode(Node):
        async def run(self, ctx):
            state["calls"] += 1
            if delay:
                await asyncio.sleep(delay)
            if state["calls"] <= failures_before_success:
                raise RuntimeError(f"transient failure {state['calls']}")
            return {"calls": state["calls"], "attempt": ctx.attempt}

    return registry, state


class RetryExecutionTests(unittest.IsolatedAsyncioTestCase):
    async def test_node_recovers_within_its_attempt_budget(self):
        registry, state = flaky_registry(failures_before_success=2)
        graph = Graph(
            [NodeSpec("f", "flaky", retry=RetryPolicy(attempts=3, backoff_s=0))], []
        )
        result = await WorkflowEngine(graph, node_registry=registry).run()

        self.assertIs(result.status, RunStatus.COMPLETED)
        self.assertEqual(state["calls"], 3)
        self.assertEqual(result.nodes["f"].attempts, 3)
        self.assertEqual(result.outputs_of("f")["attempt"], 3)
        self.assertIsNone(result.nodes["f"].error)

    async def test_exhausting_attempts_fails_with_the_last_error(self):
        registry, state = flaky_registry(failures_before_success=99)
        graph = Graph(
            [NodeSpec("f", "flaky", retry=RetryPolicy(attempts=2, backoff_s=0))], []
        )
        result = await WorkflowEngine(graph, node_registry=registry).run()

        self.assertIs(result.status, RunStatus.FAILED)
        self.assertEqual(state["calls"], 2)
        self.assertEqual(result.nodes["f"].attempts, 2)
        self.assertIn("transient failure 2", result.nodes["f"].error)

    async def test_no_retry_by_default(self):
        registry, state = flaky_registry(failures_before_success=1)
        result = await WorkflowEngine(
            Graph([NodeSpec("f", "flaky")], []), node_registry=registry
        ).run()

        self.assertIs(result.status, RunStatus.FAILED)
        self.assertEqual(state["calls"], 1)

    async def test_timeout_bounds_each_attempt(self):
        registry, state = flaky_registry(failures_before_success=0, delay=0.2)
        graph = Graph(
            [
                NodeSpec(
                    "f",
                    "flaky",
                    retry=RetryPolicy(attempts=2, timeout_s=0.02, backoff_s=0),
                )
            ],
            [],
        )
        result = await WorkflowEngine(graph, node_registry=registry).run()

        self.assertIs(result.status, RunStatus.FAILED)
        self.assertIn("TimeoutError", result.nodes["f"].error)
        self.assertEqual(state["calls"], 2)  # timed out, retried, timed out again
        self.assertLess(result.stats.total_ms, 200)  # never waited the full 0.2s twice

    async def test_a_slow_node_that_beats_its_timeout_succeeds(self):
        registry, _ = flaky_registry(failures_before_success=0, delay=0.01)
        graph = Graph([NodeSpec("f", "flaky", retry=RetryPolicy(timeout_s=1))], [])
        result = await WorkflowEngine(graph, node_registry=registry).run()

        self.assertIs(result.status, RunStatus.COMPLETED)

    async def test_backoff_is_actually_waited(self):
        registry, _ = flaky_registry(failures_before_success=1)
        graph = Graph(
            [NodeSpec("f", "flaky", retry=RetryPolicy(attempts=2, backoff_s=0.05))], []
        )
        result = await WorkflowEngine(graph, node_registry=registry).run()

        self.assertIs(result.status, RunStatus.COMPLETED)
        self.assertGreaterEqual(result.stats.total_ms, 50)

    async def test_retries_are_streamed_as_events(self):
        registry, _ = flaky_registry(failures_before_success=2)
        graph = Graph(
            [NodeSpec("f", "flaky", retry=RetryPolicy(attempts=3, backoff_s=0))], []
        )
        engine = WorkflowEngine(graph, node_registry=registry)
        events = [e async for e in engine.stream()]

        retries = [e for e in events if e.type == NODE_RETRY]
        self.assertEqual([e.data["attempt"] for e in retries], [1, 2])
        self.assertIn("transient failure 1", retries[0].data["error"])

    async def test_a_retrying_node_does_not_block_its_siblings(self):
        registry, _ = flaky_registry(failures_before_success=1)

        @registry.register("quick")
        class QuickNode(Node):
            async def run(self, ctx):
                return {"ok": True}

        graph = Graph(
            [
                NodeSpec("root", "quick"),
                NodeSpec("f", "flaky", retry=RetryPolicy(attempts=2, backoff_s=0.05)),
                NodeSpec("sibling", "quick"),
            ],
            [Edge("root", "f"), Edge("root", "sibling")],
        )
        result = await WorkflowEngine(graph, node_registry=registry).run()

        self.assertIs(result.status, RunStatus.COMPLETED)
        self.assertIs(result.status_of("sibling"), NodeStatus.COMPLETED)
        self.assertEqual(result.nodes["sibling"].wave, result.nodes["f"].wave)


if __name__ == "__main__":
    unittest.main()
