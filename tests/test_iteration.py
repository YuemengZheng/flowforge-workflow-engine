"""Iterating a sub-workflow: isolation, ordering, concurrency, failures."""

import asyncio
import unittest

from flowforge import Edge, Graph, IterationError, NodeSpec, RunStatus, WorkflowEngine
from flowforge.events import NODE_DELTA


def sub_workflow(delay=0.0):
    """A one-node sub-graph that echoes the item it was handed."""
    return {
        "id": "per_item",
        "nodes": [
            {
                "id": "work",
                "type": "stub",
                "config": {
                    "delay": delay,
                    "output": {"seen": "{{inputs.item}}", "at": "{{inputs.index}}"},
                },
            }
        ],
        "edges": [],
    }


def iterate_graph(items, **config):
    return Graph(
        [
            NodeSpec(
                "loop",
                "iterate",
                {"items": items, "workflow": sub_workflow(config.pop("delay", 0.0)), **config},
            )
        ],
        [],
    )


class IterationTests(unittest.IsolatedAsyncioTestCase):
    async def test_one_sub_run_per_item(self):
        result = await WorkflowEngine(iterate_graph(["a", "b", "c"])).run()

        self.assertIs(result.status, RunStatus.COMPLETED)
        outputs = result.outputs_of("loop")
        self.assertEqual(outputs["count"], 3)
        self.assertEqual([r["seen"] for r in outputs["results"]], ["a", "b", "c"])

    async def test_results_keep_input_order_despite_concurrent_execution(self):
        # Later items finish first; results must still come back in input order.
        graph = Graph(
            [
                NodeSpec(
                    "loop",
                    "iterate",
                    {
                        "items": [0.03, 0.02, 0.01],
                        "workflow": {
                            "nodes": [
                                {
                                    "id": "sleep",
                                    "type": "stub",
                                    "config": {
                                        "delay": "{{inputs.item}}",
                                        "output": {"slept": "{{inputs.item}}"},
                                    },
                                }
                            ]
                        },
                    },
                )
            ],
            [],
        )
        result = await WorkflowEngine(graph).run()
        self.assertEqual(
            [r["slept"] for r in result.outputs_of("loop")["results"]], [0.03, 0.02, 0.01]
        )

    async def test_items_run_concurrently(self):
        # 6 items x 30ms. Sequential would need >= 180ms.
        result = await WorkflowEngine(
            iterate_graph([1, 2, 3, 4, 5, 6], delay=0.03)
        ).run()

        self.assertIs(result.status, RunStatus.COMPLETED)
        self.assertLess(result.stats.total_ms, 120)

    async def test_max_concurrency_serialises_the_batch(self):
        result = await WorkflowEngine(
            iterate_graph([1, 2, 3, 4], delay=0.02, max_concurrency=1)
        ).run()

        self.assertIs(result.status, RunStatus.COMPLETED)
        self.assertGreaterEqual(result.stats.total_ms, 80)

    async def test_iterations_cannot_see_each_other(self):
        """Each item gets its own pool, so nothing leaks between them."""
        result = await WorkflowEngine(iterate_graph(["x", "y"])).run()
        results = result.outputs_of("loop")["results"]

        self.assertEqual(results[0]["seen"], "x")
        self.assertEqual(results[1]["seen"], "y")
        self.assertEqual([r["at"] for r in results], [0, 1])

    async def test_parent_variables_are_not_visible_inside(self):
        graph = Graph(
            [
                NodeSpec("outer", "stub", {"output": {"secret": "parent-only"}}),
                NodeSpec(
                    "loop",
                    "iterate",
                    {
                        "items": [1],
                        "workflow": {
                            "nodes": [
                                {
                                    "id": "peek",
                                    "type": "stub",
                                    "config": {
                                        "output": {"got": "{{outer.secret ?? \"unset\"}}"}
                                    },
                                }
                            ]
                        },
                    },
                ),
            ],
            [Edge("outer", "loop")],
        )
        result = await WorkflowEngine(graph).run()

        self.assertIs(result.status, RunStatus.COMPLETED)
        self.assertEqual(result.outputs_of("loop")["results"][0]["got"], "unset")

    async def test_collect_narrows_the_output(self):
        result = await WorkflowEngine(
            iterate_graph(["a", "b"], collect="seen")
        ).run()
        self.assertEqual(result.outputs_of("loop")["results"], ["a", "b"])

    async def test_collect_a_list_of_fields(self):
        result = await WorkflowEngine(
            iterate_graph(["a"], collect=["seen", "missing"])
        ).run()
        self.assertEqual(
            result.outputs_of("loop")["results"][0], {"seen": "a", "missing": None}
        )

    async def test_item_name_is_configurable(self):
        graph = Graph(
            [
                NodeSpec(
                    "loop",
                    "iterate",
                    {
                        "items": ["r1"],
                        "item_as": "row",
                        "workflow": {
                            "nodes": [
                                {
                                    "id": "w",
                                    "type": "stub",
                                    "config": {"output": {"v": "{{inputs.row}}"}},
                                }
                            ]
                        },
                    },
                )
            ],
            [],
        )
        result = await WorkflowEngine(graph).run()
        self.assertEqual(result.outputs_of("loop")["results"][0]["v"], "r1")

    async def test_items_can_come_from_upstream(self):
        graph = Graph(
            [
                NodeSpec("fetch", "stub", {"output": {"rows": [10, 20]}}),
                NodeSpec(
                    "loop", "iterate", {"items": "{{fetch.rows}}", "workflow": sub_workflow()}
                ),
            ],
            [Edge("fetch", "loop")],
        )
        result = await WorkflowEngine(graph).run()
        self.assertEqual(result.outputs_of("loop")["count"], 2)

    async def test_empty_list_is_not_an_error(self):
        result = await WorkflowEngine(iterate_graph([])).run()
        self.assertIs(result.status, RunStatus.COMPLETED)
        self.assertEqual(result.outputs_of("loop"), {"results": [], "count": 0, "failed": []})

    async def test_progress_is_streamed_per_item(self):
        engine = WorkflowEngine(iterate_graph(["a", "b", "c"]))
        events = [e async for e in engine.stream()]
        deltas = [e for e in events if e.type == NODE_DELTA]

        self.assertEqual(len(deltas), 3)
        self.assertEqual(deltas[-1].data["done"], 3)
        self.assertEqual(deltas[-1].data["total"], 3)


class IterationFailureTests(unittest.IsolatedAsyncioTestCase):
    def failing_graph(self, **config):
        return Graph(
            [
                NodeSpec(
                    "loop",
                    "iterate",
                    {
                        "items": [1, 2, 3],
                        "workflow": {
                            "nodes": [
                                {
                                    "id": "maybe",
                                    "type": "stub",
                                    "config": {
                                        "fail": "{{inputs.item}}",
                                        "output": {"v": "{{inputs.item}}"},
                                    },
                                }
                            ]
                        },
                        **config,
                    },
                )
            ],
            [],
        )

    async def test_an_item_failure_fails_the_node_by_default(self):
        result = await WorkflowEngine(self.failing_graph()).run()

        self.assertIs(result.status, RunStatus.FAILED)
        self.assertIn("item", result.nodes["loop"].error)

    async def test_skip_keeps_going_and_reports_which_items_failed(self):
        result = await WorkflowEngine(self.failing_graph(on_item_error="skip")).run()

        self.assertIs(result.status, RunStatus.COMPLETED)
        outputs = result.outputs_of("loop")
        self.assertEqual(outputs["count"], 0)
        self.assertEqual(outputs["failed"], [0, 1, 2])

    async def test_non_list_items_is_a_clear_error(self):
        result = await WorkflowEngine(
            Graph([NodeSpec("loop", "iterate", {"items": 5, "workflow": sub_workflow()})], [])
        ).run()
        self.assertIn("must resolve to a list", result.nodes["loop"].error)

    async def test_missing_workflow_is_a_clear_error(self):
        result = await WorkflowEngine(
            Graph([NodeSpec("loop", "iterate", {"items": [1]})], [])
        ).run()
        self.assertIn("nested graph", result.nodes["loop"].error)

    async def test_invalid_sub_workflow_is_reported_against_the_node(self):
        result = await WorkflowEngine(
            Graph(
                [
                    NodeSpec(
                        "loop",
                        "iterate",
                        {
                            "items": [1],
                            "workflow": {
                                "nodes": [
                                    {"id": "a", "type": "stub"},
                                    {"id": "b", "type": "stub"},
                                ],
                                "edges": [
                                    {"source": "a", "target": "b"},
                                    {"source": "b", "target": "a"},
                                ],
                            },
                        },
                    )
                ],
                [],
            )
        ).run()
        self.assertIn("cycle detected", result.nodes["loop"].error)

    async def test_a_nested_pause_is_refused_with_a_useful_message(self):
        result = await WorkflowEngine(
            Graph(
                [
                    NodeSpec(
                        "loop",
                        "iterate",
                        {
                            "items": [1],
                            "workflow": {
                                "nodes": [
                                    {"id": "ask", "type": "await_input",
                                     "config": {"prompt": "?"}}
                                ]
                            },
                        },
                    )
                ],
                [],
            )
        ).run()

        self.assertIs(result.status, RunStatus.FAILED)
        self.assertIn("cannot ask for input", result.nodes["loop"].error)

    async def test_iteration_failure_composes_with_the_error_strategy(self):
        """The node-level strategies apply to an iterate node like any other."""
        graph = Graph.from_dict(
            {
                "nodes": [
                    {
                        "id": "loop",
                        "type": "iterate",
                        "on_error": "default",
                        "error_output": {"results": [], "count": 0},
                        "config": {
                            "items": [1],
                            "workflow": {
                                "nodes": [
                                    {"id": "x", "type": "stub", "config": {"fail": "no"}}
                                ]
                            },
                        },
                    }
                ]
            }
        )
        result = await WorkflowEngine(graph).run()

        self.assertIs(result.status, RunStatus.COMPLETED)
        self.assertTrue(result.nodes["loop"].recovered)
        self.assertEqual(result.outputs_of("loop")["count"], 0)


if __name__ == "__main__":
    unittest.main()
