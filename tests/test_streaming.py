import asyncio
import json
import unittest

from flowforge import Edge, Event, EventStream, Graph, NodeSpec, RunStatus, WorkflowEngine
from flowforge.events import (
    NODE_COMPLETED,
    NODE_DELTA,
    NODE_FAILED,
    NODE_SKIPPED,
    NODE_STARTED,
    RUN_COMPLETED,
    RUN_FAILED,
    RUN_STARTED,
)


def linear_graph():
    return Graph(
        [
            NodeSpec("start", "start"),
            NodeSpec("work", "stub", {"delay": 0.01, "output": {"v": 1}}),
            NodeSpec("end", "end"),
        ],
        [Edge("start", "work"), Edge("work", "end")],
        graph_id="linear",
    )


class EventTests(unittest.TestCase):
    def test_payload_cannot_shadow_the_envelope(self):
        event = Event(type="node.started", seq=1, run_id="r", data={"type": "stub"})
        self.assertEqual(event.as_dict()["type"], "node.started")

    def test_sse_frame_shape(self):
        event = Event(type="node.completed", seq=7, run_id="abc", data={"node": "a"})
        frame = event.to_sse()

        self.assertTrue(frame.endswith("\n\n"))
        lines = frame.strip().split("\n")
        self.assertEqual(lines[0], "id: 7")
        self.assertEqual(lines[1], "event: node.completed")
        payload = json.loads(lines[2].removeprefix("data: "))
        self.assertEqual(payload["node"], "a")
        self.assertEqual(payload["run"], "abc")
        self.assertEqual(payload["seq"], 7)

    def test_terminal_flag(self):
        self.assertTrue(Event(RUN_COMPLETED, 1, "r").is_terminal)
        self.assertTrue(Event(RUN_FAILED, 1, "r").is_terminal)
        self.assertFalse(Event(NODE_STARTED, 1, "r").is_terminal)


class EventStreamTests(unittest.IsolatedAsyncioTestCase):
    async def test_producer_consumer_round_trip(self):
        stream = EventStream(run_id="r1")

        async def produce():
            await stream.emit("a", i=1)
            await stream.emit("b", i=2)
            await stream.close()

        asyncio.create_task(produce())
        received = [event async for event in stream]

        self.assertEqual([e.type for e in received], ["a", "b"])
        self.assertEqual([e.seq for e in received], [1, 2])
        self.assertTrue(all(e.run_id == "r1" for e in received))

    async def test_emit_after_close_is_ignored(self):
        stream = EventStream()
        await stream.close()
        self.assertIsNone(await stream.emit("late"))
        self.assertTrue(stream.closed)

    async def test_bounded_queue_blocks_the_producer(self):
        stream = EventStream(maxsize=1)
        await stream.emit("first")

        second = asyncio.create_task(stream.emit("second"))
        await asyncio.sleep(0)
        self.assertFalse(second.done())  # back-pressure: producer is parked

        await stream.__anext__()  # consumer drains one
        await second
        self.assertTrue(second.done())


class EngineStreamTests(unittest.IsolatedAsyncioTestCase):
    async def test_event_order_and_sequence(self):
        events = [e async for e in WorkflowEngine(linear_graph()).stream()]

        self.assertEqual(events[0].type, RUN_STARTED)
        self.assertEqual(events[-1].type, RUN_COMPLETED)
        self.assertEqual([e.seq for e in events], list(range(1, len(events) + 1)))
        self.assertEqual(
            [(e.type, e.node_id) for e in events[1:-1]],
            [
                (NODE_STARTED, "start"),
                (NODE_COMPLETED, "start"),
                (NODE_STARTED, "work"),
                (NODE_COMPLETED, "work"),
                (NODE_STARTED, "end"),
                (NODE_COMPLETED, "end"),
            ],
        )

    async def test_terminal_event_carries_stats_and_outputs(self):
        events = [e async for e in WorkflowEngine(linear_graph()).stream()]
        final = events[-1]

        self.assertEqual(final.data["status"], RunStatus.COMPLETED.value)
        self.assertEqual(final.data["stats"]["nodes_executed"], 3)
        self.assertIn("end", final.data["outputs"])

    async def test_run_id_is_shared_by_every_event(self):
        events = [e async for e in WorkflowEngine(linear_graph()).stream(run_id="fixed")]
        self.assertTrue(all(e.run_id == "fixed" for e in events))

    async def test_failure_stream_ends_with_run_failed(self):
        graph = Graph(
            [NodeSpec("boom", "stub", {"fail": "nope"})],
            [],
        )
        events = [e async for e in WorkflowEngine(graph).stream()]

        self.assertEqual(events[-1].type, RUN_FAILED)
        self.assertEqual(events[-2].type, NODE_FAILED)
        self.assertIn("nope", events[-2].data["error"])

    async def test_skipped_node_is_announced(self):
        graph = Graph(
            [
                NodeSpec(
                    "route",
                    "decision",
                    {"cases": [{"branch": "yes", "when": {"left": True}}], "default": "no"},
                ),
                NodeSpec("yes_side", "stub"),
                NodeSpec("no_side", "stub"),
            ],
            [Edge("route", "yes_side", branch="yes"), Edge("route", "no_side", branch="no")],
        )
        events = [e async for e in WorkflowEngine(graph).stream()]

        skipped = [e for e in events if e.type == NODE_SKIPPED]
        self.assertEqual([e.node_id for e in skipped], ["no_side"])

    async def test_node_deltas_arrive_before_the_node_completes(self):
        graph = Graph(
            [NodeSpec("say", "llm", {"prompt": "one two three four five six"})],
            [],
        )
        events = [e async for e in WorkflowEngine(graph).stream()]
        types = [e.type for e in events]

        deltas = [e for e in events if e.type == NODE_DELTA]
        self.assertEqual(len(deltas), 2)  # six words, three per chunk
        self.assertLess(types.index(NODE_DELTA), types.index(NODE_COMPLETED))
        self.assertEqual("".join(d.data["text"] for d in deltas), "one two three four five six")

    async def test_slow_consumer_does_not_lose_events(self):
        graph = Graph(
            [NodeSpec("say", "llm", {"prompt": " ".join(str(i) for i in range(30))})],
            [],
        )
        seen = []
        async for event in WorkflowEngine(graph).stream(maxsize=2):
            await asyncio.sleep(0)  # consumer deliberately yields on every event
            seen.append(event)

        self.assertEqual(seen[-1].type, RUN_COMPLETED)
        self.assertEqual([e.seq for e in seen], list(range(1, len(seen) + 1)))

    async def test_abandoning_the_stream_cancels_the_run(self):
        graph = Graph(
            [NodeSpec("slow", "stub", {"delay": 5}), NodeSpec("next", "stub")],
            [Edge("slow", "next")],
        )
        agen = WorkflowEngine(graph).stream()
        first = await agen.__anext__()
        self.assertEqual(first.type, RUN_STARTED)

        await agen.aclose()  # walk away mid-run; the driving task must not leak
        await asyncio.sleep(0)
        self.assertEqual(
            [t for t in asyncio.all_tasks() if t is not asyncio.current_task()], []
        )


if __name__ == "__main__":
    unittest.main()
