"""The Kafka client: framing offline, and the whole thing against a real broker.

Three layers, because they answer different questions:

* **Framing** — CRC32C against its published check value, varints, and a
  RecordBatch v2 round trip. These need no broker and would catch an encoding
  regression instantly.
* **The sink** — what happens to a run when the broker is down, which is the
  behaviour that matters most and is best tested with a broker that misbehaves on
  purpose.
* **A real broker** — the only thing that can confirm the wire format is right.
  Skipped unless ``FLOWFORGE_TEST_KAFKA=host:port`` is set:

      docker run -d --rm --name ff-kafka -p 9492:9092 \
        -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
        -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
        -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
        -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:9492 \
        -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
        -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
        -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 apache/kafka:3.9.0
      FLOWFORGE_TEST_KAFKA=127.0.0.1:9492 python3 -m unittest discover -s tests -t .
"""

import asyncio
import json
import os
import unittest
import uuid

from flowforge import Graph, WorkflowEngine
from flowforge.events import Event
from flowforge.kafka import (
    FETCH,
    METADATA,
    PRODUCE,
    SUPPORTED,
    KafkaClient,
    KafkaError,
    KafkaEventSink,
    KafkaProtocolError,
    Record,
    crc32c,
    decode_record_batches,
    decode_varint,
    encode_record_batch,
    encode_varint,
    read_events,
)

REAL_KAFKA = os.environ.get("FLOWFORGE_TEST_KAFKA")


class CRC32CTests(unittest.TestCase):
    def test_the_published_check_value(self):
        # 0xE3069283 for "123456789" is the standard CRC-32C check value. This is
        # the one assertion here that is not self-referential: it comes from the
        # specification, not from this implementation.
        self.assertEqual(crc32c(b"123456789"), 0xE3069283)

    def test_it_is_not_the_zlib_polynomial(self):
        import zlib

        self.assertNotEqual(crc32c(b"123456789"), zlib.crc32(b"123456789"))

    def test_empty_input(self):
        self.assertEqual(crc32c(b""), 0)


class VarintTests(unittest.TestCase):
    def test_round_trip_over_awkward_values(self):
        for value in (0, -1, 1, -2, 2, 63, 64, -64, -65, 300, -300, 2**31, -(2**31), 2**62):
            with self.subTest(value=value):
                encoded = encode_varint(value)
                decoded, offset = decode_varint(encoded, 0)
                self.assertEqual(decoded, value)
                self.assertEqual(offset, len(encoded))

    def test_zigzag_keeps_small_negatives_small(self):
        # Without zigzag, -1 would be a 10-byte two's-complement varint. With it,
        # the one-byte range is -64..63 (zigzag(-64) == 127, the last byte value).
        self.assertEqual(len(encode_varint(-1)), 1)
        self.assertEqual(len(encode_varint(-64)), 1)
        self.assertEqual(len(encode_varint(63)), 1)
        self.assertEqual(len(encode_varint(-65)), 2)
        self.assertEqual(len(encode_varint(64)), 2)

    def test_a_runaway_varint_is_rejected(self):
        with self.assertRaises(KafkaProtocolError):
            decode_varint(b"\xff" * 12, 0)


class RecordBatchTests(unittest.TestCase):
    def test_round_trip(self):
        records = [
            Record(value=b"first", key=b"run1", timestamp=1_700_000_000_000),
            Record(value=b"second", key=None, timestamp=1_700_000_000_005),
            Record(value=b"third", key=b"run1", timestamp=1_700_000_000_010),
        ]
        decoded = decode_record_batches(encode_record_batch(records))

        self.assertEqual([r.value for r in decoded], [b"first", b"second", b"third"])
        self.assertEqual([r.key for r in decoded], [b"run1", None, b"run1"])
        self.assertEqual([r.offset for r in decoded], [0, 1, 2])
        self.assertEqual([r.timestamp for r in decoded], [t.timestamp for t in records])

    def test_headers_round_trip(self):
        record = Record(value=b"x", headers={"event-type": b"run.started", "n": b"1"})
        decoded = decode_record_batches(encode_record_batch([record]))
        self.assertEqual(decoded[0].headers, {"event-type": b"run.started", "n": b"1"})

    def test_an_empty_batch_is_refused(self):
        with self.assertRaises(KafkaError):
            encode_record_batch([])

    def test_a_corrupted_batch_fails_its_crc(self):
        raw = bytearray(encode_record_batch([Record(value=b"tamper me")]))
        raw[-3] ^= 0xFF
        with self.assertRaises(KafkaProtocolError) as raised:
            decode_record_batches(bytes(raw))
        self.assertIn("CRC32C", str(raised.exception))

    def test_a_truncated_trailing_batch_is_ignored_not_fatal(self):
        # A fetch may cut the last batch mid-frame; that is normal.
        whole = encode_record_batch([Record(value=b"complete")])
        decoded = decode_record_batches(whole + whole[:20])
        self.assertEqual([r.value for r in decoded], [b"complete"])

    def test_the_batch_declares_its_own_length_correctly(self):
        import struct

        raw = encode_record_batch([Record(value=b"a"), Record(value=b"bb")])
        (_, batch_length) = struct.unpack(">qi", raw[:12])
        self.assertEqual(batch_length, len(raw) - 12)


class VersionNegotiationTests(unittest.TestCase):
    def test_a_client_never_asks_above_what_it_encodes(self):
        client = KafkaClient()
        client._versions = {PRODUCE: (0, 11), METADATA: (0, 12), FETCH: (0, 17)}

        self.assertEqual(client.version_for(PRODUCE), SUPPORTED[PRODUCE])
        self.assertEqual(client.version_for(METADATA), SUPPORTED[METADATA])
        self.assertEqual(client.version_for(FETCH), SUPPORTED[FETCH])

    def test_an_older_broker_caps_the_version(self):
        client = KafkaClient()
        client._versions = {PRODUCE: (0, 2)}
        self.assertEqual(client.version_for(PRODUCE), 2)

    def test_a_broker_that_dropped_old_versions_is_a_clear_error(self):
        client = KafkaClient()
        client._versions = {PRODUCE: (9, 11)}
        with self.assertRaises(KafkaError) as raised:
            client.version_for(PRODUCE)
        self.assertIn("version >= 9", str(raised.exception))


class FakeClient:
    """A broker stand-in that can be told to fail."""

    def __init__(self, fail: bool = False) -> None:
        self.fail = fail
        self.produced: list[tuple[str, Record]] = []
        self.connects = 0
        self.closed = False

    async def connect(self):
        self.connects += 1
        if self.fail:
            raise KafkaError("broker is down")

    async def ensure_topic(self, topic, attempts=10):
        if self.fail:
            raise KafkaError("broker is down")

    async def produce(self, topic, records, **kwargs):
        if self.fail:
            raise KafkaError("broker is down")
        for record in records:
            self.produced.append((topic, record))
        return len(self.produced) - 1

    async def close(self):
        self.closed = True


def event(event_type="node.completed", run_id="run1", seq=1) -> Event:
    return Event(type=event_type, seq=seq, run_id=run_id, data={"node": "a"})


class SinkTests(unittest.IsolatedAsyncioTestCase):
    """Publishing is batched and off the run's critical path, so every assertion
    here goes through ``flush()`` — the engine's side is only an enqueue."""

    async def drain(self, sink, *events):
        for one in events:
            await sink(one)
        await sink.flush(timeout_s=5)

    async def test_an_event_becomes_a_keyed_record(self):
        client = FakeClient()
        sink = KafkaEventSink(client, topic="t")

        await self.drain(sink, event())

        topic, record = client.produced[0]
        self.assertEqual(topic, "t")
        self.assertEqual(record.key, b"run1")  # keyed so one run stays ordered
        self.assertEqual(record.headers["event-type"], b"node.completed")
        self.assertEqual(json.loads(record.value)["node"], "a")
        self.assertEqual(sink.stats(), {"published": 1, "dropped": 0, "batches": 1})

    async def test_a_broker_that_is_down_does_not_break_the_run(self):
        sink = KafkaEventSink(FakeClient(fail=True), topic="t")

        await sink(event())  # must not raise

        self.assertEqual(sink.stats()["published"], 0)
        self.assertEqual(sink.stats()["dropped"], 1)

    async def test_required_mode_surfaces_the_failure(self):
        sink = KafkaEventSink(FakeClient(fail=True), topic="t", required=True)
        with self.assertRaises(KafkaError):
            await sink(event())

    async def test_required_mode_publishes_synchronously(self):
        # No flush needed: the caller asked to know, so the caller waited.
        client = FakeClient()
        sink = KafkaEventSink(client, topic="t", required=True)

        await sink(event())

        self.assertEqual(len(client.produced), 1)
        self.assertEqual(sink.stats()["published"], 1)

    async def test_many_events_become_few_batches(self):
        client = FakeClient()
        sink = KafkaEventSink(client, topic="t", batch_size=50, linger_ms=5)

        await self.drain(sink, *[event(seq=i) for i in range(120)])

        self.assertEqual(sink.stats()["published"], 120)
        # 120 events must not be 120 round trips; that was a 119x slowdown.
        self.assertLess(sink.stats()["batches"], 20)
        self.assertEqual(len(client.produced), 120)

    async def test_batching_preserves_emit_order(self):
        client = FakeClient()
        sink = KafkaEventSink(client, topic="t", batch_size=10, linger_ms=5)

        await self.drain(sink, *[event(seq=i) for i in range(40)])

        published = [json.loads(record.value)["seq"] for _, record in client.produced]
        self.assertEqual(published, list(range(40)))

    async def test_a_full_queue_drops_instead_of_blocking_the_run(self):
        # A broker that cannot keep up must cost events, not run latency.
        client = FakeClient()
        sink = KafkaEventSink(client, topic="t", max_queued=5, linger_ms=1000)
        await sink.start()

        for index in range(40):
            await sink(event(seq=index))

        self.assertGreater(sink.stats()["dropped"], 0)
        self.assertLessEqual(sink._queue.qsize(), 5)
        await sink.close()

    async def test_close_flushes_what_is_queued(self):
        client = FakeClient()
        sink = KafkaEventSink(client, topic="t", batch_size=100, linger_ms=50)

        for index in range(10):
            await sink(event(seq=index))
        await sink.close()

        self.assertEqual(sink.stats()["published"], 10)
        self.assertTrue(client.closed)

    async def test_a_failure_forces_a_reconnect_next_time(self):
        client = FakeClient(fail=True)
        sink = KafkaEventSink(client, topic="t")

        await sink(event())
        client.fail = False
        await self.drain(sink, event())

        self.assertEqual(sink.stats()["published"], 1)
        self.assertEqual(sink.stats()["dropped"], 1)
        self.assertGreaterEqual(client.connects, 2)

    async def test_types_filter_keeps_the_noisy_events_out(self):
        client = FakeClient()
        sink = KafkaEventSink(client, topic="t", types=["run.started", "run.completed"])

        await self.drain(
            sink, event("run.started"), event("node.delta"), event("run.completed")
        )

        self.assertEqual(
            [r.headers["event-type"] for _, r in client.produced],
            [b"run.started", b"run.completed"],
        )


class EngineSinkTests(unittest.IsolatedAsyncioTestCase):
    """The sink has to see the same events whether or not anyone streamed."""

    def setUp(self):
        self.graph = Graph.from_file("examples/diamond.json")

    async def test_a_non_streaming_run_still_publishes(self):
        seen = []

        async def sink(event):
            seen.append(event.type)

        await WorkflowEngine(self.graph, event_sink=sink).run({"q": "x"})

        self.assertEqual(seen[0], "run.started")
        self.assertEqual(seen[-1], "run.completed")

    async def test_a_streamed_run_publishes_exactly_what_it_streams(self):
        seen = []

        async def sink(event):
            seen.append(event.type)

        engine = WorkflowEngine(self.graph, event_sink=sink)
        streamed = [e.type async for e in engine.stream({"q": "x"})]

        self.assertEqual(streamed, seen)

    async def test_no_sink_means_no_overhead_and_no_events(self):
        result = await WorkflowEngine(self.graph).run({"q": "x"})
        self.assertTrue(result.ok)  # nothing to publish to, nothing breaks


@unittest.skipUnless(REAL_KAFKA, "set FLOWFORGE_TEST_KAFKA=host:port to run these")
class RealBrokerTests(unittest.IsolatedAsyncioTestCase):
    """Against an actual broker — the only judge of whether the framing is right."""

    async def asyncSetUp(self):
        self.topic = f"flowforge.test.{uuid.uuid4().hex[:8]}"
        self.client = KafkaClient.from_endpoint(REAL_KAFKA)
        await self.client.connect()
        await self.client.ensure_topic(self.topic)

    async def asyncTearDown(self):
        await self.client.close()

    async def test_negotiation_picks_versions_this_client_encodes(self):
        self.assertEqual(self.client.version_for(PRODUCE), SUPPORTED[PRODUCE])
        self.assertEqual(self.client.version_for(METADATA), SUPPORTED[METADATA])

    async def test_metadata_reports_a_leader(self):
        brokers, topics = await self.client.metadata([self.topic])
        self.assertTrue(brokers)
        self.assertEqual(len(topics[self.topic]), 1)
        self.assertGreaterEqual(topics[self.topic][0].leader, 0)

    async def test_a_batch_the_broker_accepts_comes_back_intact(self):
        """If the CRC32C or the varints were wrong, the broker would refuse this."""
        records = [
            Record(value=b'{"n":1}', key=b"run-a", headers={"event-type": b"one"}),
            Record(value=b'{"n":2}', key=b"run-b"),
        ]
        base = await self.client.produce(self.topic, records)
        fetched = await self.client.fetch(self.topic, offset=base)

        self.assertEqual([r.value for r in fetched[:2]], [b'{"n":1}', b'{"n":2}'])
        self.assertEqual([r.key for r in fetched[:2]], [b"run-a", b"run-b"])
        self.assertEqual(fetched[0].headers["event-type"], b"one")
        self.assertEqual([r.offset for r in fetched[:2]], [base, base + 1])

    async def test_offsets_advance_across_produces(self):
        first = await self.client.produce(self.topic, [Record(value=b"a")])
        second = await self.client.produce(self.topic, [Record(value=b"b")])
        self.assertEqual(second, first + 1)
        self.assertEqual(await self.client.earliest_offset(self.topic), 0)

    async def test_creating_an_existing_topic_reports_that_it_existed(self):
        self.assertFalse(await self.client.create_topic(self.topic))

    async def test_a_whole_run_lands_in_the_topic(self):
        sink = KafkaEventSink(
            KafkaClient.from_endpoint(REAL_KAFKA),
            topic=self.topic,
            types=["run.started", "node.completed", "run.completed"],
        )
        try:
            engine = WorkflowEngine(
                Graph.from_file("examples/diamond.json"), event_sink=sink
            )
            result = await engine.run({"q": "kafka"})
        finally:
            await sink.close()

        self.assertTrue(result.ok)
        self.assertEqual(sink.stats()["dropped"], 0)

        published = await read_events(REAL_KAFKA, self.topic)
        mine = [e for e in published if e.get("run") == result.run_id]
        self.assertEqual(mine[0]["type"], "run.started")
        self.assertEqual(mine[-1]["type"], "run.completed")
        # The terminal event carries the stats, which is what a consumer wants.
        self.assertEqual(mine[-1]["stats"]["nodes_executed"], result.stats.nodes_executed)
        self.assertEqual(
            {e["type"] for e in mine},
            {"run.started", "node.completed", "run.completed"},
        )

    async def test_a_streamed_run_publishes_and_streams_together(self):
        sink = KafkaEventSink(KafkaClient.from_endpoint(REAL_KAFKA), topic=self.topic)
        try:
            engine = WorkflowEngine(
                Graph.from_file("examples/diamond.json"), event_sink=sink
            )
            streamed = [e async for e in engine.stream({"q": "both"})]
        finally:
            await sink.close()

        published = await read_events(REAL_KAFKA, self.topic)
        run_id = streamed[0].run_id
        mine = [e for e in published if e.get("run") == run_id]
        self.assertEqual(len(mine), len(streamed))


if __name__ == "__main__":
    unittest.main()
