"""A Kafka client, and run events published to a topic.

Written against the wire protocol rather than a driver, for the same reason the
Redis client is: the interesting part is the framing, and a dependency doing it
would leave nothing to show. It is also the only way to keep the core install
empty.

Scope is one job — publish events, and read them back to prove it. That means
five APIs: ``ApiVersions`` to negotiate, ``Metadata`` to find a partition leader,
``Produce`` to write, ``ListOffsets`` and ``Fetch`` to read. No consumer groups,
no offset commits, no transactions, no compression.

Three things the protocol makes you get exactly right, and they are where the
work is:

* **Version negotiation.** Recent request versions are "flexible" — tagged fields
  and varint-prefixed strings — so this asks the broker what it supports and picks
  the newest *non-flexible* version it and this client have in common. Deliberate:
  supporting one encoding correctly beats supporting two badly.
* **RecordBatch v2.** A length-prefixed, CRC-guarded frame whose records are
  zigzag-varint encoded with deltas against the batch's base offset and timestamp.
* **CRC32C.** Castagnoli, not the CRC-32 in ``zlib``. Sending the wrong one gets
  the batch rejected as corrupt, so it is implemented here and checked against the
  published check value in the tests.
"""

from __future__ import annotations

import asyncio
import struct
import time
from contextlib import suppress
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping, Sequence

from .errors import FlowForgeError
from .events import Event

API_VERSIONS = 18
METADATA = 3
PRODUCE = 0
LIST_OFFSETS = 2
FETCH = 1
CREATE_TOPICS = 19

#: The newest non-flexible version of each API this client encodes. Negotiation
#: never picks above these, whatever the broker offers.
SUPPORTED: dict[int, int] = {
    API_VERSIONS: 0,
    METADATA: 1,
    PRODUCE: 3,
    LIST_OFFSETS: 1,
    FETCH: 4,
    CREATE_TOPICS: 0,
}

#: Error codes that mean "not ready yet" rather than "no".
UNKNOWN_TOPIC_OR_PARTITION = 3
LEADER_NOT_AVAILABLE = 5
TOPIC_ALREADY_EXISTS = 36
PENDING_ERRORS = frozenset({UNKNOWN_TOPIC_OR_PARTITION, LEADER_NOT_AVAILABLE})

CLIENT_ID = "flowforge"
MAX_FRAME_BYTES = 64 << 20


class KafkaError(FlowForgeError):
    """The broker refused, or could not be reached."""


class KafkaProtocolError(KafkaError):
    """A response this client could not parse."""


# ------------------------------------------------------------------- CRC32C


def _crc32c_table() -> list[int]:
    table = []
    for index in range(256):
        crc = index
        for _ in range(8):
            crc = (crc >> 1) ^ (0x82F63B78 if crc & 1 else 0)
        table.append(crc)
    return table


_CRC32C_TABLE = _crc32c_table()


def crc32c(data: bytes) -> int:
    """Castagnoli CRC-32, which is what RecordBatch v2 is checked with.

    ``zlib.crc32`` is the IEEE polynomial and produces a different value; a batch
    carrying it is rejected as corrupt by the broker, with an error that does not
    mention the CRC.

    A byte-at-a-time loop on purpose. Slicing-by-4 with four tables — the standard
    way to make this faster in C — was **measured slower** here (15.7 ms vs
    14.9 ms per 256 KiB): the slicing and ``int.from_bytes`` cost more than the
    loop iterations they remove, because iterating a ``bytes`` already yields ints
    without per-item allocation. The cost that mattered was one produce per event,
    and that is fixed by batching in :class:`KafkaEventSink`, not here.
    """
    crc = 0xFFFFFFFF
    for byte in data:
        crc = _CRC32C_TABLE[(crc ^ byte) & 0xFF] ^ (crc >> 8)
    return crc ^ 0xFFFFFFFF


# ------------------------------------------------------------------ encoding


def encode_varint(value: int) -> bytes:
    """Zigzag then base-128, which is how records encode their lengths."""
    unsigned = (value << 1) ^ (value >> 63) if value < 0 else value << 1
    out = bytearray()
    while True:
        chunk = unsigned & 0x7F
        unsigned >>= 7
        if unsigned:
            out.append(chunk | 0x80)
        else:
            out.append(chunk)
            return bytes(out)


def decode_varint(data: bytes, offset: int) -> tuple[int, int]:
    result = 0
    shift = 0
    while True:
        byte = data[offset]
        offset += 1
        result |= (byte & 0x7F) << shift
        if not byte & 0x80:
            break
        shift += 7
        if shift > 63:
            raise KafkaProtocolError("varint is longer than 64 bits")
    return (result >> 1) ^ -(result & 1), offset


def encode_string(value: str | None) -> bytes:
    if value is None:
        return struct.pack(">h", -1)
    raw = value.encode("utf-8")
    return struct.pack(">h", len(raw)) + raw


def encode_bytes(value: bytes | None) -> bytes:
    if value is None:
        return struct.pack(">i", -1)
    return struct.pack(">i", len(value)) + value


def encode_array(items: Sequence[bytes] | None) -> bytes:
    if items is None:
        return struct.pack(">i", -1)
    return struct.pack(">i", len(items)) + b"".join(items)


class Reader:
    """Cursor over a response body. Every read is bounds-checked."""

    def __init__(self, data: bytes) -> None:
        self.data = data
        self.offset = 0

    def _take(self, count: int) -> bytes:
        end = self.offset + count
        if end > len(self.data):
            raise KafkaProtocolError(
                f"response ended early: wanted {count} bytes at {self.offset}"
            )
        chunk = self.data[self.offset : end]
        self.offset = end
        return chunk

    def int8(self) -> int:
        return struct.unpack(">b", self._take(1))[0]

    def int16(self) -> int:
        return struct.unpack(">h", self._take(2))[0]

    def int32(self) -> int:
        return struct.unpack(">i", self._take(4))[0]

    def int64(self) -> int:
        return struct.unpack(">q", self._take(8))[0]

    def uint32(self) -> int:
        return struct.unpack(">I", self._take(4))[0]

    def boolean(self) -> bool:
        return self.int8() != 0

    def string(self) -> str | None:
        length = self.int16()
        return None if length < 0 else self._take(length).decode("utf-8")

    def raw_bytes(self) -> bytes | None:
        length = self.int32()
        return None if length < 0 else self._take(length)

    def array(self, read_one: Any) -> list[Any]:
        count = self.int32()
        return [] if count < 0 else [read_one(self) for _ in range(count)]

    def remaining(self) -> bytes:
        return self.data[self.offset :]


# ------------------------------------------------------------- record batch


@dataclass
class Record:
    value: bytes
    key: bytes | None = None
    timestamp: int = 0
    offset: int = 0
    headers: Mapping[str, bytes] = field(default_factory=dict)


def encode_record_batch(records: Sequence[Record], base_timestamp: int = 0) -> bytes:
    """One RecordBatch v2 frame.

    Records carry *deltas* — offset and timestamp relative to the batch's base —
    which is why the batch has to be built as a unit rather than streamed.
    """
    if not records:
        raise KafkaError("a record batch needs at least one record")
    first_timestamp = base_timestamp or records[0].timestamp or int(time.time() * 1000)
    max_timestamp = first_timestamp

    encoded = bytearray()
    for index, record in enumerate(records):
        timestamp = record.timestamp or first_timestamp
        max_timestamp = max(max_timestamp, timestamp)
        body = bytearray()
        body.append(0)  # record-level attributes, unused in v2
        body += encode_varint(timestamp - first_timestamp)
        body += encode_varint(index)
        if record.key is None:
            body += encode_varint(-1)
        else:
            body += encode_varint(len(record.key)) + record.key
        body += encode_varint(len(record.value)) + record.value
        body += encode_varint(len(record.headers))
        for name, value in record.headers.items():
            raw_name = name.encode("utf-8")
            body += encode_varint(len(raw_name)) + raw_name
            body += encode_varint(len(value)) + value
        encoded += encode_varint(len(body)) + body

    # Everything the CRC covers: from `attributes` to the end of the records.
    after_crc = (
        struct.pack(">hiqqqhi", 0, len(records) - 1, first_timestamp, max_timestamp, -1, -1, -1)
        + struct.pack(">i", len(records))
        + bytes(encoded)
    )
    header = struct.pack(">ib", -1, 2)  # partitionLeaderEpoch, magic
    checksum = struct.pack(">I", crc32c(after_crc))
    body = header + checksum + after_crc
    # batchLength counts everything after itself.
    return struct.pack(">qi", 0, len(body)) + body


def decode_record_batches(data: bytes) -> list[Record]:
    """Every record in a fetched partition, ignoring incomplete trailing batches.

    A fetch is allowed to cut the last batch off mid-frame — that is normal, not
    corruption, so a short tail ends the loop instead of raising.
    """
    records: list[Record] = []
    offset = 0
    while offset + 61 <= len(data):
        base_offset, batch_length = struct.unpack(">qi", data[offset : offset + 12])
        end = offset + 12 + batch_length
        if batch_length <= 0 or end > len(data):
            break
        batch = data[offset:end]
        magic = batch[16]
        if magic != 2:
            raise KafkaProtocolError(f"unsupported record batch magic {magic}")
        stated = struct.unpack(">I", batch[17:21])[0]
        if crc32c(batch[21:]) != stated:
            raise KafkaProtocolError("record batch failed its CRC32C")
        first_timestamp = struct.unpack(">q", batch[27:35])[0]
        count = struct.unpack(">i", batch[57:61])[0]
        cursor = 61
        for _ in range(count):
            length, cursor = decode_varint(batch, cursor)
            record_end = cursor + length
            cursor += 1  # attributes
            timestamp_delta, cursor = decode_varint(batch, cursor)
            offset_delta, cursor = decode_varint(batch, cursor)
            key_length, cursor = decode_varint(batch, cursor)
            key = None
            if key_length >= 0:
                key = batch[cursor : cursor + key_length]
                cursor += key_length
            value_length, cursor = decode_varint(batch, cursor)
            value = batch[cursor : cursor + value_length] if value_length >= 0 else b""
            cursor += max(0, value_length)
            header_count, cursor = decode_varint(batch, cursor)
            headers: dict[str, bytes] = {}
            for _ in range(header_count):
                name_length, cursor = decode_varint(batch, cursor)
                name = batch[cursor : cursor + name_length].decode("utf-8")
                cursor += name_length
                header_value_length, cursor = decode_varint(batch, cursor)
                header_value = b""
                if header_value_length >= 0:
                    header_value = batch[cursor : cursor + header_value_length]
                    cursor += header_value_length
                headers[name] = header_value
            records.append(
                Record(
                    value=value,
                    key=key,
                    timestamp=first_timestamp + timestamp_delta,
                    offset=base_offset + offset_delta,
                    headers=headers,
                )
            )
            cursor = record_end
        offset = end
    return records


# ------------------------------------------------------------------- client


@dataclass(frozen=True)
class Broker:
    node_id: int
    host: str
    port: int


@dataclass(frozen=True)
class PartitionMetadata:
    index: int
    leader: int


class KafkaClient:
    """One connection to one broker, speaking the five APIs this needs."""

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 9092,
        client_id: str = CLIENT_ID,
        timeout_s: float = 10.0,
    ) -> None:
        self.host = host
        self.port = port
        self.client_id = client_id
        self.timeout_s = timeout_s
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._correlation = 0
        self._versions: dict[int, tuple[int, int]] = {}
        self._lock = asyncio.Lock()

    @classmethod
    def from_endpoint(cls, endpoint: str, **kwargs: Any) -> "KafkaClient":
        host, _, port = endpoint.partition(":")
        return cls(host=host or "127.0.0.1", port=int(port or 9092), **kwargs)

    async def connect(self) -> None:
        if self._writer is not None and not self._writer.is_closing():
            return
        try:
            self._reader, self._writer = await asyncio.wait_for(
                asyncio.open_connection(self.host, self.port), timeout=self.timeout_s
            )
        except (OSError, asyncio.TimeoutError) as exc:
            raise KafkaError(f"cannot reach kafka at {self.host}:{self.port}: {exc}") from exc
        await self._negotiate()

    async def close(self) -> None:
        writer, self._writer, self._reader = self._writer, None, None
        if writer is not None and not writer.is_closing():
            writer.close()
            try:
                await writer.wait_closed()
            except (ConnectionError, OSError):
                pass

    def version_for(self, api_key: int) -> int:
        """The newest version this client encodes and the broker accepts."""
        wanted = SUPPORTED[api_key]
        if api_key not in self._versions:
            return wanted
        broker_min, broker_max = self._versions[api_key]
        chosen = min(wanted, broker_max)
        if chosen < broker_min:
            raise KafkaError(
                f"broker requires api {api_key} version >= {broker_min}; this "
                f"client encodes up to {wanted}"
            )
        return chosen

    async def _negotiate(self) -> None:
        body = await self._roundtrip(API_VERSIONS, 0, b"")
        reader = Reader(body)
        error = reader.int16()
        if error:
            raise KafkaError(f"ApiVersions failed with error {error}")
        for _ in range(max(0, reader.int32())):
            api_key, minimum, maximum = reader.int16(), reader.int16(), reader.int16()
            self._versions[api_key] = (minimum, maximum)

    async def _roundtrip(self, api_key: int, version: int, payload: bytes) -> bytes:
        if self._writer is None or self._reader is None:
            raise KafkaError("not connected")
        async with self._lock:
            self._correlation += 1
            correlation = self._correlation
            header = (
                struct.pack(">hhi", api_key, version, correlation)
                + encode_string(self.client_id)
            )
            frame = header + payload
            self._writer.write(struct.pack(">i", len(frame)) + frame)
            await self._writer.drain()

            size_bytes = await asyncio.wait_for(
                self._reader.readexactly(4), timeout=self.timeout_s
            )
            (size,) = struct.unpack(">i", size_bytes)
            if size <= 0 or size > MAX_FRAME_BYTES:
                raise KafkaProtocolError(f"implausible response size {size}")
            body = await asyncio.wait_for(
                self._reader.readexactly(size), timeout=self.timeout_s
            )
        (echoed,) = struct.unpack(">i", body[:4])
        if echoed != correlation:
            raise KafkaProtocolError(
                f"correlation id {echoed} does not match request {correlation}"
            )
        return body[4:]

    # ------------------------------------------------------------------ APIs

    async def metadata(self, topics: Sequence[str]) -> tuple[list[Broker], dict[str, list[PartitionMetadata]]]:
        version = self.version_for(METADATA)
        payload = encode_array([encode_string(topic) for topic in topics])
        reader = Reader(await self._roundtrip(METADATA, version, payload))

        brokers: list[Broker] = []
        for _ in range(max(0, reader.int32())):
            node_id = reader.int32()
            host = reader.string() or ""
            port = reader.int32()
            reader.string()  # rack, present from v1
            brokers.append(Broker(node_id, host, port))
        reader.int32()  # controller_id

        found: dict[str, list[PartitionMetadata]] = {}
        for _ in range(max(0, reader.int32())):
            error = reader.int16()
            name = reader.string() or ""
            reader.boolean()  # is_internal
            partitions = []
            for _ in range(max(0, reader.int32())):
                partition_error = reader.int16()
                index = reader.int32()
                leader = reader.int32()
                reader.array(lambda r: r.int32())  # replicas
                reader.array(lambda r: r.int32())  # isr
                if partition_error == 0:
                    partitions.append(PartitionMetadata(index, leader))
            if error and error not in PENDING_ERRORS:
                raise KafkaError(f"metadata for {name!r} failed with error {error}")
            # A pending error leaves the topic in the map with no partitions,
            # which is what `ensure_topic` polls on.
            found[name] = partitions
        return brokers, found

    async def create_topic(
        self,
        topic: str,
        partitions: int = 1,
        replication_factor: int = 1,
        timeout_ms: int = 10_000,
    ) -> bool:
        """Create a topic, returning False if it already existed.

        Explicit rather than leaning on ``auto.create.topics.enable``: that is a
        broker-side setting nobody deploying this controls, and a publisher that
        silently produces nothing because a topic is missing is the worst
        available outcome.
        """
        version = self.version_for(CREATE_TOPICS)
        request = (
            encode_string(topic)
            + struct.pack(">ih", partitions, replication_factor)
            + encode_array([])  # replica assignments: let the broker choose
            + encode_array([])  # config entries
        )
        payload = encode_array([request]) + struct.pack(">i", timeout_ms)
        reader = Reader(await self._roundtrip(CREATE_TOPICS, version, payload))

        for _ in range(max(0, reader.int32())):
            reader.string()  # topic name
            error = reader.int16()
            if error == TOPIC_ALREADY_EXISTS:
                return False
            if error:
                raise KafkaError(f"creating topic {topic!r} failed with error {error}")
            return True
        raise KafkaProtocolError("create topics response carried no result")

    async def produce(
        self,
        topic: str,
        records: Sequence[Record],
        partition: int = 0,
        acks: int = 1,
        timeout_ms: int = 10_000,
    ) -> int:
        """Append records and return the offset the first one landed at."""
        version = self.version_for(PRODUCE)
        batch = encode_record_batch(records)
        partition_data = struct.pack(">i", partition) + encode_bytes(batch)
        topic_data = encode_string(topic) + encode_array([partition_data])
        payload = (
            encode_string(None)  # transactional_id
            + struct.pack(">hi", acks, timeout_ms)
            + encode_array([topic_data])
        )
        reader = Reader(await self._roundtrip(PRODUCE, version, payload))

        for _ in range(max(0, reader.int32())):
            reader.string()  # topic name
            for _ in range(max(0, reader.int32())):
                reader.int32()  # partition index
                error = reader.int16()
                base_offset = reader.int64()
                reader.int64()  # log_append_time
                if error:
                    raise KafkaError(f"produce to {topic!r} failed with error {error}")
                return base_offset
        raise KafkaProtocolError("produce response carried no partition result")

    async def earliest_offset(self, topic: str, partition: int = 0) -> int:
        version = self.version_for(LIST_OFFSETS)
        partition_data = struct.pack(">iq", partition, -2)  # -2 = earliest
        topic_data = encode_string(topic) + encode_array([partition_data])
        payload = struct.pack(">i", -1) + encode_array([topic_data])
        reader = Reader(await self._roundtrip(LIST_OFFSETS, version, payload))

        for _ in range(max(0, reader.int32())):
            reader.string()
            for _ in range(max(0, reader.int32())):
                reader.int32()
                error = reader.int16()
                reader.int64()  # timestamp
                offset = reader.int64()
                if error:
                    raise KafkaError(f"list offsets failed with error {error}")
                return offset
        raise KafkaProtocolError("list offsets response carried no partition result")

    async def fetch(
        self,
        topic: str,
        partition: int = 0,
        offset: int = 0,
        max_wait_ms: int = 1000,
        max_bytes: int = 1 << 20,
    ) -> list[Record]:
        version = self.version_for(FETCH)
        partition_data = struct.pack(">iqi", partition, offset, max_bytes)
        topic_data = encode_string(topic) + encode_array([partition_data])
        payload = (
            struct.pack(">iiii", -1, max_wait_ms, 1, max_bytes)
            + struct.pack(">b", 0)  # isolation_level: read_uncommitted
            + encode_array([topic_data])
        )
        reader = Reader(await self._roundtrip(FETCH, version, payload))
        reader.int32()  # throttle_time_ms

        collected: list[Record] = []
        for _ in range(max(0, reader.int32())):
            reader.string()  # topic
            for _ in range(max(0, reader.int32())):
                reader.int32()  # partition
                error = reader.int16()
                reader.int64()  # high_watermark
                reader.int64()  # last_stable_offset
                reader.array(lambda r: (r.int64(), r.int64()))  # aborted transactions
                record_set = reader.raw_bytes() or b""
                if error:
                    raise KafkaError(f"fetch from {topic!r} failed with error {error}")
                collected.extend(decode_record_batches(record_set))
        return collected

    async def ensure_topic(self, topic: str, attempts: int = 10) -> None:
        """Create the topic if needed, then wait until a partition has a leader.

        Creation is asynchronous even once ``CreateTopics`` returns: metadata
        answers ``LEADER_NOT_AVAILABLE`` or ``UNKNOWN_TOPIC_OR_PARTITION`` for a
        moment while the partition is assigned, so producing immediately would
        fail for a reason that has nothing to do with the caller.
        """
        _, topics = await self.metadata([topic])
        if topics.get(topic):
            return
        await self.create_topic(topic)
        for attempt in range(attempts):
            _, topics = await self.metadata([topic])
            if topics.get(topic):
                return
            await asyncio.sleep(0.2 * (attempt + 1))
        raise KafkaError(f"topic {topic!r} has no leader after {attempts} attempts")


# --------------------------------------------------------------- event sink


class KafkaEventSink:
    """Publishes run events to a topic, as well as wherever else they go.

    **Telemetry must not sit in the critical path.** A produce is a network round
    trip, and a run emits an event per node per phase; publishing each one inline
    made a 100-node run 119× slower (0.8 ms → 92.5 ms) before this was batched.
    So the engine's side of this is a ``put_nowait`` into a bounded queue, and a
    background task drains it — collecting up to ``batch_size`` records or waiting
    ``linger_ms`` for more, then sending them as one batch. That is ``batch.size``
    and ``linger.ms`` from any real producer, and for the same reason.

    Records are keyed by run id, so one run's events land in one partition and
    stay ordered; a single flusher keeps them in emit order within it.

    Publishing is **best-effort by default**. A broker being down, or a queue
    filling because the broker is slow, increments a counter — it does not fail
    the workflow, because a run that dies because its telemetry did is the worse
    outcome. ``required=True`` inverts both halves of that: events are published
    synchronously, in order, and a failure propagates into the run. It is
    correspondingly slow, and that is the trade being asked for.
    """

    def __init__(
        self,
        client: KafkaClient,
        topic: str = "flowforge.events",
        required: bool = False,
        types: Sequence[str] | None = None,
        batch_size: int = 200,
        linger_ms: float = 20.0,
        max_queued: int = 10_000,
    ) -> None:
        self.client = client
        self.topic = topic
        self.required = required
        # None means everything. Narrowing this is how you keep node.delta out
        # of the topic, which is most of the volume in a streaming run.
        self.types = frozenset(types) if types else None
        self.batch_size = max(1, batch_size)
        self.linger_s = max(0.0, linger_ms / 1000)
        self.published = 0
        self.dropped = 0
        self.batches = 0
        self._ready = False
        self._queue: asyncio.Queue[Record] = asyncio.Queue(maxsize=max_queued)
        self._flusher: asyncio.Task[None] | None = None
        self._idle = asyncio.Event()
        self._idle.set()

    async def start(self) -> None:
        await self.client.connect()
        await self.client.ensure_topic(self.topic)
        self._ready = True
        if not self.required and (self._flusher is None or self._flusher.done()):
            self._flusher = asyncio.create_task(self._flush_forever())

    def wants(self, event: Event) -> bool:
        return self.types is None or event.type in self.types

    @staticmethod
    def record_for(event: Event) -> Record:
        return Record(
            value=event.to_json().encode("utf-8"),
            key=event.run_id.encode("utf-8") or None,
            timestamp=int(event.at * 1000),
            headers={"event-type": event.type.encode("utf-8")},
        )

    async def __call__(self, event: Event) -> None:
        """The sink signature the engine calls for every event."""
        if not self.wants(event):
            return
        record = self.record_for(event)
        if self.required:
            # Synchronous: the caller asked to know, so it also waits.
            await self._ensure_ready()
            await self._send([record])
            return
        try:
            if not self._ready:
                await self.start()
        except (KafkaError, OSError, asyncio.TimeoutError) as exc:
            self._note_drop(1, exc)
            return
        try:
            self._queue.put_nowait(record)
            self._idle.clear()
        except asyncio.QueueFull:
            # The broker cannot keep up. Dropping the newest event is the only
            # option that does not slow the run down, which is the whole point.
            self._note_drop(1, "publish queue is full")

    async def _ensure_ready(self) -> None:
        if not self._ready:
            await self.start()

    async def _flush_forever(self) -> None:
        while True:
            record = await self._queue.get()
            batch = [record]
            deadline = asyncio.get_running_loop().time() + self.linger_s
            while len(batch) < self.batch_size:
                remaining = deadline - asyncio.get_running_loop().time()
                if remaining <= 0:
                    break
                try:
                    batch.append(
                        await asyncio.wait_for(self._queue.get(), timeout=remaining)
                    )
                except (asyncio.TimeoutError, TimeoutError):
                    break
            try:
                await self._send(batch)
            except (KafkaError, OSError, asyncio.TimeoutError) as exc:
                self._note_drop(len(batch), exc)
            finally:
                if self._queue.empty():
                    self._idle.set()

    async def _send(self, batch: Sequence[Record]) -> None:
        try:
            await self._ensure_ready()
            await self.client.produce(self.topic, list(batch))
        except (KafkaError, OSError, asyncio.TimeoutError):
            self._ready = False  # force a reconnect on the next attempt
            raise
        self.published += len(batch)
        self.batches += 1

    def _note_drop(self, count: int, reason: Any) -> None:
        self.dropped += count
        self._ready = False
        if self.required:
            raise KafkaError(str(reason))
        # Logged on the first drop and then sparsely: a broker outage should not
        # bury the run's own output.
        if self.dropped == count or self.dropped % 100 < count:
            print(f"kafka: dropped {self.dropped} event(s): {reason}", flush=True)

    async def flush(self, timeout_s: float = 10.0) -> None:
        """Wait for everything queued to be sent. Used by tests and shutdown."""
        if self._flusher is None:
            return
        with suppress(asyncio.TimeoutError):
            await asyncio.wait_for(self._idle.wait(), timeout=timeout_s)

    async def close(self) -> None:
        await self.flush()
        if self._flusher is not None:
            self._flusher.cancel()
            with suppress(asyncio.CancelledError):
                await self._flusher
            self._flusher = None
        await self.client.close()

    def stats(self) -> dict[str, int]:
        return {
            "published": self.published,
            "dropped": self.dropped,
            "batches": self.batches,
        }


async def read_events(
    endpoint: str, topic: str = "flowforge.events", timeout_s: float = 10.0
) -> list[dict[str, Any]]:
    """Read a topic from its earliest offset. For the smoke test and for tests."""
    import json

    client = KafkaClient.from_endpoint(endpoint, timeout_s=timeout_s)
    try:
        await client.connect()
        await client.ensure_topic(topic)
        start = await client.earliest_offset(topic)
        records = await client.fetch(topic, offset=start)
        return [json.loads(record.value.decode("utf-8")) for record in records]
    finally:
        await client.close()
