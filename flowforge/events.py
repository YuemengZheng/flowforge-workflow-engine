"""Run events and the queue that carries them to a consumer.

The engine is the producer, an HTTP handler (or the CLI) is the consumer, and
an ``asyncio.Queue`` sits between them. Giving the queue a ``maxsize`` turns it
into a back-pressure valve: when the consumer falls behind, ``emit`` suspends
the producing node instead of letting the queue grow without bound.
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from time import time
from typing import Any, AsyncIterator, Mapping

RUN_STARTED = "run.started"
RUN_RESUMED = "run.resumed"
RUN_COMPLETED = "run.completed"
RUN_FAILED = "run.failed"
RUN_PAUSED = "run.paused"
NODE_STARTED = "node.started"
NODE_DELTA = "node.delta"
NODE_RETRY = "node.retry"
NODE_COMPLETED = "node.completed"
NODE_SKIPPED = "node.skipped"
NODE_PAUSED = "node.paused"
NODE_FAILED = "node.failed"

# A paused run is over as far as this connection is concerned — the client must
# come back with an answer, which starts a new stream.
TERMINAL_EVENTS = frozenset({RUN_COMPLETED, RUN_FAILED, RUN_PAUSED})


@dataclass(frozen=True)
class Event:
    """One thing that happened during a run."""

    type: str
    seq: int
    run_id: str
    at: float = field(default_factory=time)
    data: Mapping[str, Any] = field(default_factory=dict)

    @property
    def node_id(self) -> str | None:
        return self.data.get("node")

    @property
    def is_terminal(self) -> bool:
        return self.type in TERMINAL_EVENTS

    def as_dict(self) -> dict[str, Any]:
        # Envelope fields are written last so a payload key can never shadow
        # them — a node whose data carries "type" must not rewrite the event's.
        return {
            **self.data,
            "type": self.type,
            "seq": self.seq,
            "run": self.run_id,
            "at": round(self.at, 6),
        }

    def to_json(self) -> str:
        return json.dumps(self.as_dict(), ensure_ascii=False, default=str)

    def to_sse(self) -> str:
        """Render as a Server-Sent Events frame, including the trailing blank line.

        ``id:`` lets a client resume with ``Last-Event-ID``; ``event:`` lets it
        subscribe to one event type via ``addEventListener``.
        """
        return f"id: {self.seq}\nevent: {self.type}\ndata: {self.to_json()}\n\n"


class EventStream:
    """Single-producer, single-consumer channel of :class:`Event`.

    Consume it with ``async for``; iteration ends when the producer calls
    ``close()``. ``maxsize`` bounds the buffer — 0 means unbounded.

    ``sink`` is a second destination, awaited for every event before it is
    queued: a Kafka topic, a log, an audit trail. ``buffer=False`` sends *only*
    to the sink and skips the queue, which is what a non-streaming run wants —
    otherwise events for a run nobody is consuming pile up until it ends.
    """

    _CLOSED = object()

    def __init__(
        self,
        run_id: str = "",
        maxsize: int = 0,
        sink: Any = None,
        buffer: bool = True,
    ) -> None:
        self.run_id = run_id
        self._queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=maxsize)
        self._seq = 0
        self._closed = False
        self._sink = sink
        self._buffer = buffer

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def emitted(self) -> int:
        return self._seq

    async def emit(self, event_type: str, **data: Any) -> Event | None:
        """Publish an event. Suspends when a bounded queue is full."""
        if self._closed:
            return None
        self._seq += 1
        event = Event(
            type=event_type, seq=self._seq, run_id=self.run_id, data=dict(data)
        )
        if self._sink is not None:
            # Before the queue, so a sink sees an event even if the consumer
            # abandons the stream immediately afterwards.
            await self._sink(event)
        if self._buffer:
            await self._queue.put(event)
        return event

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        await self._queue.put(self._CLOSED)

    def __aiter__(self) -> AsyncIterator[Event]:
        return self

    async def __anext__(self) -> Event:
        item = await self._queue.get()
        if item is self._CLOSED:
            raise StopAsyncIteration
        return item


async def sse_body(events: AsyncIterator[Event]) -> AsyncIterator[str]:
    """Adapt an event stream into SSE frames, ready for an HTTP response body."""
    async for event in events:
        yield event.to_sse()
