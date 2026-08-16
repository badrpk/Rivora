from __future__ import annotations

from dataclasses import dataclass, asdict
from hashlib import sha256
import json
from typing import Any, Dict, Iterable, List, Optional, Set


@dataclass(frozen=True)
class Event:
    event_id: str
    topic: str
    sequence: int
    payload: Dict[str, Any]


class EventStream:
    """Deterministic in-memory event stream with cursor replay.

    Rivora intentionally focuses on event ordering and replay semantics. Network
    transports (WebSocket/SSE/etc.) can be adapters around this core instead of
    being embedded in the domain model.
    """

    def __init__(self, *, retention: int = 1000) -> None:
        if retention < 1:
            raise ValueError("retention must be >= 1")
        self.retention = retention
        self._events: List[Event] = []
        self._ids: Set[str] = set()
        self._next_sequence = 1

    @staticmethod
    def _event_id(topic: str, payload: Dict[str, Any], client_key: str = "") -> str:
        canonical = json.dumps(
            {"topic": topic, "payload": payload, "client_key": client_key},
            sort_keys=True,
            separators=(",", ":"),
        )
        return sha256(canonical.encode("utf-8")).hexdigest()[:24]

    def publish(
        self,
        topic: str,
        payload: Dict[str, Any],
        *,
        client_key: str = "",
    ) -> Event:
        topic = topic.strip()
        if not topic:
            raise ValueError("topic is required")

        event_id = self._event_id(topic, payload, client_key)
        for existing in self._events:
            if existing.event_id == event_id:
                return existing

        event = Event(
            event_id=event_id,
            topic=topic,
            sequence=self._next_sequence,
            payload=json.loads(json.dumps(payload)),
        )
        self._next_sequence += 1
        self._events.append(event)
        self._ids.add(event_id)

        if len(self._events) > self.retention:
            removed = self._events[:-self.retention]
            self._events = self._events[-self.retention:]
            for old in removed:
                self._ids.discard(old.event_id)

        return event

    def replay(
        self,
        *,
        after: int = 0,
        topics: Optional[Iterable[str]] = None,
        limit: Optional[int] = None,
    ) -> List[Event]:
        if after < 0:
            raise ValueError("after must be >= 0")
        if limit is not None and limit < 1:
            raise ValueError("limit must be >= 1")

        allowed = set(topics) if topics is not None else None
        events = [
            event
            for event in self._events
            if event.sequence > after
            and (allowed is None or event.topic in allowed)
        ]
        return events[:limit] if limit is not None else events

    def cursor(self) -> int:
        return self._events[-1].sequence if self._events else 0

    def snapshot(self) -> str:
        return json.dumps(
            [asdict(event) for event in self._events],
            sort_keys=True,
            separators=(",", ":"),
        )

    def stats(self) -> Dict[str, Any]:
        topics: Dict[str, int] = {}
        for event in self._events:
            topics[event.topic] = topics.get(event.topic, 0) + 1
        return {
            "events": len(self._events),
            "cursor": self.cursor(),
            "retention": self.retention,
            "topics": dict(sorted(topics.items())),
        }
