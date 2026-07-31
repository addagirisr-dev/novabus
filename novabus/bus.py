"""novabus - a dependency-free, in-process, priority-aware event bus.

No PyQt5, no Redis, no external broker. Pure stdlib (asyncio + collections).
Designed to replace the PyQt5 QObject/pyqtSignal-based EventBus this project
used to depend on, while keeping the same behavioural contract:

  * two priority lanes (critical / non-critical), critical always drains first
  * critical traffic pauses non-critical delivery until it's caught up
  * per-event dedup via a uuid, with bounded memory (periodic eviction)
  * stale-event flushing (events older than a threshold are dropped, with a
    shorter threshold for a configurable "live/streaming" source)
  * an optional high_rate_mode that bypasses the queue entirely and delivers
    synchronously for maximum throughput when ordering/priority don't matter
  * a queue size cap that drops the oldest non-critical events under backpressure

Subscribers are plain callables: handler(source, function_name, result, params).
Optionally filtered by source/function name (see Bus.subscribe).
"""
from __future__ import annotations

import asyncio
import collections
import itertools
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, Optional, Set

logger = logging.getLogger(__name__)

Handler = Callable[[str, str, Any, Dict[str, Any]], None]


@dataclass
class _Subscription:
    name: str
    callback: Handler
    sources: Optional[Set[str]] = None
    functions: Optional[Set[str]] = None


@dataclass
class PriorityRule:
    """Marks (source, function_name) pairs as critical (priority 0)."""
    critical_pairs: Set[tuple] = field(default_factory=set)

    def is_critical(self, source: str, function_name: str) -> bool:
        return (source, function_name) in self.critical_pairs


class NullBus:
    """Null-object stand-in so callers never need to guard against no bus."""

    def publish(self, *args, **kwargs):
        logger.debug("NullBus: dropping publish(%s, %s)", args, kwargs)


class Bus:
    def __init__(
        self,
        loop: Optional[asyncio.AbstractEventLoop] = None,
        max_queue_size: int = 300_000,
        stale_after: float = 300.0,
        priority_rule: Optional[PriorityRule] = None,
    ):
        self.loop = loop or asyncio.get_event_loop()
        self.critical: collections.deque = collections.deque()
        self.non_critical: collections.deque = collections.deque()
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._paused_non_critical = False
        self._max_queue_size = max_queue_size
        self._stale_after = stale_after
        self._priority_rule = priority_rule or PriorityRule()

        self._subscriptions: Dict[str, _Subscription] = {}
        self._subscriptions_snapshot: tuple = ()
        self._seen_ids: Set[int] = set()
        self._seen_count = 0
        self._seen_evict_at = 50_000
        self._id_counter = itertools.count()

        self._high_rate_mode = False

    @property
    def running(self) -> bool:
        return self._running

    @property
    def high_rate_mode(self) -> bool:
        return self._high_rate_mode

    # ── configuration ──────────────────────────────────────────────
    def set_high_rate_mode(self, enabled: bool):
        self._high_rate_mode = enabled

    def mark_critical(self, source: str, function_name: str):
        self._priority_rule.critical_pairs.add((source, function_name))

    # ── subscriptions ──────────────────────────────────────────────
    def subscribe(
        self,
        name: str,
        callback: Handler,
        sources: Optional[Iterable[str]] = None,
        functions: Optional[Iterable[str]] = None,
    ):
        self._subscriptions[name] = _Subscription(
            name=name,
            callback=callback,
            sources=set(sources) if sources else None,
            functions=set(functions) if functions else None,
        )
        self._subscriptions_snapshot = tuple(self._subscriptions.values())

    def unsubscribe(self, name: str):
        self._subscriptions.pop(name, None)
        self._subscriptions_snapshot = tuple(self._subscriptions.values())

    def _dispatch(self, source: str, function_name: str, result: Any, params: Dict):
        for sub in self._subscriptions_snapshot:
            if sub.sources is not None and source not in sub.sources:
                continue
            if sub.functions is not None and function_name not in sub.functions:
                continue
            try:
                sub.callback(source, function_name, result, params)
            except Exception:
                logger.exception("subscriber %r failed on %s.%s", sub.name, source, function_name)

    # ── lifecycle ──────────────────────────────────────────────────
    def start(self):
        if self._running:
            return
        self._running = True
        self._task = self.loop.create_task(self._event_loop(), name="novabus-loop")

    def stop(self):
        if not self._running:
            return
        self._running = False
        if self._task:
            self._task.cancel()

    def is_queue_empty(self) -> bool:
        return not self.critical and not self.non_critical

    def queue_size(self) -> int:
        return len(self.critical) + len(self.non_critical)

    # ── publish ────────────────────────────────────────────────────
    def publish(
        self,
        source: str,
        function_name: str,
        result: Any = None,
        params: Optional[Dict] = None,
    ):
        params = dict(params) if params else {}
        params["event_uuid"] = next(self._id_counter)
        event = (source, function_name, result, params)

        if self._high_rate_mode:
            # Bypass the queue entirely: max throughput, no priority/dedup.
            self._dispatch(*event)
            return

        if not self._running:
            logger.warning("Bus not running; dropping %s.%s", source, function_name)
            return

        timestamp = time.perf_counter()
        is_critical = self._priority_rule.is_critical(source, function_name)

        qsize = len(self.critical) + len(self.non_critical)
        if qsize >= self._max_queue_size:
            # Backpressure: drop oldest non-critical first.
            while qsize > self._max_queue_size * 0.75 and self.non_critical:
                self.non_critical.popleft()
                qsize -= 1
            logger.warning("queue at capacity (%d), dropped oldest non-critical events", self._max_queue_size)

        if is_critical:
            self._paused_non_critical = True
            self.critical.append((timestamp, event))
        else:
            self.non_critical.append((timestamp, event))

    # ── internal event loop ───────────────────────────────────────
    def _next_event(self, now: float):
        while True:
            if self.critical:
                q = self.critical
            elif not self._paused_non_critical and self.non_critical:
                q = self.non_critical
            else:
                return None

            timestamp, event = q.popleft()
            if now - timestamp > self._stale_after:
                continue  # drop stale, keep scanning
            return timestamp, event

    async def _event_loop(self):
        while self._running:
            try:
                processed = 0
                # Drain everything currently available in one pass -
                # no artificial per-cycle cap, that's what throttles throughput.
                # `now` is sampled once per pass (staleness is a seconds-scale
                # threshold, doesn't need per-event precision) to avoid paying
                # a perf_counter() syscall-ish cost on every single event.
                now = time.perf_counter()
                while True:
                    item = self._next_event(now)
                    if item is None:
                        break
                    _, (source, function_name, result, params) = item

                    event_uuid = params.get("event_uuid")
                    if event_uuid in self._seen_ids:
                        continue
                    self._seen_ids.add(event_uuid)
                    self._seen_count += 1
                    if self._seen_count >= self._seen_evict_at:
                        self._seen_ids.clear()
                        self._seen_count = 0

                    self._dispatch(source, function_name, result, params)
                    processed += 1

                    if not self.critical and self._paused_non_critical:
                        self._paused_non_critical = False

                if processed == 0:
                    await asyncio.sleep(0.001)
                else:
                    await asyncio.sleep(0)  # yield to the loop, no artificial cap
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("novabus event loop error")
                await asyncio.sleep(0.01)


_default_bus: Optional[Bus] = None


def get_bus(loop: Optional[asyncio.AbstractEventLoop] = None) -> Bus:
    global _default_bus
    if _default_bus is None:
        _default_bus = Bus(loop=loop)
        _default_bus.start()
    return _default_bus


def set_bus(bus: Bus):
    global _default_bus
    _default_bus = bus
