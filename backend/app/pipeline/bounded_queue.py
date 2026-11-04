"""Real bounded queue with overflow policies and measured backpressure."""

from __future__ import annotations

import threading
import time
from collections import deque
from typing import Any

from app.schemas.run_config import OverflowPolicy


class BoundedQueue:
    def __init__(self, capacity: int, policy: OverflowPolicy) -> None:
        self.capacity = capacity
        self.policy = policy
        self._dq: deque[Any] = deque()
        self._cv = threading.Condition()
        # counters (all guarded by self._cv)
        self.enqueued = 0          # admitted into the queue
        self.dequeued = 0
        self.dropped_newest = 0    # incoming records rejected at admission
        self.evicted_oldest = 0    # queued records evicted to make room (drop_oldest)
        self.blocked_seconds = 0.0
        self.blocked_events = 0
        self.max_depth = 0
        self.currently_blocked = False
        self._blocked_since = 0.0

    # ------------------------------------------------------------------
    def put_many(self, items: list[Any], abort: threading.Event) -> tuple[int, int, int]:
        """Admit items according to policy. Returns (admitted, dropped, abandoned).

        ``dropped`` = incoming records rejected at admission (drop_newest).
        ``abandoned`` = records never admitted because the run was aborted while blocked.
        Evictions (drop_oldest) are tracked separately in ``evicted_oldest``.
        """
        admitted = 0
        rejected = 0
        abandoned = 0
        i = 0
        n = len(items)
        with self._cv:
            while i < n:
                if len(self._dq) < self.capacity:
                    # fast path: add as many as fit
                    room = self.capacity - len(self._dq)
                    take = min(room, n - i)
                    self._dq.extend(items[i:i + take])
                    self.enqueued += take
                    admitted += take
                    i += take
                    if len(self._dq) > self.max_depth:
                        self.max_depth = len(self._dq)
                    self._cv.notify_all()
                    continue
                # queue is full
                if self.policy is OverflowPolicy.DROP_NEWEST:
                    rejected += n - i
                    self.dropped_newest += n - i
                    i = n
                elif self.policy is OverflowPolicy.DROP_OLDEST:
                    self._dq.popleft()
                    self.evicted_oldest += 1
                    self._dq.append(items[i])
                    self.enqueued += 1
                    admitted += 1
                    i += 1
                else:  # BLOCK -> backpressure
                    start = time.monotonic()
                    self.currently_blocked = True
                    self._blocked_since = start
                    self.blocked_events += 1
                    try:
                        while len(self._dq) >= self.capacity and not abort.is_set():
                            self._cv.wait(timeout=0.05)
                    finally:
                        self.blocked_seconds += time.monotonic() - start
                        self.currently_blocked = False
                    if abort.is_set():
                        abandoned += n - i
                        i = n
        return admitted, rejected, abandoned

    def get_batch(self, max_items: int, timeout: float = 0.05) -> list[Any]:
        with self._cv:
            if not self._dq:
                self._cv.wait(timeout=timeout)
            if not self._dq:
                return []
            k = min(max_items, len(self._dq))
            batch = [self._dq.popleft() for _ in range(k)]
            self.dequeued += k
            self._cv.notify_all()
            return batch

    def clear(self) -> int:
        with self._cv:
            k = len(self._dq)
            self._dq.clear()
            self._cv.notify_all()
            return k

    def depth(self) -> int:
        with self._cv:
            return len(self._dq)

    def metrics(self) -> dict[str, Any]:
        with self._cv:
            blocked = self.blocked_seconds
            if self.currently_blocked:
                blocked += time.monotonic() - self._blocked_since
            return {
                "depth": len(self._dq),
                "capacity": self.capacity,
                "utilization": len(self._dq) / self.capacity,
                "enqueued": self.enqueued,
                "dequeued": self.dequeued,
                "dropped_newest": self.dropped_newest,
                "evicted_oldest": self.evicted_oldest,
                "blocked_seconds": blocked,
                "blocked_events": self.blocked_events,
                "max_depth": self.max_depth,
                "blocked_now": self.currently_blocked,
                "policy": self.policy.value,
            }
