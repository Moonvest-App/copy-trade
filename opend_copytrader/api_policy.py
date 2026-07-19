from __future__ import annotations

import threading
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable


class RateLimitError(RuntimeError):
    """Raised locally before a request would violate a broker pacing rule."""

    def __init__(self, broker: str, retry_after: float):
        self.broker = broker
        self.retry_after = max(float(retry_after), 0.0)
        super().__init__(f"{broker} API 正在限频冷却，请约 {self.retry_after:.1f} 秒后重试")


@dataclass(frozen=True, slots=True)
class EndpointRule:
    prefix: str
    min_interval: float


class ApiPacer:
    """Conservative global window + endpoint spacing + adaptive 429 cooldown."""

    def __init__(
        self,
        broker: str,
        *,
        max_calls: int,
        period_seconds: float,
        endpoint_rules: tuple[EndpointRule, ...] = (),
        clock: Callable[[], float] = time.monotonic,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        self.broker = broker
        self.max_calls = max(int(max_calls), 1)
        self.period_seconds = max(float(period_seconds), 0.01)
        self.endpoint_rules = tuple(endpoint_rules)
        self._clock = clock
        self._sleeper = sleeper
        self._lock = threading.RLock()
        self._calls: deque[float] = deque()
        self._last_by_prefix: dict[str, float] = {}
        self._cooldown_until = 0.0
        self._rate_errors = 0
        self._last_429_at: str | None = None

    def _prefix(self, route: str) -> str:
        matches = [rule.prefix for rule in self.endpoint_rules if route.startswith(rule.prefix)]
        return max(matches, key=len) if matches else "*"

    def _min_interval(self, prefix: str) -> float:
        for rule in self.endpoint_rules:
            if rule.prefix == prefix:
                return max(float(rule.min_interval), 0.0)
        return 0.0

    def acquire(self, route: str, *, max_wait: float = 1.5) -> None:
        """Wait briefly when safe; fail locally instead of risking a remote ban."""

        prefix = self._prefix(route)
        started = self._clock()
        while True:
            with self._lock:
                now = self._clock()
                while self._calls and now - self._calls[0] >= self.period_seconds:
                    self._calls.popleft()
                wait_for = max(self._cooldown_until - now, 0.0)
                if len(self._calls) >= self.max_calls:
                    wait_for = max(wait_for, self.period_seconds - (now - self._calls[0]) + 0.01)
                last = self._last_by_prefix.get(prefix)
                if last is not None:
                    wait_for = max(wait_for, self._min_interval(prefix) - (now - last))
                if wait_for <= 0:
                    self._calls.append(now)
                    self._last_by_prefix[prefix] = now
                    return
            remaining = max_wait - (self._clock() - started)
            if wait_for > remaining:
                raise RateLimitError(self.broker, wait_for)
            self._sleeper(max(wait_for, 0.001))

    def record_success(self) -> None:
        with self._lock:
            if self._rate_errors:
                self._rate_errors -= 1

    def record_429(self, retry_after: float | None = None) -> float:
        with self._lock:
            self._rate_errors += 1
            adaptive = min(600.0, float(2 ** min(self._rate_errors, 9)))
            cooldown = max(float(retry_after or 0), adaptive)
            self._cooldown_until = max(self._cooldown_until, self._clock() + cooldown)
            self._last_429_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
            return cooldown

    def status(self) -> dict[str, object]:
        with self._lock:
            now = self._clock()
            while self._calls and now - self._calls[0] >= self.period_seconds:
                self._calls.popleft()
            return {
                "broker": self.broker,
                "window_limit": self.max_calls,
                "window_seconds": self.period_seconds,
                "requests_in_window": len(self._calls),
                "cooldown_remaining_seconds": round(max(self._cooldown_until - now, 0.0), 1),
                "last_429_at": self._last_429_at,
                "local_guard": True,
            }
