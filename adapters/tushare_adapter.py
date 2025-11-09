"""Tushare adapter with simple rate limiting & retry logic (synchronous minimal version)."""
from __future__ import annotations
import time
import threading
from typing import List, Dict, Any, Optional

try:
    import tushare as ts  # type: ignore
except Exception:  # pragma: no cover - optional dependency for tests
    ts = None

class RateLimiter:
    def __init__(self, rate_per_sec: float):
        self.interval = 1.0 / max(rate_per_sec, 0.1)
        self._lock = threading.Lock()
        self._last = 0.0

    def wait(self):
        with self._lock:
            now = time.time()
            delta = now - self._last
            if delta < self.interval:
                time.sleep(self.interval - delta)
            self._last = time.time()

class TushareAdapter:
    def __init__(self, token: Optional[str], rate_limit_per_sec: float = 4.0, max_retries: int = 2):
        self.enabled = bool(token and ts)
        self._max_retries = max_retries
        self._rl = RateLimiter(rate_limit_per_sec)
        if self.enabled:
            ts.set_token(token)
            self._pro = ts.pro_api()
        else:
            self._pro = None

    def fetch_index_daily(self, ts_code: str, start_date: str, end_date: str):
        if not self.enabled:
            return None
        last_err = None
        for _ in range(self._max_retries + 1):
            try:
                self._rl.wait()
                df = self._pro.index_daily(ts_code=ts_code, start_date=start_date, end_date=end_date)  # type: ignore[attr-defined]
                return df
            except Exception as e:  # pragma: no cover - network dependent
                last_err = e
                time.sleep(0.5)
        raise RuntimeError(f"Tushare fetch_index_daily failed after retries: {last_err}")

__all__ = ["TushareAdapter"]
