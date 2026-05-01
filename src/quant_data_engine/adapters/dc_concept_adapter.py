"""东方财富概念板块：Tushare ``dc_index`` / ``dc_member``，带限频与重试。"""
from __future__ import annotations

import logging
import threading
import time
from typing import Any, Optional

import pandas as pd

logger = logging.getLogger(__name__)

try:
    import tushare as ts  # type: ignore
except Exception:  # pragma: no cover
    ts = None


class _RateLimiter:
    def __init__(self, rate_per_sec: float):
        self.interval = 1.0 / max(rate_per_sec, 0.1)
        self._lock = threading.Lock()
        self._last = 0.0

    def wait(self) -> None:
        with self._lock:
            now = time.time()
            delta = now - self._last
            if delta < self.interval:
                time.sleep(self.interval - delta)
            self._last = time.time()


class DcConceptAdapter:
    """封装 ``dc_index``（板块列表与涨跌等）与 ``dc_member``（概念成分）。"""

    def __init__(self, token: Optional[str], rate_limit_per_sec: float = 4.0, max_retries: int = 2):
        self.enabled = bool(token and ts)
        self._max_retries = max_retries
        self._rl = _RateLimiter(rate_limit_per_sec)
        if self.enabled:
            ts.set_token(token)  # type: ignore[union-attr]
            self._pro = ts.pro_api()  # type: ignore[union-attr]
        else:
            self._pro = None

    def _retry_call(self, func, **kwargs: Any) -> Any:
        if not self.enabled or self._pro is None:
            return None
        last_err: Optional[BaseException] = None
        for _ in range(self._max_retries + 1):
            try:
                self._rl.wait()
                return func(**kwargs)
            except Exception as e:  # pragma: no cover - network
                last_err = e
                time.sleep(0.5)
        raise RuntimeError(f"Tushare API failed after retries: {last_err}")

    def fetch_dc_index(
        self,
        trade_date: str = "",
        idx_type: str = "概念板块",
    ) -> pd.DataFrame:
        """东方财富概念板块列表（接口 ``dc_index``）。

        官方必选 ``idx_type``；``trade_date`` 为 YYYYMMDD 时按该交易日查询（与 theme_lag 锚定日一致）。
        """
        if not self.enabled or self._pro is None:
            return pd.DataFrame()
        kw: dict[str, Any] = {"idx_type": idx_type}
        td = "".join(c for c in str(trade_date) if c.isdigit())[:8]
        if len(td) == 8:
            kw["trade_date"] = td
        df = self._retry_call(self._pro.dc_index, **kw)  # type: ignore[attr-defined]
        if df is None:
            return pd.DataFrame()
        return df if isinstance(df, pd.DataFrame) else pd.DataFrame()

    def fetch_dc_member_for_concept(
        self,
        concept_ts_code: str,
        trade_date: str = "",
    ) -> pd.DataFrame:
        """某概念板块 ``ts_code``（如 ``BK0428.DC``）的成分股。

        ``trade_date`` 为空时不传该参数（由数据源返回当前口径）。
        """
        if not self.enabled or self._pro is None:
            return pd.DataFrame()
        kw: dict[str, Any] = {"ts_code": concept_ts_code}
        if trade_date and str(trade_date).strip():
            kw["trade_date"] = str(trade_date).replace("-", "")[:8]
        df = self._retry_call(self._pro.dc_member, **kw)  # type: ignore[attr-defined]
        if df is None:
            return pd.DataFrame()
        return df if isinstance(df, pd.DataFrame) else pd.DataFrame()


__all__ = ["DcConceptAdapter"]
