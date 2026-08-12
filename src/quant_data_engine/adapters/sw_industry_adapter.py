"""申万行业：Tushare index_classify / index_member_all / sw_daily。"""
from __future__ import annotations

import time
import threading
from typing import Any, Callable, Dict, List, Optional

import pandas as pd

try:
    import tushare as ts  # type: ignore
except Exception:  # pragma: no cover
    ts = None


class RateLimiter:
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


def _drop_empty_str_params(**kwargs: Any) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k, v in kwargs.items():
        if v is None:
            continue
        if isinstance(v, str) and v.strip() == "":
            continue
        out[k] = v
    return out


class SwIndustryAdapter:
    """Tushare 申万：分类、成分(分级)、行业指数日线。sw_daily 支持按日全市场 + offset 分页。"""

    # Tushare sw_daily 单次行数常见上限
    _SW_DAILY_PAGE = 2000
    _MEMBER_PAGE = 2000

    def __init__(self, token: Optional[str], rate_limit_per_sec: float = 4.0, max_retries: int = 2):
        self.enabled = bool(token and ts)
        self._max_retries = max_retries
        self._rl = RateLimiter(rate_limit_per_sec)
        if self.enabled:
            ts.set_token(token)  # type: ignore[union-attr]
            self._pro = ts.pro_api()  # type: ignore[union-attr]
        else:
            self._pro = None

    def _retry(self, fn: Callable[..., pd.DataFrame], **kw: Any) -> pd.DataFrame:
        if not self.enabled or self._pro is None:
            return pd.DataFrame()
        last: Optional[Exception] = None
        for _ in range(self._max_retries + 1):
            try:
                self._rl.wait()
                df = fn(**kw)
                if df is None:
                    return pd.DataFrame()
                return df
            except Exception as e:  # pragma: no cover
                last = e
                time.sleep(0.4)
        raise RuntimeError(f"Tushare call failed: {last}")

    def fetch_index_classify(
        self,
        index_code: str = "",
        level: str = "",
        parent_code: str = "",
        src: str = "SW2021",
    ) -> pd.DataFrame:
        """Tushare ``index_classify``。申万常用 ``src=SW2021`` 或 ``SW2014``。"""
        if not self.enabled or self._pro is None:
            return pd.DataFrame()
        kw = _drop_empty_str_params(
            index_code=index_code,
            level=level,
            parent_code=parent_code,
            src=src,
        )
        return self._retry(self._pro.index_classify, **kw)  # type: ignore[attr-defined]

    def fetch_index_member_all(
        self,
        l1_code: str = "",
        l2_code: str = "",
        l3_code: str = "",
        ts_code: str = "",
        is_new: str = "",
    ) -> pd.DataFrame:
        """
        单页 ``index_member_all``。全量请用 ``fetch_index_member_all_paged``。
        """
        if not self.enabled or self._pro is None:
            return pd.DataFrame()
        kw = _drop_empty_str_params(
            l1_code=l1_code,
            l2_code=l2_code,
            l3_code=l3_code,
            ts_code=ts_code,
            is_new=is_new,
        )
        return self._retry(self._pro.index_member_all, **kw)  # type: ignore[attr-defined]

    def fetch_index_member_all_paged(
        self,
        l1_code: str = "",
        l2_code: str = "",
        l3_code: str = "",
        ts_code: str = "",
        is_new: str = "",
    ) -> pd.DataFrame:
        """offset 分页拉满当前筛选条件下的成分表。"""
        if not self.enabled or self._pro is None:
            return pd.DataFrame()
        chunks: List[pd.DataFrame] = []
        offset = 0
        while True:
            if not self.enabled or self._pro is None:
                break
            kw = _drop_empty_str_params(
                l1_code=l1_code,
                l2_code=l2_code,
                l3_code=l3_code,
                ts_code=ts_code,
                is_new=is_new,
            )
            kw["offset"] = str(offset)
            kw["limit"] = str(self._MEMBER_PAGE)
            df = self._retry(self._pro.index_member_all, **kw)  # type: ignore[attr-defined]
            if df is None or df.empty:
                break
            chunks.append(df)
            if len(df) < self._MEMBER_PAGE:
                break
            offset += self._MEMBER_PAGE
        if not chunks:
            return pd.DataFrame()
        return pd.concat(chunks, ignore_index=True)

    def fetch_sw_daily(
        self,
        ts_code: str = "",
        trade_date: str = "",
        start_date: str = "",
        end_date: str = "",
        offset: str = "0",
        limit: str = "2000",
    ) -> pd.DataFrame:
        """单页 ``sw_daily``。"""
        if not self.enabled or self._pro is None:
            return pd.DataFrame()
        kw = _drop_empty_str_params(
            ts_code=ts_code,
            trade_date=trade_date,
            start_date=start_date,
            end_date=end_date,
        )
        kw["offset"] = offset
        kw["limit"] = limit
        return self._retry(self._pro.sw_daily, **kw)  # type: ignore[attr-defined]

    def fetch_sw_daily_by_trade_date_all(self, trade_date: str) -> pd.DataFrame:
        """
        按 **单交易日** 拉取当日全部申万行业指数行情（多页 offset 直至无数据）。
        ``trade_date``: YYYYMMDD
        """
        if not self.enabled or self._pro is None or not str(trade_date).strip():
            return pd.DataFrame()
        parts: List[pd.DataFrame] = []
        offset = 0
        while True:
            df = self.fetch_sw_daily(
                trade_date=trade_date.replace("-", "")[:8],
                offset=str(offset),
                limit=str(self._SW_DAILY_PAGE),
            )
            if df is None or df.empty:
                break
            parts.append(df)
            if len(df) < self._SW_DAILY_PAGE:
                break
            offset += self._SW_DAILY_PAGE
        if not parts:
            return pd.DataFrame()
        return pd.concat(parts, ignore_index=True)

    def fetch_sw_daily_for_index_range(
        self, ts_code: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        """单只申万行业指数 ``ts_code`` 在日期区间内的日线，分页拉取。"""
        if not self.enabled or not ts_code:
            return pd.DataFrame()
        parts: List[pd.DataFrame] = []
        offset = 0
        while True:
            df = self.fetch_sw_daily(
                ts_code=ts_code,
                start_date=start_date.replace("-", "")[:8],
                end_date=end_date.replace("-", "")[:8],
                offset=str(offset),
                limit=str(self._SW_DAILY_PAGE),
            )
            if df is None or df.empty:
                break
            parts.append(df)
            if len(df) < self._SW_DAILY_PAGE:
                break
            offset += self._SW_DAILY_PAGE
        if not parts:
            return pd.DataFrame()
        return pd.concat(parts, ignore_index=True)


__all__ = ["SwIndustryAdapter"]
