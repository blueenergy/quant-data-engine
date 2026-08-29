"""Ingest Tushare HK / US equity: universe, calendar, daily OHLCV, adj factors.

Daily interfaces are paged until an empty page, except HK ``hk_daily`` which
is a single by-date call (listed HK ~2700 < 5000 row cap). That interface is
extremely rate-limited on the current token (1/min, then 1/hour), so this
pipeline: one call per date, no ``hk_daily_adj`` unless asked, skip dates
already in Mongo, and sleep ``min_interval_sec`` *between* days (never
before the first call).
"""
from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Sequence

import pandas as pd

from quant_data_engine.repositories.overseas_equity_repository import OverseasEquityRepository

logger = logging.getLogger(__name__)

_PAGE_LIMIT = 6000
_MAX_PAGES = 80
_HK_DAILY_PAGE_LIMIT = 5000
_HK_BASIC_STATUSES = ("L", "D")
_PERMISSION_MARKERS = ("没有接口", "访问权限")
_RATE_MARKERS = ("频率超限",)


class OverseasPermissionError(RuntimeError):
    """Tushare rejected the interface (paid / points gate)."""


class OverseasRateLimitError(RuntimeError):
    """Tushare rate-limited the interface."""


def classify_tushare_error(exc: BaseException) -> str:
    text = str(exc)
    if any(m in text for m in _PERMISSION_MARKERS):
        return "permission"
    if any(m in text for m in _RATE_MARKERS):
        return "rate_limit"
    return "error"


def _year_chunks(start: str, end: str) -> List[tuple]:
    s = datetime.strptime(start, "%Y%m%d")
    e = datetime.strptime(end, "%Y%m%d")
    out = []
    year = s.year
    while year <= e.year:
        chunk_start = max(s, datetime(year, 1, 1)).strftime("%Y%m%d")
        chunk_end = min(e, datetime(year, 12, 31)).strftime("%Y%m%d")
        out.append((chunk_start, chunk_end))
        year += 1
    return out


class OverseasEquityPipeline:
    def __init__(
        self,
        adapter,
        repository: OverseasEquityRepository,
        page_limit: int = _PAGE_LIMIT,
        max_pages: int = _MAX_PAGES,
        min_interval_sec: float = 0.0,
    ):
        self._adapter = adapter
        self._repo = repository
        self.market = repository.market
        self._page_limit = page_limit
        self._max_pages = max_pages
        self._min_interval_sec = max(float(min_interval_sec or 0.0), 0.0)
        self._adj_denied = False

    @property
    def enabled(self) -> bool:
        return bool(getattr(self._adapter, "enabled", False))

    def _fetch_all(self, fetch_fn: Callable, **params) -> Optional[pd.DataFrame]:
        frames: List[pd.DataFrame] = []
        offset = 0
        for _ in range(self._max_pages):
            try:
                df = fetch_fn(offset=offset, limit=self._page_limit, **params)
            except Exception as e:
                self._raise_classified(e)
            if df is None or getattr(df, "empty", True):
                break
            frames.append(df)
            offset += len(df)
            if len(df) < self._page_limit:
                break
        else:
            logger.warning(
                "%s.%s hit max_pages=%d; result may be truncated",
                self.market,
                getattr(fetch_fn, "__name__", "fetch"),
                self._max_pages,
            )
        if not frames:
            return None
        return pd.concat(frames, ignore_index=True)

    @staticmethod
    def _raise_classified(exc: BaseException) -> None:
        kind = classify_tushare_error(exc)
        if kind == "permission":
            raise OverseasPermissionError(str(exc)) from exc
        if kind == "rate_limit":
            raise OverseasRateLimitError(str(exc)) from exc
        raise exc

    def run_basic(self) -> Dict[str, Any]:
        stats = {"records": 0, "status": "success", "pages": 0}
        if not self.enabled:
            stats["status"] = "skipped"
            stats["reason"] = "adapter_disabled"
            return stats
        try:
            if self.market == "hk":
                frames = []
                for status in _HK_BASIC_STATUSES:
                    df = self._adapter.fetch_hk_basic(list_status=status)
                    if df is not None and not df.empty:
                        frames.append(df)
                df = pd.concat(frames, ignore_index=True) if frames else None
            else:
                df = self._fetch_all(self._adapter.fetch_us_basic)
            stats["records"] = self._repo.upsert_basic(df)
            stats["pages"] = 0 if df is None else 1
            logger.info("[%s_basic] upserted %s rows", self.market, stats["records"])
        except OverseasPermissionError as e:
            stats["status"] = "failed"
            stats["reason"] = "permission_denied"
            stats["error"] = str(e)
            logger.error("[%s_basic] permission denied: %s", self.market, e)
        except Exception as e:
            kind = classify_tushare_error(e)
            if kind == "permission":
                stats["status"] = "failed"
                stats["reason"] = "permission_denied"
            else:
                stats["status"] = "failed"
                stats["reason"] = kind
            stats["error"] = str(e)
            logger.error("[%s_basic] failed: %s", self.market, e)
        return stats

    def run_tradecal(self, start_date: str, end_date: str) -> Dict[str, Any]:
        stats = {"records": 0, "status": "success", "chunks": 0, "start": start_date, "end": end_date}
        if not self.enabled:
            stats["status"] = "skipped"
            stats["reason"] = "adapter_disabled"
            return stats
        fetch = self._adapter.fetch_hk_tradecal if self.market == "hk" else self._adapter.fetch_us_tradecal
        try:
            frames = []
            for chunk_start, chunk_end in _year_chunks(start_date, end_date):
                df = fetch(start_date=chunk_start, end_date=chunk_end)
                stats["chunks"] += 1
                if df is not None and not df.empty:
                    frames.append(df)
            df = pd.concat(frames, ignore_index=True) if frames else None
            stats["records"] = self._repo.upsert_calendar(df)
            logger.info(
                "[%s_tradecal %s..%s] upserted %s rows",
                self.market, start_date, end_date, stats["records"],
            )
        except Exception as e:
            kind = classify_tushare_error(e)
            stats["status"] = "failed"
            stats["reason"] = "permission_denied" if kind == "permission" else kind
            stats["error"] = str(e)
            logger.error("[%s_tradecal] failed: %s", self.market, e)
        return stats

    def _fetch_daily_for_date(self, trade_date: str, *, want_adj: bool = True) -> tuple:
        """Return (daily_df, adj_df, daily_source). adj_df may be None."""
        if self.market == "hk":
            daily_fn = self._adapter.fetch_hk_daily
            adj_fn = self._adapter.fetch_hk_daily_adj
            daily_source = "tushare_hk_daily"
            adj_source = "tushare_hk_daily_adj"
        else:
            daily_fn = self._adapter.fetch_us_daily
            adj_fn = self._adapter.fetch_us_daily_adj
            daily_source = "tushare_us_daily"
            adj_source = "tushare_us_daily_adj"

        daily_df = None
        daily_err = None
        try:
            if self.market == "hk":
                # One call: listed HK ~2700, API cap 5000. A second page would
                # consume the hourly hk_daily slot.
                daily_df = daily_fn(trade_date=trade_date, limit=_HK_DAILY_PAGE_LIMIT)
            else:
                daily_df = self._fetch_all(daily_fn, trade_date=trade_date)
        except OverseasPermissionError as e:
            daily_err = e
            logger.warning("[%s_daily %s] no permission; will try daily_adj", self.market, trade_date)
        except OverseasRateLimitError:
            raise
        except Exception as e:
            try:
                self._raise_classified(e)
            except OverseasPermissionError as perm:
                daily_err = perm
                logger.warning("[%s_daily %s] no permission; will try daily_adj", self.market, trade_date)

        adj_df = None
        if want_adj and not self._adj_denied:
            try:
                adj_df = self._fetch_all(adj_fn, trade_date=trade_date)
            except OverseasPermissionError:
                self._adj_denied = True
                logger.info("[%s_daily_adj %s] no permission; skipping adj for later dates", self.market, trade_date)

        if daily_df is None or getattr(daily_df, "empty", True):
            if adj_df is not None and not adj_df.empty:
                return adj_df, adj_df, adj_source
            if daily_err is not None:
                raise daily_err
        return daily_df, adj_df, daily_source

    def run_daily(
        self,
        dates: Sequence[str],
        *,
        want_adj: bool = True,
        skip_complete_min_symbols: int = 0,
    ) -> Dict[str, Any]:
        stats: Dict[str, Any] = {
            "dates": 0,
            "daily_records": 0,
            "adj_records": 0,
            "status": "success",
            "failed_dates": [],
            "skipped_dates": [],
        }
        if not self.enabled:
            stats["status"] = "skipped"
            stats["reason"] = "adapter_disabled"
            return stats
        if not dates:
            stats["status"] = "skipped"
            stats["reason"] = "no_dates"
            return stats

        counts: Dict[str, int] = {}
        min_complete = max(int(skip_complete_min_symbols or 0), 0)
        if min_complete:
            getter = getattr(self._repo, "daily_counts_by_date", None)
            if callable(getter):
                counts = getter(dates) or {}

        pending = []
        for day in dates:
            if min_complete and int(counts.get(day, 0) or 0) >= min_complete:
                stats["skipped_dates"].append(day)
                logger.info(
                    "[%s_daily %s] already has %s rows (>= %s); skip",
                    self.market, day, counts.get(day, 0), min_complete,
                )
                continue
            pending.append(day)
        if not pending:
            stats["status"] = "skipped"
            stats["reason"] = "already_present"
            return stats

        for i, day in enumerate(pending):
            if i > 0 and self._min_interval_sec:
                logger.info(
                    "[%s_daily] sleeping %.0fs before %s (rate-limit guard)",
                    self.market, self._min_interval_sec, day,
                )
                time.sleep(self._min_interval_sec)
            try:
                daily_df, adj_df, source = self._fetch_daily_for_date(day, want_adj=want_adj)
                n_daily = self._repo.upsert_daily(daily_df, source=source)
                n_adj = 0
                if want_adj and adj_df is not None and not adj_df.empty:
                    n_adj = self._repo.upsert_adj(adj_df)
                stats["dates"] += 1
                stats["daily_records"] += n_daily
                stats["adj_records"] += n_adj
                logger.info("[%s_daily %s] daily=%s adj=%s", self.market, day, n_daily, n_adj)
            except OverseasPermissionError as e:
                stats["status"] = "failed"
                stats["reason"] = "permission_denied"
                stats["error"] = str(e)
                stats["failed_dates"].append(day)
                logger.error("[%s_daily] permission denied on %s: %s", self.market, day, e)
                break
            except OverseasRateLimitError as e:
                stats["status"] = "partial"
                stats["reason"] = "rate_limit"
                stats["error"] = str(e)
                stats["failed_dates"].append(day)
                logger.error(
                    "[%s_daily] rate-limited on %s: %s. "
                    "hk_daily on this token is ~1 call/min then 1/hour; "
                    "wait before re-running — extra retries lock the hourly slot.",
                    self.market, day, e,
                )
                break
            except Exception as e:
                kind = classify_tushare_error(e)
                if kind == "permission":
                    stats["status"] = "failed"
                    stats["reason"] = "permission_denied"
                    stats["error"] = str(e)
                    stats["failed_dates"].append(day)
                    break
                stats["status"] = "partial"
                stats["failed_dates"].append(day)
                logger.error("[%s_daily %s] failed: %s", self.market, day, e)
        return stats

    def open_days(self, start_date: str, end_date: str) -> List[str]:
        fetch = self._adapter.fetch_hk_tradecal if self.market == "hk" else self._adapter.fetch_us_tradecal
        frames = []
        for chunk_start, chunk_end in _year_chunks(start_date, end_date):
            df = fetch(start_date=chunk_start, end_date=chunk_end, is_open="1")
            if df is not None and not df.empty:
                frames.append(df)
        if not frames:
            return []
        cal = pd.concat(frames, ignore_index=True)
        col = "cal_date" if "cal_date" in cal.columns else "trade_date"
        if "is_open" in cal.columns:
            cal = cal[pd.to_numeric(cal["is_open"], errors="coerce") == 1]
        days = cal[col].astype(str).str.replace("-", "", regex=False).tolist()
        return sorted({d for d in days if len(d) == 8})
