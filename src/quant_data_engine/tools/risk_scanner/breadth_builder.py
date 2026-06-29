"""Reusable builder for daily A-share breadth atomic metrics.

Encapsulates the volume_price / stock_info / trade_cal loading plus the
``compute_breadth_table`` call so that both the standalone pre-aggregation task
(``run_market_breadth_daily``) and the regime scanner share a single
implementation and can never diverge in methodology.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import pandas as pd

from .breadth_signals import compute_breadth_table
from .instruments import BREADTH_THRESHOLDS

logger = logging.getLogger(__name__)


def _offset_date(date_str: str, days: int) -> str:
    return (datetime.strptime(date_str, "%Y%m%d") + timedelta(days=days)).strftime("%Y%m%d")


class MarketBreadthBuilder:
    """Builds one ``market_breadth_daily`` row for a given trade date.

    Holds a sliding per-date cache of ``volume_price`` frames so that backfill
    over a contiguous date range only loads one additional trading day per step.
    """

    def __init__(self, repo, *, lookback_calendar_days: int = 700):
        self.repo = repo
        self.db = repo.db
        self.lookback_calendar_days = lookback_calendar_days
        self._stock_daily_cache: Dict[str, pd.DataFrame] = {}
        self._stock_info_cache: Optional[pd.DataFrame] = None

    def build(self, trade_date: str, *, warnings: Optional[List[str]] = None) -> Dict[str, object]:
        """Compute breadth atomic metrics for *trade_date*.

        Returns the breadth document ready for ``repo.upsert_breadth``.
        """
        if warnings is None:
            warnings = []
        lookback_trading_days = int(BREADTH_THRESHOLDS["breadth_lookback_trading_days"])
        exclude_ipo_trading_days = int(BREADTH_THRESHOLDS["breadth_exclude_ipo_trading_days"])
        start_date = _offset_date(trade_date, -self.lookback_calendar_days)
        recent_trade_dates = self._load_recent_trade_dates(
            trade_date,
            lookback_trading_days + 210,
        )
        if recent_trade_dates:
            start_date = recent_trade_dates[0]

        stock_daily = self._load_stock_daily(recent_trade_dates or (start_date, trade_date), trade_date)
        stock_info = self._load_stock_info()
        trade_cal = (
            self._load_trade_cal_from_dates(recent_trade_dates)
            if recent_trade_dates
            else self._load_trade_cal_from_daily(stock_daily)
        )
        breadth_today = compute_breadth_table(
            stock_daily,
            stock_info,
            trade_date,
            trade_cal,
            lookback_trading_days=lookback_trading_days,
            exclude_ipo_trading_days=exclude_ipo_trading_days,
            warnings=warnings,
        )
        return dict(breadth_today)

    def _load_stock_daily(self, start_date_or_dates, end_date: str) -> pd.DataFrame:
        if isinstance(start_date_or_dates, list):
            requested_dates = list(dict.fromkeys(str(date) for date in start_date_or_dates))
            missing_dates = [date for date in requested_dates if date not in self._stock_daily_cache]
            if missing_dates:
                docs = list(self.db["volume_price"].find(
                    {"trade_date": {"$in": missing_dates}},
                    {"_id": 0, "symbol": 1, "ts_code": 1, "trade_date": 1, "pct_chg": 1, "close": 1, "high": 1, "low": 1},
                ).sort("trade_date", 1))
                loaded = pd.DataFrame(docs)
                if not loaded.empty:
                    for date, frame in loaded.groupby("trade_date", sort=False):
                        self._stock_daily_cache[str(date)] = frame.reset_index(drop=True)
                for date in missing_dates:
                    self._stock_daily_cache.setdefault(date, pd.DataFrame())
            if requested_dates:
                first_date = requested_dates[0]
                for cached_date in list(self._stock_daily_cache):
                    if cached_date < first_date:
                        del self._stock_daily_cache[cached_date]
            frames = [self._stock_daily_cache[date] for date in requested_dates if not self._stock_daily_cache[date].empty]
            return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        else:
            query = {"trade_date": {"$gte": start_date_or_dates, "$lte": end_date}}
            docs = list(self.db["volume_price"].find(
                query,
                {"_id": 0, "symbol": 1, "ts_code": 1, "trade_date": 1, "pct_chg": 1, "close": 1, "high": 1, "low": 1},
            ).sort("trade_date", 1))
            return pd.DataFrame(docs)

    def _load_recent_trade_dates(self, trade_date: str, count: int) -> List[str]:
        docs = list(self.db["index_prices"].find(
            {"ts_code": str(BREADTH_THRESHOLDS["hindenburg_benchmark"]), "trade_date": {"$lte": trade_date}},
            {"_id": 0, "trade_date": 1},
        ).sort("trade_date", -1).limit(count))
        return sorted(str(doc["trade_date"]) for doc in docs if doc.get("trade_date"))

    def _load_stock_info(self) -> pd.DataFrame:
        if self._stock_info_cache is not None:
            return self._stock_info_cache.copy()
        docs = list(self.db["stock_info"].find(
            {},
            {"_id": 0, "symbol": 1, "ts_code": 1, "name": 1, "list_date": 1, "list_status": 1, "delist_date": 1},
        ))
        self._stock_info_cache = pd.DataFrame(docs)
        return self._stock_info_cache.copy()

    @staticmethod
    def _load_trade_cal_from_daily(stock_daily: pd.DataFrame) -> pd.DataFrame:
        if stock_daily is None or stock_daily.empty or "trade_date" not in stock_daily.columns:
            return pd.DataFrame(columns=["cal_date", "is_open"])
        dates = sorted(stock_daily["trade_date"].astype(str).str.replace("-", "", regex=False).unique())
        return pd.DataFrame({"cal_date": dates, "is_open": 1})

    @staticmethod
    def _load_trade_cal_from_dates(dates: List[str]) -> pd.DataFrame:
        return pd.DataFrame({"cal_date": dates, "is_open": 1})


__all__ = ["MarketBreadthBuilder"]
