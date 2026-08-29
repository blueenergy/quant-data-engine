"""Persist Tushare HK / US equity into market-specific Mongo collections.

A-share tables (stock_info / volume_price / trade_calendar) stay untouched:
HK and US calendars diverge from SSE, and mixing them would trip A-share
coverage contracts (min_symbols_latest_day, unique trade_date, etc.).
"""
from __future__ import annotations

import logging
import math
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence

import pandas as pd
from pymongo import MongoClient, UpdateOne

logger = logging.getLogger(__name__)

MARKETS: Dict[str, Dict[str, str]] = {
    "hk": {
        "info": "hk_stock_info",
        "calendar": "hk_trade_calendar",
        "daily": "hk_volume_price",
        "adj": "hk_adj_factor",
        "exchange": "HKEX",
        "source_basic": "tushare_hk_basic",
        "source_cal": "tushare_hk_tradecal",
        "source_daily": "tushare_hk_daily",
        "source_adj": "tushare_hk_daily_adj",
    },
    "us": {
        "info": "us_stock_info",
        "calendar": "us_trade_calendar",
        "daily": "us_volume_price",
        "adj": "us_adj_factor",
        "exchange": "US",
        "source_basic": "tushare_us_basic",
        "source_cal": "tushare_us_tradecal",
        "source_daily": "tushare_us_daily",
        "source_adj": "tushare_us_daily_adj",
    },
}


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, (list, dict, tuple, set)):
        return False
    try:
        return bool(pd.isna(value))
    except (ValueError, TypeError):
        return False


def _clean_value(value: Any) -> Any:
    if _is_missing(value):
        return None
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    if hasattr(value, "item") and not isinstance(value, (bytes, str)):
        try:
            return value.item()
        except Exception:
            return value
    return value


def _float_or_none(value: Any) -> Optional[float]:
    cleaned = _clean_value(value)
    if cleaned is None:
        return None
    try:
        return float(cleaned)
    except (TypeError, ValueError):
        return None


def _int_or_none(value: Any) -> Optional[int]:
    cleaned = _clean_value(value)
    if cleaned is None:
        return None
    try:
        return int(float(cleaned))
    except (TypeError, ValueError):
        return None


class OverseasEquityRepository:
    def __init__(self, mongo_uri: str, data_db: str, market: str):
        if market not in MARKETS:
            raise ValueError(f"unknown market {market!r}; expected hk|us")
        self.market = market
        self.spec = MARKETS[market]
        self._client = MongoClient(mongo_uri)
        db = self._client[data_db]
        self.info_col = db[self.spec["info"]]
        self.calendar_col = db[self.spec["calendar"]]
        self.daily_col = db[self.spec["daily"]]
        self.adj_col = db[self.spec["adj"]]
        self._ensure_indexes()

    def _ensure_indexes(self) -> None:
        try:
            self.info_col.create_index([("ts_code", 1)], unique=True, background=True)
            self.info_col.create_index([("symbol", 1)], background=True)
            self.info_col.create_index([("name", 1)], background=True)
            self.info_col.create_index([("pinyin", 1)], sparse=True, background=True)
            self.calendar_col.create_index([("cal_date", 1)], unique=True, background=True)
            self.daily_col.create_index(
                [("symbol", 1), ("trade_date", -1)], unique=True, background=True
            )
            self.daily_col.create_index([("trade_date", -1)], background=True)
            self.adj_col.create_index(
                [("symbol", 1), ("trade_date", -1)], unique=True, background=True
            )
        except Exception as e:  # pragma: no cover
            logger.warning("%s index ensure: %s", self.market, e)

    def upsert_basic(self, df: Optional[pd.DataFrame]) -> int:
        if df is None or df.empty or "ts_code" not in df.columns:
            return 0
        now = datetime.utcnow()
        ops: List[UpdateOne] = []
        for raw in df.to_dict("records"):
            ts_code = str(_clean_value(raw.get("ts_code")) or "").strip()
            if not ts_code:
                continue
            doc = {k: _clean_value(v) for k, v in raw.items() if k != "_id"}
            doc["ts_code"] = ts_code
            doc["symbol"] = ts_code
            doc["market"] = self.market.upper()
            spell = str(doc.get("cn_spell") or "").strip()
            if spell and not doc.get("pinyin"):
                doc["pinyin"] = spell.upper()
            doc["source"] = self.spec["source_basic"]
            doc["updated_at"] = now
            ops.append(UpdateOne({"ts_code": ts_code}, {"$set": doc}, upsert=True))
        return self._bulk(self.info_col, ops)

    def upsert_calendar(self, df: Optional[pd.DataFrame]) -> int:
        if df is None or df.empty:
            return 0
        date_col = "cal_date" if "cal_date" in df.columns else "trade_date"
        if date_col not in df.columns:
            return 0
        now = datetime.utcnow()
        ops: List[UpdateOne] = []
        for raw in df.to_dict("records"):
            cal_date = str(_clean_value(raw.get(date_col)) or "").replace("-", "").strip()
            if len(cal_date) != 8:
                continue
            is_open = _int_or_none(raw.get("is_open"))
            pretrade = str(_clean_value(raw.get("pretrade_date")) or "").replace("-", "").strip()
            doc = {
                "cal_date": cal_date,
                "trade_date": cal_date,
                "is_open": is_open,
                "pretrade_date": pretrade or None,
                "exchange": self.spec["exchange"],
                "source": self.spec["source_cal"],
                "updated_at": now,
            }
            ops.append(UpdateOne({"cal_date": cal_date}, {"$set": doc}, upsert=True))
        return self._bulk(self.calendar_col, ops)

    def upsert_daily(self, df: Optional[pd.DataFrame], *, source: Optional[str] = None) -> int:
        if df is None or df.empty or "ts_code" not in df.columns:
            return 0
        now = datetime.now().strftime("%Y%m%d %H:%M:%S")
        src = source or self.spec["source_daily"]
        ops: List[UpdateOne] = []
        for raw in df.to_dict("records"):
            ts_code = str(_clean_value(raw.get("ts_code")) or "").strip()
            trade_date = str(_clean_value(raw.get("trade_date")) or "").replace("-", "").strip()
            if not ts_code or len(trade_date) != 8:
                continue
            vol = raw.get("volume", raw.get("vol"))
            pct = raw.get("pct_chg", raw.get("pct_change"))
            doc = {
                "symbol": ts_code,
                "ts_code": ts_code,
                "trade_date": trade_date,
                "open": _float_or_none(raw.get("open")),
                "high": _float_or_none(raw.get("high")),
                "low": _float_or_none(raw.get("low")),
                "close": _float_or_none(raw.get("close")),
                "pre_close": _float_or_none(raw.get("pre_close")),
                "change": _float_or_none(raw.get("change")),
                "pct_chg": _float_or_none(pct),
                "volume": _int_or_none(vol) or 0,
                "amount": _float_or_none(raw.get("amount")),
                "vwap": _float_or_none(raw.get("vwap")),
                "update_time": now,
                "source": src,
            }
            ops.append(
                UpdateOne({"symbol": ts_code, "trade_date": trade_date}, {"$set": doc}, upsert=True)
            )
        return self._bulk(self.daily_col, ops)

    def daily_counts_by_date(self, dates: Sequence[str]) -> Dict[str, int]:
        """Row counts in the daily collection for each YYYYMMDD in ``dates``."""
        wanted = []
        for raw in dates:
            day = str(raw or "").replace("-", "").strip()
            if len(day) == 8:
                wanted.append(day)
        if not wanted:
            return {}
        out = {d: 0 for d in wanted}
        try:
            for row in self.daily_col.aggregate(
                [
                    {"$match": {"trade_date": {"$in": wanted}}},
                    {"$group": {"_id": "$trade_date", "n": {"$sum": 1}}},
                ]
            ):
                key = str(row.get("_id") or "")
                if key in out:
                    out[key] = int(row.get("n") or 0)
        except Exception as e:  # pragma: no cover
            logger.warning("%s daily_counts_by_date: %s", self.market, e)
        return out

    def upsert_adj(self, df: Optional[pd.DataFrame], *, source: Optional[str] = None) -> int:
        if df is None or df.empty or "ts_code" not in df.columns:
            return 0
        now = datetime.now().strftime("%Y%m%d %H:%M:%S")
        src = source or self.spec["source_adj"]
        ops: List[UpdateOne] = []
        for raw in df.to_dict("records"):
            ts_code = str(_clean_value(raw.get("ts_code")) or "").strip()
            trade_date = str(_clean_value(raw.get("trade_date")) or "").replace("-", "").strip()
            factor = raw.get("adj_factor", raw.get("cum_adjfactor"))
            if not ts_code or len(trade_date) != 8:
                continue
            doc = {
                "symbol": ts_code,
                "ts_code": ts_code,
                "trade_date": trade_date,
                "adj_factor": _float_or_none(factor),
                "update_time": now,
                "source": src,
            }
            ops.append(
                UpdateOne({"symbol": ts_code, "trade_date": trade_date}, {"$set": doc}, upsert=True)
            )
        return self._bulk(self.adj_col, ops)

    @staticmethod
    def _bulk(col, ops: Sequence[UpdateOne]) -> int:
        if not ops:
            return 0
        res = col.bulk_write(list(ops), ordered=False)
        return int((res.upserted_count or 0) + (res.modified_count or 0))
