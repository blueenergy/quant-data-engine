"""Trading-day helpers backed by Mongo ``trade_calendar``."""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Callable
from zoneinfo import ZoneInfo

CN_TZ = ZoneInfo("Asia/Shanghai")
logger = logging.getLogger(__name__)


class MongoTradeCalendarChecker:
    """Return True when ``trade_date`` exists in ``quant_data.trade_calendar``."""

    def __init__(self, data_db: Any, *, collection_name: str = "trade_calendar") -> None:
        self.collection = data_db[collection_name]

    def is_trading_day(self, now: datetime) -> bool:
        if now.tzinfo is None:
            now = now.replace(tzinfo=CN_TZ)
        else:
            now = now.astimezone(CN_TZ)
        if now.weekday() >= 5:
            return False
        trade_date = now.strftime("%Y%m%d")
        try:
            doc = self.collection.find_one({"trade_date": trade_date}, {"_id": 1})
        except Exception as exc:
            logger.warning("trade_calendar lookup failed for %s: %s", trade_date, exc)
            return True
        if doc is None:
            logger.warning("trade_calendar missing %s; treating as non-trading day", trade_date)
            return False
        return True


def make_trading_day_checker(data_db: Any | None) -> Callable[[datetime], bool] | None:
    if data_db is None:
        return None
    return MongoTradeCalendarChecker(data_db).is_trading_day
