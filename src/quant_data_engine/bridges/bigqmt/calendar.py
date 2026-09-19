"""Trading-day helpers backed by Mongo ``trade_calendar``."""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
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


def make_range_trading_day_checker(
    data_db: Any | None,
    start: str,
    end: str,
    *,
    collection_name: str = "trade_calendar",
    now: datetime | None = None,
) -> tuple[Callable[[datetime], bool] | None, tuple[str, ...]]:
    """Return ``(checker, open_days)`` for ``[start, end]`` from one query.

    :meth:`MongoTradeCalendarChecker.is_trading_day` issues one ``find_one`` per
    call, so enumerating a 50-day window per symbol would be thousands of
    queries. This reads the whole window once and answers from a set.

    ``trade_calendar`` stores open days only, so set membership *is* the answer
    and ``is_open`` needs no test. A missing handle or an empty calendar yields
    ``(None, ())`` — callers then fall back to weekday-only filtering rather
    than seeing every day as closed.

    The returned days are clamped to sessions that can have a **complete** bar
    set: never later than today, and today itself only once the session has
    closed. ``trade_calendar`` is populated through 20261231, so without this a
    window ending today (or in the future) would report every remaining day as
    missing, and running before the close would condemn today for every symbol.
    """
    if data_db is None:
        return None, ()
    now = now or datetime.now(CN_TZ)
    if now.tzinfo is None:
        now = now.replace(tzinfo=CN_TZ)
    else:
        now = now.astimezone(CN_TZ)
    today = now.strftime("%Y%m%d")
    # 15:05 leaves room for the closing bar to land and be written.
    last_settled = today if now.strftime("%H%M") >= "1505" else ""
    upper = min(str(end), today)
    if upper == today and not last_settled:
        upper = _previous_day(today)
    if upper < str(start):
        return None, ()
    try:
        cursor = data_db[collection_name].find(
            {"trade_date": {"$gte": str(start), "$lte": upper}},
            {"trade_date": 1, "_id": 0},
        )
        # YYYYMMDD only; a malformed entry would otherwise make every symbol a
        # permanent gap and put the whole universe on repeat downloads.
        days = tuple(
            sorted(
                day
                for day in {str(doc.get("trade_date") or "") for doc in cursor}
                if len(day) == 8 and day.isdigit()
            )
        )
    except Exception as exc:
        logger.warning("trade_calendar range lookup %s..%s failed: %s", start, end, exc)
        return None, ()
    if not days:
        return None, ()
    day_set = frozenset(days)

    def checker(when: datetime) -> bool:
        if when.tzinfo is not None:
            when = when.astimezone(CN_TZ)
        return when.strftime("%Y%m%d") in day_set

    return checker, days


def _previous_day(ymd: str) -> str:
    return (datetime.strptime(ymd, "%Y%m%d") - timedelta(days=1)).strftime("%Y%m%d")
