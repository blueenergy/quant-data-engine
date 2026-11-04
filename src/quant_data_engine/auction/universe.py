"""Symbol-universe helpers shared by live capture and first-board scoring."""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


def normalize_symbol(raw: str) -> str:
    """Return an upper-case ``CODE.MARKET`` symbol; infer the market when absent."""
    text = str(raw or "").strip().upper()
    if not text:
        return ""
    if "." in text:
        return text
    if text.startswith(("6", "5", "9")):
        return text + ".SH"
    if text.startswith(("0", "3")):
        return text + ".SZ"
    if text.startswith(("4", "8")):
        return text + ".BJ"
    return text


def latest_limit_up_date(db: Any, before: str) -> str | None:
    """Latest ``daily_limit_up.date`` strictly before ``before``."""
    doc = db.daily_limit_up.find_one(
        {"date": {"$lt": str(before)}},
        {"_id": 0, "date": 1},
        sort=[("date", -1)],
    )
    return str(doc["date"]) if doc and doc.get("date") else None


def load_first_board_pool_with_date(db: Any, trade_date: str) -> tuple[str | None, list[str]]:
    """Yesterday's first-board limit-ups plus the date they were selected on.

    First board = ``streak == 1`` (limit-up yesterday, not the day before).
    If ``streak`` is unavailable, fall back to all sealed limit-ups minus the
    previous day's limit-ups.
    """
    latest_date = latest_limit_up_date(db, trade_date)
    if not latest_date:
        return None, []
    pool = set(db.daily_limit_up.distinct(
        "symbol", {"date": latest_date, "status": "limit_up", "streak": 1}
    ))
    if not pool:
        pool = set(db.daily_limit_up.distinct(
            "symbol", {"date": latest_date, "status": "limit_up"}
        ))
    if not pool:
        pool = set(db.daily_limit_up.distinct("symbol", {"date": latest_date}))
    prev_date = latest_limit_up_date(db, latest_date)
    if prev_date:
        prev_pool = set(db.daily_limit_up.distinct("symbol", {"date": prev_date}))
        first = pool - prev_pool
        if first:
            logger.info(
                "limit-up pool %s: %d first-board (of %d) after excluding %s",
                latest_date, len(first), len(pool), prev_date,
            )
            return latest_date, sorted(first)
    logger.info("limit-up pool %s: %d symbols (no prev-day exclusion)", latest_date, len(pool))
    return latest_date, sorted(pool)


def load_first_board_pool(db: Any, trade_date: str) -> list[str]:
    """Yesterday's first-board limit-ups (symbols only)."""
    _, pool = load_first_board_pool_with_date(db, trade_date)
    return pool
