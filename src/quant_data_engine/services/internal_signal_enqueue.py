"""Enqueue internal_signal_refresh tasks after financial data sync."""

from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence

from pymongo import MongoClient

logger = logging.getLogger(__name__)

ENABLE_ENV = "ENABLE_INTERNAL_SIGNAL_ENQUEUE"
PRIORITY_ENV = "INTERNAL_SIGNAL_ENQUEUE_PRIORITY"
BATCH_ENV = "INTERNAL_SIGNAL_ENQUEUE_BATCH_SIZE"


def enqueue_enabled() -> bool:
    return os.getenv(ENABLE_ENV, "false").lower() in {"1", "true", "yes"}


def _priority() -> int:
    try:
        return int(os.getenv(PRIORITY_ENV, "3"))
    except (TypeError, ValueError):
        return 3


def _batch_size() -> int:
    try:
        return max(1, int(os.getenv(BATCH_ENV, "200")))
    except (TypeError, ValueError):
        return 200


def _normalize_symbols(symbols: Iterable[str]) -> List[str]:
    seen = set()
    ordered: List[str] = []
    for raw in symbols:
        symbol = str(raw or "").strip().upper()
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        ordered.append(symbol)
    return ordered


def enqueue_internal_signal_refresh(
    finance_db,
    symbols: Sequence[str],
    *,
    source: str = "financial_sync",
    score_date: Optional[str] = None,
    priority: Optional[int] = None,
) -> Dict[str, Any]:
    """Insert low-priority internal_signal_refresh tasks for affected symbols."""

    normalized = _normalize_symbols(symbols)
    if not normalized:
        return {"enqueued": 0, "skipped": 0, "symbols": []}

    col = finance_db["analysis_tasks"]
    as_of = score_date or datetime.utcnow().strftime("%Y%m%d")
    task_priority = priority if priority is not None else _priority()
    enqueued = 0
    skipped = 0
    inserted_ids: List[str] = []

    for batch_start in range(0, len(normalized), _batch_size()):
        batch = normalized[batch_start : batch_start + _batch_size()]
        pending = {
            symbol: col.find_one(
                {
                    "type": "internal_signal_refresh",
                    "params.symbol": symbol,
                    "status": {"$in": ["pending", "processing"]},
                },
                {"_id": 1},
            )
            for symbol in batch
        }
        docs = []
        for symbol in batch:
            if pending.get(symbol):
                skipped += 1
                continue
            docs.append(
                {
                    "type": "internal_signal_refresh",
                    "task_type": "internal_signal_refresh",
                    "status": "pending",
                    "priority": task_priority,
                    "user_id": "system",
                    "params": {
                        "symbol": symbol,
                        "score_date": as_of,
                        "force": False,
                        "source": source,
                    },
                    "source": source,
                    "created_at": datetime.utcnow(),
                }
            )
        if docs:
            result = col.insert_many(docs)
            enqueued += len(result.inserted_ids)
            inserted_ids.extend(str(task_id) for task_id in result.inserted_ids)

    logger.info(
        "internal_signal_refresh enqueue complete: enqueued=%s skipped=%s source=%s",
        enqueued,
        skipped,
        source,
    )
    return {
        "enqueued": enqueued,
        "skipped": skipped,
        "symbols": normalized,
        "task_ids": inserted_ids,
    }


def enqueue_after_financial_sync(
    settings,
    affected_ts_codes: Sequence[str],
    *,
    score_date: Optional[str] = None,
) -> Dict[str, Any]:
    if not enqueue_enabled():
        logger.info("internal_signal_refresh enqueue disabled (%s)", ENABLE_ENV)
        return {"enabled": False, "enqueued": 0, "skipped": 0}
    client = MongoClient(settings.mongo_uri)
    finance_db = client[os.getenv("DB_NAME", "finance")]
    result = enqueue_internal_signal_refresh(
        finance_db,
        affected_ts_codes,
        source="financial_sync",
        score_date=score_date,
    )
    return {"enabled": True, **result}


__all__ = [
    "enqueue_after_financial_sync",
    "enqueue_enabled",
    "enqueue_internal_signal_refresh",
]
