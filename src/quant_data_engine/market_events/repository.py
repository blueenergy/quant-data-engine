"""MongoDB persistence for market_events collection."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from pymongo import ASCENDING, MongoClient
from pymongo.collection import Collection

logger = logging.getLogger(__name__)

COLLECTION_NAME = "market_events"


def get_collection(client: MongoClient, db_name: str) -> Collection:
    col = client[db_name][COLLECTION_NAME]
    col.create_index([("identity_key", ASCENDING)], unique=True)
    col.create_index([("event_time", ASCENDING)])
    col.create_index([("importance", ASCENDING), ("event_time", ASCENDING)])
    col.create_index([("symbol", ASCENDING), ("event_time", ASCENDING)])
    return col


def _merge_sources(existing: List[Dict[str, Any]], incoming: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out = list(existing or [])
    seen = {s.get("name") for s in out}
    for src in incoming or []:
        name = src.get("name")
        if name and name not in seen:
            out.append(src)
            seen.add(name)
    return out


def _merge_metrics(existing: Optional[Dict[str, Any]], incoming: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Field-wise merge: keep existing non-null values when incoming is null/empty."""
    out = dict(existing or {})
    for key, value in (incoming or {}).items():
        if value is None or value == "":
            continue
        out[key] = value
    return out


def upsert_events(col: Collection, docs: List[Dict[str, Any]]) -> int:
    changed = 0
    for doc in docs:
        key = doc["identity_key"]
        existing = col.find_one({"identity_key": key})
        if existing:
            update = {
                "title": doc.get("title"),
                "event_time": doc.get("event_time"),
                "region": doc.get("region"),
                "category": doc.get("category"),
                "event_subtype": doc.get("event_subtype"),
                "symbol": doc.get("symbol"),
                "symbol_name": doc.get("symbol_name"),
                "metric_code": doc.get("metric_code"),
                "ref_period": doc.get("ref_period"),
                "importance": doc.get("importance"),
                "status": doc.get("status"),
                "metrics": _merge_metrics(existing.get("metrics"), doc.get("metrics")),
                "sources": _merge_sources(existing.get("sources"), doc.get("sources")),
                "updated_at": doc.get("updated_at"),
                "fetched_at": doc.get("fetched_at"),
            }
            if existing.get("llm_brief") is not None:
                update.pop("llm_brief", None)
            if existing.get("related_news"):
                update.pop("related_news", None)
            col.update_one({"identity_key": key}, {"$set": update})
        else:
            col.insert_one(doc)
        changed += 1
    return changed


def cleanup_old_events(col: Collection, retention_days: int, now: Optional[datetime] = None) -> int:
    now = now or datetime.now(timezone.utc)
    cutoff = now - timedelta(days=retention_days)
    result = col.delete_many({"event_time": {"$lt": cutoff}})
    return int(result.deleted_count or 0)
