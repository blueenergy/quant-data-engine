"""DataContractStateRepository — persists contract health snapshots.

One document per contract id in ``finance.data_contract_state``. This collection
is the source of truth for the data-pulse UI's contract health panel and also
stores ``last_alert_at`` for alert suppression (cooldown).

Document shape:
  contract_id: str
  label: str
  status: str            # ok | violated | repairing | repaired | failed | unknown
  coverage_pct: float
  lag_trading_days: int
  expected_days: int
  present_days: int
  missing_days: int
  latest_actual: str|None
  latest_expected: str|None
  symbols_latest_day: int|None
  violations: list[str]
  missing_ranges: list[[start, end]]   # truncated
  repair: dict|None      # {attempted, chunks, succeeded, failed, errors}
  last_reconcile_at: datetime UTC
  last_alert_at: datetime UTC|None
  updated_at: datetime UTC
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, Optional

from pymongo import MongoClient

_log = logging.getLogger(__name__)

_MISSING_RANGES_MAX = 50


class DataContractStateRepository:
    def __init__(self, mongo_uri: str, db_name: str):
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.coll = self.db["data_contract_state"]
        self._ensure_indexes()

    def _ensure_indexes(self) -> None:
        try:
            self.coll.create_index("contract_id", unique=True, background=True)
        except Exception:
            pass
        try:
            self.coll.create_index([("status", 1), ("updated_at", -1)], background=True)
        except Exception:
            pass

    def get(self, contract_id: str) -> Optional[Dict[str, Any]]:
        return self.coll.find_one({"contract_id": contract_id})

    def upsert_snapshot(self, contract_id: str, snapshot: Dict[str, Any]) -> bool:
        """Upsert the latest health snapshot for a contract.

        ``last_alert_at`` is intentionally NOT set here; use mark_alerted().
        """
        now = datetime.utcnow()
        doc = dict(snapshot)
        ranges = doc.get("missing_ranges") or []
        if len(ranges) > _MISSING_RANGES_MAX:
            doc["missing_ranges"] = ranges[:_MISSING_RANGES_MAX]
            doc["missing_ranges_truncated"] = True
        doc["contract_id"] = contract_id
        doc["last_reconcile_at"] = now
        doc["updated_at"] = now
        res = self.coll.update_one(
            {"contract_id": contract_id},
            {"$set": doc},
            upsert=True,
        )
        return bool(res.upserted_id or res.modified_count)

    def mark_alerted(self, contract_id: str) -> None:
        now = datetime.utcnow()
        self.coll.update_one(
            {"contract_id": contract_id},
            {"$set": {"last_alert_at": now, "updated_at": now}},
            upsert=True,
        )

    def last_alert_at(self, contract_id: str) -> Optional[datetime]:
        doc = self.coll.find_one({"contract_id": contract_id}, {"last_alert_at": 1})
        return (doc or {}).get("last_alert_at")


__all__ = ["DataContractStateRepository"]
