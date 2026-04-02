"""JobStatusRepository - records ingestion job runs for monitoring & auditing.

Document shape (finance.job_status):
  job_type: str
  date: str (YYYYMMDD)
  status: str  # complete | failed | partial
  completed_at, updated_at: datetime UTC
  records_changed: optional int
  error: optional str (truncated)
  ... extra fields from callers (codes, start, end, etc.)
"""
from __future__ import annotations
from typing import Any, Dict, Optional
from pymongo import MongoClient
from datetime import datetime

_ERR_MAX = 4000


class JobStatusRepository:
    def __init__(self, mongo_uri: str, db_name: str):
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.coll = self.db["job_status"]
        self._ensure_indexes()

    def _ensure_indexes(self):
        try:
            self.coll.create_index([("job_type", 1), ("date", -1)], unique=True, background=True)
        except Exception:
            pass

    def record(
        self,
        job_type: str,
        date: str,
        payload: Optional[Dict[str, Any]] = None,
        *,
        status: Optional[str] = None,
        error: Optional[str] = None,
    ) -> bool:
        """Upsert one run record. Merges payload into the document with explicit status/time fields."""
        now = datetime.utcnow()
        payload = dict(payload or {})
        if status is not None:
            payload["status"] = status
        elif "status" not in payload:
            payload["status"] = "complete"
        if error:
            payload["error"] = str(error)[:_ERR_MAX]
        doc = {
            "job_type": job_type,
            "date": date,
            "completed_at": now,
            "updated_at": now,
            **payload,
        }
        res = self.coll.replace_one(
            {"job_type": job_type, "date": date},
            doc,
            upsert=True,
        )
        return bool(res.upserted_id or res.modified_count or res.matched_count)


__all__ = ["JobStatusRepository"]
