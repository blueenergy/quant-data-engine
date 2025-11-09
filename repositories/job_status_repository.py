"""JobStatusRepository - records ingestion job runs for monitoring & auditing."""
from __future__ import annotations
from typing import Any, Dict
from pymongo import MongoClient
from datetime import datetime

class JobStatusRepository:
    def __init__(self, mongo_uri: str, db_name: str):
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.coll = self.db["job_status"]
        self._ensure_indexes()

    def _ensure_indexes(self):
        try:
            self.coll.create_index([("job_type", 1), ("date", 1)], unique=True, background=True)
        except Exception:
            pass

    def record(self, job_type: str, date: str, payload: Dict[str, Any]) -> bool:
        doc = {
            "job_type": job_type,
            "date": date,
            "completed_at": datetime.utcnow(),
            **payload,
        }
        res = self.coll.replace_one({"job_type": job_type, "date": date}, doc, upsert=True)
        return bool(res.upserted_id or res.modified_count)

__all__ = ["JobStatusRepository"]
