"""PipelineTasksRepository — generic task runs (Mongo collection pipeline_tasks).

First use: domain=data_collection, one upsert per (domain, task_type, business date),
mirroring ingestion semantics of job_status for a smooth migration away from job_status.

Document shape (finance.pipeline_tasks):
  domain: str           # e.g. data_collection; future: analysis
  task_type: str        # e.g. stock_daily (same strings as job_status job_type)
  date: str             # YYYYMMDD business date
  status: str           # complete | failed | partial | pending
  completed_at, updated_at: datetime UTC
  error: optional str (truncated)
  payload: dict         # records_changed, mode, and other task-specific fields
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, Optional

from pymongo import MongoClient

_ERR_MAX = 4000
_log = logging.getLogger(__name__)

DATA_COLLECTION_DOMAIN = "data_collection"


class PipelineTasksRepository:
    def __init__(self, mongo_uri: str, db_name: str):
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.coll = self.db["pipeline_tasks"]
        self._ensure_indexes()

    def _ensure_indexes(self) -> None:
        try:
            self.coll.create_index(
                [("domain", 1), ("task_type", 1), ("date", -1)],
                unique=True,
                background=True,
            )
        except Exception:
            pass
        try:
            self.coll.create_index(
                [("domain", 1), ("status", 1), ("updated_at", -1)],
                background=True,
            )
        except Exception:
            pass

    def record_data_collection(
        self,
        task_type: str,
        date: str,
        payload: Optional[Dict[str, Any]] = None,
        *,
        status: Optional[str] = None,
        error: Optional[str] = None,
        sync_uri: Optional[str] = None,
    ) -> bool:
        """Upsert one data_collection run (same cardinality as job_status per task_type+date)."""
        now = datetime.utcnow()
        inner = dict(payload or {})
        if status is not None:
            st = status
            inner.pop("status", None)
        else:
            st = inner.pop("status", None) or "complete"
        err = error
        if err:
            err = str(err)[:_ERR_MAX]
        doc: Dict[str, Any] = {
            "domain": DATA_COLLECTION_DOMAIN,
            "task_type": task_type,
            "date": date,
            "status": st,
            "completed_at": now,
            "updated_at": now,
            "payload": inner,
        }
        if err:
            doc["error"] = err
        filt = {
            "domain": DATA_COLLECTION_DOMAIN,
            "task_type": task_type,
            "date": date,
        }
        res = self.coll.replace_one(filt, doc, upsert=True)
        ok = bool(res.upserted_id or res.modified_count or res.matched_count)
        if sync_uri:
            try:
                secondary = MongoClient(sync_uri)
                try:
                    db_name = self.db.name
                    coll = secondary[db_name]["pipeline_tasks"]
                    coll.replace_one(filt, doc, upsert=True)
                    try:
                        coll.create_index(
                            [("domain", 1), ("task_type", 1), ("date", -1)],
                            unique=True,
                            background=True,
                        )
                    except Exception:
                        pass
                finally:
                    secondary.close()
            except Exception as e:
                _log.warning(
                    "pipeline_tasks mirror write failed (primary ok): %s",
                    e,
                    exc_info=_log.isEnabledFor(logging.DEBUG),
                )
        return ok


__all__ = ["PipelineTasksRepository", "DATA_COLLECTION_DOMAIN"]
