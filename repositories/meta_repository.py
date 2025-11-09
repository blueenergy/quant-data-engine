"""MetaRepository - handles lightweight index_meta maintenance."""
from __future__ import annotations
from typing import Any, Dict, Optional
from pymongo import MongoClient
from datetime import datetime

class MetaRepository:
    def __init__(self, mongo_uri: str, db_name: str):
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.coll = self.db["index_meta"]
        self._ensure_indexes()

    def _ensure_indexes(self):  # idempotent
        try:
            self.coll.create_index([("ts_code", 1)], unique=True, background=True)
        except Exception:
            pass

    def upsert(self, ts_code: str, name: Optional[str] = None, market: Optional[str] = None, alias: Optional[list] = None) -> bool:
        alias = alias or []
        doc: Dict[str, Any] = {
            "ts_code": ts_code,
            "name": name,
            "market": market,
            "alias": alias,
            "last_refresh": datetime.utcnow(),
        }
        res = self.coll.replace_one({"ts_code": ts_code}, doc, upsert=True)
        return bool(res.upserted_id or res.modified_count)

__all__ = ["MetaRepository"]
