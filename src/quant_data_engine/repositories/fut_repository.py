"""FuturesRepository - persistence for fut_daily and fut_mapping."""
from __future__ import annotations

from typing import Any, Dict, List, Sequence

from pymongo import ASCENDING, MongoClient, UpdateOne


class FutRepository:
    def __init__(self, mongo_uri: str, db_name: str):
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.daily_coll = self.db["fut_daily"]
        self.mapping_coll = self.db["fut_mapping"]
        self._ensure_indexes()

    def _ensure_indexes(self) -> None:
        try:
            self.daily_coll.create_index(
                [("ts_code", ASCENDING), ("trade_date", ASCENDING)],
                unique=True,
                name="fut_daily_code_date_uidx",
                background=True,
            )
            self.mapping_coll.create_index(
                [("ts_code", ASCENDING), ("trade_date", ASCENDING)],
                unique=True,
                name="fut_mapping_code_date_uidx",
                background=True,
            )
        except Exception:
            pass

    def upsert_mapping(self, records: Sequence[Dict[str, Any]]) -> int:
        if not records:
            return 0
        ops = [
            UpdateOne(
                {"ts_code": r["ts_code"], "trade_date": r["trade_date"]},
                {"$set": r},
                upsert=True,
            )
            for r in records
        ]
        res = self.mapping_coll.bulk_write(ops, ordered=False)
        return (res.upserted_count or 0) + (res.modified_count or 0)

    def upsert_daily(self, records: Sequence[Dict[str, Any]]) -> int:
        if not records:
            return 0
        ops = [
            UpdateOne(
                {"ts_code": r["ts_code"], "trade_date": r["trade_date"]},
                {"$set": r},
                upsert=True,
            )
            for r in records
        ]
        res = self.daily_coll.bulk_write(ops, ordered=False)
        return (res.upserted_count or 0) + (res.modified_count or 0)


__all__ = ["FutRepository"]
