"""IndustryRepository - handles persistence for industry and ETF data."""
from __future__ import annotations
from typing import Sequence, Dict, Any, Optional
from pymongo import MongoClient, UpdateOne
import logging

logger = logging.getLogger(__name__)

class IndustryRepository:
    def __init__(self, mongo_uri: str, db_name: str):
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.industry_coll = self.db["market_industry_quotes_daily"]
        self.etf_coll = self.db["market_etf_quotes_daily"]
        self.stats_coll = self.db["industry_limitup_stats"] # Unified stats collection
        self._ensure_indexes()

    def _ensure_indexes(self):
        try:
            self.industry_coll.create_index([("industry_name", 1), ("date", 1)], unique=True, background=True)
            self.etf_coll.create_index([("etf_code", 1), ("date", 1)], unique=True, background=True)
            self.stats_coll.create_index([("industry_name", 1), ("period", 1), ("date", 1)], unique=True, background=True)
        except Exception as e:
            logger.warning(f"Error creating indexes: {e}")

    def upsert_industry_daily(self, records: Sequence[Dict[str, Any]]) -> int:
        if not records:
            return 0
        ops = []
        for r in records:
            key = {"industry_name": r["industry_name"], "date": r["date"]}
            ops.append(UpdateOne(key, {"$set": r}, upsert=True))
        res = self.industry_coll.bulk_write(ops, ordered=False)
        return (res.upserted_count or 0) + (res.modified_count or 0)

    def upsert_etf_daily(self, records: Sequence[Dict[str, Any]]) -> int:
        if not records:
            return 0
        ops = []
        for r in records:
            key = {"etf_code": r["etf_code"], "date": r["date"]}
            ops.append(UpdateOne(key, {"$set": r}, upsert=True))
        res = self.etf_coll.bulk_write(ops, ordered=False)
        return (res.upserted_count or 0) + (res.modified_count or 0)

    def get_latest_industry_date(self, industry_name: str) -> Optional[str]:
        doc = self.industry_coll.find_one({"industry_name": industry_name}, sort=[("date", -1)])
        return doc["date"] if doc else None

    def get_latest_etf_date(self, etf_code: str) -> Optional[str]:
        doc = self.etf_coll.find_one({"etf_code": etf_code}, sort=[("date", -1)])
        return doc["date"] if doc else None

__all__ = ["IndustryRepository"]
