"""IndustryRepository - handles persistence for industry limit-up statistics."""
from __future__ import annotations
from pymongo import MongoClient
import logging

logger = logging.getLogger(__name__)

class IndustryRepository:
    def __init__(self, mongo_uri: str, db_name: str):
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.stats_coll = self.db["industry_limitup_stats"]
        self._ensure_indexes()

    def _ensure_indexes(self):
        try:
            self.stats_coll.create_index([("industry_name", 1), ("period", 1), ("date", 1)], unique=True, background=True)
        except Exception as e:
            logger.warning(f"Error creating indexes: {e}")

__all__ = ["IndustryRepository"]
