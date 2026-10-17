"""Persistence for Tushare cyq_perf (chip distribution) records."""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional, Sequence

import pandas as pd
from pymongo import MongoClient, UpdateOne


class CyqRepository:
    """Repository for raw Tushare chip-distribution records."""

    def __init__(self, mongo_uri: str, db_name: str):
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.coll = self.db["cyq_perf"]
        self.state_coll = self.db["collect_state"]
        self._ensure_indexes()

    def _ensure_indexes(self) -> None:
        try:
            # The (symbol, trade_date) index predates this repository (created
            # for the 2026-10-17 backfill); the identical spec resolves to the
            # existing index instead of adding a duplicate.
            self.coll.create_index(
                [("symbol", 1), ("trade_date", -1)], background=True
            )
            self.coll.create_index(
                [("trade_date", -1)], name="cyq_trade_date_idx", background=True
            )
        except Exception as exc:
            print(f"Warning: cyq_perf index creation failed: {exc}")

    def upsert_cyq_perf(self, records: Sequence[Dict[str, Any]]) -> int:
        """Upsert raw ``cyq_perf`` records by symbol/date."""
        if not records:
            return 0

        ops = []
        now = datetime.now()
        for record in records:
            symbol = record.get("symbol") or record.get("ts_code")
            trade_date = record.get("trade_date")
            if not symbol or not trade_date:
                continue

            doc = dict(record)
            doc["symbol"] = symbol
            doc["data_source"] = doc.get("data_source") or "tushare"
            doc["synced_at"] = now
            key = {"symbol": symbol, "trade_date": trade_date}
            ops.append(UpdateOne(key, {"$set": doc}, upsert=True))

        if not ops:
            return 0
        result = self.coll.bulk_write(ops, ordered=False)
        return (result.upserted_count or 0) + (result.modified_count or 0)

    def update_last_sync_date(self, date_str: str) -> None:
        try:
            self.state_coll.update_one(
                {"_id": "cyq_perf_last_date"},
                {"$set": {"last_date": date_str}},
                upsert=True,
            )
        except Exception as exc:
            print(f"Failed to update cyq_perf last sync date: {exc}")

    def get_last_sync_date(self) -> Optional[str]:
        try:
            doc = self.state_coll.find_one({"_id": "cyq_perf_last_date"})
            return doc.get("last_date") if doc else None
        except Exception:
            return None

    def get_cyq_data(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        cursor = self.coll.find(
            {"symbol": symbol, "trade_date": {"$gte": start_date, "$lte": end_date}},
            sort=[("trade_date", 1)],
        )
        docs = list(cursor)
        return pd.DataFrame(docs) if docs else pd.DataFrame()


__all__ = ["CyqRepository"]
