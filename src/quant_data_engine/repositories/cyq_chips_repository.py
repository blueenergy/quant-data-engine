"""Persistence for Tushare cyq_chips (per-price chip distribution) records."""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional, Sequence

from pymongo import MongoClient, UpdateOne


class CyqChipsRepository:
    """Repository for raw Tushare ``cyq_chips`` records.

    One document per (symbol, trade_date); ``chips`` keeps the raw
    ``[{price, percent}, ...]`` bins and ``s_chip`` precomputes the
    normalized Shannon entropy (same definition as stock-scoring-system's
    ``utils.chip_entropy``, so both consumers agree bit-for-bit).
    """

    def __init__(self, mongo_uri: str, db_name: str):
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.coll = self.db["cyq_chips"]
        self.state_coll = self.db["collect_state"]
        self._ensure_indexes()

    def _ensure_indexes(self) -> None:
        try:
            self.coll.create_index(
                [("symbol", 1), ("trade_date", -1)], background=True
            )
        except Exception as exc:
            print(f"Warning: cyq_chips index creation failed: {exc}")

    def upsert_chips(self, records: Sequence[Dict[str, Any]]) -> int:
        """Upsert ``cyq_chips`` records by symbol/date."""
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
            ops.append(
                UpdateOne(
                    {"symbol": symbol, "trade_date": trade_date},
                    {"$set": doc},
                    upsert=True,
                )
            )

        if not ops:
            return 0
        result = self.coll.bulk_write(ops, ordered=False)
        return (result.upserted_count or 0) + (result.modified_count or 0)

    def update_last_sync_date(self, date_str: str) -> None:
        try:
            self.state_coll.update_one(
                {"_id": "cyq_chips_last_date"},
                {"$set": {"last_date": date_str}},
                upsert=True,
            )
        except Exception as exc:
            print(f"Failed to update cyq_chips last sync date: {exc}")

    def get_last_sync_date(self) -> Optional[str]:
        try:
            doc = self.state_coll.find_one({"_id": "cyq_chips_last_date"})
            return doc.get("last_date") if doc else None
        except Exception:
            return None


__all__ = ["CyqChipsRepository"]
