"""Persistence for Tushare stk_nineturn records."""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional, Sequence

import pandas as pd
from pymongo import MongoClient, UpdateOne


class NineturnRepository:
    """Repository for raw Tushare nine-turn records."""

    def __init__(self, mongo_uri: str, db_name: str):
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.coll = self.db["stk_nineturn"]
        self.state_coll = self.db["collect_state"]
        self._ensure_indexes()

    def _ensure_indexes(self) -> None:
        try:
            self.coll.create_index(
                [("symbol", 1), ("trade_date", -1), ("freq", 1)],
                name="nineturn_symbol_date_freq_idx",
                background=True,
            )
            self.coll.create_index(
                [("trade_date", -1), ("freq", 1)],
                name="nineturn_trade_date_freq_idx",
                background=True,
            )
        except Exception as exc:
            print(f"Warning: Nineturn index creation failed: {exc}")

    def upsert_nineturn(self, records: Sequence[Dict[str, Any]]) -> int:
        """Upsert raw ``stk_nineturn`` records by symbol/date/frequency."""
        if not records:
            return 0

        ops = []
        now = datetime.utcnow()
        for record in records:
            symbol = record.get("symbol") or record.get("ts_code")
            trade_date = record.get("trade_date")
            freq = record.get("freq") or "daily"
            if not symbol or not trade_date:
                continue

            doc = dict(record)
            doc["symbol"] = symbol
            doc["freq"] = freq
            doc["updated_at"] = now
            key = {"symbol": symbol, "trade_date": trade_date, "freq": freq}
            ops.append(UpdateOne(key, {"$set": doc}, upsert=True))

        if not ops:
            return 0
        result = self.coll.bulk_write(ops, ordered=False)
        return (result.upserted_count or 0) + (result.modified_count or 0)

    def update_last_sync_date(self, date_str: str, *, freq: str = "daily") -> None:
        try:
            self.state_coll.update_one(
                {"_id": f"stk_nineturn_last_date_{freq}"},
                {"$set": {"last_date": date_str, "freq": freq}},
                upsert=True,
            )
        except Exception as exc:
            print(f"Failed to update nineturn last sync date: {exc}")

    def get_last_sync_date(self, *, freq: str = "daily") -> Optional[str]:
        try:
            doc = self.state_coll.find_one({"_id": f"stk_nineturn_last_date_{freq}"})
            return doc.get("last_date") if doc else None
        except Exception:
            return None

    def get_nineturn_data(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        *,
        freq: str = "daily",
    ) -> pd.DataFrame:
        cursor = self.coll.find(
            {
                "symbol": symbol,
                "freq": freq,
                "trade_date": {"$gte": start_date, "$lte": end_date},
            },
            sort=[("trade_date", 1)],
        )
        docs = list(cursor)
        return pd.DataFrame(docs) if docs else pd.DataFrame()


__all__ = ["NineturnRepository"]
