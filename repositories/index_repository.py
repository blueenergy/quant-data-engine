"""IndexRepository - handles persistence and retrieval for index daily prices."""
from __future__ import annotations
from typing import Sequence, Dict, Any
from pymongo import MongoClient, UpdateOne
import pandas as pd

class IndexRepository:
    def __init__(self, mongo_uri: str, db_name: str):
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.coll = self.db["index_prices"]
        self._ensure_indexes()

    def _ensure_indexes(self):
        try:
            self.coll.create_index([("ts_code", 1), ("trade_date", 1)], unique=True, background=True)
        except Exception:
            pass

    def upsert_daily(self, records: Sequence[Dict[str, Any]]) -> int:
        if not records:
            return 0
        ops = []
        for r in records:
            key = {"ts_code": r["ts_code"], "trade_date": r["trade_date"]}
            ops.append(UpdateOne(key, {"$set": r}, upsert=True))
        res = self.coll.bulk_write(ops, ordered=False)
        return (res.upserted_count or 0) + (res.modified_count or 0)

    def get_series(self, ts_code: str, start_date: str, end_date: str) -> pd.Series:
        cursor = self.coll.find({"ts_code": ts_code, "trade_date": {"$gte": start_date, "$lte": end_date}}).sort("trade_date", 1)
        docs = list(cursor)
        if not docs:
            return pd.Series(dtype=float, name=f"{ts_code}_close")
        df = pd.DataFrame(docs)
        ser = df["close"].astype(float)
        ser.index = pd.to_datetime(df["trade_date"])  # daily index
        ser.name = f"{ts_code}_close"
        return ser

    def get_normalized(self, ts_code: str, start_date: str, end_date: str) -> pd.Series:
        ser = self.get_series(ts_code, start_date, end_date)
        if ser.empty:
            return ser.rename(f"{ts_code}_norm")
        base = ser.iloc[0]
        if base == 0:
            return ser.rename(f"{ts_code}_norm")
        return (ser / base).rename(f"{ts_code}_norm")

__all__ = ["IndexRepository"]
