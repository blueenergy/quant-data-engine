"""Persist fund_basic (all markets, not only ETF)."""
from __future__ import annotations

import logging
import math
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence

import pandas as pd
from pymongo import MongoClient, UpdateOne

logger = logging.getLogger(__name__)


class FundBasicRepository:
    def __init__(self, mongo_uri: str, data_db: str):
        self._client = MongoClient(mongo_uri)
        self._col = self._client[data_db]["fund_basic"]
        self._ensure_indexes()

    def _ensure_indexes(self) -> None:
        try:
            self._col.create_index([("ts_code", 1)], unique=True, background=True)
            self._col.create_index([("fund_type", 1), ("invest_type", 1)], background=True)
        except Exception as e:  # pragma: no cover
            logger.warning("fund_basic index ensure: %s", e)

    @staticmethod
    def _is_missing(value: Any) -> bool:
        if value is None:
            return True
        if isinstance(value, (list, dict, tuple, set)):
            return False
        try:
            result = pd.isna(value)
            return bool(result)
        except (ValueError, TypeError):
            return False

    @staticmethod
    def _clean(row: Dict[str, Any]) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        for k, v in row.items():
            if k == "_id":
                continue
            if FundBasicRepository._is_missing(v):
                out[k] = None
            elif isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                out[k] = None
            elif hasattr(v, "item") and not isinstance(v, (bytes, str)):
                out[k] = v.item()
            else:
                out[k] = v
        out["source"] = "tushare_fund_basic"
        out["updated_at"] = datetime.utcnow()
        return out

    def upsert_from_dataframe(self, df: pd.DataFrame) -> int:
        if df is None or df.empty or "ts_code" not in df.columns:
            return 0
        records = [self._clean(r) for r in df.to_dict("records")]
        return self.upsert_records(records)

    def upsert_records(self, records: Sequence[Dict[str, Any]]) -> int:
        ops = [
            UpdateOne({"ts_code": r["ts_code"]}, {"$set": r}, upsert=True)
            for r in records
            if r.get("ts_code")
        ]
        if not ops:
            return 0
        res = self._col.bulk_write(ops, ordered=False)
        return int((res.upserted_count or 0) + (res.modified_count or 0))

    def find_all(self, projection: Optional[Dict[str, int]] = None) -> List[Dict[str, Any]]:
        return list(self._col.find({}, projection or {"_id": 0}))
