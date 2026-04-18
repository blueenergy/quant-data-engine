"""Persist Tushare fund_portfolio (公募基金持仓) into外部数据标准库 fund_holdings。

与 ETF/行情等一致，集合落在 **DATA_DB_NAME**（默认 ``quant_data``），
不使用 MONGO_DB（finance）存放原始披露数据。
"""
from __future__ import annotations

import logging
import math
from datetime import datetime
from typing import Any, Dict, List, Sequence

import pandas as pd
from pymongo import MongoClient, UpdateOne

logger = logging.getLogger(__name__)


class FundPortfolioRepository:
    """Writes to ``{DATA_DB_NAME}.fund_holdings`` (same pattern as ``EtfRepository``)."""

    def __init__(self, mongo_uri: str, data_db: str):
        self._client = MongoClient(mongo_uri)
        self._db = self._client[data_db]
        self._data_db_name = data_db
        self._col = self._db["fund_holdings"]
        logger.info(
            "FundPortfolioRepository: external data DB=%s collection=fund_holdings",
            data_db,
        )
        self._ensure_indexes()

    def _ensure_indexes(self) -> None:
        try:
            self._col.create_index(
                [("ts_code", 1), ("ann_date", 1), ("end_date", 1), ("symbol", 1)],
                unique=True,
                background=True,
            )
            self._col.create_index([("ann_date", -1)])
            self._col.create_index([("end_date", -1)])
            self._col.create_index([("symbol", 1)])
        except Exception as e:  # pragma: no cover
            logger.warning("fund_holdings index ensure: %s", e)

    @staticmethod
    def _clean_record(row: Dict[str, Any]) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        for k, v in row.items():
            if k == "_id":
                continue
            if pd.isna(v):
                out[k] = None
            elif isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                out[k] = None
            elif hasattr(v, "item"):
                out[k] = v.item()
            else:
                out[k] = v
        out["source"] = "tushare_fund_portfolio"
        out["updated_at"] = datetime.utcnow()
        return out

    def upsert_from_dataframe(self, df: pd.DataFrame) -> int:
        if df is None or df.empty:
            return 0
        required = {"ts_code", "ann_date", "end_date", "symbol"}
        cols = set(df.columns)
        missing = required - cols
        if missing:
            logger.error("fund_portfolio DataFrame missing columns: %s", missing)
            return 0
        records: List[Dict[str, Any]] = []
        for row in df.to_dict("records"):
            rec = self._clean_record(row)
            records.append(rec)
        return self.upsert_records(records)

    def upsert_records(self, records: Sequence[Dict[str, Any]]) -> int:
        if not records:
            return 0
        ops = [
            UpdateOne(
                {
                    "ts_code": r["ts_code"],
                    "ann_date": r["ann_date"],
                    "end_date": r["end_date"],
                    "symbol": r["symbol"],
                },
                {"$set": r},
                upsert=True,
            )
            for r in records
            if r.get("ts_code") and r.get("ann_date") and r.get("end_date") and r.get("symbol")
        ]
        if not ops:
            return 0
        try:
            res = self._col.bulk_write(ops, ordered=False)
            return int((res.upserted_count or 0) + (res.modified_count or 0))
        except Exception as e:
            logger.error("fund_holdings bulk_write failed: %s", e)
            return 0
