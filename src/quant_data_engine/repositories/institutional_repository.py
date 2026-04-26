"""Mongo 持久化：top10 截面、share_float、block_trade、综合快照。"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
from pymongo import MongoClient, UpdateOne


class InstitutionalRepository:
    def __init__(self, mongo_uri: str, db_name: str):
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.holders = self.db["top10_holders"]
        self.floatholders = self.db["top10_floatholders"]
        self.share_float = self.db["share_float"]
        self.block_trade = self.db["block_trade"]
        self.snapshot = self.db["institutional_deep_snapshot"]
        self._ensure_indexes()

    def _ensure_indexes(self) -> None:
        try:
            for c in (self.holders, self.floatholders):
                c.create_index([("ts_code", 1), ("end_date", -1)], background=True)
            self.share_float.create_index([("ts_code", 1), ("float_date", 1)], background=True, sparse=True)
            self.block_trade.create_index([("ts_code", 1), ("trade_date", -1)], background=True)
            self.snapshot.create_index([("ts_code", 1), ("as_of", -1)], background=True, unique=True)
        except Exception:  # pragma: no cover
            pass

    @staticmethod
    def _df_records(df: Optional[pd.DataFrame]) -> List[Dict[str, Any]]:
        if df is None or df.empty:
            return []
        out: List[Dict[str, Any]] = []
        for r in df.to_dict(orient="records"):
            row: Dict[str, Any] = {}
            for k, v in r.items():
                if v is None or (isinstance(v, float) and pd.isna(v)):
                    row[k] = None
                elif hasattr(v, "item"):
                    try:
                        row[k] = v.item()
                    except Exception:
                        row[k] = v
                else:
                    row[k] = v
            out.append(row)
        return out

    def upsert_top10_floatholders(
        self, ts_code: str, period: str, df: pd.DataFrame, extra: Optional[Dict[str, Any]] = None
    ) -> int:
        return self._upsert_top10(
            self.floatholders, ts_code, period, df, kind="top10_floatholders", extra=extra
        )

    def upsert_top10_holders(
        self, ts_code: str, period: str, df: pd.DataFrame, extra: Optional[Dict[str, Any]] = None
    ) -> int:
        return self._upsert_top10(self.holders, ts_code, period, df, kind="top10_holders", extra=extra)

    def _upsert_top10(
        self,
        coll: Any,
        ts_code: str,
        period: str,
        df: pd.DataFrame,
        kind: str,
        extra: Optional[Dict[str, Any]] = None,
    ) -> int:
        if df is None or df.empty:
            return 0
        ex = extra or {}
        # 一行一条 + (ts_code,end_date,rank) 去重
        if "end_date" not in df.columns and period:
            df = df.copy()
            df["end_date"] = period
        end_col = "end_date" if "end_date" in df.columns else None
        if not end_col:
            return 0
        ops: List[UpdateOne] = []
        for i, r in enumerate(df.to_dict(orient="records")):
            ed = str(int(r.get(end_col))) if r.get(end_col) is not None else str(period)
            raw = dict(r)
            for k, v in list(raw.items()):
                if isinstance(v, float) and pd.isna(v):
                    raw[k] = None
            key = {
                "ts_code": ts_code,
                "end_date": ed,
                "holder_name": (raw.get("holder_name") or raw.get("name") or f"__idx_{i}").strip() or f"__idx_{i}",
                "kind": kind,
            }
            doc = {**ex, "kind": kind, "ts_code": ts_code, "end_date": ed, "rank_hint": i, "raw": raw, "source": "tushare", "update_time": datetime.now().isoformat()}
            ops.append(UpdateOne(key, {"$set": doc}, upsert=True))
        if not ops:
            return 0
        r = coll.bulk_write(ops, ordered=False)
        return (r.upserted_count or 0) + (r.modified_count or 0)

    def replace_share_float(self, ts_code: str, df: Optional[pd.DataFrame]) -> int:
        if df is None or df.empty:
            return 0
        self.share_float.delete_many({"ts_code": ts_code})
        records = self._df_records(df)
        for d in records:
            d["ts_code"] = ts_code
            d["source"] = "tushare"
            d["update_time"] = datetime.now().isoformat()
        self.share_float.insert_many(records)
        return len(records)

    def replace_block_trade(
        self, ts_code: str, start_s: str, end_s: str, df: Optional[pd.DataFrame]
    ) -> int:
        if df is None or df.empty:
            return 0
        self.block_trade.delete_many(
            {
                "ts_code": ts_code,
                "trade_date": {
                    "$gte": start_s,
                    "$lte": end_s,
                },
            }
        )
        records = self._df_records(df)
        for d in records:
            d["ts_code"] = ts_code
            d["ingest_start"] = start_s
            d["ingest_end"] = end_s
            d["source"] = "tushare"
            d["update_time"] = datetime.now().isoformat()
        if records:
            self.block_trade.insert_many(records)
        return len(records)

    def upsert_snapshot(self, doc: Dict[str, Any]) -> int:
        ts_code = doc.get("ts_code", "")
        asof = doc.get("as_of", "")
        if not ts_code or not asof:
            return 0
        self.snapshot.update_one(
            {"ts_code": ts_code, "as_of": asof},
            {"$set": doc},
            upsert=True,
        )
        return 1
