"""东方财富概念板块数据落库（quant_data）：dc_index / dc_member。"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence

import pandas as pd
from pymongo import MongoClient, UpdateOne


def _norm_yyyymmdd(d: Any) -> str:
    if d is None or (isinstance(d, float) and pd.isna(d)):
        return ""
    s = str(d).strip().replace("-", "")[:8]
    digits = "".join(c for c in s if c.isdigit())
    return digits[:8] if len(digits) >= 8 else digits


def _clean_record(r: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in r.items():
        if isinstance(v, float) and pd.isna(v):
            out[k] = None
        else:
            out[k] = v
    return out


class DcConceptRepository:
    def __init__(self, mongo_uri: str, db_name: str):
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.index_coll = self.db["dc_concept_index"]
        self.member_coll = self.db["dc_concept_member"]
        self.meta_coll = self.db["dc_concept_sync_meta"]
        self._ensure_indexes()

    def _ensure_indexes(self) -> None:
        try:
            self.index_coll.create_index(
                [("ts_code", 1), ("trade_date", 1)],
                name="dc_idx_code_date",
                background=True,
                unique=True,
            )
            self.index_coll.create_index([("trade_date", -1)], name="dc_idx_date", background=True)
            self.member_coll.create_index(
                [("concept_ts_code", 1), ("as_of_date", 1), ("con_code", 1)],
                name="dc_mem_concept_date_stock",
                background=True,
                unique=True,
            )
            self.member_coll.create_index([("con_code", 1), ("as_of_date", -1)], name="dc_mem_stock_date", background=True)
            self.member_coll.create_index("concept_ts_code", background=True)
            self.meta_coll.create_index("key", unique=True, background=True)
        except Exception:
            pass

    def upsert_index_rows(
        self,
        records: Sequence[Dict[str, Any]],
        trade_date: str,
        sync_time_iso: str,
    ) -> int:
        """按 ``ts_code`` + ``trade_date`` upsert 板块截面。"""
        td = _norm_yyyymmdd(trade_date)
        if not td or not records:
            return 0
        ops: List[UpdateOne] = []
        for r in records:
            tc = str(r.get("ts_code", "")).strip()
            if not tc:
                continue
            doc = _clean_record(dict(r))
            doc["trade_date"] = td
            doc["sync_time"] = sync_time_iso
            doc["source"] = "tushare"
            doc["update_time"] = datetime.now().isoformat()
            key = {"ts_code": tc, "trade_date": td}
            ops.append(UpdateOne(key, {"$set": doc}, upsert=True))
        if not ops:
            return 0
        r2 = self.index_coll.bulk_write(ops, ordered=False)
        return (r2.upserted_count or 0) + (r2.modified_count or 0)

    def upsert_member_rows(
        self,
        concept_ts_code: str,
        as_of_date: str,
        rows: Sequence[Dict[str, Any]],
    ) -> int:
        """成分：唯一键 ``concept_ts_code`` + ``as_of_date`` + ``con_code``（股票标识）。"""
        concept_ts_code = str(concept_ts_code).strip()
        ad = _norm_yyyymmdd(as_of_date)
        if not concept_ts_code or not ad or not rows:
            return 0
        ops: List[UpdateOne] = []
        for r in rows:
            raw = _clean_record(dict(r))
            con = str(raw.get("con_code") or raw.get("ts_code") or "").strip()
            if not con:
                continue
            doc = {**raw, "concept_ts_code": concept_ts_code, "as_of_date": ad, "con_code": con}
            doc["source"] = "tushare"
            doc["update_time"] = datetime.now().isoformat()
            key = {"concept_ts_code": concept_ts_code, "as_of_date": ad, "con_code": con}
            ops.append(UpdateOne(key, {"$set": doc}, upsert=True))
        if not ops:
            return 0
        r2 = self.member_coll.bulk_write(ops, ordered=False)
        return (r2.upserted_count or 0) + (r2.modified_count or 0)

    def list_concept_ts_codes_for_trade_date(self, trade_date: str) -> List[str]:
        td = _norm_yyyymmdd(trade_date)
        if not td:
            return []
        cur = self.index_coll.find({"trade_date": td}, projection={"ts_code": 1, "_id": 0})
        out: List[str] = []
        for d in cur:
            c = d.get("ts_code")
            if c and str(c).strip():
                out.append(str(c).strip())
        return sorted(set(out))

    def latest_index_trade_date(self) -> Optional[str]:
        doc = self.index_coll.find_one(sort=[("trade_date", -1)], projection={"trade_date": 1})
        if not doc:
            return None
        return _norm_yyyymmdd(doc.get("trade_date"))

    def get_meta(self, key: str) -> Optional[str]:
        m = self.meta_coll.find_one({"key": key})
        return m.get("value") if m else None

    def set_meta(self, key: str, value: str) -> None:
        self.meta_coll.update_one(
            {"key": key},
            {"$set": {"value": value, "ts": datetime.now().isoformat()}},
            upsert=True,
        )


__all__ = ["DcConceptRepository"]
