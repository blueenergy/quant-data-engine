"""申万 Tushare 数据落库：分类、成分、行业指数日线（quant_data 库）。"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence

import pandas as pd
from pymongo import MongoClient, UpdateOne


class SwIndustryRepository:
    def __init__(self, mongo_uri: str, db_name: str):
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.classify_coll = self.db["sw_index_classify"]
        self.member_coll = self.db["sw_index_member"]
        self.daily_coll = self.db["sw_daily"]
        self.meta_coll = self.db["sw_industry_sync_meta"]  # 断点: last sw_daily trade_date
        self._ensure_indexes()

    def _ensure_indexes(self) -> None:
        try:
            self.classify_coll.create_index(
                [("index_code", 1), ("src", 1)], name="sw_ic_code_src", background=True, unique=True
            )
            self.member_coll.create_index(
                [("con_code", 1), ("src", 1)], name="sw_m_con_src", background=True, unique=True
            )
            self.member_coll.create_index("l1_code", background=True)
            self.daily_coll.create_index(
                [("ts_code", 1), ("trade_date", -1)], name="sw_d_code_date", background=True, unique=True
            )
            self.daily_coll.create_index([("trade_date", -1)], name="sw_d_date", background=True)
            self.meta_coll.create_index("key", unique=True, background=True)
        except Exception:
            pass

    def upsert_classify(self, records: Sequence[Dict[str, Any]], src: str) -> int:
        if not records:
            return 0
        ops: List[UpdateOne] = []
        for r in records:
            ic = r.get("index_code", "")
            doc = {**r, "src": src, "update_time": datetime.now().isoformat()}
            key = {"index_code": ic, "src": src}
            ops.append(UpdateOne(key, {"$set": doc}, upsert=True))
        r2 = self.classify_coll.bulk_write(ops, ordered=False)
        return (r2.upserted_count or 0) + (r2.modified_count or 0)

    def upsert_members(self, records: Sequence[Dict[str, Any]], src: str) -> int:
        if not records:
            return 0
        ops: List[UpdateOne] = []
        for r in records:
            con = r.get("con_code") or r.get("ts_code", "")
            doc = {**r, "src": src, "update_time": datetime.now().isoformat()}
            key = {"con_code": con, "src": src}
            ops.append(UpdateOne(key, {"$set": doc}, upsert=True))
        r2 = self.member_coll.bulk_write(ops, ordered=False)
        return (r2.upserted_count or 0) + (r2.modified_count or 0)

    def upsert_sw_daily(self, records: Sequence[Dict[str, Any]]) -> int:
        if not records:
            return 0
        ops: List[UpdateOne] = []
        for r in records:
            tc = r.get("ts_code", "")
            td = r.get("trade_date", "")
            if not tc or not td:
                continue
            s = str(int(td)) if not isinstance(td, str) else td.replace("-", "")[:8]
            doc = dict(r)
            for k, v in list(doc.items()):
                if isinstance(v, float) and pd.isna(v):
                    doc[k] = None
            doc["trade_date"] = s
            doc["update_time"] = datetime.now().isoformat()
            doc["source"] = "tushare"
            key = {"ts_code": tc, "trade_date": s}
            ops.append(UpdateOne(key, {"$set": doc}, upsert=True))
        if not ops:
            return 0
        r2 = self.daily_coll.bulk_write(ops, ordered=False)
        return (r2.upserted_count or 0) + (r2.modified_count or 0)

    def get_l1_index_for_stock(self, stock_ts: str, src: str = "SW2021") -> Optional[str]:
        """从成分表取该股对应 L1 申万行业指数代码（``801xxx.SI``）。"""
        if not stock_ts:
            return None
        cands: List[str] = [stock_ts]
        if "." in stock_ts:
            cands.append(stock_ts.split(".", 1)[0])
        else:
            for suf in (".SH", ".SZ", ".BJ"):
                cands.append(f"{stock_ts}{suf}")
        for c in cands:
            doc = self.member_coll.find_one(
                {"con_code": c, "src": src},
                sort=[("in_date", -1)],
            )
            if doc:
                return doc.get("l1_code") or doc.get("l1_index")
        return None

    def get_sw_close_series(
        self, index_ts: str, end_date: str, min_rows: int = 65
    ) -> List[tuple[str, float]]:
        """升序 ``(trade_date, close)`` 列表，end_date 之前若干条。``end_date`` YYYYMMDD。"""
        end_s = str(end_date).replace("-", "")[:8]
        cur = self.daily_coll.find(
            {"ts_code": index_ts, "trade_date": {"$lte": end_s}},
            projection={"trade_date": 1, "close": 1},
        ).sort("trade_date", -1).limit(min_rows)
        rows: List[tuple[str, float]] = []
        for d in cur:
            c = d.get("close")
            if c is None:
                continue
            try:
                rows.append((str(d["trade_date"])[:8], float(c)))
            except (TypeError, ValueError):
                continue
        rows.sort(key=lambda x: x[0])
        return rows

    def get_meta(self, key: str) -> Optional[str]:
        m = self.meta_coll.find_one({"key": key})
        return m.get("value") if m else None

    def set_meta(self, key: str, value: str) -> None:
        self.meta_coll.update_one(
            {"key": key}, {"$set": {"value": value, "ts": datetime.now().isoformat()}}, upsert=True
        )

    def get_distinct_l1_codes(self, src: str = "SW2021") -> List[str]:
        return [x for x in self.classify_coll.distinct("index_code", {"level": "L1", "src": src}) if x]


__all__ = ["SwIndustryRepository"]
