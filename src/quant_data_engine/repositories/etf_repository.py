"""EtfRepository - persistence for sector ETF daily prices."""
from __future__ import annotations
import logging
from typing import Sequence, Dict, Any
from pymongo import MongoClient, UpdateOne
import pandas as pd

logger = logging.getLogger(__name__)


class EtfRepository:
    def __init__(self, mongo_uri: str, db_name: str):
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        
        # New collections for comprehensive ETF analysis
        self.coll_basic = self.db["etf_basic"]
        self.coll_daily = self.db["etf_daily"]
        self.coll_adj = self.db["etf_adj_factor"]
        self.coll_share = self.db["etf_shares"]
        self.coll_share_size = self.db["etf_share_size"]

        # Legacy/Compatibility
        self.coll = self.db["etf_prices"]
        self._ensure_indexes()

    def _ensure_indexes(self):
        """与 API 查询一致：按 ts_code 过滤 + trade_date 排序依赖 (ts_code, trade_date) 复合索引。"""
        specs = [
            ("etf_basic", self.coll_basic, [("ts_code", 1)], {"unique": True}),
            ("etf_daily", self.coll_daily, [("ts_code", 1), ("trade_date", 1)], {"unique": True}),
            # 数据合约用 find({}).sort({trade_date:-1}).limit(1) 取最新日期；
            # 只有 (ts_code, trade_date) 时这是 3.1M 文档的 COLLSCAN（每次 ~2s）。
            ("etf_daily_trade_date", self.coll_daily, [("trade_date", -1)], {}),
            ("etf_adj_factor", self.coll_adj, [("ts_code", 1), ("trade_date", 1)], {"unique": True}),
            ("etf_adj_factor_trade_date", self.coll_adj, [("trade_date", -1)], {}),
            ("etf_shares", self.coll_share, [("ts_code", 1), ("trade_date", 1)], {"unique": True}),
            (
                "etf_share_size",
                self.coll_share_size,
                [("ts_code", 1), ("trade_date", 1)],
                {"unique": True},
            ),
            (
                "etf_share_size_trade_date_total_size",
                self.coll_share_size,
                [("trade_date", 1), ("total_size", -1)],
                {},
            ),
            ("etf_prices(legacy)", self.coll, [("ts_code", 1), ("trade_date", 1)], {"unique": True}),
        ]
        for name, coll, keys, opts in specs:
            try:
                coll.create_index(keys, background=True, **opts)
            except Exception as e:
                logger.warning(
                    "ETF index create failed collection=%s keys=%s opts=%s: %s — "
                    "若已有脏数据（重复 ts_code+trade_date），需先清理再建唯一索引；"
                    "否则 find({ts_code}) 会退化为全表扫描。",
                    name,
                    keys,
                    opts,
                    e,
                )

    def upsert_basic(self, records: Sequence[Dict[str, Any]]) -> int:
        if not records: return 0
        ops = [UpdateOne({"ts_code": r["ts_code"]}, {"$set": r}, upsert=True) for r in records]
        res = self.coll_basic.bulk_write(ops, ordered=False)
        return (res.upserted_count or 0) + (res.modified_count or 0)

    def upsert_daily_prices(self, records: Sequence[Dict[str, Any]]) -> int:
        if not records: return 0
        ops = [UpdateOne({"ts_code": r["ts_code"], "trade_date": r["trade_date"]}, {"$set": r}, upsert=True) for r in records]
        res = self.coll_daily.bulk_write(ops, ordered=False)
        return (res.upserted_count or 0) + (res.modified_count or 0)

    def upsert_adj_factors(self, records: Sequence[Dict[str, Any]]) -> int:
        if not records: return 0
        ops = [UpdateOne({"ts_code": r["ts_code"], "trade_date": r["trade_date"]}, {"$set": r}, upsert=True) for r in records]
        res = self.coll_adj.bulk_write(ops, ordered=False)
        return (res.upserted_count or 0) + (res.modified_count or 0)

    def upsert_shares(self, records: Sequence[Dict[str, Any]]) -> int:
        if not records: return 0
        ops = [UpdateOne({"ts_code": r["ts_code"], "trade_date": r["trade_date"]}, {"$set": r}, upsert=True) for r in records]
        res = self.coll_share.bulk_write(ops, ordered=False)
        return (res.upserted_count or 0) + (res.modified_count or 0)

    def upsert_etf_share_size(self, records: Sequence[Dict[str, Any]]) -> int:
        """Tushare etf_share_size 字段落库（total_share/total_size/nav/close 等）。"""
        if not records:
            return 0
        ops = [
            UpdateOne({"ts_code": r["ts_code"], "trade_date": r["trade_date"]}, {"$set": r}, upsert=True)
            for r in records
        ]
        res = self.coll_share_size.bulk_write(ops, ordered=False)
        return (res.upserted_count or 0) + (res.modified_count or 0)

    # Legacy upsert_daily provided for backward compatibility
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


__all__ = ["EtfRepository"]
