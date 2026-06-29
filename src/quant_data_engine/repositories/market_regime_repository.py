"""Repository for A-share market regime breadth and valuation reports."""
from __future__ import annotations

import dataclasses
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import pandas as pd
from pymongo import ASCENDING, DESCENDING, MongoClient, UpdateOne

logger = logging.getLogger(__name__)


class MarketRegimeRepository:
    """Persists breadth daily rows, index CAPE rows, and regime reports."""

    BREADTH_COLL = "market_breadth_daily"
    CAPE_COLL = "index_cape_daily"
    REPORT_COLL = "market_regime_reports"
    INDEX_BASIC_COLL = "index_daily_basic"

    def __init__(self, mongo_uri: str, db_name: str):
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.breadth_coll = self.db[self.BREADTH_COLL]
        self.cape_coll = self.db[self.CAPE_COLL]
        self.report_coll = self.db[self.REPORT_COLL]
        self.index_basic_coll = self.db[self.INDEX_BASIC_COLL]
        self._ensure_indexes()

    def _ensure_indexes(self) -> None:
        try:
            self.breadth_coll.create_index([("trade_date", ASCENDING)], unique=True, name="market_breadth_date_uidx", background=True)
            self.breadth_coll.create_index([("trade_date", DESCENDING)], name="market_breadth_date_desc_idx", background=True)
            self.cape_coll.create_index([("index_code", ASCENDING), ("trade_date", ASCENDING)], unique=True, name="index_cape_code_date_uidx", background=True)
            self.cape_coll.create_index([("trade_date", DESCENDING)], name="index_cape_date_desc_idx", background=True)
            self.report_coll.create_index([("scan_date", ASCENDING)], unique=True, name="market_regime_scan_date_uidx", background=True)
            self.report_coll.create_index([("scan_date", DESCENDING)], name="market_regime_scan_date_desc_idx", background=True)
            self.index_basic_coll.create_index([("ts_code", ASCENDING), ("trade_date", ASCENDING)], unique=True, name="index_daily_basic_code_date_uidx", background=True)
        except Exception as exc:
            logger.warning("MarketRegimeRepository index creation warning: %s", exc)

    def upsert_breadth(self, doc: Dict[str, Any]) -> bool:
        try:
            payload = dict(doc)
            payload["updated_at"] = datetime.utcnow()
            self.breadth_coll.update_one({"trade_date": payload["trade_date"]}, {"$set": payload}, upsert=True)
            return True
        except Exception as exc:
            logger.error("upsert_breadth failed: %s", exc)
            return False

    def upsert_cape(self, doc: Dict[str, Any]) -> bool:
        try:
            payload = dict(doc)
            payload["updated_at"] = datetime.utcnow()
            self.cape_coll.update_one(
                {"index_code": payload["index_code"], "trade_date": payload["trade_date"]},
                {"$set": payload},
                upsert=True,
            )
            return True
        except Exception as exc:
            logger.error("upsert_cape failed: %s", exc)
            return False

    def upsert_many_cape(self, docs: List[Dict[str, Any]]) -> int:
        if not docs:
            return 0
        now = datetime.utcnow()
        ops = []
        for doc in docs:
            payload = dict(doc)
            payload["updated_at"] = now
            ops.append(UpdateOne(
                {"index_code": payload["index_code"], "trade_date": payload["trade_date"]},
                {"$set": payload},
                upsert=True,
            ))
        result = self.cape_coll.bulk_write(ops, ordered=False)
        return (result.upserted_count or 0) + (result.modified_count or 0)

    def upsert_index_daily_basic(self, rows: List[Dict[str, Any]]) -> int:
        if not rows:
            return 0
        now = datetime.utcnow()
        ops = []
        for row in rows:
            payload = dict(row)
            payload["updated_at"] = now
            ops.append(UpdateOne(
                {"ts_code": payload["ts_code"], "trade_date": payload["trade_date"]},
                {"$set": payload},
                upsert=True,
            ))
        result = self.index_basic_coll.bulk_write(ops, ordered=False)
        return (result.upserted_count or 0) + (result.modified_count or 0)

    def upsert_report(self, report) -> bool:
        try:
            doc = self._report_to_doc(report)
            self.report_coll.update_one({"scan_date": doc["scan_date"]}, {"$set": doc}, upsert=True)
            return True
        except Exception as exc:
            logger.error("upsert_report failed: %s", exc)
            return False

    def get_report(self, scan_date: str) -> Optional[Dict[str, Any]]:
        return self.report_coll.find_one({"scan_date": scan_date}, {"_id": 0})

    def get_latest(self, limit: int = 30) -> List[Dict[str, Any]]:
        return list(self.report_coll.find({}, {"_id": 0}).sort("scan_date", DESCENDING).limit(limit))

    def get_breadth(self, trade_date: str) -> Optional[Dict[str, Any]]:
        return self.breadth_coll.find_one({"trade_date": trade_date}, {"_id": 0})

    def get_breadth_range(self, start: str, end: str) -> pd.DataFrame:
        docs = list(self.breadth_coll.find({"trade_date": {"$gte": start, "$lte": end}}, {"_id": 0}).sort("trade_date", ASCENDING))
        return pd.DataFrame(docs)

    def get_index_basic_range(self, ts_code: str, start: str, end: str) -> pd.DataFrame:
        docs = list(self.index_basic_coll.find({"ts_code": ts_code, "trade_date": {"$gte": start, "$lte": end}}, {"_id": 0}).sort("trade_date", ASCENDING))
        return pd.DataFrame(docs)

    def get_cape_range(self, index_code: str, start: str, end: str) -> pd.DataFrame:
        docs = list(self.cape_coll.find({"index_code": index_code, "trade_date": {"$gte": start, "$lte": end}}, {"_id": 0}).sort("trade_date", ASCENDING))
        return pd.DataFrame(docs)

    def get_hindenburg_triggers(self, end_date: str, window_days: int = 30) -> List[str]:
        start_date = (datetime.strptime(end_date, "%Y%m%d") - timedelta(days=window_days)).strftime("%Y%m%d")
        docs = self.report_coll.find(
            {
                "scan_date": {"$gte": start_date, "$lte": end_date},
                "signals.hindenburg.triggered": True,
            },
            {"scan_date": 1, "_id": 0},
        ).sort("scan_date", ASCENDING)
        return [doc["scan_date"] for doc in docs if doc.get("scan_date") != end_date]

    @staticmethod
    def _report_to_doc(report) -> Dict[str, Any]:
        d = dataclasses.asdict(report)
        return {
            "scan_date": d["scan_date"],
            "generated_at": d["generated_at"],
            "signals": {
                "mcl": d.get("mcl"),
                "hindenburg": d.get("hindenburg"),
                "cape": d.get("cape", []),
            },
            "composite_alert": d.get("composite_alert", "low"),
            "warnings": d.get("warnings", []),
            "updated_at": datetime.utcnow(),
        }


__all__ = ["MarketRegimeRepository"]