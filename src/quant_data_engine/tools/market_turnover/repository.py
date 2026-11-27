"""Persistence for the two turnover collections.

``market_turnover_daily`` lives in the data database (a derived daily fact,
rebuilt from ``volume_price`` + ``etf_daily``). ``market_turnover_intraday``
lives in the realtime database next to the other intraday snapshots.
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, Optional

from pymongo import ASCENDING, DESCENDING, MongoClient

logger = logging.getLogger(__name__)


class TurnoverRepository:
    DAILY_COLL = "market_turnover_daily"
    INTRADAY_COLL = "market_turnover_intraday"
    ALERTS_COLL = "turnover_alerts"

    def __init__(self, mongo_uri: str, data_db: str, realtime_db: str):
        self.client = MongoClient(mongo_uri)
        self.data_db = self.client[data_db]
        self.realtime_db = self.client[realtime_db]
        self.daily_coll = self.data_db[self.DAILY_COLL]
        self.intraday_coll = self.realtime_db[self.INTRADAY_COLL]
        self.alerts_coll = self.realtime_db[self.ALERTS_COLL]
        self._ensure_indexes()

    def _ensure_indexes(self) -> None:
        try:
            self.daily_coll.create_index(
                [("trade_date", ASCENDING)], unique=True, name="turnover_daily_date_uidx", background=True
            )
            self.daily_coll.create_index(
                [("trade_date", DESCENDING)], name="turnover_daily_date_desc_idx", background=True
            )
            self.intraday_coll.create_index(
                [("trade_date", ASCENDING), ("snapshot_time", ASCENDING)],
                unique=True,
                name="turnover_intraday_date_time_uidx",
                background=True,
            )
            self.intraday_coll.create_index(
                [("trade_date", DESCENDING), ("snapshot_time", DESCENDING)],
                name="turnover_intraday_date_time_desc_idx",
                background=True,
            )
            self.alerts_coll.create_index(
                [("trade_date", ASCENDING), ("stage", ASCENDING)],
                unique=True,
                name="turnover_alert_date_stage_uidx",
                background=True,
            )
        except Exception as exc:  # pragma: no cover - index races are non-fatal
            logger.warning("TurnoverRepository index creation warning: %s", exc)

    def upsert_daily(self, doc: Dict[str, Any]) -> bool:
        payload = dict(doc)
        payload["updated_at"] = datetime.utcnow()
        self.daily_coll.update_one({"trade_date": payload["trade_date"]}, {"$set": payload}, upsert=True)
        return True

    def upsert_intraday(self, doc: Dict[str, Any]) -> bool:
        payload = dict(doc)
        payload["updated_at"] = datetime.utcnow()
        self.intraday_coll.update_one(
            {"trade_date": payload["trade_date"], "snapshot_time": payload["snapshot_time"]},
            {"$set": payload},
            upsert=True,
        )
        return True

    def has_intraday(self, trade_date: str, snapshot_time: str) -> bool:
        return (
            self.intraday_coll.find_one(
                {"trade_date": trade_date, "snapshot_time": snapshot_time}, {"_id": 1}
            )
            is not None
        )

    def alert_exists(self, trade_date: str, stage: str) -> bool:
        doc = self.alerts_coll.find_one({"trade_date": trade_date, "stage": stage}, {"_id": 1})
        return doc is not None

    def record_alert(self, trade_date: str, stage: str, ratio: float, *, sent: bool, error: Optional[str] = None) -> None:
        self.alerts_coll.update_one(
            {"trade_date": trade_date, "stage": stage},
            {
                "$set": {
                    "trade_date": trade_date,
                    "stage": stage,
                    "ratio": round(float(ratio), 4),
                    "sent": bool(sent),
                    "error": error,
                    "updated_at": datetime.utcnow(),
                }
            },
            upsert=True,
        )

    def latest_intraday_snapshot(self) -> Optional[str]:
        doc = self.intraday_coll.find_one(
            {}, {"snapshot_time": 1}, sort=[("snapshot_time", DESCENDING)]
        )
        return doc.get("snapshot_time") if doc else None
