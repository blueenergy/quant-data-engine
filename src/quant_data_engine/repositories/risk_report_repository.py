"""RiskReportRepository — persists and queries market risk scan results.

Collection: ``risk_reports`` (in data_db / quant_data)

Document shape::

    {
        "scan_date":   "20260502",         # YYYYMMDD — primary query key
        "sector":      "semicon",           # sector profile key
        "generated_at": "2026-05-02 19:31:00",
        "overall_risk_score":  72.3,        # 0-100
        "overall_risk_level":  "high",      # low|medium|high|extreme
        "signals": {
            "ma_deviation":           [...],
            "rsi_signals":            [...],
            "speculation_heat":       [...],
            "factor_crowding":        {...} | null,
            "tech_relative_strength": {...} | null,
        },
        "warnings": [...],
        "updated_at": ISODate
    }

Index strategy:
  - unique (scan_date, sector) — one report per sector per day
  - descending scan_date — fast "latest N days" queries
"""
from __future__ import annotations

import dataclasses
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from pymongo import ASCENDING, DESCENDING, MongoClient, UpdateOne

logger = logging.getLogger(__name__)


class RiskReportRepository:
    """Stores and retrieves ``RiskReport`` objects from MongoDB."""

    COLLECTION = "risk_reports"

    def __init__(self, mongo_uri: str, db_name: str):
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.coll = self.db[self.COLLECTION]
        self._ensure_indexes()

    def _ensure_indexes(self) -> None:
        try:
            self.coll.create_index(
                [("scan_date", ASCENDING), ("sector", ASCENDING)],
                unique=True,
                name="risk_report_date_sector_uidx",
                background=True,
            )
            self.coll.create_index(
                [("scan_date", DESCENDING)],
                name="risk_report_date_desc_idx",
                background=True,
            )
        except Exception as exc:
            logger.warning("RiskReportRepository index creation warning: %s", exc)

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def upsert_report(self, report) -> bool:
        """Persist a ``RiskReport`` dataclass. Returns True on success."""
        try:
            doc = self._to_doc(report)
            key = {"scan_date": doc["scan_date"], "sector": doc["sector"]}
            self.coll.update_one(key, {"$set": doc}, upsert=True)
            logger.info(
                "upsert_report: date=%s sector=%s score=%.1f",
                doc["scan_date"], doc["sector"], doc["overall_risk_score"],
            )
            return True
        except Exception as exc:
            logger.error("upsert_report failed: %s", exc)
            return False

    def upsert_many(self, reports: List) -> int:
        """Bulk upsert a list of ``RiskReport`` objects. Returns op count."""
        if not reports:
            return 0
        ops = [
            UpdateOne(
                {"scan_date": d["scan_date"], "sector": d["sector"]},
                {"$set": d},
                upsert=True,
            )
            for d in (self._to_doc(r) for r in reports)
        ]
        result = self.coll.bulk_write(ops, ordered=False)
        return (result.upserted_count or 0) + (result.modified_count or 0)

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def get_report(self, scan_date: str, sector: str) -> Optional[Dict[str, Any]]:
        """Fetch a single report. Returns raw dict or None."""
        return self.coll.find_one(
            {"scan_date": scan_date, "sector": sector}, {"_id": 0}
        )

    def get_latest(self, sector: str, limit: int = 30) -> List[Dict[str, Any]]:
        """Most recent *limit* reports for a sector, newest first."""
        return list(
            self.coll.find({"sector": sector}, {"_id": 0})
            .sort("scan_date", DESCENDING)
            .limit(limit)
        )

    def get_range(
        self, sector: str, start_date: str, end_date: str
    ) -> List[Dict[str, Any]]:
        """Reports for a sector in [start_date, end_date], ascending."""
        return list(
            self.coll.find(
                {"sector": sector, "scan_date": {"$gte": start_date, "$lte": end_date}},
                {"_id": 0},
            ).sort("scan_date", ASCENDING)
        )

    def get_date_all_sectors(self, scan_date: str) -> List[Dict[str, Any]]:
        """All-sector snapshot for one date (dashboard overview)."""
        return list(self.coll.find({"scan_date": scan_date}, {"_id": 0}))

    def list_available_dates(self, sector: str, limit: int = 90) -> List[str]:
        """Dates that have a saved report for *sector* (newest first)."""
        return [
            doc["scan_date"]
            for doc in self.coll.find(
                {"sector": sector}, {"scan_date": 1, "_id": 0}
            ).sort("scan_date", DESCENDING).limit(limit)
        ]

    # ------------------------------------------------------------------
    # Internal helper
    # ------------------------------------------------------------------

    @staticmethod
    def _to_doc(report) -> Dict[str, Any]:
        d = dataclasses.asdict(report)
        return {
            "scan_date": d["scan_date"],
            "sector": d["sector"],
            "generated_at": d["generated_at"],
            "overall_risk_score": d["overall_risk_score"],
            "overall_risk_level": d["overall_risk_level"],
            "signals": {
                "ma_deviation": d.get("ma_deviation", []),
                "rsi_signals": d.get("rsi_signals", []),
                "speculation_heat": d.get("speculation_heat", []),
                "factor_crowding": d.get("factor_crowding"),
                "tech_relative_strength": d.get("tech_relative_strength"),
            },
            "warnings": d.get("warnings", []),
            "updated_at": datetime.utcnow(),
        }


__all__ = ["RiskReportRepository"]
