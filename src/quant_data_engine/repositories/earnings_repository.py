import logging
import math
from typing import List, Dict, Any
from pymongo import UpdateOne

logger = logging.getLogger(__name__)


def _clean_key(value: Any) -> Any:
    """Normalise a key field so NaN/None from ``to_dict("records")`` is storable."""
    if value is None:
        return ""
    if isinstance(value, float) and math.isnan(value):
        return ""
    return value

class EarningsRepository:
    """Repository for managing earnings forecast and express reports data."""
    
    def __init__(self, db):
        self._db = db
        # 业绩预告 collection
        self._forecast_col = self._db["earnings_forecast"]
        # 业绩快报 collection
        self._express_col = self._db["earnings_express"]
        # 财报披露计划 collection
        self._disclosure_col = self._db["earnings_disclosure"]
        # 券商盈利预测 collection
        self._report_rc_col = self._db["earnings_report_rc"]
        
        self._ensure_indexes()

    def _ensure_indexes(self):
        """Create unique indexes to ensure idempotency."""
        # A company can announce the same report period more than once: the first
        # 预告/快报 plus later revisions. ``end_date`` alone is therefore NOT unique
        # — keying on (ts_code, end_date) overwrote the original with every revision
        # and lost the revision history that makes a surprise measurable.
        # ``ann_date`` is what turns a row into a *version*.
        for col in (self._forecast_col, self._express_col):
            self._drop_legacy_period_index(col)
            col.create_index(
                [("ts_code", 1), ("end_date", -1), ("ann_date", -1)],
                unique=True
            )
            col.create_index([("ann_date", -1)])

        # Disclosure schedules genuinely are one row per report period — the API
        # returns the *current* schedule, not a history — so this key stays.
        self._disclosure_col.create_index(
            [("ts_code", 1), ("end_date", -1)],
            unique=True
        )
        self._disclosure_col.create_index([("pre_date", -1)])

        self._report_rc_col.create_index(
            [("ts_code", 1), ("report_date", -1), ("org_name", 1)],
            unique=True
        )
        self._report_rc_col.create_index([("report_date", -1)])
        logger.info("Earnings Forecast/Express/Disclosure/ReportRC indexes ensured.")

    @staticmethod
    def _drop_legacy_period_index(col) -> None:
        """Drop the old (ts_code, end_date) unique index on forecast/express.

        This is not optional: while it exists MongoDB keeps rejecting a second
        announcement for the same period, so revisions stay unrepresentable no
        matter what the upsert filter says.
        """
        legacy = "ts_code_1_end_date_-1"
        if legacy in col.index_information():
            col.drop_index(legacy)
            logger.warning("Dropped legacy unique index %s on %s", legacy, col.name)

    @staticmethod
    def _version_filter(rec: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "ts_code": _clean_key(rec.get("ts_code")),
            "end_date": _clean_key(rec.get("end_date")),
            "ann_date": _clean_key(rec.get("ann_date")),
        }

    def _upsert_versions(self, col, records: List[Dict[str, Any]], label: str) -> int:
        """Upsert announcement versions keyed by (ts_code, end_date, ann_date).

        Dedupes inside the batch first: with ``ordered=False`` a single duplicate
        key aborts the entire bulk_write, which the caller would then swallow as
        "0 records written" — silent total loss for that batch.
        """
        if not records:
            return 0

        by_key: Dict[tuple, Dict[str, Any]] = {}
        for rec in records:
            filt = self._version_filter(rec)
            # Store the *normalised* key fields too: if the filter cleaned a NaN
            # but the document kept it, the next identical upsert would filter on
            # "" , miss the nan-keyed doc, and insert a second copy of it.
            by_key[(filt["ts_code"], filt["end_date"], filt["ann_date"])] = {**rec, **filt}

        operations = [
            UpdateOne(self._version_filter(rec), {"$set": rec}, upsert=True)
            for rec in by_key.values()
        ]

        try:
            result = col.bulk_write(operations, ordered=False)
            return result.upserted_count + result.modified_count
        except Exception as e:
            logger.error(f"Failed to upsert earnings {label} ({len(operations)} ops): {e}")
            return 0

    def upsert_forecasts(self, records: List[Dict[str, Any]]) -> int:
        """Upsert earnings forecast records (one document per announcement version)."""
        return self._upsert_versions(self._forecast_col, records, "forecasts")

    def upsert_express(self, records: List[Dict[str, Any]]) -> int:
        """Upsert earnings express records (one document per announcement version)."""
        return self._upsert_versions(self._express_col, records, "expresses")

    def upsert_disclosure(self, records: List[Dict[str, Any]]) -> int:
        """Upsert earnings disclosure schedule records."""
        if not records:
            return 0
            
        operations = [
            UpdateOne(
                {"ts_code": rec["ts_code"], "end_date": rec["end_date"]},
                {"$set": rec},
                upsert=True
            )
            for rec in records
        ]
        
        try:
            result = self._disclosure_col.bulk_write(operations, ordered=False)
            return result.upserted_count + result.modified_count
        except Exception as e:
            logger.error(f"Failed to upsert earnings disclosure schedules: {e}")
            return 0

    def upsert_report_rc(self, records: List[Dict[str, Any]]) -> int:
        """Upsert earnings broker report forecasts (report_rc)."""
        if not records:
            return 0
            
        operations = [
            UpdateOne(
                {"ts_code": rec["ts_code"], "report_date": rec["report_date"], "org_name": rec.get("org_name", "")},
                {"$set": rec},
                upsert=True
            )
            for rec in records
        ]
        
        try:
            result = self._report_rc_col.bulk_write(operations, ordered=False)
            return result.upserted_count + result.modified_count
        except Exception as e:
            logger.error(f"Failed to upsert broker reports: {e}")
            return 0
