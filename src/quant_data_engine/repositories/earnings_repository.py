import logging
from typing import List, Dict, Any
from pymongo import UpdateOne

logger = logging.getLogger(__name__)

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
        # A single company can only have one forecast/express for a specific report period (end_date)
        # Using ts_code + end_date (report period) as unique identifier
        self._forecast_col.create_index(
            [("ts_code", 1), ("end_date", -1)],
            unique=True
        )
        self._forecast_col.create_index([("ann_date", -1)])
        
        self._express_col.create_index(
            [("ts_code", 1), ("end_date", -1)],
            unique=True
        )
        self._express_col.create_index([("ann_date", -1)])
        
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

    def upsert_forecasts(self, records: List[Dict[str, Any]]) -> int:
        """Upsert earnings forecast records."""
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
            result = self._forecast_col.bulk_write(operations, ordered=False)
            return result.upserted_count + result.modified_count
        except Exception as e:
            logger.error(f"Failed to upsert earnings forecasts: {e}")
            return 0

    def upsert_express(self, records: List[Dict[str, Any]]) -> int:
        """Upsert earnings express records."""
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
            result = self._express_col.bulk_write(operations, ordered=False)
            return result.upserted_count + result.modified_count
        except Exception as e:
            logger.error(f"Failed to upsert earnings expresses: {e}")
            return 0

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
