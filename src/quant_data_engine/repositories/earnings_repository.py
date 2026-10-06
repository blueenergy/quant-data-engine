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
            self._drop_legacy_unique_index(col, "ts_code_1_end_date_-1")
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

        # One broker report forecasts several years and comes back as one row per
        # ``quarter``. Keying without it kept only the row that happened to land
        # last in the batch — the *furthest* year — so 70% of the collection was
        # 2028Q4 and the per-year consensus the readers aggregate was unfillable.
        self._drop_legacy_unique_index(self._report_rc_col, "ts_code_1_report_date_-1_org_name_1")
        self._report_rc_col.create_index(
            [("ts_code", 1), ("report_date", -1), ("org_name", 1), ("quarter", 1)],
            unique=True
        )
        self._report_rc_col.create_index([("report_date", -1)])
        logger.info("Earnings Forecast/Express/Disclosure/ReportRC indexes ensured.")

    @staticmethod
    def _drop_legacy_unique_index(col, index_name: str) -> None:
        """Drop a unique index that a wider key has superseded.

        This is not optional: while the old index exists MongoDB keeps rejecting
        the second row of what is now a perfectly distinct key, so the new
        dimension stays unrepresentable no matter what the upsert filter says.
        The rejection surfaces as "0 records written" rather than an error,
        because the caller swallows the ``BulkWriteError``.
        """
        if index_name in col.index_information():
            col.drop_index(index_name)
            logger.warning("Dropped legacy unique index %s on %s", index_name, col.name)

    @staticmethod
    def _key_filter(rec: Dict[str, Any], key_fields: tuple) -> Dict[str, Any]:
        return {field: _clean_key(rec.get(field)) for field in key_fields}

    def _upsert_by_key(self, col, records: List[Dict[str, Any]], key_fields: tuple, label: str) -> int:
        """Upsert records under the composite key ``key_fields``.

        Dedupes inside the batch first: with ``ordered=False`` a single duplicate
        key aborts the entire bulk_write, which the caller would then swallow as
        "0 records written" — silent total loss for that batch. Normalising the
        key fields (NaN → "") can itself create duplicates, so this has to run
        *after* normalisation, never before.
        """
        if not records:
            return 0

        by_key: Dict[tuple, Dict[str, Any]] = {}
        for rec in records:
            filt = self._key_filter(rec, key_fields)
            # Store the *normalised* key fields too: if the filter cleaned a NaN
            # but the document kept it, the next identical upsert would filter on
            # "", miss the nan-keyed doc, and insert a second copy of it.
            by_key[tuple(filt[field] for field in key_fields)] = {**rec, **filt}

        operations = [
            UpdateOne(self._key_filter(rec, key_fields), {"$set": rec}, upsert=True)
            for rec in by_key.values()
        ]

        try:
            result = col.bulk_write(operations, ordered=False)
            return result.upserted_count + result.modified_count
        except Exception as e:
            logger.error(f"Failed to upsert earnings {label} ({len(operations)} ops): {e}")
            return 0

    # A company can announce the same report period more than once: the first
    # 预告/快报 plus later revisions. ``ann_date`` is what turns a row into a
    # *version*; keying on ``end_date`` alone lost the revision history.
    _VERSION_KEY = ("ts_code", "end_date", "ann_date")
    # A broker report forecasts several years, one row per ``quarter``. Dropping
    # that dimension kept only the furthest year (70% of the collection).
    _BROKER_KEY = ("ts_code", "report_date", "org_name", "quarter")

    def upsert_forecasts(self, records: List[Dict[str, Any]]) -> int:
        """Upsert earnings forecast records (one document per announcement version)."""
        return self._upsert_by_key(self._forecast_col, records, self._VERSION_KEY, "forecasts")

    def upsert_express(self, records: List[Dict[str, Any]]) -> int:
        """Upsert earnings express records (one document per announcement version)."""
        return self._upsert_by_key(self._express_col, records, self._VERSION_KEY, "expresses")

    def upsert_disclosure(self, records: List[Dict[str, Any]]) -> int:
        """Upsert earnings disclosure schedule records.

        One row per report period — the API returns the *current* schedule, not a
        history — so the key has no version dimension.
        """
        return self._upsert_by_key(
            self._disclosure_col, records, ("ts_code", "end_date"), "disclosure schedules"
        )

    def upsert_report_rc(self, records: List[Dict[str, Any]]) -> int:
        """Upsert broker report forecasts (report_rc).

        One research report covers several forecast years and arrives as one row
        per ``quarter``; keying without it kept only whichever row the batch
        happened to write last — the furthest year — leaving the per-year
        consensus the readers aggregate unfillable.
        """
        return self._upsert_by_key(self._report_rc_col, records, self._BROKER_KEY, "broker reports")
