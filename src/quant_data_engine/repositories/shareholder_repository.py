"""ShareholderRepository — persistence for shareholder / chips datasets.

Collections (database: quant_data):
  - stk_holdernumber : 股东户数（季度，one row per ts_code+end_date）
  - stk_holdertrade  : 股东增减持（公告驱动）
  - share_float      : 限售股解禁（公告驱动，前瞻 float_date）
  - repurchase       : 股票回购（公告驱动，含 proc 阶段）
  - hk_hold          : 沪深港股通持股明细（季度快照，含北向 A 股与南向港股）

Keying strategy:
  - Snapshot collections (stk_holdernumber, hk_hold) have a clean natural key
    (ts_code + period date), so we key on that.
  - Announcement collections (stk_holdertrade, share_float, repurchase) have no
    stable natural key — Tushare returns several rows per announcement that can
    share any subset of fields, and sometimes exact duplicates. We therefore key
    on a deterministic content hash (``_uid``) of the source fields. Identical
    rows collapse (idempotent); genuinely different rows stay distinct.

All upserts also de-duplicate within a batch so a single fetch never issues two
conflicting upserts for the same key.
"""
from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
from pymongo import UpdateOne

logger = logging.getLogger(__name__)

_META_FIELDS = ("synced_at", "data_source", "_uid", "_id")


def _clean(value: Any) -> Any:
    """NaN/NaT -> None so Mongo stores explicit nulls rather than float('nan')."""
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    return value


def _row_uid(doc: Dict[str, Any]) -> str:
    """Deterministic content hash of source fields (excludes meta fields)."""
    payload = {k: v for k, v in doc.items() if k not in _META_FIELDS}
    blob = json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha1(blob.encode("utf-8")).hexdigest()


class ShareholderRepository:
    """Repository for shareholder / chips collections."""

    def __init__(self, db):
        self._db = db
        self.holdernumber_col = db["stk_holdernumber"]
        self.holdertrade_col = db["stk_holdertrade"]
        self.share_float_col = db["share_float"]
        self.repurchase_col = db["repurchase"]
        self.hk_hold_col = db["hk_hold"]
        self._ensure_indexes()

    @staticmethod
    def _safe_index(col, keys, **kw) -> None:
        try:
            col.create_index(keys, **kw)
        except Exception as e:  # pragma: no cover - index race / pre-existing dup
            logger.warning("create_index %s on %s failed: %s", keys, col.name, e)

    def _ensure_indexes(self) -> None:
        # Snapshot collections: natural unique key
        self._safe_index(self.holdernumber_col, [("ts_code", 1), ("end_date", -1)], unique=True)
        self._safe_index(self.holdernumber_col, [("end_date", -1)])

        self._safe_index(self.hk_hold_col, [("ts_code", 1), ("trade_date", -1)], unique=True)
        self._safe_index(self.hk_hold_col, [("trade_date", -1)])
        self._safe_index(self.hk_hold_col, [("exchange", 1), ("trade_date", -1)])

        # Announcement collections: content-hash unique key + query indexes
        for col in (self.holdertrade_col, self.share_float_col, self.repurchase_col):
            self._safe_index(col, [("_uid", 1)], unique=True)
            self._safe_index(col, [("ts_code", 1), ("ann_date", -1)])
            self._safe_index(col, [("ann_date", -1)])
        self._safe_index(self.share_float_col, [("float_date", -1)])
        self._safe_index(self.repurchase_col, [("ts_code", 1), ("proc", 1)])

    def _upsert(
        self,
        collection,
        df: Optional[pd.DataFrame],
        key_fields: Optional[List[str]] = None,
        *,
        use_hash: bool = False,
    ) -> int:
        """Upsert a DataFrame, de-duplicating within the batch (NaN-safe)."""
        if df is None or getattr(df, "empty", True):
            return 0
        sync_time = datetime.now()
        deduped: Dict[Any, UpdateOne] = {}
        for _, row in df.iterrows():
            doc = {k: _clean(v) for k, v in row.to_dict().items()}
            if use_hash:
                uid = _row_uid(doc)
                doc["_uid"] = uid
                key = {"_uid": uid}
                dedupe_key = uid
            else:
                key = {f: doc.get(f) for f in key_fields}
                dedupe_key = tuple(str(doc.get(f)) for f in key_fields)
            doc["synced_at"] = sync_time
            doc["data_source"] = "tushare"
            deduped[dedupe_key] = UpdateOne(key, {"$set": doc}, upsert=True)
        ops = list(deduped.values())
        if not ops:
            return 0
        try:
            result = collection.bulk_write(ops, ordered=False)
            return (result.upserted_count or 0) + (result.modified_count or 0)
        except Exception as e:
            logger.error("shareholder upsert failed for %s: %s", collection.name, e)
            return 0

    def upsert_holdernumber(self, df: Optional[pd.DataFrame]) -> int:
        return self._upsert(self.holdernumber_col, df, ["ts_code", "end_date"])

    def upsert_hk_hold(self, df: Optional[pd.DataFrame]) -> int:
        return self._upsert(self.hk_hold_col, df, ["ts_code", "trade_date"])

    def upsert_holdertrade(self, df: Optional[pd.DataFrame]) -> int:
        return self._upsert(self.holdertrade_col, df, use_hash=True)

    def upsert_share_float(self, df: Optional[pd.DataFrame]) -> int:
        return self._upsert(self.share_float_col, df, use_hash=True)

    def upsert_repurchase(self, df: Optional[pd.DataFrame]) -> int:
        return self._upsert(self.repurchase_col, df, use_hash=True)


__all__ = ["ShareholderRepository"]
