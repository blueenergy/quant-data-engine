"""Persistence for Tushare dragon-tiger / hot-money collections.

Collections (``quant_data``):

- ``top_list``  龙虎榜每日明细, keyed by ``(trade_date, ts_code, reason)``
- ``top_inst``  龙虎榜机构/营业部成交明细, **not** uniquely keyed
- ``hm_list``   游资名录, snapshot keyed by ``name``

``top_inst`` repeats ``exalter="机构专用"`` once per institutional seat for
the same ``(ts_code, side, reason)``, so a natural-key upsert would silently
collapse distinct seats.  Both daily tables are therefore replaced per
``trade_date`` (delete + insert): re-running a day is idempotent, and stale
rows from an earlier partial pull disappear.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional, Sequence

import pandas as pd
from pymongo import MongoClient


class DragonTigerRepository:
    """Repository for ``top_list`` / ``top_inst`` / ``hm_list``."""

    def __init__(self, mongo_uri: str, db_name: str):
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.top_list = self.db["top_list"]
        self.top_inst = self.db["top_inst"]
        self.hm_list = self.db["hm_list"]
        self.state_coll = self.db["collect_state"]
        self._ensure_indexes()

    def _ensure_indexes(self) -> None:
        try:
            self.top_list.create_index(
                [("trade_date", -1), ("ts_code", 1)],
                name="top_list_date_ts_idx",
                background=True,
            )
            self.top_list.create_index(
                [("ts_code", 1), ("trade_date", -1)],
                name="top_list_ts_date_idx",
                background=True,
            )
            self.top_inst.create_index(
                [("trade_date", -1), ("ts_code", 1)],
                name="top_inst_date_ts_idx",
                background=True,
            )
            self.top_inst.create_index(
                [("exalter", 1), ("trade_date", -1)],
                name="top_inst_exalter_date_idx",
                background=True,
            )
            self.hm_list.create_index(
                [("name", 1)],
                name="hm_list_name_idx",
                unique=True,
                background=True,
            )
        except Exception as exc:
            print(f"Warning: dragon-tiger index creation failed: {exc}")

    @staticmethod
    def _stamp(records: Sequence[Dict[str, Any]]) -> list[Dict[str, Any]]:
        now = datetime.utcnow()
        stamped = []
        for record in records:
            doc = dict(record)
            doc["updated_at"] = now
            stamped.append(doc)
        return stamped

    def replace_day(
        self,
        collection_name: str,
        trade_date: str,
        records: Sequence[Dict[str, Any]],
    ) -> int:
        """Delete one trading day's rows and insert the freshly fetched set."""
        coll = self.db[collection_name]
        coll.delete_many({"trade_date": trade_date})
        if not records:
            return 0
        coll.insert_many(self._stamp(records), ordered=False)
        return len(records)

    def replace_hm_list(self, records: Sequence[Dict[str, Any]]) -> int:
        """Replace the whole hot-money snapshot (small, slow-moving)."""
        if not records:
            return 0
        self.hm_list.delete_many({})
        self.hm_list.insert_many(self._stamp(records), ordered=False)
        return len(records)

    def update_last_sync_date(self, kind: str, date_str: str) -> None:
        try:
            self.state_coll.update_one(
                {"_id": f"dragon_tiger_last_date_{kind}"},
                {"$set": {"last_date": date_str, "kind": kind}},
                upsert=True,
            )
        except Exception as exc:
            print(f"Failed to update dragon-tiger last sync date ({kind}): {exc}")

    def get_last_sync_date(self, kind: str) -> Optional[str]:
        try:
            doc = self.state_coll.find_one({"_id": f"dragon_tiger_last_date_{kind}"})
            return doc.get("last_date") if doc else None
        except Exception:
            return None

    def get_top_list(
        self,
        ts_code: str,
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        cursor = self.top_list.find(
            {
                "ts_code": ts_code,
                "trade_date": {"$gte": start_date, "$lte": end_date},
            },
            sort=[("trade_date", 1)],
        )
        docs = list(cursor)
        return pd.DataFrame(docs) if docs else pd.DataFrame()

    def get_top_inst(
        self,
        ts_code: str,
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        cursor = self.top_inst.find(
            {
                "ts_code": ts_code,
                "trade_date": {"$gte": start_date, "$lte": end_date},
            },
            sort=[("trade_date", 1)],
        )
        docs = list(cursor)
        return pd.DataFrame(docs) if docs else pd.DataFrame()


__all__ = ["DragonTigerRepository"]
