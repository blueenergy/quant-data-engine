"""Persist active-fund inventory snapshots (one doc per stock × period pair)."""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional
from uuid import uuid4

from pymongo import MongoClient, UpdateOne

logger = logging.getLogger(__name__)

UNIVERSE_ACTIVE = "active_equity"


class FundInventoryRepository:
    def __init__(self, mongo_uri: str, data_db: str):
        self._client = MongoClient(mongo_uri)
        self._col = self._client[data_db]["fund_inventory_snapshots"]
        self._ensure_indexes()

    def _ensure_indexes(self) -> None:
        try:
            self._col.create_index(
                [("symbol", 1), ("period_curr", 1), ("period_prev", 1), ("universe", 1)],
                unique=True,
                background=True,
            )
            self._col.create_index(
                [
                    ("universe", 1),
                    ("period_curr", 1),
                    ("price_adjusted", 1),
                    ("state", 1),
                    ("net_ex_price", -1),
                ],
                background=True,
            )
            self._col.create_index(
                [
                    ("universe", 1),
                    ("period_curr", 1),
                    ("price_adjusted", 1),
                    ("net_ex_price", -1),
                ],
                background=True,
            )
            self._col.create_index(
                [
                    ("symbol", 1),
                    ("universe", 1),
                    ("price_adjusted", 1),
                    ("period_curr", -1),
                ],
                background=True,
            )
            self._col.create_index(
                [("universe", 1), ("period_curr", -1)],
                background=True,
            )
        except Exception as e:  # pragma: no cover
            logger.warning("fund_inventory_snapshots index ensure: %s", e)

    def upsert_snapshots(self, snapshots: Iterable[Dict[str, Any]]) -> int:
        return self._write_snapshots(snapshots)

    def _write_snapshots(
        self,
        snapshots: Iterable[Dict[str, Any]],
        *,
        run_id: Optional[str] = None,
    ) -> int:
        ops = []
        now = datetime.utcnow()
        for snap in snapshots:
            symbol = snap.get("symbol")
            period_curr = snap.get("period_curr")
            period_prev = snap.get("period_prev")
            universe = snap.get("universe") or UNIVERSE_ACTIVE
            if not symbol or not period_curr or not period_prev:
                continue
            doc = dict(snap)
            doc["universe"] = universe
            doc["updated_at"] = now
            if run_id:
                doc["_snapshot_run_id"] = run_id
            ops.append(
                UpdateOne(
                    {
                        "symbol": symbol,
                        "period_curr": period_curr,
                        "period_prev": period_prev,
                        "universe": universe,
                    },
                    {"$set": doc},
                    upsert=True,
                )
            )
        if not ops:
            return 0
        res = self._col.bulk_write(ops, ordered=False)
        return int((res.upserted_count or 0) + (res.modified_count or 0))

    def replace_snapshots(self, snapshots: Iterable[Dict[str, Any]]) -> int:
        """Replace one report-pair snapshot after a successful full build."""
        rows = list(snapshots)
        scopes = {
            (
                row.get("period_prev"),
                row.get("period_curr"),
                row.get("universe") or UNIVERSE_ACTIVE,
            )
            for row in rows
            if row.get("period_prev") and row.get("period_curr")
        }
        if not rows or len(scopes) != 1:
            raise ValueError("replace_snapshots requires one non-empty report-pair scope")
        period_prev, period_curr, universe = next(iter(scopes))
        run_id = uuid4().hex
        written = self._write_snapshots(rows, run_id=run_id)
        self._col.delete_many(
            {
                "period_prev": period_prev,
                "period_curr": period_curr,
                "universe": universe,
                "_snapshot_run_id": {"$ne": run_id},
            }
        )
        return written

    def latest_period(self, universe: str = UNIVERSE_ACTIVE) -> Optional[str]:
        doc = self._col.find_one(
            {"universe": universe},
            {"_id": 0, "period_curr": 1},
            sort=[("period_curr", -1)],
        )
        return (doc or {}).get("period_curr")

    def find_for_symbol(
        self,
        candidates: List[str],
        *,
        universe: str = UNIVERSE_ACTIVE,
    ) -> Optional[Dict[str, Any]]:
        keys = [str(c).split(".")[0] if c else "" for c in candidates]
        keys = [k for k in keys if k]
        if not keys:
            return None
        return self._col.find_one(
            {"symbol": {"$in": keys + candidates}, "universe": universe},
            {"_id": 0},
            sort=[("period_curr", -1)],
        )
