"""Persist copy-homework snapshots: products, stocks, pair meta."""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional
from uuid import uuid4

from pymongo import MongoClient, UpdateOne

logger = logging.getLogger(__name__)

UNIVERSE_ACTIVE = "active_equity"


class FundCopyworkRepository:
    def __init__(self, mongo_uri: str, data_db: str):
        self._client = MongoClient(mongo_uri)
        db = self._client[data_db]
        self.products = db["fund_copywork_products"]
        self.stocks = db["fund_copywork_stocks"]
        self.meta = db["fund_copywork_meta"]
        self._ensure_indexes()

    def _ensure_indexes(self) -> None:
        try:
            self.products.create_index(
                [("fund_key", 1), ("period_curr", 1), ("period_prev", 1), ("universe", 1)],
                unique=True,
                background=True,
            )
            self.products.create_index(
                [("universe", 1), ("period_curr", 1), ("rank", 1)],
                background=True,
            )
            self.stocks.create_index(
                [("symbol", 1), ("period_curr", 1), ("period_prev", 1), ("universe", 1)],
                unique=True,
                background=True,
            )
            self.stocks.create_index(
                [("universe", 1), ("period_curr", 1), ("n_operated", -1)],
                background=True,
            )
            self.meta.create_index(
                [("period_curr", 1), ("period_prev", 1), ("universe", 1)],
                unique=True,
                background=True,
            )
        except Exception as exc:  # pragma: no cover
            logger.warning("fund_copywork index ensure: %s", exc)

    def replace_pair(
        self,
        *,
        period_prev: str,
        period_curr: str,
        universe: str,
        meta: Dict[str, Any],
        products: Iterable[Dict[str, Any]],
        stocks: Iterable[Dict[str, Any]],
    ) -> Dict[str, int]:
        run_id = uuid4().hex
        now = datetime.utcnow()
        scope = {
            "period_prev": period_prev,
            "period_curr": period_curr,
            "universe": universe,
        }
        n_products = self._replace(
            self.products,
            products,
            scope=scope,
            run_id=run_id,
            now=now,
            extra={"universe": universe},
        )
        n_stocks = self._replace(
            self.stocks,
            stocks,
            scope=scope,
            run_id=run_id,
            now=now,
            extra={"universe": universe},
        )
        doc = dict(meta)
        doc.update(scope)
        doc["updated_at"] = now
        doc["_snapshot_run_id"] = run_id
        self.meta.update_one(scope, {"$set": doc}, upsert=True)
        return {"products": n_products, "stocks": n_stocks}

    def _replace(
        self,
        coll,
        rows: Iterable[Dict[str, Any]],
        *,
        scope: Dict[str, str],
        run_id: str,
        now: datetime,
        extra: Optional[Dict[str, Any]] = None,
    ) -> int:
        ops: List[UpdateOne] = []
        extra = extra or {}
        for row in rows:
            doc = dict(row)
            doc.update(scope)
            doc.update(extra)
            doc["updated_at"] = now
            doc["_snapshot_run_id"] = run_id
            if coll is self.products:
                key = {
                    "fund_key": doc.get("fund_key"),
                    **scope,
                }
                if not key["fund_key"]:
                    continue
            else:
                key = {
                    "symbol": doc.get("symbol"),
                    **scope,
                }
                if not key["symbol"]:
                    continue
            ops.append(UpdateOne(key, {"$set": doc}, upsert=True))
        written = 0
        if ops:
            res = coll.bulk_write(ops, ordered=False)
            written = int((res.upserted_count or 0) + (res.modified_count or 0))
        coll.delete_many({**scope, "_snapshot_run_id": {"$ne": run_id}})
        return written
