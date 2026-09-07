"""Mongo index_constituents writes (aligned with quantFinance IndexService cache layout)."""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List

from pymongo import MongoClient
from quant_data_engine.revision_ledger import (
    INDEX_CONSTITUENTS_SOURCE,
    require_active_ledger_token,
)


class IndexConstituentsRepository:
    def __init__(self, mongo_uri: str, data_db: str) -> None:
        self._col = MongoClient(mongo_uri)[data_db]["index_constituents"]
        self._ensure_indexes()

    def _ensure_indexes(self) -> None:
        try:
            self._col.create_index([("index_code", 1), ("symbol", 1)])
            self._col.create_index([("update_date", 1)])
            self._col.create_index([("index_code", 1), ("weight_trade_date", -1), ("update_date", -1)])
        except Exception:
            pass

    def replace_for_sync_day(
        self, store_key: str, update_date: str, documents: List[Dict[str, Any]]
    ) -> int:
        """Delete existing rows for (store_key, update_date) then insert fresh set."""
        require_active_ledger_token(INDEX_CONSTITUENTS_SOURCE)
        self._col.delete_many({"index_code": store_key, "update_date": update_date})
        if not documents:
            return 0
        self._col.insert_many(documents)
        return len(documents)

    def replace_for_weight_trade_date(
        self,
        store_key: str,
        weight_trade_date: str,
        documents: List[Dict[str, Any]],
    ) -> int:
        """Upsert one historical snapshot keyed by ``(index_code, weight_trade_date)``.

        Used by historical backfill tasks. Keeps the latest ``update_date`` row set
        for a given ``weight_trade_date`` and does not touch other snapshots.
        """
        require_active_ledger_token(INDEX_CONSTITUENTS_SOURCE)
        wt = str(weight_trade_date or "").replace("-", "")[:8]
        self._col.delete_many({"index_code": store_key, "weight_trade_date": wt})
        if not documents:
            return 0
        self._col.insert_many(documents)
        return len(documents)


def build_documents(
    *,
    store_key: str,
    update_date: str,
    weight_trade_date: str,
    df_rows: List[Dict[str, Any]],
    basic_by_ts: Dict[str, Dict[str, str]],
) -> List[Dict[str, Any]]:
    """Build Mongo docs matching quantFinance _cache_constituents shape (+ weight_trade_date)."""
    now = datetime.now().strftime("%Y%m%d %H:%M:%S")
    out: List[Dict[str, Any]] = []
    for row in df_rows:
        sym = str(row.get("con_code", "")).strip()
        if not sym:
            continue
        meta = basic_by_ts.get(sym, {})
        w = row.get("weight")
        try:
            weight = float(w) if w is not None else 0.0
        except (TypeError, ValueError):
            weight = 0.0
        out.append(
            {
                "index_code": store_key,
                "update_date": update_date,
                "symbol": sym,
                "name": meta.get("name", ""),
                "industry": meta.get("industry", ""),
                "market_cap": 0.0,
                "weight": weight,
                "update_time": now,
                "weight_trade_date": weight_trade_date,
            }
        )
    return out


__all__ = ["IndexConstituentsRepository", "build_documents"]
