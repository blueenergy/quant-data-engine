"""主题补涨候选结果落库 ``quant_data.theme_lag_candidates``。"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List

from pymongo import MongoClient


class ThemeLagRepository:
    def __init__(self, mongo_uri: str, data_db: str) -> None:
        self.client = MongoClient(mongo_uri)
        self.col = self.client[data_db]["theme_lag_candidates"]
        self._ensure_indexes()

    def _ensure_indexes(self) -> None:
        try:
            self.col.create_index(
                [("trade_date", 1), ("index_store_key", 1), ("bucket_id", 1), ("symbol", 1)],
                name="theme_lag_unique_row",
                background=True,
                unique=True,
            )
            self.col.create_index([("trade_date", -1), ("index_store_key", 1)], name="theme_lag_date_idx", background=True)
        except Exception:
            pass

    def replace_for_trade_date(self, trade_date: str, index_store_key: str, documents: List[Dict[str, Any]]) -> int:
        """按交易日 + 指数别名整批替换，幂等。"""
        self.col.delete_many({"trade_date": trade_date, "index_store_key": index_store_key})
        if not documents:
            return 0
        now = datetime.now().isoformat()
        for d in documents:
            d.setdefault("update_time", now)
        self.col.insert_many(documents)
        return len(documents)


__all__ = ["ThemeLagRepository"]
