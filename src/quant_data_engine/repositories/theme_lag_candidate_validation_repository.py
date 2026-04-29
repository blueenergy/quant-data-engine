"""主题补涨候选验证结果落库 ``quant_data.theme_lag_candidate_validation``。"""
from __future__ import annotations

from typing import Any, Dict, List

from pymongo import MongoClient


COLLECTION_NAME = "theme_lag_candidate_validation"


class ThemeLagCandidateValidationRepository:
    def __init__(self, mongo_uri: str, data_db: str) -> None:
        self.client = MongoClient(mongo_uri)
        self.col = self.client[data_db][COLLECTION_NAME]
        self._candidates = self.client[data_db]["theme_lag_candidates"]
        self._ensure_indexes()

    def _ensure_indexes(self) -> None:
        try:
            self.col.create_index(
                [("anchor_trade_date", 1), ("index_store_key", 1)],
                name="tlcv_anchor_index_unique",
                background=True,
                unique=True,
            )
            self.col.create_index([("anchor_trade_date", -1)], name="tlcv_anchor_desc", background=True)
        except Exception:
            pass

    def list_distinct_symbols(self, anchor_trade_date: str, index_store_key: str) -> List[str]:
        td = "".join(c for c in str(anchor_trade_date) if c.isdigit())[:8]
        sk = str(index_store_key).strip()
        return sorted(
            self._candidates.distinct("symbol", {"trade_date": td, "index_store_key": sk})
        )

    def replace_validation_doc(self, doc: Dict[str, Any]) -> bool:
        """整文档 upsert（§6.1），自然键 ``anchor_trade_date`` + ``index_store_key``。"""
        anchor = doc.get("anchor_trade_date")
        sk = doc.get("index_store_key")
        if not anchor or not sk:
            return False
        filt = {"anchor_trade_date": anchor, "index_store_key": sk}
        res = self.col.replace_one(filt, doc, upsert=True)
        return bool(res.upserted_id or res.modified_count or res.matched_count)


__all__ = ["COLLECTION_NAME", "ThemeLagCandidateValidationRepository"]
