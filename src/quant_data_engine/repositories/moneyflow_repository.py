"""MoneyFlowRepository - handles persistence for money flow data."""
from __future__ import annotations
from typing import Sequence, Dict, Any, Optional
from pymongo import MongoClient, UpdateOne
from datetime import datetime
import pandas as pd


class MoneyFlowRepository:
    """Repository for money flow data."""
    
    def __init__(self, mongo_uri: str, db_name: str):
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.coll = self.db["money_flow"]
        self.state_coll = self.db["collect_state"]
        self._ensure_indexes()

    def _ensure_indexes(self):
        """Create necessary indexes for money flow collection."""
        try:
            # Explicitly name the index to avoid conflicts
            self.coll.create_index([("symbol", 1), ("trade_date", 1)], name="symbol_trade_date_idx", background=True)
            self.coll.create_index([("trade_date", -1)], name="moneyflow_trade_date_desc_idx", background=True)
        except Exception as e:
            print(f"Warning: Index creation failed: {e}")

    def upsert_moneyflow(self, records: Sequence[Dict[str, Any]]) -> int:
        """Upsert money flow data.
        
        Args:
            records: List of dicts with money flow data. Each should have:
                    symbol, trade_date, and various money flow indicators
                    
        Returns:
            Number of upserted + modified documents
        """
        if not records:
            return 0
        
        ops = []
        for r in records:
            # Use ts_code as symbol (with exchange suffix)
            symbol = r.get("symbol") or r.get("ts_code", "")
                    
            doc = dict(r)
            doc["symbol"] = symbol
            
            key = {"symbol": symbol, "trade_date": r["trade_date"]}
            ops.append(UpdateOne(key, {"$set": doc}, upsert=True))
        
        if ops:
            result = self.coll.bulk_write(ops, ordered=False)
            return (result.upserted_count or 0) + (result.modified_count or 0)
        return 0

    def update_last_sync_date(self, date_str: str) -> None:
        """Update the last successful sync date in state collection.
        
        Args:
            date_str: Date string in YYYYMMDD format
        """
        try:
            self.state_coll.update_one(
                {'_id': 'money_flow_last_date'},
                {'$set': {'last_date': date_str}},
                upsert=True
            )
        except Exception as e:
            print(f"Failed to update last sync date: {e}")

    def get_last_sync_date(self) -> Optional[str]:
        """Get the last successful sync date from state collection.
        
        Returns:
            Date string (YYYYMMDD) or None
        """
        try:
            doc = self.state_coll.find_one({'_id': 'money_flow_last_date'})
            return doc.get('last_date') if doc else None
        except Exception:
            return None

    def get_moneyflow_data(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        """Get money flow data for a stock.
        
        Args:
            symbol: Stock symbol
            start_date: Start date (YYYYMMDD)
            end_date: End date (YYYYMMDD)
            
        Returns:
            DataFrame with money flow data
        """
        cursor = self.coll.find(
            {
                "symbol": symbol,
                "trade_date": {"$gte": start_date, "$lte": end_date}
            },
            sort=[("trade_date", 1)]
        )
        docs = list(cursor)
        return pd.DataFrame(docs) if docs else pd.DataFrame()


__all__ = ["MoneyFlowRepository"]
