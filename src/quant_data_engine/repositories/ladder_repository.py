"""Repository for limit-up ladder data storage.

Handles MongoDB operations for daily limit-up stocks, ladder aggregations,
and sector flow data.
"""

from datetime import datetime
from typing import Dict, List, Optional, Any
from pymongo import MongoClient, ASCENDING, DESCENDING
from collections import defaultdict


class LadderRepository:
    """Repository for limit-up ladder data."""
    
    def __init__(self, mongo_uri: str, mongo_db: str):
        self.client = MongoClient(mongo_uri)
        self.db = self.client[mongo_db]
        self.daily_limit_up = self.db["daily_limit_up"]
        self.limit_up_ladder = self.db["limit_up_ladder"]
        self.sector_flow = self.db["sector_flow"]
        
        self._ensure_indexes()
    
    def _ensure_indexes(self):
        """Create necessary indexes."""
        # daily_limit_up indexes
        self.daily_limit_up.create_index([("date", ASCENDING), ("symbol", ASCENDING)], unique=True)
        self.daily_limit_up.create_index([("date", ASCENDING), ("streak", DESCENDING)])
        self.daily_limit_up.create_index([("date", ASCENDING), ("primary_sector", ASCENDING)])
        
        # limit_up_ladder indexes
        self.limit_up_ladder.create_index([("date", ASCENDING)], unique=True)
        
        # sector_flow indexes
        self.sector_flow.create_index([("date", ASCENDING), ("sector", ASCENDING)], unique=True)
    
    def save_daily_stock(self, doc: Dict[str, Any]) -> bool:
        """Save a single limit-up stock record."""
        try:
            # Ensure output is clean
            if "_id" in doc:
                del doc["_id"]
                
            self.daily_limit_up.update_one(
                {"date": doc["date"], "symbol": doc["symbol"]},
                {"$set": doc},
                upsert=True
            )
            return True
        except Exception as e:
            print(f"Error saving {doc.get('symbol')}: {e}")
            return False

    def mark_as_broken(self, date: str, symbol: str) -> bool:
        """Mark a stock as broken (no longer limit up)."""
        try:
            self.daily_limit_up.update_one(
                {"date": date, "symbol": symbol},
                {"$set": {"status": "broken", "limit_up_status": "open", "updated_at": datetime.now()}}
            )
            return True
        except Exception as e:
            print(f"Error marking {symbol} as broken: {e}")
            return False
    
    def save_ladder(self, doc: Dict[str, Any]) -> bool:
        """Save ladder aggregation data."""
        try:
            self.limit_up_ladder.update_one(
                {"date": doc["date"]},
                {"$set": doc},
                upsert=True
            )
            return True
        except Exception as e:
            print(f"Error saving ladder for {doc.get('date')}: {e}")
            return False
    
    def save_sector_flow(self, doc: Dict[str, Any]) -> bool:
        """Save sector flow data."""
        try:
            self.sector_flow.update_one(
                {"date": doc["date"], "sector": doc["sector"]},
                {"$set": doc},
                upsert=True
            )
            return True
        except Exception as e:
            print(f"Error saving sector flow: {e}")
            return False
    
    def get_yesterday_stocks(self, date: str) -> Dict[str, int]:
        """Get yesterday's limit-up stocks with their streak counts."""
        # Simple date calculation (doesn't handle weekends perfectly)
        from datetime import datetime, timedelta
        dt = datetime.strptime(date, "%Y%m%d")
        prev_dt = dt - timedelta(days=1)
        while prev_dt.weekday() >= 5:  # Skip weekends
            prev_dt -= timedelta(days=1)
        yesterday = prev_dt.strftime("%Y%m%d")
        
        stocks = {}
        for doc in self.daily_limit_up.find({"date": yesterday}):
            stocks[doc["symbol"]] = doc["streak"]
        return stocks
    
    def get_daily_stocks(self, date: str) -> List[Dict]:
        """Get all limit-up stocks for a date."""
        return list(self.daily_limit_up.find({"date": date}))
    
    def get_last_sync_date(self) -> Optional[str]:
        """Get the most recent synced date."""
        doc = self.limit_up_ladder.find_one(
            {},
            sort=[("date", DESCENDING)]
        )
        return doc["date"] if doc else None
    
    def get_ladder(self, date: str) -> Optional[Dict]:
        """Get ladder data for a date."""
        return self.limit_up_ladder.find_one({"date": date})
    
    def count_by_date(self, date: str) -> int:
        """Count limit-up stocks for a date."""
        return self.daily_limit_up.count_documents({"date": date})
