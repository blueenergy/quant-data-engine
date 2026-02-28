"""StockRepository - handles persistence for stock daily OHLCV and basic indicators."""
from __future__ import annotations
from typing import Sequence, Dict, Any, Optional, List
from pymongo import MongoClient, UpdateOne
from datetime import datetime
import pandas as pd


class StockRepository:
    """Repository for stock daily OHLCV data and basic indicators."""
    
    def __init__(self, mongo_uri: str, db_name: str):
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.volume_price_coll = self.db["volume_price"]
        self.daily_basic_coll = self.db["financial_daily_basic"]
        self._ensure_indexes()

    def _ensure_indexes(self):
        """Create necessary indexes for efficient queries."""
        try:
            # Index for volume_price collection - explicitly named
            self.volume_price_coll.create_index([("symbol", 1), ("trade_date", 1)], name="symbol_trade_date_idx", background=True)
            self.volume_price_coll.create_index([("trade_date", 1)], name="trade_date_idx", background=True)
            
            # Index for daily_basic collection
            self.daily_basic_coll.create_index([("ts_code", 1), ("trade_date", -1)], background=True)
            self.daily_basic_coll.create_index([("trade_date", -1)], background=True)
        except Exception as e:
            print(f"Warning: Index creation failed: {e}")

    # ==================== Daily OHLCV ====================
    
    def upsert_daily(self, records: Sequence[Dict[str, Any]]) -> int:
        """Upsert stock daily OHLCV data.
        
        Args:
            records: List of dicts with keys: symbol, trade_date, open, high, low, close, 
                    pre_close, change, pct_chg, volume, amount
                    
        Returns:
            Number of upserted + modified documents
        """
        if not records:
            return 0
        
        ops = []
        for r in records:
            # Use ts_code as symbol (with exchange suffix)
            symbol = r.get("symbol") or r.get("ts_code", "")
            
            doc = {
                "symbol": symbol,
                "trade_date": r["trade_date"],
                # sanitize NaN -> None so Mongo does not store NaN
                "open": float(r["open"]) if (r.get("open") is not None and not pd.isna(r.get("open"))) else None,
                "high": float(r["high"]) if (r.get("high") is not None and not pd.isna(r.get("high"))) else None,
                "low": float(r["low"]) if (r.get("low") is not None and not pd.isna(r.get("low"))) else None,
                "close": float(r["close"]) if (r.get("close") is not None and not pd.isna(r.get("close"))) else None,
                "pre_close": float(r.get("pre_close", 0)) if r.get("pre_close") is not None else None,
                "change": float(r.get("change", 0)) if r.get("change") is not None else None,
                "pct_chg": float(r.get("pct_chg", 0)) if r.get("pct_chg") is not None else None,
                "volume": int(r.get("volume", 0)) if r.get("volume") is not None else 0,
                "amount": float(r.get("amount", 0)) if r.get("amount") is not None else None,
                "update_time": datetime.now().strftime('%Y%m%d %H:%M:%S'),
                "source": "tushare"
            }
            
            key = {"symbol": symbol, "trade_date": r["trade_date"]}
            ops.append(UpdateOne(key, {"$set": doc}, upsert=True))
        
        result = self.volume_price_coll.bulk_write(ops, ordered=False)
        return (result.upserted_count or 0) + (result.modified_count or 0)

    # ==================== Daily Basic Indicators ====================
    
    def upsert_daily_basic(self, records: Sequence[Dict[str, Any]]) -> int:
        """Upsert stock daily basic indicators.
        
        Args:
            records: List of dicts with keys: ts_code, trade_date, pe, pe_ttm, pb, ps, etc.
                    
        Returns:
            Number of upserted + modified documents
        """
        if not records:
            return 0
        
        ops = []
        current_time = datetime.now()
        
        for r in records:
            # Use ts_code as symbol (with exchange suffix)
            symbol = r.get("symbol") or r.get("ts_code", "")
            
            doc = {
                "ts_code": r["ts_code"],
                "symbol": symbol,
                "trade_date": r["trade_date"],
                "end_date": r["trade_date"],  # For compatibility with existing collection
                "pe": float(r["pe"]) if r.get("pe") is not None and pd.notna(r.get("pe")) else None,
                "pe_ttm": float(r["pe_ttm"]) if r.get("pe_ttm") is not None and pd.notna(r.get("pe_ttm")) else None,
                "pb": float(r["pb"]) if r.get("pb") is not None and pd.notna(r.get("pb")) else None,
                "ps": float(r["ps"]) if r.get("ps") is not None and pd.notna(r.get("ps")) else None,
                "ps_ttm": float(r["ps_ttm"]) if r.get("ps_ttm") is not None and pd.notna(r.get("ps_ttm")) else None,
                "total_mv": float(r["total_mv"]) if r.get("total_mv") is not None and pd.notna(r.get("total_mv")) else None,
                "circ_mv": float(r["circ_mv"]) if r.get("circ_mv") is not None and pd.notna(r.get("circ_mv")) else None,
                "turnover_rate": float(r["turnover_rate"]) if r.get("turnover_rate") is not None and pd.notna(r.get("turnover_rate")) else None,
                "volume_ratio": float(r["volume_ratio"]) if r.get("volume_ratio") is not None and pd.notna(r.get("volume_ratio")) else None,
                "update_time": r.get("update_time", current_time.strftime('%Y%m%d %H:%M:%S')),
                "synced_at": current_time,
                "data_source": "tushare"
            }
            
            key = {"ts_code": r["ts_code"], "end_date": r["trade_date"]}
            ops.append(UpdateOne(key, {"$set": doc}, upsert=True))
        
        result = self.daily_basic_coll.bulk_write(ops, ordered=False)
        return (result.upserted_count or 0) + (result.modified_count or 0)

    # ==================== Query Methods ====================
    
    def get_latest_trade_date(self, symbol: Optional[str] = None) -> Optional[str]:
        """Get the latest trade date in the database.
        
        Args:
            symbol: If specified, get latest date for this symbol. Otherwise, get global latest.
            
        Returns:
            Trade date string (YYYYMMDD) or None
        """
        query = {"symbol": symbol} if symbol else {}
        doc = self.volume_price_coll.find_one(query, sort=[("trade_date", -1)])
        return doc["trade_date"] if doc else None

    def get_stock_data(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        """Get stock OHLCV data for a date range.
        
        Args:
            symbol: Stock symbol
            start_date: Start date (YYYYMMDD)
            end_date: End date (YYYYMMDD)
            
        Returns:
            DataFrame with OHLCV data
        """
        cursor = self.volume_price_coll.find(
            {
                "symbol": symbol,
                "trade_date": {"$gte": start_date, "$lte": end_date}
            },
            sort=[("trade_date", 1)]
        )
        docs = list(cursor)
        if not docs:
            return pd.DataFrame()
        return pd.DataFrame(docs)


__all__ = ["StockRepository"]
