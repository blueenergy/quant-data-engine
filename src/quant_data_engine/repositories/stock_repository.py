"""StockRepository - handles persistence for stock daily OHLCV and basic indicators."""
from __future__ import annotations
from typing import Sequence, Dict, Any, Optional, List
from urllib.parse import urlsplit, urlunsplit
from pymongo import MongoClient, UpdateOne
from datetime import datetime
import pandas as pd


def _mask_mongo_uri(uri: str) -> str:
    """Hide credentials in a Mongo URI before logging it."""
    try:
        parts = urlsplit(uri)
        if parts.username or parts.password:
            host = parts.hostname or ""
            if parts.port:
                host = f"{host}:{parts.port}"
            netloc = f"***@{host}"
            return urlunsplit((parts.scheme, netloc, parts.path, "", ""))
        return urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))
    except Exception:
        return "<hidden>"


class StockRepository:
    """Repository for stock daily OHLCV data and basic indicators."""
    
    def __init__(
        self,
        mongo_uri: str,
        db_name: str,
        realtime_db_name: Optional[str] = None,
        sync_uri: Optional[str] = None,
    ):
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.realtime_db = self.client[realtime_db_name or db_name]
        self.volume_price_coll = self.db["volume_price"]
        self.realtime_latest_coll = self.realtime_db["volume_price_realtime_latest"]
        self.daily_basic_coll = self.db["financial_daily_basic"]
        self.stock_adj_factor_coll = self.db["stock_adj_factor"]
        self.stock_weekly_coll = self.db["stock_weekly"]
        self.stock_monthly_coll = self.db["stock_monthly"]
        self.sync_client = None
        self.sync_volume_price_coll = None
        self.sync_realtime_latest_coll = None
        self.sync_stock_adj_factor_coll = None
        if sync_uri:
            try:
                self.sync_client = MongoClient(sync_uri)
                self.sync_db = self.sync_client[db_name]
                self.sync_realtime_db = self.sync_client[realtime_db_name or db_name]
                self.sync_volume_price_coll = self.sync_db["volume_price"]
                self.sync_realtime_latest_coll = self.sync_realtime_db["volume_price_realtime_latest"]
                self.sync_stock_adj_factor_coll = self.sync_db["stock_adj_factor"]
                print(f"  [StockRepository] Secondary Sync DB connected ({_mask_mongo_uri(sync_uri)})")
            except Exception as e:
                print(f"  [StockRepository] Failed to connect to Sync DB: {e}")
        self._ensure_indexes()

    def _ensure_indexes(self):
        """Create necessary indexes for efficient queries."""
        try:
            # Use descending trade_date to match existing DB index schema
            self.volume_price_coll.create_index(
                [("symbol", 1), ("trade_date", -1)],
                name="volume_price_symbol_date_idx",
                background=True
            )
            self.volume_price_coll.create_index([("trade_date", -1)], name="trade_date_idx", background=True)
            self.realtime_latest_coll.create_index(
                [("symbol", 1)],
                unique=True,
                name="volume_price_realtime_latest_symbol_idx",
                background=True,
            )
            self.realtime_latest_coll.create_index(
                [("trade_date", -1), ("snapshot_time", -1)],
                name="volume_price_realtime_latest_time_idx",
                background=True,
            )
            
            # Index for daily_basic collection
            self.daily_basic_coll.create_index([("ts_code", 1), ("trade_date", -1)], background=True)
            self.daily_basic_coll.create_index(
                [("ts_code", 1), ("end_date", -1)],
                name="daily_basic_ts_end_date_idx",
                background=True,
            )
            self.daily_basic_coll.create_index([("trade_date", -1)], background=True)

            self.stock_adj_factor_coll.create_index(
                [("symbol", 1), ("trade_date", -1)],
                name="stock_adj_factor_symbol_date_idx",
                unique=True,
                background=True,
            )
            self.stock_adj_factor_coll.create_index(
                [("trade_date", -1)],
                name="stock_adj_factor_trade_date_idx",
                background=True,
            )

            self.stock_weekly_coll.create_index(
                [("ts_code", 1), ("trade_date", -1)],
                name="stock_weekly_ts_date_idx",
                background=True,
            )
            self.stock_weekly_coll.create_index([("trade_date", -1)], name="stock_weekly_trade_date_idx", background=True)

            self.stock_monthly_coll.create_index(
                [("ts_code", 1), ("trade_date", -1)],
                name="stock_monthly_ts_date_idx",
                background=True,
            )
            self.stock_monthly_coll.create_index([("trade_date", -1)], name="stock_monthly_trade_date_idx", background=True)
            if self.sync_volume_price_coll is not None and self.sync_realtime_latest_coll is not None:
                self.sync_volume_price_coll.create_index(
                    [("symbol", 1), ("trade_date", -1)],
                    name="volume_price_symbol_date_idx",
                    background=True,
                )
                self.sync_volume_price_coll.create_index([("trade_date", -1)], name="trade_date_idx", background=True)
                if self.sync_stock_adj_factor_coll is not None:
                    self.sync_stock_adj_factor_coll.create_index(
                        [("symbol", 1), ("trade_date", -1)],
                        name="stock_adj_factor_symbol_date_idx",
                        unique=True,
                        background=True,
                    )
                    self.sync_stock_adj_factor_coll.create_index(
                        [("trade_date", -1)],
                        name="stock_adj_factor_trade_date_idx",
                        background=True,
                    )
                self.sync_realtime_latest_coll.create_index(
                    [("symbol", 1)],
                    unique=True,
                    name="volume_price_realtime_latest_symbol_idx",
                    background=True,
                )
                self.sync_realtime_latest_coll.create_index(
                    [("trade_date", -1), ("snapshot_time", -1)],
                    name="volume_price_realtime_latest_time_idx",
                    background=True,
                )
        except Exception as e:
            print(f"Warning: Index creation failed: {e}")

    # ==================== Adjustment Factors ====================

    def upsert_adj_factors(self, records: Sequence[Dict[str, Any]]) -> int:
        """Upsert stock adjustment factors into ``stock_adj_factor``.

        ``volume_price`` stores raw prices; scoring/backtests will join these
        factors at read time to compute hfq/qfq prices.
        """
        if not records:
            return 0

        ops = []
        now = datetime.now().strftime("%Y%m%d %H:%M:%S")
        for r in records:
            symbol = r.get("symbol") or r.get("ts_code", "")
            trade_date = r.get("trade_date")
            if not symbol or not trade_date:
                continue
            adj_factor = r.get("adj_factor")
            doc = {
                "symbol": symbol,
                "ts_code": symbol,
                "trade_date": str(trade_date),
                "adj_factor": self._float_or_none(adj_factor),
                "update_time": r.get("update_time") or now,
                "source": r.get("source") or "tushare",
            }
            ops.append(
                UpdateOne(
                    {"symbol": symbol, "trade_date": str(trade_date)},
                    {"$set": doc},
                    upsert=True,
                )
            )

        if not ops:
            return 0
        result = self.stock_adj_factor_coll.bulk_write(ops, ordered=False)
        if self.sync_stock_adj_factor_coll is not None:
            try:
                self.sync_stock_adj_factor_coll.bulk_write(ops, ordered=False)
            except Exception as e:
                print(f"Warning: StockRepository sync upsert_adj_factors failed: {e}")
        return (result.upserted_count or 0) + (result.modified_count or 0)

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
            volume_value = r.get("volume", r.get("vol", 0))
            
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
                "volume": int(float(volume_value)) if (volume_value is not None and not pd.isna(volume_value)) else 0,
                "amount": float(r.get("amount", 0)) if r.get("amount") is not None else None,
                "update_time": datetime.now().strftime('%Y%m%d %H:%M:%S'),
                "source": r.get("source") or "tushare",
            }
            source_mode = r.get("source_mode")
            if source_mode:
                doc["source_mode"] = source_mode
            universe_type = r.get("universe_type")
            if universe_type:
                doc["universe_type"] = universe_type
            snapshot_time = r.get("realtime_snapshot_time") or r.get("snapshot_time")
            if snapshot_time:
                doc["realtime_snapshot_time"] = snapshot_time
                realtime_extra = {
                    "amount_yuan": r.get("realtime_amount_yuan"),
                    "num": r.get("realtime_num"),
                    "ask_price1": r.get("realtime_ask_price1"),
                    "ask_volume1": r.get("realtime_ask_volume1"),
                    "bid_price1": r.get("realtime_bid_price1"),
                    "bid_volume1": r.get("realtime_bid_volume1"),
                    "trade_time": r.get("realtime_trade_time"),
                }
                for key, value in realtime_extra.items():
                    if value is not None:
                        doc[f"realtime_{key}"] = value
                doc["realtime_daily_snapshot"] = {
                    "symbol": symbol,
                    "trade_date": r["trade_date"],
                    "snapshot_time": snapshot_time,
                    "open": doc["open"],
                    "high": doc["high"],
                    "low": doc["low"],
                    "close": doc["close"],
                    "pre_close": doc["pre_close"],
                    "change": doc["change"],
                    "pct_chg": doc["pct_chg"],
                    "volume": doc["volume"],
                    "amount": doc["amount"],
                    **{k: v for k, v in realtime_extra.items() if v is not None},
                    "source": doc["source"],
                    "source_mode": source_mode,
                    "universe_type": universe_type,
                    "captured_at": doc["update_time"],
                }
            
            key = {"symbol": symbol, "trade_date": r["trade_date"]}
            ops.append(UpdateOne(key, {"$set": doc}, upsert=True))
        
        result = self.volume_price_coll.bulk_write(ops, ordered=False)
        if self.sync_volume_price_coll is not None:
            try:
                self.sync_volume_price_coll.bulk_write(ops, ordered=False)
            except Exception as e:
                print(f"Warning: StockRepository sync upsert_daily failed: {e}")
        return (result.upserted_count or 0) + (result.modified_count or 0)

    def upsert_realtime_latest(self, records: Sequence[Dict[str, Any]]) -> int:
        """Upsert the hot latest realtime daily K snapshot by symbol.

        This collection intentionally stays small (roughly one row per stock)
        so Compass, frontend realtime panels, and future Redis mirroring can
        read the current market state without scanning historical daily bars.
        """
        if not records:
            return 0

        ops = []
        updated_at = datetime.now().strftime('%Y%m%d %H:%M:%S')
        for r in records:
            symbol = r.get("symbol") or r.get("ts_code", "")
            if not symbol:
                continue
            volume_value = r.get("volume", r.get("vol", 0))
            snapshot_time = r.get("realtime_snapshot_time") or r.get("snapshot_time")
            data_source = r.get("source_mode") or r.get("source") or "tushare"
            doc = {
                "symbol": symbol,
                "trade_date": r["trade_date"],
                "snapshot_time": snapshot_time,
                "open": float(r["open"]) if (r.get("open") is not None and not pd.isna(r.get("open"))) else None,
                "high": float(r["high"]) if (r.get("high") is not None and not pd.isna(r.get("high"))) else None,
                "low": float(r["low"]) if (r.get("low") is not None and not pd.isna(r.get("low"))) else None,
                "close": float(r["close"]) if (r.get("close") is not None and not pd.isna(r.get("close"))) else None,
                "pre_close": float(r.get("pre_close", 0)) if r.get("pre_close") is not None else None,
                "change": float(r.get("change", 0)) if r.get("change") is not None else None,
                "pct_chg": float(r.get("pct_chg", 0)) if r.get("pct_chg") is not None else None,
                "volume": int(float(volume_value)) if (volume_value is not None and not pd.isna(volume_value)) else 0,
                "amount": float(r.get("amount", 0)) if r.get("amount") is not None else None,
                "realtime_amount_yuan": r.get("realtime_amount_yuan"),
                "realtime_num": r.get("realtime_num"),
                "realtime_ask_price1": r.get("realtime_ask_price1"),
                "realtime_ask_volume1": r.get("realtime_ask_volume1"),
                "realtime_bid_price1": r.get("realtime_bid_price1"),
                "realtime_bid_volume1": r.get("realtime_bid_volume1"),
                "realtime_trade_time": r.get("realtime_trade_time"),
                "data_source": data_source,
                "updated_at": updated_at,
            }
            universe_type = r.get("universe_type")
            if universe_type:
                doc["universe_type"] = universe_type
            ops.append(
                UpdateOne(
                    {"symbol": symbol},
                    {"$set": doc, "$unset": {"ts_code": "", "source": "", "source_mode": ""}},
                    upsert=True,
                )
            )

        if not ops:
            return 0
        result = self.realtime_latest_coll.bulk_write(ops, ordered=False)
        if self.sync_realtime_latest_coll is not None:
            try:
                self.sync_realtime_latest_coll.bulk_write(ops, ordered=False)
            except Exception as e:
                print(f"Warning: StockRepository sync upsert_realtime_latest failed: {e}")
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
                "turnover_rate_f": float(r["turnover_rate_f"]) if r.get("turnover_rate_f") is not None and pd.notna(r.get("turnover_rate_f")) else None,
                "volume_ratio": float(r["volume_ratio"]) if r.get("volume_ratio") is not None and pd.notna(r.get("volume_ratio")) else None,
                "dv_ratio": float(r["dv_ratio"]) if r.get("dv_ratio") is not None and pd.notna(r.get("dv_ratio")) else None,
                "dv_ttm": float(r["dv_ttm"]) if r.get("dv_ttm") is not None and pd.notna(r.get("dv_ttm")) else None,
                "update_time": r.get("update_time", current_time.strftime('%Y%m%d %H:%M:%S')),
                "synced_at": current_time,
                "data_source": "tushare"
            }
            
            key = {"ts_code": r["ts_code"], "end_date": r["trade_date"]}
            ops.append(UpdateOne(key, {"$set": doc}, upsert=True))
        
        result = self.daily_basic_coll.bulk_write(ops, ordered=False)
        return (result.upserted_count or 0) + (result.modified_count or 0)

    # ==================== Weekly / Monthly OHLCV (Tushare weekly / monthly) ====================

    def _float_or_none(self, v):
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return None
        try:
            return float(v)
        except (TypeError, ValueError):
            return None

    def upsert_weekly_bars(self, records: Sequence[Dict[str, Any]]) -> int:
        """Upsert stock weekly bars into ``stock_weekly`` (key: ts_code + trade_date)."""
        if not records:
            return 0
        ops = []
        now = datetime.now().strftime("%Y%m%d %H:%M:%S")
        for r in records:
            ts = r.get("ts_code") or r.get("symbol") or ""
            td = r.get("trade_date")
            if not ts or not td:
                continue
            doc = {
                "ts_code": ts,
                "symbol": ts,
                "trade_date": td,
                "open": self._float_or_none(r.get("open")),
                "high": self._float_or_none(r.get("high")),
                "low": self._float_or_none(r.get("low")),
                "close": self._float_or_none(r.get("close")),
                "pre_close": self._float_or_none(r.get("pre_close")),
                "change": self._float_or_none(r.get("change")),
                "pct_chg": self._float_or_none(r.get("pct_chg")),
                "volume": int(float(r["vol"])) if r.get("vol") is not None and not pd.isna(r.get("vol")) else 0,
                "amount": self._float_or_none(r.get("amount")),
                "update_time": now,
                "source": "tushare",
            }
            ops.append(UpdateOne({"ts_code": ts, "trade_date": td}, {"$set": doc}, upsert=True))
        if not ops:
            return 0
        result = self.stock_weekly_coll.bulk_write(ops, ordered=False)
        return (result.upserted_count or 0) + (result.modified_count or 0)

    def upsert_monthly_bars(self, records: Sequence[Dict[str, Any]]) -> int:
        """Upsert stock monthly bars into ``stock_monthly`` (key: ts_code + trade_date)."""
        if not records:
            return 0
        ops = []
        now = datetime.now().strftime("%Y%m%d %H:%M:%S")
        for r in records:
            ts = r.get("ts_code") or r.get("symbol") or ""
            td = r.get("trade_date")
            if not ts or not td:
                continue
            doc = {
                "ts_code": ts,
                "symbol": ts,
                "trade_date": td,
                "open": self._float_or_none(r.get("open")),
                "high": self._float_or_none(r.get("high")),
                "low": self._float_or_none(r.get("low")),
                "close": self._float_or_none(r.get("close")),
                "pre_close": self._float_or_none(r.get("pre_close")),
                "change": self._float_or_none(r.get("change")),
                "pct_chg": self._float_or_none(r.get("pct_chg")),
                "volume": int(float(r["vol"])) if r.get("vol") is not None and not pd.isna(r.get("vol")) else 0,
                "amount": self._float_or_none(r.get("amount")),
                "update_time": now,
                "source": "tushare",
            }
            ops.append(UpdateOne({"ts_code": ts, "trade_date": td}, {"$set": doc}, upsert=True))
        if not ops:
            return 0
        result = self.stock_monthly_coll.bulk_write(ops, ordered=False)
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
