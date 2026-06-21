"""Stock daily data pipeline: fetch -> validate -> upsert -> summary.

Handles both daily OHLCV and daily_basic indicators collection.
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import List, Dict, Any, Optional
import pandas as pd


@dataclass
class StockDailyResult:
    """Result for a single stock or date."""
    identifier: str  # stock code or trade date
    daily_inserted: int
    basic_inserted: int
    issues: List[str]


class StockDailyPipeline:
    """Pipeline for stock daily data (OHLCV + basic indicators)."""
    
    def __init__(self, adapter, repository, validator=None):
        self.adapter = adapter
        self.repository = repository
        self.validator = validator

    def run_by_stock(
        self,
        ts_codes: List[str],
        start_date: str,
        end_date: str,
        *,
        include_daily: bool = True,
        include_basic: bool = True,
        progress: bool = False,
    ) -> Dict[str, Any]:
        """Run pipeline by iterating through stocks (traditional mode).
        
        Args:
            ts_codes: List of stock codes
            start_date: Start date (YYYYMMDD)
            end_date: End date (YYYYMMDD)
            
        Returns:
            Summary dict with results and statistics
        """
        results: List[StockDailyResult] = []
        
        total = len(ts_codes)
        for idx, ts_code in enumerate(ts_codes, start=1):
            daily_count = 0
            basic_count = 0

            if include_daily:
                # Fetch daily OHLCV
                df_daily = self.adapter.fetch_daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
                
                if df_daily is not None and not df_daily.empty:
                    # Optional validation
                    if self.validator:
                        v = self.validator.validate(df_daily)
                        if not v["is_valid"]:
                            results.append(StockDailyResult(ts_code, 0, 0, v["issues"]))
                            continue
                    
                    # Transform to records
                    records = self._transform_daily(df_daily, ts_code)
                    daily_count = self.repository.upsert_daily(records)

            if include_basic:
                # Fetch daily_basic
                df_basic = self.adapter.fetch_daily_basic(ts_code=ts_code, start_date=start_date, end_date=end_date)
                
                if df_basic is not None and not df_basic.empty:
                    records = self._transform_basic(df_basic, ts_code)
                    basic_count = self.repository.upsert_daily_basic(records)
            
            results.append(StockDailyResult(ts_code, daily_count, basic_count, []))
            if progress:
                print(
                    f"[stock_daily] stock {idx}/{total} {ts_code} "
                    f"daily={daily_count} basic={basic_count}",
                    flush=True,
                )
        
        return self._build_summary(results)

    def run_by_date(
        self,
        trade_dates: List[str],
        *,
        include_daily: bool = True,
        include_basic: bool = True,
        progress: bool = False,
    ) -> Dict[str, Any]:
        """Run pipeline by iterating through dates (bulk mode).
        
        More efficient for collecting all stocks on specific dates.
        
        Args:
            trade_dates: List of trade dates (YYYYMMDD)
            
        Returns:
            Summary dict with results and statistics
        """
        results: List[StockDailyResult] = []
        
        total = len(trade_dates)
        for idx, trade_date in enumerate(trade_dates, start=1):
            daily_count = 0
            basic_count = 0

            if include_daily:
                # Fetch all stocks for this date
                df_daily = self.adapter.fetch_daily(trade_date=trade_date)
                
                if df_daily is not None and not df_daily.empty:
                    records = self._transform_daily(df_daily)
                    daily_count = self.repository.upsert_daily(records)

            if include_basic:
                # Fetch daily_basic for all stocks
                df_basic = self.adapter.fetch_daily_basic(trade_date=trade_date)
                
                if df_basic is not None and not df_basic.empty:
                    records = self._transform_basic(df_basic)
                    basic_count = self.repository.upsert_daily_basic(records)
            
            results.append(StockDailyResult(trade_date, daily_count, basic_count, []))
            if progress:
                print(
                    f"[stock_daily] date {idx}/{total} {trade_date} "
                    f"daily={daily_count} basic={basic_count}",
                    flush=True,
                )
        
        return self._build_summary(results)

    def _transform_daily(self, df: pd.DataFrame, ts_code: Optional[str] = None) -> List[Dict[str, Any]]:
        """Transform daily OHLCV DataFrame to records."""
        records = []
        for _, row in df.iterrows():
            code = ts_code or row.get('ts_code', '')
            
            records.append({
                "symbol": code,
                "ts_code": code,
                "trade_date": row.trade_date,
                "open": row.open,
                "high": row.high,
                "low": row.low,
                "close": row.close,
                "pre_close": row.get('pre_close', 0),
                "change": row.get('change', 0),
                "pct_chg": row.get('pct_chg', 0),
                "volume": row.get('vol', 0),
                "amount": row.get('amount', 0),
            })
        return records

    def _transform_basic(self, df: pd.DataFrame, ts_code: Optional[str] = None) -> List[Dict[str, Any]]:
        """Transform daily_basic DataFrame to records."""
        records = []
        for _, row in df.iterrows():
            code = ts_code or row.get('ts_code', '')
            
            record = {
                "ts_code": code,
                "symbol": code,
                "trade_date": row.trade_date,
            }
            
            # Add all available fields
            for field in ['pe', 'pe_ttm', 'pb', 'ps', 'ps_ttm', 'total_mv', 'circ_mv',
                         'turnover_rate', 'turnover_rate_f', 'volume_ratio', 'dv_ratio', 'dv_ttm']:
                if field in row.index:
                    record[field] = row[field]
            
            records.append(record)
        return records

    def _build_summary(self, results: List[StockDailyResult]) -> Dict[str, Any]:
        """Build summary dict from results."""
        total_daily = sum(r.daily_inserted for r in results)
        total_basic = sum(r.basic_inserted for r in results)
        total_issues = sum(len(r.issues) for r in results)
        
        return {
            "total_processed": len(results),
            "total_daily_records": total_daily,
            "total_basic_records": total_basic,
            "total_issues": total_issues,
            "results": results
        }


__all__ = ["StockDailyPipeline", "StockDailyResult"]
