"""Money flow data pipeline: fetch -> transform -> upsert -> summary."""
from __future__ import annotations
from dataclasses import dataclass
from typing import List, Dict, Any, Optional
import pandas as pd


@dataclass
class MoneyFlowResult:
    """Result for money flow collection on a specific date."""
    trade_date: str
    records_inserted: int
    issues: List[str]


class MoneyFlowPipeline:
    """Pipeline for money flow data collection."""
    
    def __init__(self, adapter, repository):
        """Initialize pipeline.
        
        Args:
            adapter: StockAdapter instance
            repository: MoneyFlowRepository instance
        """
        self.adapter = adapter
        self.repository = repository

    def run_by_date(self, trade_dates: List[str]) -> Dict[str, Any]:
        """Run pipeline by iterating through dates (recommended for money flow).
        
        Args:
            trade_dates: List of trade dates (YYYYMMDD)
            
        Returns:
            Summary dict with results and statistics
        """
        results: List[MoneyFlowResult] = []
        
        for trade_date in trade_dates:
            try:
                # Fetch money flow for all stocks on this date
                df = self.adapter.fetch_moneyflow(trade_date=trade_date)
                
                if df is None or df.empty:
                    results.append(MoneyFlowResult(trade_date, 0, ["no_data"]))
                    continue
                
                # Transform to records
                records = self._transform(df)
                
                # Upsert to database
                count = self.repository.upsert_moneyflow(records)
                
                # Update last sync date
                self.repository.update_last_sync_date(trade_date)
                
                results.append(MoneyFlowResult(trade_date, count, []))
                
            except Exception as e:
                results.append(MoneyFlowResult(trade_date, 0, [f"Error: {str(e)}"]))
        
        return self._build_summary(results)

    def run_by_stock(self, ts_codes: List[str], start_date: str, end_date: str) -> Dict[str, Any]:
        """Run pipeline by iterating through stocks (alternative mode).
        
        Args:
            ts_codes: List of stock codes
            start_date: Start date (YYYYMMDD)
            end_date: End date (YYYYMMDD)
            
        Returns:
            Summary dict with results and statistics
        """
        all_records = []
        issues = []
        
        for ts_code in ts_codes:
            try:
                df = self.adapter.fetch_moneyflow(ts_code=ts_code, start_date=start_date, end_date=end_date)
                
                if df is not None and not df.empty:
                    records = self._transform(df)
                    all_records.extend(records)
                    
            except Exception as e:
                issues.append(f"{ts_code}: {str(e)}")
        
        # Bulk insert all records
        count = 0
        if all_records:
            count = self.repository.upsert_moneyflow(all_records)
        
        return {
            "total_stocks": len(ts_codes),
            "total_records": count,
            "total_issues": len(issues),
            "issues": issues
        }

    def _transform(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Transform money flow DataFrame to records.
        
        Args:
            df: DataFrame with money flow data
            
        Returns:
            List of record dicts ready for insertion
        """
        records = []
        for _, row in df.iterrows():
            ts_code = row.get('ts_code', '')
            
            # Use ts_code directly as symbol (already includes exchange suffix)
            record = row.to_dict()
            record['symbol'] = ts_code
            
            # Ensure trade_date exists
            if 'trade_date' not in record:
                record['trade_date'] = row.name if hasattr(row, 'name') else ''
            
            records.append(record)
        
        return records

    def _build_summary(self, results: List[MoneyFlowResult]) -> Dict[str, Any]:
        """Build summary dict from results."""
        total_records = sum(r.records_inserted for r in results)
        total_issues = sum(len(r.issues) for r in results)
        success_count = sum(1 for r in results if not r.issues)
        
        return {
            "total_dates": len(results),
            "success": success_count,
            "failed": len(results) - success_count,
            "total_records": total_records,
            "total_issues": total_issues,
            "results": results
        }


__all__ = ["MoneyFlowPipeline", "MoneyFlowResult"]
