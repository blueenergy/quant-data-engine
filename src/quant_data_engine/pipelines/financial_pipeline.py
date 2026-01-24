"""Financial data pipeline: fetch -> transform -> upsert -> summary.

Handles income statement, balance sheet, cash flow, and financial indicators.
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import List, Dict, Any, Optional
import pandas as pd


@dataclass
class FinancialResult:
    """Result for a single stock's financial data collection."""
    ts_code: str
    income_count: int
    balance_count: int
    cashflow_count: int
    indicator_count: int
    issues: List[str]


class FinancialPipeline:
    """Pipeline for financial statements and indicators."""
    
    def __init__(self, adapter, repository):
        self.adapter = adapter
        self.repository = repository

    def run(self, ts_codes: List[str], start_date: Optional[str] = None, 
           end_date: Optional[str] = None, period: Optional[str] = None,
           data_types: Optional[List[str]] = None) -> Dict[str, Any]:
        """Run financial data collection pipeline.
        
        Args:
            ts_codes: List of stock codes
            start_date: Start date (YYYYMMDD)
            end_date: End date (YYYYMMDD)
            period: Specific report period (YYYYMMDD)
            data_types: List of data types to collect. Options:
                       ['income', 'balance', 'cashflow', 'financial_indicator']
                       If None, collects all types.
                       
        Returns:
            Summary dict with results and statistics
        """
        if data_types is None:
            data_types = ['income', 'balance', 'cashflow', 'financial_indicator']
        
        results: List[FinancialResult] = []
        
        for ts_code in ts_codes:
            income_count = 0
            balance_count = 0
            cashflow_count = 0
            indicator_count = 0
            issues = []
            
            try:
                # Income statement
                if 'income' in data_types:
                    df_income = self.adapter.fetch_income(
                        ts_code=ts_code,
                        start_date=start_date,
                        end_date=end_date,
                        period=period
                    )
                    if df_income is not None and not df_income.empty:
                        income_count = self.repository.upsert_income(df=df_income)
                
                # Balance sheet
                if 'balance' in data_types:
                    df_balance = self.adapter.fetch_balancesheet(
                        ts_code=ts_code,
                        start_date=start_date,
                        end_date=end_date,
                        period=period
                    )
                    if df_balance is not None and not df_balance.empty:
                        balance_count = self.repository.upsert_balance(df=df_balance)
                
                # Cash flow
                if 'cashflow' in data_types:
                    df_cashflow = self.adapter.fetch_cashflow(
                        ts_code=ts_code,
                        start_date=start_date,
                        end_date=end_date,
                        period=period
                    )
                    if df_cashflow is not None and not df_cashflow.empty:
                        cashflow_count = self.repository.upsert_cashflow(df=df_cashflow)
                
                # Financial indicators
                if 'financial_indicator' in data_types:
                    df_indicator = self.adapter.fetch_fina_indicator(
                        ts_code=ts_code,
                        start_date=start_date,
                        end_date=end_date,
                        period=period
                    )
                    if df_indicator is not None and not df_indicator.empty:
                        indicator_count = self.repository.upsert_indicators(df=df_indicator)
                
            except Exception as e:
                issues.append(f"Error: {str(e)}")
            
            results.append(FinancialResult(
                ts_code=ts_code,
                income_count=income_count,
                balance_count=balance_count,
                cashflow_count=cashflow_count,
                indicator_count=indicator_count,
                issues=issues
            ))
        
        return self._build_summary(results)

    def _build_summary(self, results: List[FinancialResult]) -> Dict[str, Any]:
        """Build summary dict from results."""
        total_income = sum(r.income_count for r in results)
        total_balance = sum(r.balance_count for r in results)
        total_cashflow = sum(r.cashflow_count for r in results)
        total_indicator = sum(r.indicator_count for r in results)
        success_count = sum(1 for r in results if not r.issues)
        failed_count = len(results) - success_count
        
        return {
            "total_processed": len(results),
            "success": success_count,
            "failed": failed_count,
            "income_records": total_income,
            "balance_records": total_balance,
            "cashflow_records": total_cashflow,
            "indicator_records": total_indicator,
            "results": results
        }


__all__ = ["FinancialPipeline", "FinancialResult"]
