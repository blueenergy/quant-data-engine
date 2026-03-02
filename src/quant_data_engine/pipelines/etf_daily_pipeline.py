"""ETF daily pipeline: fetch -> validate -> upsert -> summary."""
from __future__ import annotations
from dataclasses import dataclass
from typing import List, Dict, Any
import pandas as pd

@dataclass
class EtfDailyResult:
    ts_code: str
    inserted: int
    issues: List[str]

class EtfDailyPipeline:
    def __init__(self, adapter, repository, validator):
        self.adapter = adapter
        self.repository = repository
        self.validator = validator

    def run(self, codes: List[str], start_date: str, end_date: str) -> Dict[str, Any]:
        results: List[EtfDailyResult] = []
        
        # 1. Fetch and Upsert ETF Basic Info (only needs to be done once per run, not per code)
        try:
            df_basic = self.adapter.fetch_fund_basic(market='E', status='L')
            if df_basic is not None and not df_basic.empty:
                # Filter for our requested codes
                df_basic = df_basic[df_basic['ts_code'].isin(codes)]
                if not df_basic.empty:
                    basic_records = df_basic.to_dict('records')
                    self.repository.upsert_basic(basic_records)
        except Exception as e:
            print(f"Warning: Failed to fetch ETF basic info: {e}")
            
        for code in codes:
            # 2. Fetch Daily Prices
            df_daily = self.adapter.fetch_etf_daily(code, start_date=start_date, end_date=end_date)
            # 3. Fetch Adjustment Factors
            df_adj = self.adapter.fetch_fund_adj(ts_code=code, start_date=start_date, end_date=end_date)
            # 4. Fetch Share Scale
            df_share = self.adapter.fetch_fund_share(ts_code=code, start_date=start_date, end_date=end_date)
            
            if df_daily is None or df_daily.empty:
                results.append(EtfDailyResult(code, 0, ["no_daily_data"]))
                continue
            
            v = self.validator.validate(df_daily)
            if not v["is_valid"]:
                results.append(EtfDailyResult(code, 0, v["issues"]))
                continue
                
            # Process Daily Prices
            daily_records = []
            for _, row in df_daily.iterrows():
                daily_records.append({
                    "ts_code": code,
                    "trade_date": row.trade_date,
                    "open": float(row.open) if pd.notna(row.open) else None,
                    "high": float(row.high) if pd.notna(row.high) else None,
                    "low": float(row.low) if pd.notna(row.low) else None,
                    "close": float(row.close) if pd.notna(row.close) else None,
                    "vol": float(row.vol) if 'vol' in row and pd.notna(row.vol) else None,
                    "amount": float(row.amount) if 'amount' in row and pd.notna(row.amount) else None,
                    "source": "tushare",
                })
            
            count = self.repository.upsert_daily_prices(daily_records)
            
            # Process Adj Factors
            if df_adj is not None and not df_adj.empty:
                adj_records = df_adj.to_dict('records')
                self.repository.upsert_adj_factors(adj_records)
                
            # Process Share Scale
            if df_share is not None and not df_share.empty:
                share_records = df_share.to_dict('records')
                self.repository.upsert_shares(share_records)

            results.append(EtfDailyResult(code, count, v["issues"]))
        return {
            "run_codes": codes,
            "results": [r.__dict__ for r in results],
        }

__all__ = ["EtfDailyPipeline"]
