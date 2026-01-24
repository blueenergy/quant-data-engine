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
        for code in codes:
            df = self.adapter.fetch_etf_daily(code, start_date=start_date, end_date=end_date)
            if df is None or df.empty:
                results.append(EtfDailyResult(code, 0, ["no_data"]))
                continue
            v = self.validator.validate(df)
            if not v["is_valid"]:
                results.append(EtfDailyResult(code, 0, v["issues"]))
                continue
            records = []
            for _, row in df.iterrows():
                records.append(
                    {
                        "ts_code": code,
                        "trade_date": row.trade_date,
                        "open": float(row.open) if pd.notna(row.open) else None,
                        "high": float(row.high) if pd.notna(row.high) else None,
                        "low": float(row.low) if pd.notna(row.low) else None,
                        "close": float(row.close) if pd.notna(row.close) else None,
                        "source": "tushare",
                        "entity": "ETF",
                    }
                )
            count = self.repository.upsert_daily(records)
            results.append(EtfDailyResult(code, count, v["issues"]))
        return {
            "run_codes": codes,
            "results": [r.__dict__ for r in results],
        }

__all__ = ["EtfDailyPipeline"]
