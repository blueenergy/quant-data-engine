"""Index daily pipeline: fetch -> validate -> upsert -> summary."""
from __future__ import annotations
from dataclasses import dataclass
from typing import List, Dict, Any
import pandas as pd

@dataclass
class IndexDailyResult:
    ts_code: str
    inserted: int
    issues: List[str]

class IndexDailyPipeline:
    def __init__(self, adapter, repository, validator):
        self.adapter = adapter
        self.repository = repository
        self.validator = validator

    def run(self, codes: List[str], start_date: str, end_date: str, backfill_days: int = 3) -> Dict[str, Any]:
        results: List[IndexDailyResult] = []
        for code in codes:
            # fetch
            df = self.adapter.fetch_index_daily(code, start_date=start_date, end_date=end_date)
            if df is None or df.empty:
                results.append(IndexDailyResult(code, 0, ["no_data"]))
                continue
            # validate
            v = self.validator.validate(df)
            if not v["is_valid"]:
                results.append(IndexDailyResult(code, 0, v["issues"]))
                continue
            # transform -> records list
            records = []
            for _, row in df.iterrows():
                records.append(
                    {
                        "ts_code": code,
                        "trade_date": row.trade_date,
                        "open": float(row.open),
                        "high": float(row.high),
                        "low": float(row.low),
                        "close": float(row.close),
                        "source": "tushare",
                    }
                )
            # upsert
            count = self.repository.upsert_daily(records)
            results.append(IndexDailyResult(code, count, v["issues"]))
        # summary dict
        return {
            "run_codes": codes,
            "results": [r.__dict__ for r in results],
        }

__all__ = ["IndexDailyPipeline"]
