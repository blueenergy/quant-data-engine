"""Tushare cyq_perf pipeline: fetch -> normalize -> upsert."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List

import pandas as pd


CYQ_FIELDS = (
    "ts_code,trade_date,his_low,his_high,"
    "cost_5pct,cost_15pct,cost_50pct,cost_85pct,cost_95pct,"
    "weight_avg,winner_rate"
)


@dataclass
class CyqResult:
    trade_date: str
    records_inserted: int
    issues: List[str] = field(default_factory=list)


class CyqPipeline:
    """Pipeline for raw Tushare chip-distribution records (cyq_perf)."""

    def __init__(self, adapter, repository):
        self.adapter = adapter
        self.repository = repository

    def run_by_date(self, trade_dates: List[str]) -> Dict[str, Any]:
        results: List[CyqResult] = []
        for trade_date in trade_dates:
            try:
                df = self.adapter.fetch_cyq_perf(
                    trade_date=trade_date,
                    fields=CYQ_FIELDS,
                )
                if df is None or df.empty:
                    results.append(CyqResult(trade_date, 0, ["no_data"]))
                    continue

                records = self._transform(df)
                count = self.repository.upsert_cyq_perf(records)
                self.repository.update_last_sync_date(trade_date)
                results.append(CyqResult(trade_date, count, []))
            except Exception as exc:
                results.append(CyqResult(trade_date, 0, [f"Error: {exc}"]))

        return self._build_summary(results)

    @staticmethod
    def _normalize_trade_date(value: Any) -> str:
        text = str(value or "").strip()
        digits = "".join(ch for ch in text if ch.isdigit())
        return digits[:8] if len(digits) >= 8 else text

    def _transform(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        records: List[Dict[str, Any]] = []
        for _, row in df.iterrows():
            record = {
                k: (None if (v is None or (isinstance(v, float) and pd.isna(v))) else v)
                for k, v in row.to_dict().items()
            }
            ts_code = record.get("ts_code") or ""
            record["symbol"] = ts_code
            record["trade_date"] = self._normalize_trade_date(record.get("trade_date"))
            record["data_source"] = "tushare"
            records.append(record)
        return records

    @staticmethod
    def _build_summary(results: List[CyqResult]) -> Dict[str, Any]:
        total_records = sum(item.records_inserted for item in results)
        total_issues = sum(len(item.issues) for item in results)
        success_count = sum(1 for item in results if not item.issues)
        return {
            "total_dates": len(results),
            "success": success_count,
            "failed": len(results) - success_count,
            "total_records": total_records,
            "total_issues": total_issues,
            "results": results,
        }


__all__ = ["CyqPipeline", "CyqResult", "CYQ_FIELDS"]
