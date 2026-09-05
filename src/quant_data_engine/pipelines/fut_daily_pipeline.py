"""Futures daily pipeline: fetch main-contract fut_daily + fut_mapping -> upsert."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import pandas as pd


@dataclass
class FutDailyResult:
    ts_code: str
    inserted: int
    issues: List[str]


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _optional_float(value: Any) -> Optional[float]:
    try:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


class FutDailyPipeline:
    def __init__(self, adapter, repository, validator):
        self.adapter = adapter
        self.repository = repository
        self.validator = validator

    def run(self, codes: List[str], start_date: str, end_date: str) -> Dict[str, Any]:
        results: List[FutDailyResult] = []
        for code in codes:
            try:
                inserted, issues = self._sync_one(code, start_date, end_date)
                results.append(FutDailyResult(code, inserted, issues))
            except Exception as exc:  # pragma: no cover - network dependent
                results.append(FutDailyResult(code, 0, [f"error:{exc}"]))
        return {
            "run_codes": codes,
            "results": [r.__dict__ for r in results],
        }

    def _sync_one(self, ts_code: str, start_date: str, end_date: str) -> tuple[int, List[str]]:
        issues: List[str] = []

        mapping_df = self.adapter.fetch_fut_mapping(
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
        )
        contract_by_date: Dict[str, str] = {}
        if mapping_df is not None and not mapping_df.empty:
            mapping_records = []
            for _, row in mapping_df.iterrows():
                trade_date = str(row.trade_date)
                mapping_ts = str(row.mapping_ts_code)
                contract_by_date[trade_date] = mapping_ts
                mapping_records.append(
                    {
                        "ts_code": ts_code,
                        "trade_date": trade_date,
                        "mapping_ts_code": mapping_ts,
                        "source": "tushare",
                    }
                )
            self.repository.upsert_mapping(mapping_records)
        else:
            issues.append("no_mapping")

        daily_df = self.adapter.fetch_fut_daily(
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
        )
        if daily_df is None or daily_df.empty:
            issues.append("no_data")
            return 0, issues

        v = self.validator.validate(daily_df)
        if not v["is_valid"]:
            issues.extend(v["issues"])
            return 0, issues

        records: List[Dict[str, Any]] = []
        for _, row in daily_df.iterrows():
            trade_date = str(row.trade_date)
            records.append(
                {
                    "ts_code": ts_code,
                    "trade_date": trade_date,
                    "open": _safe_float(row.open),
                    "high": _safe_float(row.high),
                    "low": _safe_float(row.low),
                    "close": _safe_float(row.close),
                    "vol": _safe_float(getattr(row, "vol", None)),
                    "amount": _safe_float(getattr(row, "amount", None)),
                    "settle": _optional_float(getattr(row, "settle", None)),
                    "oi": _optional_float(getattr(row, "oi", None)),
                    "contract": contract_by_date.get(trade_date),
                    "source": "tushare",
                }
            )

        count = self.repository.upsert_daily(records)
        issues.extend(v["issues"])
        return count, issues


__all__ = ["FutDailyPipeline", "FutDailyResult"]
