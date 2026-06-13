"""Tushare stk_nineturn pipeline: fetch -> normalize -> upsert."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import pandas as pd


NINETURN_FIELDS = (
    "ts_code,trade_date,freq,open,high,low,close,vol,amount,"
    "up_count,down_count,nine_up_turn,nine_down_turn"
)


@dataclass
class NineturnResult:
    trade_date: str
    records_inserted: int
    issues: List[str]


class NineturnPipeline:
    """Pipeline for raw Tushare nine-turn records."""

    def __init__(self, adapter, repository):
        self.adapter = adapter
        self.repository = repository

    def run_by_date(self, trade_dates: List[str], *, freq: str = "daily") -> Dict[str, Any]:
        results: List[NineturnResult] = []
        for trade_date in trade_dates:
            try:
                df = self.adapter.fetch_nineturn(
                    trade_date=trade_date,
                    freq=freq,
                    fields=NINETURN_FIELDS,
                )
                if df is None or df.empty:
                    results.append(NineturnResult(trade_date, 0, ["no_data"]))
                    continue

                records = self._transform(df, freq=freq)
                count = self.repository.upsert_nineturn(records)
                self.repository.update_last_sync_date(trade_date, freq=freq)
                results.append(NineturnResult(trade_date, count, []))
            except Exception as exc:
                results.append(NineturnResult(trade_date, 0, [f"Error: {exc}"]))

        return self._build_summary(results)

    def run_by_stock(
        self,
        ts_codes: List[str],
        start_date: str,
        end_date: str,
        *,
        freq: str = "daily",
    ) -> Dict[str, Any]:
        all_records: List[Dict[str, Any]] = []
        issues: List[str] = []

        for ts_code in ts_codes:
            try:
                df = self.adapter.fetch_nineturn(
                    ts_code=ts_code,
                    freq=freq,
                    start_date=start_date,
                    end_date=end_date,
                    fields=NINETURN_FIELDS,
                )
                if df is not None and not df.empty:
                    all_records.extend(self._transform(df, freq=freq))
            except Exception as exc:
                issues.append(f"{ts_code}: {exc}")

        count = self.repository.upsert_nineturn(all_records) if all_records else 0
        if count:
            self.repository.update_last_sync_date(end_date, freq=freq)
        return {
            "total_stocks": len(ts_codes),
            "total_records": count,
            "total_issues": len(issues),
            "issues": issues,
        }

    @staticmethod
    def _normalize_trade_date(value: Any) -> str:
        text = str(value or "").strip()
        digits = "".join(ch for ch in text if ch.isdigit())
        return digits[:8] if len(digits) >= 8 else text

    def _transform(self, df: pd.DataFrame, *, freq: Optional[str] = None) -> List[Dict[str, Any]]:
        records: List[Dict[str, Any]] = []
        for _, row in df.iterrows():
            record = row.to_dict()
            ts_code = record.get("ts_code") or ""
            record["symbol"] = ts_code
            record["trade_date"] = self._normalize_trade_date(record.get("trade_date"))
            record["freq"] = record.get("freq") or freq or "daily"
            records.append(record)
        return records

    @staticmethod
    def _build_summary(results: List[NineturnResult]) -> Dict[str, Any]:
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


__all__ = ["NineturnPipeline", "NineturnResult", "NINETURN_FIELDS"]
