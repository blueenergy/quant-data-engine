"""Tushare dragon-tiger pipeline: fetch -> normalize -> replace per day.

The three tables are stored raw (no derived trader/sector features).  Feature
building belongs to the research layer so that mapping rules can change
without rewriting the collection or the backfill.

PIT note: 龙虎榜 is published after the close of trading day T and is
available before the T+1 auction, so it can be joined to next-day boards
without look-ahead.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence

import pandas as pd


@dataclass
class DragonTigerDayResult:
    trade_date: str
    top_list_rows: int = 0
    top_inst_rows: int = 0
    issues: List[str] = field(default_factory=list)


class DragonTigerPipeline:
    """Pipeline for ``top_list`` / ``top_inst`` / ``hm_list``."""

    DAILY_KINDS = ("top_list", "top_inst")

    def __init__(self, adapter, repository):
        self.adapter = adapter
        self.repository = repository

    def run_by_date(
        self,
        trade_dates: Sequence[str],
        *,
        kinds: Sequence[str] = DAILY_KINDS,
    ) -> Dict[str, Any]:
        results: List[DragonTigerDayResult] = []
        for trade_date in trade_dates:
            result = DragonTigerDayResult(trade_date=trade_date)
            try:
                if "top_list" in kinds:
                    result.top_list_rows = self._sync_day("top_list", trade_date, result)
                if "top_inst" in kinds:
                    result.top_inst_rows = self._sync_day("top_inst", trade_date, result)
            except Exception as exc:  # keep the loop alive for a long backfill
                result.issues.append(f"Error: {exc}")
            results.append(result)

        for kind in kinds:
            last_ok = [
                item.trade_date
                for item in results
                if not item.issues and getattr(item, f"{kind}_rows") > 0
            ]
            if last_ok:
                self.repository.update_last_sync_date(kind, max(last_ok))

        return self._build_summary(results)

    def _sync_day(
        self,
        kind: str,
        trade_date: str,
        result: DragonTigerDayResult,
    ) -> int:
        fetch = getattr(self.adapter, f"fetch_{kind}")
        df = fetch(trade_date=trade_date)
        if df is None or df.empty:
            result.issues.append(f"{kind}: no_data")
            return 0
        records = self._transform(df)
        return self.repository.replace_day(kind, trade_date, records)

    def run_hm_list(self) -> Dict[str, Any]:
        df = self.adapter.fetch_hm_list()
        if df is None or df.empty:
            return {"total_records": 0, "issues": ["hm_list: no_data"]}
        records = self._transform(df, has_trade_date=False)
        count = self.repository.replace_hm_list(records)
        return {"total_records": count, "issues": []}

    @staticmethod
    def _normalize_trade_date(value: Any) -> str:
        text = str(value or "").strip()
        digits = "".join(ch for ch in text if ch.isdigit())
        return digits[:8] if len(digits) >= 8 else text

    def _transform(
        self,
        df: pd.DataFrame,
        *,
        has_trade_date: bool = True,
    ) -> List[Dict[str, Any]]:
        records: List[Dict[str, Any]] = []
        for _, row in df.iterrows():
            record = row.to_dict()
            # pandas turns missing numerics into NaN; Mongo rejects NaN as a
            # document value in some drivers, so drop empty cells entirely.
            record = {
                key: value
                for key, value in record.items()
                if value is not None and not (isinstance(value, float) and pd.isna(value))
            }
            if has_trade_date:
                record["trade_date"] = self._normalize_trade_date(record.get("trade_date"))
                record["symbol"] = record.get("ts_code") or ""
            else:
                record["symbol"] = record.get("name") or ""
            records.append(record)
        return records

    @staticmethod
    def _build_summary(results: List[DragonTigerDayResult]) -> Dict[str, Any]:
        total_list = sum(item.top_list_rows for item in results)
        total_inst = sum(item.top_inst_rows for item in results)
        failed = [item for item in results if item.issues]
        return {
            "total_dates": len(results),
            "failed_dates": len(failed),
            "top_list_rows": total_list,
            "top_inst_rows": total_inst,
            "issues": [
                f"{item.trade_date}: {issue}"
                for item in failed
                for issue in item.issues
            ],
            "results": results,
        }


__all__ = ["DragonTigerPipeline", "DragonTigerDayResult"]
