"""Tushare cyq_chips pipeline: per-symbol fetch -> normalize -> upsert."""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence

import pandas as pd


CYQ_CHIPS_FIELDS = "ts_code,trade_date,price,percent"


@dataclass
class CyqChipsResult:
    ts_code: str
    records_changed: int
    dates: int = 0
    issues: List[str] = field(default_factory=list)


def normalized_chip_entropy(chips: Sequence[Dict[str, Any]]) -> Optional[float]:
    """Normalized Shannon entropy for a Tushare ``cyq_chips`` distribution.

    Mirrors ``stock-scoring-system/utils/chip_entropy.py``: percentages are
    treated as an empirical distribution and divided by ``log(N)`` so values
    are comparable across days with different bin counts.
    """
    weights: List[float] = []
    for chip in chips or []:
        try:
            value = float(chip.get("percent", 0) or 0)
        except (TypeError, ValueError):
            continue
        if value > 0 and math.isfinite(value):
            weights.append(value)

    if not weights:
        return None
    if len(weights) == 1:
        return 0.0
    total = sum(weights)
    if total <= 0:
        return None

    entropy = 0.0
    for value in weights:
        p = value / total
        entropy -= p * math.log(p)
    return entropy / math.log(len(weights))


class CyqChipsPipeline:
    """Pipeline for raw Tushare per-price chip-distribution records."""

    def __init__(self, adapter, repository):
        self.adapter = adapter
        self.repository = repository

    def run_by_symbols(
        self,
        ts_codes: Sequence[str],
        start_date: str,
        end_date: str,
    ) -> Dict[str, Any]:
        results: List[CyqChipsResult] = []
        for ts_code in ts_codes:
            try:
                df = self.adapter.fetch_cyq_chips(
                    ts_code=ts_code,
                    start_date=start_date,
                    end_date=end_date,
                    fields=CYQ_CHIPS_FIELDS,
                )
                if df is None or df.empty:
                    results.append(CyqChipsResult(ts_code, 0, 0, ["no_data"]))
                    continue

                records = self._transform(ts_code, df)
                changed = self.repository.upsert_chips(records)
                results.append(
                    CyqChipsResult(ts_code, changed, len(records), [])
                )
            except Exception as exc:
                results.append(CyqChipsResult(ts_code, 0, 0, [f"Error: {exc}"]))

        return self._build_summary(results)

    @staticmethod
    def _normalize_trade_date(value: Any) -> str:
        text = str(value or "").strip()
        digits = "".join(ch for ch in text if ch.isdigit())
        return digits[:8] if len(digits) >= 8 else text

    def _transform(
        self, ts_code: str, df: pd.DataFrame
    ) -> List[Dict[str, Any]]:
        records: List[Dict[str, Any]] = []
        for trade_date, group in df.groupby("trade_date"):
            chips: List[Dict[str, Any]] = []
            for _, row in group.iterrows():
                price = row.get("price")
                percent = row.get("percent")
                chips.append(
                    {
                        "price": (
                            None
                            if price is None
                            or (isinstance(price, float) and pd.isna(price))
                            else float(price)
                        ),
                        "percent": (
                            None
                            if percent is None
                            or (isinstance(percent, float) and pd.isna(percent))
                            else float(percent)
                        ),
                    }
                )
            trade_date_text = self._normalize_trade_date(trade_date)
            records.append(
                {
                    "symbol": ts_code,
                    "ts_code": ts_code,
                    "trade_date": trade_date_text,
                    "chips": chips,
                    "chip_bins": len(chips),
                    "chip_entropy_n": len(
                        [
                            c
                            for c in chips
                            if (c.get("percent") or 0) > 0
                        ]
                    ),
                    "s_chip": normalized_chip_entropy(chips),
                }
            )
        return records

    @staticmethod
    def _build_summary(results: List[CyqChipsResult]) -> Dict[str, Any]:
        total_records = sum(item.records_changed for item in results)
        total_issues = sum(len(item.issues) for item in results)
        success_count = sum(1 for item in results if not item.issues)
        return {
            "total_symbols": len(results),
            "success": success_count,
            "failed": len(results) - success_count,
            "total_records": total_records,
            "total_issues": total_issues,
            "results": results,
        }


__all__ = ["CyqChipsPipeline", "CyqChipsResult", "CYQ_CHIPS_FIELDS", "normalized_chip_entropy"]
