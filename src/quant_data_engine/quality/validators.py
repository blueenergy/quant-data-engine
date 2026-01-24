"""Validation rules for index/price data."""
from __future__ import annotations
from typing import Dict, Any
import pandas as pd

class IndexPriceValidator:
    def validate(self, df: pd.DataFrame) -> Dict[str, Any]:
        issues = []
        if df.empty:
            issues.append("empty")
            return {"is_valid": False, "issues": issues, "records": 0}
        required = ["open", "high", "low", "close"]
        for col in required:
            if col not in df.columns:
                issues.append(f"missing:{col}")
        if "high" in df.columns and "low" in df.columns:
            bad = df[df["high"] < df["low"]]
            if not bad.empty:
                issues.append(f"high_lt_low:{len(bad)}")
        for col in ["open", "high", "low", "close"]:
            if col in df.columns:
                nonpos = df[df[col] <= 0]
                if not nonpos.empty:
                    issues.append(f"non_positive_{col}:{len(nonpos)}")
        return {"is_valid": len(issues) == 0, "issues": issues, "records": len(df)}

__all__ = ["IndexPriceValidator"]
