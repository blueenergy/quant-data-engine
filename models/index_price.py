"""Data shape definition for index price (lightweight)."""
from __future__ import annotations
from typing import TypedDict

class IndexPriceDoc(TypedDict, total=False):
    ts_code: str
    trade_date: str  # YYYYMMDD
    open: float
    high: float
    low: float
    close: float
    source: str

__all__ = ["IndexPriceDoc"]
