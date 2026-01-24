"""Utility helpers for date windows."""
from __future__ import annotations
from datetime import datetime, timedelta


def recent_days(days: int) -> tuple[str, str]:
    end = datetime.today()
    start = end - timedelta(days=days)
    return start.strftime("%Y%m%d"), end.strftime("%Y%m%d")

__all__ = ["recent_days"]
