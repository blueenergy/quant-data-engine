"""Helpers for stock weekly / monthly bar sync (Tushare trade_date semantics)."""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import List, TYPE_CHECKING

if TYPE_CHECKING:
    from quant_data_engine.adapters.stock_adapter import StockAdapter


def get_open_days(adapter: "StockAdapter", start: str, end: str) -> List[str]:
    """Return sorted YYYYMMDD open days in [start, end] (SSE calendar)."""
    df = adapter.fetch_trade_cal(exchange="SSE", start_date=start, end_date=end)
    if df is None or df.empty:
        return []
    days = df[df["is_open"] == 1]["cal_date"].astype(str).str.replace("-", "").tolist()
    return sorted(set(days))


def last_n_weekly_bar_trade_dates(adapter: "StockAdapter", end: str, n: int) -> List[str]:
    """
    Last n *completed* weekly bar dates (last open day of each ISO week), ending on or before ``end``.

    Lookback must cover at least ``n`` ISO weeks (~n*7 calendar days); previously a fixed 400d window
    caused ``--recent-weeks 100`` to see too few weeks and mis-handle long backfills.
    """
    if n < 1:
        return []
    end_dt = datetime.strptime(end, "%Y%m%d")
    # ~7 days/week + margin; cap at ~8y to avoid absurd trade_cal payloads
    lookback_days = min(max(400, n * 7 + 45), 3000)
    start_dt = end_dt - timedelta(days=lookback_days)
    start = start_dt.strftime("%Y%m%d")
    open_days = get_open_days(adapter, start, end)
    if not open_days:
        return []
    # group by ISO year-week, take max trading day per week
    week_to_max: dict[tuple[int, int], str] = {}
    for d in open_days:
        dt = datetime.strptime(d, "%Y%m%d")
        key = (dt.isocalendar()[0], dt.isocalendar()[1])
        prev = week_to_max.get(key)
        if prev is None or d > prev:
            week_to_max[key] = d
    ordered = sorted(week_to_max.values())
    return ordered[-n:]


def last_n_completed_calendar_month_end_trade_dates(adapter: "StockAdapter", end: str, n: int) -> List[str]:
    """
    Last ``n`` *completed* calendar months: for each month, the last open trading day.
    Skips the calendar month that contains ``end`` (so a run on the 1st picks the prior month).
    """
    if n < 1:
        return []
    end_dt = datetime.strptime(end, "%Y%m%d")
    first_of_end_month = end_dt.replace(day=1)
    cursor = first_of_end_month - timedelta(days=1)  # last calendar day of month before ``end``'s month
    out: List[str] = []
    for _ in range(n):
        y, m = cursor.year, cursor.month
        first = datetime(y, m, 1)
        last_cal = (first + timedelta(days=32)).replace(day=1) - timedelta(days=1)
        start_m = first.strftime("%Y%m%d")
        end_m = last_cal.strftime("%Y%m%d")
        days = get_open_days(adapter, start_m, end_m)
        if days:
            out.append(days[-1])
        cursor = first - timedelta(days=1)
    return list(reversed(out))
