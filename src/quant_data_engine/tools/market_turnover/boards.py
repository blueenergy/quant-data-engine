"""Board classification and turnover aggregation for A-share volume monitoring.

Two independent splits are produced:

* stocks  -> 沪市主板 / 深市主板 / 创业板 / 科创板 / 北交所 (mutually exclusive)
* funds   -> 股票型 / 债券型 / 货币型 / 其他, classified from Tushare
  ``fund_basic.fund_type``

Stocks and funds are different instruments, so buying an ETF never counts in its
constituents' turnover and the two splits never overlap. They are added together
only in the daily series — ETFs have no intraday feed, so the realtime series
stays stock-only.

Amounts from ``volume_price`` / ``etf_daily`` are in **thousand CNY** (Tushare
convention); every value returned here is in 亿元.
"""
from __future__ import annotations

from typing import Any, Dict, Iterable, Mapping, Optional

#: Tushare amount unit is 千元; 1 亿元 = 100_000 千元.
AMOUNT_THOUSAND_TO_YI = 1e5

STOCK_BOARD_KEYS = ("sh", "sz", "cx", "star", "bse", "other")
BOARD_LABELS: Dict[str, str] = {
    "sh": "沪市主板",
    "sz": "深市主板",
    "cx": "创业板",
    "star": "科创板",
    "bse": "北交所",
    "other": "其他",
}

ETF_TYPE_KEYS = ("equity", "bond", "money", "other")
ETF_TYPE_LABELS: Dict[str, str] = {
    "equity": "股票型",
    "bond": "债券型",
    "money": "货币型",
    "other": "其他",
}

#: ``volume_price`` rows written by intraday snapshots, not end-of-day exchange data.
#: The same exclusion list the stock_daily data contract uses — a day backed only by
#: snapshots must not be mistaken for a finished session.
STOCK_SNAPSHOT_SOURCES = ("miniqmt", "tushare_rt_k", "eastmoney_snapshot")

_FUND_TYPE_GROUP = {"股票型": "equity", "债券型": "bond", "货币型": "money"}


def classify_board(symbol: Optional[str]) -> str:
    """Map an A-share code such as ``688981.SH`` to its board key.

    The five boards are mutually exclusive: 科创板 and 创业板 are carved out
    before the 沪市/深市 catch-alls, so summing ``sh + star`` gives 沪市 and
    ``sz + cx`` gives 深市 without double counting.
    """
    if not symbol:
        return "other"
    code, _, market = str(symbol).partition(".")
    market = market.upper()
    if market == "SH":
        if code.startswith(("688", "689")):
            return "star"
        if code.startswith("6"):
            return "sh"
        return "other"
    if market == "SZ":
        if code.startswith(("300", "301", "302")):
            return "cx"
        if code.startswith("0"):
            return "sz"
        return "other"
    if market == "BJ":
        return "bse"
    return "other"


def classify_fund_type(raw_type: Optional[str]) -> str:
    """Group a Tushare ``fund_basic`` type into equity / bond / money / other."""
    return _FUND_TYPE_GROUP.get(str(raw_type or "").strip(), "other")


def _amount_yi(value: Any) -> float:
    try:
        amount = float(value)
    except (TypeError, ValueError):
        return 0.0
    if amount != amount:  # NaN
        return 0.0
    return amount / AMOUNT_THOUSAND_TO_YI


def aggregate_boards(rows: Iterable[Mapping[str, Any]]) -> Dict[str, Any]:
    """Sum stock turnover into 亿元 per board.

    ``rows`` yields ``{"symbol": ..., "amount": ...}`` where ``amount`` is in
    thousand CNY. Returns per-board amounts/counts plus the stock-only total.
    """
    amounts = {key: 0.0 for key in STOCK_BOARD_KEYS}
    counts = {key: 0 for key in STOCK_BOARD_KEYS}
    for row in rows:
        board = classify_board(row.get("symbol"))
        amounts[board] += _amount_yi(row.get("amount"))
        counts[board] += 1
    total = sum(amounts[key] for key in STOCK_BOARD_KEYS)
    return {
        "amounts": amounts,
        "counts": counts,
        "total": total,
        "n_stocks": sum(counts.values()),
    }


def aggregate_etf(
    rows: Iterable[Mapping[str, Any]],
    fund_types: Mapping[str, Optional[str]],
) -> Dict[str, Any]:
    """Sum listed-fund turnover into 亿元 per fund type.

    ``rows`` yields ``{"ts_code": ..., "amount": ...}``; ``fund_types`` maps
    ``ts_code`` to the raw Tushare fund type. Codes missing from the map fall
    into ``other`` rather than being dropped, so the total stays complete.
    """
    amounts = {key: 0.0 for key in ETF_TYPE_KEYS}
    counts = {key: 0 for key in ETF_TYPE_KEYS}
    for row in rows:
        group = classify_fund_type(fund_types.get(row.get("ts_code")))
        amounts[group] += _amount_yi(row.get("amount"))
        counts[group] += 1
    return {
        "amounts": amounts,
        "counts": counts,
        "total": sum(amounts.values()),
        "n_funds": sum(counts.values()),
    }
