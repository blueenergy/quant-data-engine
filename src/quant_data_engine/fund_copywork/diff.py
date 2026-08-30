"""按产品对比两期持仓，并按股票汇总机构动作。"""
from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from quant_data_engine.fund_copywork.actions import (
    ACTION_DECREASED,
    ACTION_EXITED,
    ACTION_INCREASED,
    ACTION_LABELS,
    ACTION_NEW,
    ACTION_UNCHANGED,
    classify_action,
    holding_change,
)
from quant_data_engine.fund_inventory.active_funds import share_class_key
from quant_data_engine.fund_inventory.aggregate import stock_key


def _as_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _strategy_key(row: Mapping[str, Any], fund_meta: Mapping[str, Mapping[str, Any]]) -> str:
    ts_code = str(row.get("ts_code") or "").strip()
    info = fund_meta.get(ts_code) or {}
    return share_class_key(
        name=info.get("name") or row.get("fund_name"),
        ts_code=ts_code,
        management=info.get("management"),
    )


def canonical_holdings(
    rows: Iterable[Mapping[str, Any]],
    fund_meta: Mapping[str, Mapping[str, Any]],
) -> Dict[str, Dict[str, Dict[str, Any]]]:
    """fund_key -> symbol -> canonical holding (A/C 取最大市值)。"""
    books: Dict[str, Dict[str, Dict[str, Any]]] = defaultdict(dict)
    for row in rows:
        key = _strategy_key(row, fund_meta)
        symbol = stock_key(row.get("symbol") or row.get("stock_code"))
        if not key or not symbol:
            continue
        ts_code = str(row.get("ts_code") or "").strip()
        info = fund_meta.get(ts_code) or {}
        mkv = _as_float(row.get("mkv")) or 0.0
        current = books[key].get(symbol)
        if current is None or mkv > float(current.get("mkv") or 0.0):
            books[key][symbol] = {
                "fund_key": key,
                "ts_code": ts_code,
                "symbol": symbol,
                "mkv": mkv,
                "amount": _as_float(row.get("amount")),
                "name": info.get("name") or row.get("fund_name"),
                "management": info.get("management"),
            }
    return dict(books)


def _diff_one(
    prev: Optional[Mapping[str, Any]],
    curr: Optional[Mapping[str, Any]],
) -> Dict[str, Any]:
    prev_exists = prev is not None
    curr_exists = curr is not None
    change = holding_change(
        (prev or {}).get("amount"),
        (curr or {}).get("amount"),
        (prev or {}).get("mkv"),
        (curr or {}).get("mkv"),
    )
    action = classify_action(
        prev_exists=prev_exists,
        curr_exists=curr_exists,
        change=change,
    )
    src = curr or prev or {}
    return {
        "symbol": src.get("symbol"),
        "action": action,
        "action_label": ACTION_LABELS[action],
        "change_pct": None if change is None else round(float(change) * 100.0, 2),
        "mkv_prev": (prev or {}).get("mkv"),
        "mkv_curr": (curr or {}).get("mkv"),
        "amount_prev": (prev or {}).get("amount"),
        "amount_curr": (curr or {}).get("amount"),
        "ts_code": src.get("ts_code"),
    }


def product_snapshot(
    fund_key: str,
    prev_book: Mapping[str, Mapping[str, Any]],
    curr_book: Mapping[str, Mapping[str, Any]],
    *,
    period_prev: str,
    period_curr: str,
    compare_mode: str,
    meta: Optional[Mapping[str, Any]] = None,
    rank: Optional[int] = None,
    source: str = "auto",
) -> Dict[str, Any]:
    symbols = sorted(set(prev_book) | set(curr_book))
    holdings = [
        _diff_one(prev_book.get(symbol), curr_book.get(symbol))
        for symbol in symbols
    ]
    counts = {
        ACTION_NEW: 0,
        ACTION_INCREASED: 0,
        ACTION_DECREASED: 0,
        ACTION_EXITED: 0,
        ACTION_UNCHANGED: 0,
    }
    for row in holdings:
        counts[str(row["action"])] = counts.get(str(row["action"]), 0) + 1
    info = meta or next(iter(curr_book.values()), None) or next(iter(prev_book.values()), None) or {}
    aum_prev = sum(float(row.get("mkv") or 0.0) for row in prev_book.values())
    aum_curr = sum(float(row.get("mkv") or 0.0) for row in curr_book.values())
    return {
        "fund_key": fund_key,
        "ts_code": info.get("ts_code"),
        "name": info.get("name"),
        "management": info.get("management"),
        "period_prev": period_prev,
        "period_curr": period_curr,
        "compare_mode": compare_mode,
        "rank": rank,
        "source": source,
        "aum_prev": aum_prev,
        "aum_curr": aum_curr,
        "n_new": counts[ACTION_NEW],
        "n_increased": counts[ACTION_INCREASED],
        "n_decreased": counts[ACTION_DECREASED],
        "n_exited": counts[ACTION_EXITED],
        "n_unchanged": counts[ACTION_UNCHANGED],
        "holdings": holdings,
    }


def consensus_snapshots(
    prev_books: Mapping[str, Mapping[str, Mapping[str, Any]]],
    curr_books: Mapping[str, Mapping[str, Mapping[str, Any]]],
    *,
    period_prev: str,
    period_curr: str,
    compare_mode: str,
    stock_names: Optional[Mapping[str, str]] = None,
    universe: str = "active_equity",
) -> List[Dict[str, Any]]:
    """全宇宙主动产品：每只股票有多少家在新买/加/减/清。"""
    names = stock_names or {}
    by_symbol: Dict[str, List[Tuple[str, Dict[str, Any]]]] = defaultdict(list)
    fund_keys = set(prev_books) | set(curr_books)
    for fund_key in fund_keys:
        prev_book = prev_books.get(fund_key) or {}
        curr_book = curr_books.get(fund_key) or {}
        for symbol in set(prev_book) | set(curr_book):
            diff = _diff_one(prev_book.get(symbol), curr_book.get(symbol))
            if diff["action"] == ACTION_UNCHANGED:
                continue
            by_symbol[symbol].append((fund_key, diff))

    out: List[Dict[str, Any]] = []
    for symbol, rows in by_symbol.items():
        counts = {
            ACTION_NEW: 0,
            ACTION_INCREASED: 0,
            ACTION_DECREASED: 0,
            ACTION_EXITED: 0,
        }
        cards: List[Dict[str, Any]] = []
        for fund_key, diff in rows:
            action = str(diff["action"])
            counts[action] = counts.get(action, 0) + 1
            prev_info = (prev_books.get(fund_key) or {}).get(symbol) or {}
            curr_info = (curr_books.get(fund_key) or {}).get(symbol) or {}
            cards.append(
                {
                    "fund_key": fund_key,
                    "name": curr_info.get("name") or prev_info.get("name"),
                    "management": curr_info.get("management") or prev_info.get("management"),
                    "action": action,
                    "action_label": diff["action_label"],
                    "change_pct": diff["change_pct"],
                }
            )
        cards.sort(key=lambda row: abs(float(row.get("change_pct") or 0.0)), reverse=True)
        operated = sum(counts.values())
        out.append(
            {
                "symbol": symbol,
                "name": names.get(symbol),
                "universe": universe,
                "period_prev": period_prev,
                "period_curr": period_curr,
                "compare_mode": compare_mode,
                "n_operated": operated,
                "n_new": counts[ACTION_NEW],
                "n_increased": counts[ACTION_INCREASED],
                "n_decreased": counts[ACTION_DECREASED],
                "n_exited": counts[ACTION_EXITED],
                "cards": cards[:12],
            }
        )
    out.sort(key=lambda row: int(row.get("n_operated") or 0), reverse=True)
    return out
