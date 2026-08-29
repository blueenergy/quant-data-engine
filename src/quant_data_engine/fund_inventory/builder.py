"""把两期 fund_holdings 行组装成全市场快照列表。"""
from __future__ import annotations

from collections import defaultdict
import re
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence

from quant_data_engine.fund_inventory.active_funds import is_active_equity_fund
from quant_data_engine.fund_inventory.aggregate import (
    build_stock_snapshot,
    crop_fund_top10,
    positions_from_rows,
    stock_key,
)
from quant_data_engine.fund_inventory.periods import disclosure_kind

PriceLookup = Callable[[str], Mapping[str, Any]]
StockInfoLookup = Callable[[str], Mapping[str, Any]]
_A_SHARE_RE = re.compile(r"^\d{6}\.(SH|SZ|BJ)$")


def active_fund_meta(fund_basic_rows: Iterable[Mapping[str, Any]]) -> Dict[str, Mapping[str, Any]]:
    out: Dict[str, Mapping[str, Any]] = {}
    for row in fund_basic_rows:
        if not is_active_equity_fund(row):
            continue
        code = str(row.get("ts_code") or "").strip()
        if code:
            out[code] = row
    return out


def filter_active_holdings(
    rows: Sequence[Mapping[str, Any]],
    active_codes: Mapping[str, Any],
) -> List[Mapping[str, Any]]:
    if not active_codes:
        return []
    return [r for r in rows if str(r.get("ts_code") or "").strip() in active_codes]


def group_rows_by_stock(rows: Sequence[Mapping[str, Any]]) -> Dict[str, List[Mapping[str, Any]]]:
    grouped: Dict[str, List[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        key = stock_key(row.get("symbol") or row.get("stock_code"))
        if key:
            grouped[key].append(row)
    return grouped


def build_universe_snapshots(
    prev_rows: Sequence[Mapping[str, Any]],
    curr_rows: Sequence[Mapping[str, Any]],
    *,
    fund_meta: Mapping[str, Mapping[str, Any]],
    period_prev: str,
    period_curr: str,
    compare_mode: str,
    price_lookup: Optional[PriceLookup] = None,
    stock_info_lookup: Optional[StockInfoLookup] = None,
    universe: str = "active_equity",
) -> List[Dict[str, Any]]:
    prev_active = filter_active_holdings(prev_rows, fund_meta)
    curr_active = filter_active_holdings(curr_rows, fund_meta)
    if compare_mode == "top10_vs_top10":
        prev_active = crop_fund_top10(prev_active)
        curr_active = crop_fund_top10(curr_active)

    prev_by = group_rows_by_stock(prev_active)
    curr_by = group_rows_by_stock(curr_active)
    symbols = sorted(set(prev_by) | set(curr_by))
    snapshots: List[Dict[str, Any]] = []
    for sym in symbols:
        info = dict(stock_info_lookup(sym) if stock_info_lookup else {})
        ts_code = str(info.get("ts_code") or "").upper()
        if stock_info_lookup and not _A_SHARE_RE.fullmatch(ts_code):
            continue
        prices = dict(price_lookup(sym) if price_lookup else {})
        snap = build_stock_snapshot(
            symbol=ts_code or sym,
            name=info.get("name"),
            industry=info.get("industry"),
            period_prev=period_prev,
            period_curr=period_curr,
            compare_mode=compare_mode,
            prev_positions=positions_from_rows(prev_by.get(sym, []), fund_meta=fund_meta),
            curr_positions=positions_from_rows(curr_by.get(sym, []), fund_meta=fund_meta),
            price_prev=prices.get("price_prev"),
            price_curr=prices.get("price_curr"),
            adj_prev=prices.get("adj_prev"),
            adj_curr=prices.get("adj_curr"),
            universe=universe,
        )
        if not snap["price_adjusted"]:
            continue
        snap["disclosure_prev"] = disclosure_kind(period_prev)
        snap["disclosure_curr"] = disclosure_kind(period_curr)
        snapshots.append(snap)
    return snapshots
