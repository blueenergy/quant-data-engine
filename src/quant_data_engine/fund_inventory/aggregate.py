"""按股票聚合两期主动基金持仓，并剥离股价膨胀。"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

from quant_data_engine.fund_inventory.active_funds import share_class_key
from quant_data_engine.fund_inventory.classify import classify_inventory_state


def net_ex_price_from_mv(
    mv_prev: float,
    mv_curr: float,
    price_ret: Optional[float],
) -> Dict[str, Optional[float]]:
    """``净增减 = MV_t - MV_{t-1} * (1+r)``；``r`` 缺失时只返回表面市值差。"""
    prev = float(mv_prev or 0.0)
    curr = float(mv_curr or 0.0)
    surface = curr - prev
    if price_ret is None:
        return {
            "mv_prev": prev,
            "mv_curr": curr,
            "price_ret": None,
            "mv_price_effect": None,
            "net_ex_price": None,
            "surface_mv_chg": surface,
        }
    r = float(price_ret)
    inflated = prev * (1.0 + r)
    return {
        "mv_prev": prev,
        "mv_curr": curr,
        "price_ret": r,
        "mv_price_effect": inflated - prev,
        "net_ex_price": curr - inflated,
        "surface_mv_chg": surface,
    }


def price_return(
    price_prev: Optional[float],
    price_curr: Optional[float],
    adj_prev: Optional[float] = None,
    adj_curr: Optional[float] = None,
) -> Optional[float]:
    if not price_prev or not price_curr or price_prev <= 0 or price_curr <= 0:
        return None
    if not adj_prev or not adj_curr or adj_prev <= 0 or adj_curr <= 0:
        return None
    p0 = float(price_prev)
    p1 = float(price_curr)
    return (p1 * float(adj_curr)) / (p0 * float(adj_prev)) - 1.0


def stock_key(symbol: Any) -> str:
    s = str(symbol or "").strip().upper()
    if "." in s:
        code, suffix = s.split(".", 1)
        if suffix == "HK" and code.isdigit():
            return f"{code.zfill(5)}.HK"
        return code
    return s


def crop_fund_top10(
    rows: Sequence[Mapping[str, Any]],
    *,
    fund_field: str = "ts_code",
    mkv_field: str = "mkv",
    n: int = 10,
) -> List[Mapping[str, Any]]:
    """季报对齐：每个基金只保留市值最大的 N 只股票。"""
    by_fund: Dict[str, List[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        by_fund[str(row.get(fund_field) or "")].append(row)
    out: List[Mapping[str, Any]] = []
    for items in by_fund.values():
        ranked = sorted(
            items,
            key=lambda x: float(x.get(mkv_field) or 0.0),
            reverse=True,
        )
        out.extend(ranked[:n])
    return out


@dataclass
class FundPosition:
    fund_key: str
    ts_code: str
    mkv: float
    shares: Optional[float] = None


def _as_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def positions_from_rows(
    rows: Iterable[Mapping[str, Any]],
    *,
    fund_meta: Optional[Mapping[str, Mapping[str, Any]]] = None,
) -> Dict[str, FundPosition]:
    """Collapse A/C share classes to one disclosed underlying portfolio.

    Tushare normally repeats the same total-fund holding under every share
    class.  Summing those rows double-counts the fund, so keep the largest
    disclosed position as the canonical representative.
    """
    meta = fund_meta or {}
    acc: Dict[str, FundPosition] = {}
    for row in rows:
        ts_code = str(row.get("ts_code") or "").strip()
        info = meta.get(ts_code) or {}
        key = share_class_key(
            name=info.get("name") or row.get("fund_name"),
            ts_code=ts_code,
            management=info.get("management"),
        )
        mkv = _as_float(row.get("mkv")) or 0.0
        shares = _as_float(row.get("amount"))
        if key not in acc:
            acc[key] = FundPosition(fund_key=key, ts_code=ts_code, mkv=mkv, shares=shares)
            continue
        cur = acc[key]
        if mkv > cur.mkv:
            acc[key] = FundPosition(
                fund_key=key,
                ts_code=ts_code,
                mkv=mkv,
                shares=shares,
            )
    return acc


def build_stock_snapshot(
    *,
    symbol: str,
    name: Optional[str] = None,
    industry: Optional[str] = None,
    period_prev: str,
    period_curr: str,
    compare_mode: str,
    prev_positions: Mapping[str, FundPosition],
    curr_positions: Mapping[str, FundPosition],
    price_prev: Optional[float] = None,
    price_curr: Optional[float] = None,
    adj_prev: Optional[float] = None,
    adj_curr: Optional[float] = None,
    universe: str = "active_equity",
) -> Dict[str, Any]:
    prev = dict(prev_positions)
    curr = dict(curr_positions)
    mv_prev = sum(p.mkv for p in prev.values())
    mv_curr = sum(p.mkv for p in curr.values())
    ret = price_return(price_prev, price_curr, adj_prev, adj_curr)
    mv_stats = net_ex_price_from_mv(mv_prev, mv_curr, ret)

    new_keys = set(curr) - set(prev)
    exited_keys = set(prev) - set(curr)
    incumbent_keys = set(curr) & set(prev)
    new_inflow = sum(curr[k].mkv for k in new_keys)
    exited_prev_mv = sum(prev[k].mkv for k in exited_keys)
    price_adjusted = mv_stats["net_ex_price"] is not None
    classified = (
        classify_inventory_state(mv_stats["net_ex_price"], new_inflow)
        if price_adjusted
        else {
            "state": None,
            "state_label": None,
            "new_inflow": new_inflow,
            "incumbent_delta": None,
            "net_ex_price": None,
        }
    )

    return {
        "symbol": stock_key(symbol),
        "ts_code": symbol,
        "name": name,
        "industry": industry,
        "universe": universe,
        "period_prev": period_prev,
        "period_curr": period_curr,
        "compare_mode": compare_mode,
        "price_adjusted": price_adjusted,
        "price_prev": price_prev,
        "price_curr": price_curr,
        "n_funds_prev": len(prev),
        "n_funds_curr": len(curr),
        "n_new": len(new_keys),
        "n_exited": len(exited_keys),
        "n_incumbent": len(incumbent_keys),
        "exited_prev_mv": exited_prev_mv,
        **mv_stats,
        "state": classified["state"],
        "state_label": classified["state_label"],
        "new_inflow": classified["new_inflow"],
        "incumbent_delta": classified["incumbent_delta"],
        "net_ex_price": classified["net_ex_price"],
    }
