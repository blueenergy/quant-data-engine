"""过去 N 个自然日窗口内的大宗：笔数、金额、与日线收盘价的近似折溢价（需传入 daily 收盘价）。"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd


@dataclass
class BlockTradeWindowStats:
    start_date: str
    end_date: str
    trade_days: int
    total_amount: float
    total_volume: float
    avg_discount_pct: Optional[float]
    discount_days_ratio: Optional[float]
    risk_note: str


def _to_close_map(daily: pd.DataFrame) -> Dict[str, float]:
    m: Dict[str, float] = {}
    if daily is None or daily.empty or "close" not in daily.columns:
        return m
    dc = "trade_date" if "trade_date" in daily.columns else None
    if not dc:
        return m
    for _, r in daily.iterrows():
        ds = r[dc]
        s = str(int(ds)) if not isinstance(ds, str) else ds.replace("-", "")[:8]
        if pd.isna(r.get("close")):
            continue
        try:
            m[s] = float(r["close"])
        except (TypeError, ValueError):
            pass
    return m


def block_trade_window_stats(
    block: pd.DataFrame,
    daily: pd.DataFrame,
    as_of: Optional[date] = None,
    lookback_days: int = 183,
) -> BlockTradeWindowStats:
    """
    汇总 ``as_of-lookback_days`` 至 ``as_of`` 的 block_trade；用 ``daily`` 的收盘价计算折溢价
    (close - block_price) / close * 100；缺失收盘则该笔不计入均价。
    """
    d_end = as_of or date.today()
    d_start = d_end - timedelta(days=lookback_days)
    s_start, s_end = d_start.strftime("%Y%m%d"), d_end.strftime("%Y%m%d")
    st = BlockTradeWindowStats(
        start_date=s_start, end_date=s_end, trade_days=0, total_amount=0.0, total_volume=0.0,
        avg_discount_pct=None, discount_days_ratio=None, risk_note="",
    )
    if block is None or block.empty:
        st.risk_note = "无大宗数据"
        return st
    close_m = _to_close_map(daily)
    tcol = "trade_date" if "trade_date" in block.columns else None
    pcol = "price" if "price" in block.columns else "deal_price" if "deal_price" in block.columns else None
    acol = "amount" if "amount" in block.columns else None
    vcol = "vol" if "vol" in block.columns else "volume" if "volume" in block.columns else None
    if not tcol or not pcol:
        st.risk_note = "列不全"
        return st
    discounts: List[float] = []
    dset: set[str] = set()
    for _, r in block.iterrows():
        ds = r[tcol]
        ds_s = str(int(ds)) if not isinstance(ds, str) else str(ds).replace("-", "")[:8]
        if len(ds_s) < 8:
            continue
        di = datetime.strptime(ds_s, "%Y%m%d").date()
        if di < d_start or di > d_end:
            continue
        dset.add(ds_s)
        try:
            pr = float(r[pcol])
        except (TypeError, ValueError):
            continue
        cl = close_m.get(ds_s)
        if cl and cl > 0:
            discounts.append((cl - pr) / cl * 100.0)
        if acol and pd.notna(r.get(acol)):
            try:
                st.total_amount += float(r[acol])
            except (TypeError, ValueError):
                pass
        if vcol and pd.notna(r.get(vcol)):
            try:
                st.total_volume += float(r[vcol])
            except (TypeError, ValueError):
                pass
    st.trade_days = len(dset)
    if discounts:
        st.avg_discount_pct = float(np.mean(discounts))
        st.discount_days_ratio = float(np.sum(np.array(discounts) > 0) / max(len(discounts), 1))
    if st.avg_discount_pct is not None and st.avg_discount_pct > 3.0 and st.trade_days >= 3:
        st.risk_note = "近窗大宗均价相对收盘折价偏厚、笔数较多，关注抛压/承接"
    elif st.total_amount > 0 and st.trade_days == 0:
        st.risk_note = "有成交无有效日期/收盘对照"
    else:
        st.risk_note = "可结合减持公告与解禁再判断"
    return st
