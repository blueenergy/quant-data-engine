"""申万 L1 行业指数 60 日收益 vs 个股 60 日收益（需 Mongo 已存 sw_daily + sw_index_member）。"""
from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from quant_data_engine.repositories.sw_industry_repository import SwIndustryRepository
from quant_data_engine.utils.returns import n_trading_day_return_percent


def _close_series_from_stock_daily(daily: pd.DataFrame) -> List[Tuple[str, float]]:
    if daily is None or daily.empty or "close" not in daily.columns:
        return []
    dcol = "trade_date" if "trade_date" in daily.columns else None
    if not dcol:
        return []
    rows: List[Tuple[str, float]] = []
    for _, r in daily.iterrows():
        if pd.isna(r.get("close")):
            continue
        ds = r[dcol]
        s = str(int(ds)) if not isinstance(ds, str) else str(ds).replace("-", "")[:8]
        try:
            rows.append((s, float(r["close"])))
        except (TypeError, ValueError):
            pass
    rows.sort(key=lambda x: x[0])
    return rows


def compute_shenwan_l1_excess_60d(
    stock_ts: str,
    as_of: date,
    stock_daily: Optional[pd.DataFrame],
    sw_repo: SwIndustryRepository,
    sw_src: str = "SW2021",
) -> Optional[Dict[str, Any]]:
    """
    计算个股与申万 L1 行业指数 60 交易日涨跌幅及超额(百分点)。
    无成分映射或指数行情不足时返回 None。
    """
    if not stock_ts or sw_repo is None or stock_daily is None or stock_daily.empty:
        return None
    l1 = sw_repo.get_l1_index_for_stock(stock_ts, sw_src)
    if not l1:
        return None
    end_s = as_of.strftime("%Y%m%d")
    st_series = _close_series_from_stock_daily(stock_daily)
    if len(st_series) < 65:
        return None
    ix_series = sw_repo.get_sw_close_series(l1, end_s, min_rows=70)
    if len(ix_series) < 65:
        return None
    r_stock = n_trading_day_return_percent(st_series, end_s, 60)
    r_idx = n_trading_day_return_percent(ix_series, end_s, 60)
    if r_stock is None or r_idx is None:
        return None
    return {
        "sw_src": sw_src,
        "sw_l1_index_code": l1,
        "ret_stock_60d": round(r_stock, 4),
        "ret_index_60d": round(r_idx, 4),
        "excess_60d": round(r_stock - r_idx, 4),
    }
