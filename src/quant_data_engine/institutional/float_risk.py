"""未来 N 个自然月内解禁：累计规模与占流通/总股本比例分档（若列存在）。"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

import pandas as pd


@dataclass
class ShareFloatWindowResult:
    window_start: str
    window_end: str
    rows: List[Dict[str, Any]]
    total_float_shares: Optional[float]
    max_ratio_to_total: Optional[float]
    max_ratio_to_float: Optional[float]
    risk_tier: str  # "high" | "mid" | "low" | "unknown"


def _parse_yyyymmdd(s: str) -> Optional[date]:
    s = (s or "").replace("-", "")[:8]
    if len(s) != 8 or not s.isdigit():
        return None
    try:
        return datetime.strptime(s, "%Y%m%d").date()
    except ValueError:
        return None


def _col(df: pd.DataFrame, *names: str) -> Optional[str]:
    for n in names:
        if n in df.columns:
            return n
    return None


def share_float_next_window(
    df: pd.DataFrame,
    as_of: Optional[date] = None,
    months: int = 6,
) -> ShareFloatWindowResult:
    """
    从 ``share_float`` 接口结果中筛出 ``float_date`` 落在 (as_of, as_of+months] 内的行，并粗算风险档。

    若 ``float_ratio``/占流通等列不存在，则 ``total_float_shares`` 可能仅求和成功，ratio 为 None，risk 为 ``unknown``。
    """
    d0 = as_of or date.today()
    d1 = d0 + timedelta(days=int(months * 31))  # 约半年
    ws, we = d0.strftime("%Y%m%d"), d1.strftime("%Y%m%d")
    out_rows: List[Dict[str, Any]] = []
    if df is None or df.empty:
        return ShareFloatWindowResult(
            window_start=ws, window_end=we, rows=[], total_float_shares=None,
            max_ratio_to_total=None, max_ratio_to_float=None, risk_tier="unknown",
        )
    c_date = _col(df, "float_date", "floatdate")
    if not c_date:
        return ShareFloatWindowResult(
            window_start=ws, window_end=we, rows=[], total_float_shares=None,
            max_ratio_to_total=None, max_ratio_to_float=None, risk_tier="unknown",
        )
    c_share = _col(df, "float_share", "float_shares", "ft_share")
    c_rt = _col(df, "float_ratio", "float_ratio_to_total", "to_total", "total_ratio")
    c_rf = _col(df, "float_ratio_flt", "holder_ratio", "of_float")

    total_sh: float = 0.0
    rmax_t: Optional[float] = None
    rmax_f: Optional[float] = None
    for _, r in df.iterrows():
        fd = r.get(c_date)
        if pd.isna(fd):
            continue
        fd_s = str(int(fd)) if not isinstance(fd, str) else fd.replace("-", "")
        fdi = _parse_yyyymmdd(fd_s)
        if fdi is None or fdi <= d0 or fdi > d1:
            continue
        rec = {k: (None if pd.isna(r[k]) else r[k]) for k in df.columns}
        out_rows.append({str(k): rec[k] for k in rec})
        if c_share and pd.notna(r.get(c_share)):
            try:
                total_sh += float(r[c_share])
            except (TypeError, ValueError):
                pass
        if c_rt and pd.notna(r.get(c_rt)):
            try:
                v = float(r[c_rt])
                rmax_t = v if rmax_t is None else max(rmax_t, v)
            except (TypeError, ValueError):
                pass
        if c_rf and pd.notna(r.get(c_rf)):
            try:
                v = float(r[c_rf])
                rmax_f = v if rmax_f is None else max(rmax_f, v)
            except (TypeError, ValueError):
                pass

    tier = "unknown"
    ref = rmax_t if rmax_t is not None else rmax_f
    if ref is not None:
        if ref >= 5.0:
            tier = "high"
        elif ref >= 2.0:
            tier = "mid"
        else:
            tier = "low"

    return ShareFloatWindowResult(
        window_start=ws,
        window_end=we,
        rows=out_rows,
        total_float_shares=total_sh if total_sh > 0 else None,
        max_ratio_to_total=rmax_t,
        max_ratio_to_float=rmax_f,
        risk_tier=tier,
    )
