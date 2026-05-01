"""十大股东/流通股东两期之间的新进、退出、增减持（按归一化名与原始名双轨）。"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import pandas as pd

from quant_data_engine.institutional.name_normalize import (
    entity_count_bucket,
    normalize_institution_key,
)


@dataclass
class Top10DiffResult:
    period_prev: str
    period_curr: str
    new_holders: List[Dict[str, Any]] = field(default_factory=list)
    exited_holders: List[Dict[str, Any]] = field(default_factory=list)
    increased: List[Dict[str, Any]] = field(default_factory=list)
    decreased: List[Dict[str, Any]] = field(default_factory=list)
    new_intl: List[Dict[str, Any]] = field(default_factory=list)


def _row_map(
    df: pd.DataFrame,
) -> tuple[Dict[str, Dict[str, Any]], Dict[str, List[Dict[str, Any]]]]:
    """
    返回: norm_key -> 合并行(持股累加) ; raw_name -> [rows]（同名多行少见，保留列表）
    """
    by_norm: Dict[str, Dict[str, Any]] = {}
    by_raw: Dict[str, List[Dict[str, Any]]] = {}
    if df is None or df.empty:
        return by_norm, by_raw
    # Tushare 列名：holder_name, hold_amount, end_date, hold_float_ratio, pct_hold 等
    name_col = "holder_name" if "holder_name" in df.columns else "name"
    amt_col = "hold_amount" if "hold_amount" in df.columns else None
    ratio_col = None
    for c in ("hold_ratio", "pct_hold", "hold_float_ratio"):
        if c in df.columns:
            ratio_col = c
            break
    for _, r in df.iterrows():
        raw = str(r.get(name_col, "") or "").strip()
        if not raw:
            continue
        k = normalize_institution_key(raw)
        # 国内/未识别户不能共用同一 "OTHER" 键，否则进出前十会被误合并
        slot_key = k if k != "OTHER" else f"OTHER|{raw}"
        amt: Optional[float] = None
        if amt_col and amt_col in df.columns and pd.notna(r.get(amt_col)):
            try:
                amt = float(r[amt_col])
            except (TypeError, ValueError):
                amt = None
        ratio: Optional[float] = None
        if ratio_col and ratio_col in df.columns and pd.notna(r.get(ratio_col)):
            try:
                ratio = float(r[ratio_col])
            except (TypeError, ValueError):
                ratio = None
        rec = {
            "holder_name": raw,
            "norm": k,
            "hold_amount": amt,
            "hold_ratio": ratio,
        }
        if raw not in by_raw:
            by_raw[raw] = []
        by_raw[raw].append(rec)
        if slot_key not in by_norm:
            by_norm[slot_key] = {"norm": k, "holder_name": raw, "hold_amount": 0.0, "hold_ratio": ratio}
            if amt is not None:
                by_norm[slot_key]["hold_amount"] = amt
        else:
            if amt is not None:
                by_norm[slot_key]["hold_amount"] = (by_norm[slot_key].get("hold_amount") or 0) + float(amt)
    return by_norm, by_raw


def diff_top10_periods(
    df_prev: pd.DataFrame,
    df_curr: pd.DataFrame,
    period_prev: str,
    period_curr: str,
) -> Top10DiffResult:
    """
    比较两期 top10 接口返回的 DataFrame，产出新进/退出/增减与「新进且归一化为国际机构标签」的列表。
    """
    a, _ = _row_map(df_prev)
    b, _ = _row_map(df_curr)
    res = Top10DiffResult(period_prev=period_prev, period_curr=period_curr)
    keys_a, keys_b = set(a), set(b)

    for k in keys_b - keys_a:
        row = dict(b[k])
        res.new_holders.append(row)
        if k != "OTHER" and k:
            res.new_intl.append(row)

    for k in keys_a - keys_b:
        res.exited_holders.append(dict(a[k]))

    for k in keys_a & keys_b:
        ap = a[k].get("hold_amount")
        bp = b[k].get("hold_amount")
        if ap is not None and bp is not None and bp > ap:
            res.increased.append({"norm": k, "holder_name": b[k].get("holder_name"), "delta": bp - ap, "from": ap, "to": bp})
        if ap is not None and bp is not None and bp < ap:
            res.decreased.append({"norm": k, "holder_name": b[k].get("holder_name"), "delta": bp - ap, "from": ap, "to": bp})
    return res


def summarize_new_intl_entities(new_intl: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    对 ``new_intl`` 按 ``entity_count_bucket`` 聚合：raw 槽位数、实体桶数、剔除 HKSC 后的实体数及明细。

    机会分与 ``snapshot.new_intl_count`` 默认对齐 ``entity_count_ex_hksc``（北向名义持有人不单计为一家外资主体）。
    """
    from collections import defaultdict

    by_bucket: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in new_intl or []:
        n = str(row.get("norm") or "")
        b = entity_count_bucket(n)
        by_bucket[b].append(dict(row))
    keys = sorted(by_bucket.keys())
    entity_count = len(by_bucket)
    entity_count_ex_hksc = len([k for k in keys if k != "HKSC"])
    by_entity: List[Dict[str, Any]] = []
    for b in keys:
        rows = by_bucket[b]
        norms_seen = sorted({str(r.get("norm") or "") for r in rows})
        holder_names = [str(r.get("holder_name") or "") for r in rows]
        by_entity.append(
            {
                "entity_bucket": b,
                "norms_seen": norms_seen,
                "holder_names": holder_names,
            }
        )
    return {
        "raw_count": len(new_intl or []),
        "entity_count": entity_count,
        "entity_count_ex_hksc": entity_count_ex_hksc,
        "by_entity": by_entity,
        "entity_buckets_ordered": keys,
    }
