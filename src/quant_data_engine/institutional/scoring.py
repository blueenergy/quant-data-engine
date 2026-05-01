"""机构新进、滞涨、解禁与大宗的简易综合分与标签（避免单因子，仅作排序参考）。"""
from __future__ import annotations

import logging
from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from typing import Any, Dict, List, Optional

import pandas as pd

from quant_data_engine.institutional.block_trade_risk import BlockTradeWindowStats
from quant_data_engine.institutional.float_risk import ShareFloatWindowResult
from quant_data_engine.institutional.holder_diff import Top10DiffResult, summarize_new_intl_entities

_log = logging.getLogger(__name__)


@dataclass
class InstitutionalSnapshot:
    ts_code: str
    as_of: str
    score_opportunity: float
    score_risk: float
    score_composite: float
    tags: List[str] = field(default_factory=list)
    float_risk_tier: str = ""
    block_avg_discount_pct: Optional[float] = None
    new_intl_count: int = 0
    ret_20d: Optional[float] = None
    ret_60d: Optional[float] = None
    detail: Dict[str, Any] = field(default_factory=dict)

    def to_mongo(self) -> Dict[str, Any]:
        d = asdict(self)
        d["update_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return d


def _returns_from_daily(daily: pd.DataFrame, as_of: date) -> tuple[Optional[float], Optional[float]]:
    if daily is None or daily.empty or "close" not in daily.columns:
        return None, None
    dcol = "trade_date" if "trade_date" in daily.columns else None
    if not dcol:
        return None, None
    rows: List[tuple[str, float]] = []
    for _, r in daily.iterrows():
        if pd.isna(r.get("close")):
            continue
        ds = r[dcol]
        s = str(int(ds)) if not isinstance(ds, str) else ds.replace("-", "")[:8]
        try:
            rows.append((s, float(r["close"])))
        except (TypeError, ValueError):
            pass
    if len(rows) < 5:
        return None, None
    rows.sort(key=lambda x: x[0])
    dmap = {t[0]: t[1] for t in rows}
    dates = [t[0] for t in rows]
    end_s = as_of.strftime("%Y%m%d")
    # 找 <= end 的最后一日
    last = None
    for s in reversed(dates):
        if s <= end_s:
            last = s
            break
    if not last:
        return None, None
    def shift_back(trading_list: List[str], k: int) -> Optional[str]:
        idx = next((i for i, t in enumerate(trading_list) if t == last), None)
        if idx is None or idx < k:
            return None
        return trading_list[idx - k]
    c_last = dmap.get(last)
    d20 = shift_back(dates, 20)
    d60 = shift_back(dates, 60)
    r20 = r60 = None
    if c_last and d20 and d20 in dmap and dmap[d20]:
        r20 = (c_last - dmap[d20]) / dmap[d20] * 100.0
    if c_last and d60 and d60 in dmap and dmap[d60]:
        r60 = (c_last - dmap[d60]) / dmap[d60] * 100.0
    return r20, r60


def build_institutional_snapshot(
    ts_code: str,
    as_of: date,
    float_res: ShareFloatWindowResult,
    block_stats: BlockTradeWindowStats,
    diff_float: Top10DiffResult,
    daily: Optional[pd.DataFrame] = None,
    intl_debug: bool = False,
) -> InstitutionalSnapshot:
    """
    权重大致：机会侧 = 新国际机构数 + 滞涨(温和区间)；风险侧 = 解禁档 + 大宗折价。
    composite 高 ≈ 机会相对高且风险相对低（线性近似，**非投资建议**）。

    ``new_intl_count`` 与机会分中的机构数 = ``entity_count_ex_hksc``（实体桶去重且不含 HKSC）；
    原始规则命中槽位数见 ``detail.new_intl_raw_count``。
    """
    r20, r60 = _returns_from_daily(daily, as_of) if daily is not None else (None, None)
    intl_sum = summarize_new_intl_entities(diff_float.new_intl)
    new_i_raw = int(intl_sum["raw_count"])
    new_i_entity = int(intl_sum["entity_count"])
    new_i_ex_hksc = int(intl_sum["entity_count_ex_hksc"])
    new_i = new_i_ex_hksc
    if intl_debug:
        _log.warning("intl_debug %s new_intl_entities: %s", ts_code, intl_sum)
    # 机会：新机构（不含 HKSC 名义户；FIL 等与 Fidelity 同桶）
    s_opp = min(10.0, new_i * 2.5)
    # 滞涨：20/60 日收益在 [-8%,8%] 加成分
    stagnant = 0.0
    if r20 is not None and r60 is not None and -8 <= r20 <= 8 and -8 <= r60 <= 8:
        stagnant = 2.0
    s_opp = min(10.0, s_opp + stagnant)
    if new_i == 0 and stagnant == 0:
        s_opp = max(0.0, s_opp * 0.2)

    # 风险
    s_risk = 0.0
    if float_res.risk_tier == "high":
        s_risk += 4.0
    elif float_res.risk_tier == "mid":
        s_risk += 2.5
    elif float_res.risk_tier == "low":
        s_risk += 1.0
    if block_stats.avg_discount_pct is not None and block_stats.avg_discount_pct > 0:
        s_risk = min(10.0, s_risk + min(4.0, block_stats.avg_discount_pct / 3.0))

    comp = s_opp - 0.55 * s_risk
    comp = max(0.0, min(10.0, 5.0 + comp * 0.3))

    tags: List[str] = []
    if new_i_raw:
        tags.append("intl_new_float")
    if stagnant > 0:
        tags.append("mild_momentum_20_60")
    if float_res.risk_tier in ("high", "mid"):
        tags.append(f"unlock_risk_{float_res.risk_tier}")
    if block_stats.avg_discount_pct is not None and block_stats.avg_discount_pct > 2.5:
        tags.append("block_discount")

    return InstitutionalSnapshot(
        ts_code=ts_code,
        as_of=as_of.strftime("%Y%m%d"),
        score_opportunity=round(s_opp, 3),
        score_risk=round(s_risk, 3),
        score_composite=round(float(comp), 3),
        tags=tags,
        float_risk_tier=float_res.risk_tier,
        block_avg_discount_pct=round(block_stats.avg_discount_pct, 4) if block_stats.avg_discount_pct is not None else None,
        new_intl_count=new_i,
        ret_20d=round(r20, 3) if r20 is not None else None,
        ret_60d=round(r60, 3) if r60 is not None else None,
        detail={
            "new_holders_float": len(diff_float.new_holders),
            "float_window_rows": len(float_res.rows),
            "block_trade_note": block_stats.risk_note,
            "new_intl_raw_count": new_i_raw,
            "new_intl_entity_count": new_i_entity,
            "new_intl_entity_count_ex_hksc": new_i_ex_hksc,
            "new_intl_by_entity": intl_sum["by_entity"],
        },
    )
