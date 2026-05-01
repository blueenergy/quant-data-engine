"""在原始数据与计分之外，生成交叉验证与中文分析结论（与计划书条款对齐，非投资建议）。"""
from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional, Set

# 计划书第 1 节：Tushare 与终端（同花顺等）时效差异
CAVEAT_TUSHARE_FRESH = (
    "Tushare 的股东/解禁/大宗等多来自公告与接口落库，更新节奏常晚于同花顺、东方财富等终端的「实时/准实时」F10 展示，"
    "对「刚新进/刚变动」的跟踪建议以**终端+交易所公告**为准，本结果用于量化流程与历史截面复盘。"
)

CAVEAT_TOP10_SCOPE = (
    "本管线仅使用「前十大流通股东」与「前十大股东」的接口截面；**未进前十的建仓/减持**在官方披露与终端上可能已可见，"
    "此处**不会**体现，解读「真新进/被动进前十」时需结合 F10/季报全文。"
)

CAVEAT_INDUSTRY = (
    "计划书要求的「行业相对强度（产业/板块 β）」在**未接入行业指数/板块收益**时，不自动输出「个股α」判断；"
    "可后续在引擎中增加「股票→行业指数 + 同区间收益差」后打标。当前仅含个股 20/60 日涨跌幅(若有日线)。"
)

CAVEAT_BLOCK_BUYER_LOCK = (
    "大宗买方是否带 6 个月锁定期等，**以公司公告与交易所披露为准**；本库仅对成交价相对日线收盘的折溢价作统计性提示。"
)


def _non_empty(s: object) -> str:
    t = (str(s) if s is not None else "").strip()
    return t


def _names_in_share_float_rows(rows: List[Dict[str, Any]]) -> Set[str]:
    out: Set[str] = set()
    for r in rows or []:
        for k in ("holder_name", "holder", "h_name", "name", "shareholder"):
            if k in r:
                v = r.get(k)
            else:
                v = None
            if v is not None and _non_empty(v):
                out.add(_non_empty(v))
    return out


def _current_float_holder_names(holder_debug: Optional[Dict[str, Any]], curr_period: str) -> Set[str]:
    if not holder_debug or not curr_period:
        return set()
    rows = (holder_debug.get("by_period") or {}).get(curr_period) or []
    s: Set[str] = set()
    for row in rows:
        nm = _non_empty(row.get("holder_name", ""))
        if nm:
            s.add(nm)
    return s


def _fuzzy_name_overlap(
    float_names: Set[str], top10_names: Set[str], min_len: int = 2
) -> List[str]:
    """
    粗查解禁表股东名与流通十大名是否**可能同指**（子串等），供人工核公告，不作因果结论。
    """
    if not float_names or not top10_names:
        return []
    matches: List[str] = []
    for a in float_names:
        if len(a) < min_len:
            continue
        for b in top10_names:
            if len(b) < min_len:
                continue
            if a in b or b in a:
                t = f"{a} ↔ 流通十大: {b}"
                if t not in matches:
                    matches.append(t)
    return matches[:8]


@dataclass
class PlanCoverage:
    top10_floatholders_4q: str = "已: 与 top10_holders 一同拉取并做 diff"
    top10_holders_4q: str = "已: 同周期截面入库/分析"
    share_float_6m: str = "已: 未来约 6 月窗内行与风险分档(若列齐全)"
    block_trade_6m: str = "已: 折溢价、金额/有成交日(有日线时)"
    industry_vs_stock: str = "未: 需行业/板块指数收益(后续可接 ths/自定义指数)"
    narrative_cross_check: str = "已: 本模块交叉验证+行动建议(见 analysis)"


@dataclass
class StructuredAnalysis:
    caveats: List[str] = field(default_factory=list)
    plan_coverage: Dict[str, str] = field(default_factory=dict)
    positive_signals: List[Dict[str, str]] = field(default_factory=list)
    risk_signals: List[Dict[str, str]] = field(default_factory=list)
    overlap_unlock_with_float_top10: List[str] = field(default_factory=list)
    sector_beta_note: str = ""
    recommended_actions: List[str] = field(default_factory=list)
    narrative_zh: str = ""
    tushare_vs_terminal_note: str = CAVEAT_TUSHARE_FRESH

    def to_dict(self) -> Dict[str, Any]:
        return {
            "caveats": self.caveats,
            "plan_coverage": {k: v for k, v in self.plan_coverage.items()},
            "positive_signals": self.positive_signals,
            "risk_signals": self.risk_signals,
            "overlap_unlock_with_float_top10": self.overlap_unlock_with_float_top10,
            "sector_beta_note": self.sector_beta_note,
            "recommended_actions": self.recommended_actions,
            "narrative_zh": self.narrative_zh,
            "tushare_vs_terminal_note": self.tushare_vs_terminal_note,
        }


def build_structured_analysis(pipeline_result: Dict[str, Any]) -> Dict[str, Any]:
    """
    输入 ``InstitutionalPipeline.run_one`` 的返回（可含 ``holder_debug`` 与
    ``share_float_window_raw_rows``），输出可写 Mongo/CLI 的 analysis 字典。
    """
    snap = pipeline_result.get("snapshot") or {}
    _diff = pipeline_result.get("diff_float") or {}
    float_w = pipeline_result.get("float_window") or {}
    block = pipeline_result.get("block_trade") or {}
    holder_debug: Optional[Dict[str, Any]] = pipeline_result.get("holder_debug")
    periods: List[str] = list(pipeline_result.get("periods") or [])
    p_curr = periods[0] if len(periods) >= 1 else ""

    pl_cov = asdict(PlanCoverage())
    det_snap = snap.get("detail") or {}
    sw60 = det_snap.get("shenwan_l1_60d")
    if isinstance(sw60, dict) and sw60.get("excess_60d") is not None:
        pl_cov["industry_vs_stock"] = (
            f"已: 申万L1 指数{sw60.get('sw_l1_index_code', '')} "
            f"60 日相对超额≈{sw60.get('excess_60d')}% (Tushare index_member+sw_daily)"
        )

    caveats = [
        CAVEAT_TUSHARE_FRESH,
        CAVEAT_TOP10_SCOPE,
        CAVEAT_INDUSTRY,
        CAVEAT_BLOCK_BUYER_LOCK,
    ]
    if isinstance(sw60, dict) and sw60.get("excess_60d") is not None:
        caveats = [c for c in caveats if c is not CAVEAT_INDUSTRY]
        caveats.append(
            "行业相对：已用申万 L1 与本地 sw_daily 计算 60 日超额；"
            "申万版本切换(2014/2021)时请以成分与指数代码为准。"
        )

    pos: List[Dict[str, str]] = []
    risk: List[Dict[str, str]] = []

    ni = int(snap.get("new_intl_count") or 0)
    if ni > 0:
        ni_raw = det_snap.get("new_intl_raw_count")
        raw_part = (
            f" raw规则槽位={ni_raw}"
            if ni_raw is not None and int(ni_raw) != ni
            else ""
        )
        pos.append(
            {
                "title": "国际机构(规则归一)在流通十大两期 diff 中呈新进",
                "detail": (
                    f"new_intl_count={ni}（实体去重且不含 HKSC；详见 detail.new_intl_by_entity）"
                    f"{raw_part}；需对照**终端(同花顺)**与公告，核实 QFII/产品户与股数。"
                ),
            }
        )
    st = snap.get("tags") or []
    if "mild_momentum_20_60" in (st or []):
        mtxt = "仅作价格层描述，不替代基本面；"
        if not (isinstance(sw60, dict) and sw60.get("excess_60d") is not None):
            mtxt += "未接行业指数时无相对行业表现。"
        else:
            mtxt += " 行业β参见 detail.shenwan_l1_60d。"
        pos.append(
            {
                "title": "20/60 日涨跌幅落在「温和」区间(与机构建仓期滞涨叙事相容,非证伪/证实)",
                "detail": mtxt,
            }
        )

    tier = (snap.get("float_risk_tier") or float_w.get("risk_tier") or "").lower()
    if tier in ("high", "mid"):
        risk.append(
            {
                "title": f"未来约 6 个月有解禁风险分档: {tier}",
                "detail": f"窗内行数约 {float_w.get('rows_in_window', 0)}；以占总/流通股本比例与股东性质为准。",
            }
        )
    elif tier == "unknown" and (float_w.get("rows_in_window") or 0) > 0:
        risk.append(
            {
                "title": "近窗有解禁日程但占比等字段缺失",
                "detail": "风险档为 unknown，请在终端核对单批/累计占流通。",
            }
        )

    bd: Optional[float] = None
    if snap.get("block_avg_discount_pct") is not None:
        try:
            bd = float(snap.get("block_avg_discount_pct"))  # type: ignore[arg-type]
        except (TypeError, ValueError):
            bd = None
    if bd is not None and bd > 1.0:
        risk.append(
            {
                "title": "近窗大宗相对收盘存在平均正折价(卖方让价/承接偏弱等解读需多因子)",
                "detail": f"avg_discount≈{bd:.2f}%。",
            }
        )

    bamt = block.get("total_amount")
    tdays = int(block.get("trade_days") or 0)
    try:
        bamt_f = float(bamt) if bamt is not None else 0.0
    except (TypeError, ValueError):
        bamt_f = 0.0
    if bamt_f > 0 and tdays >= 5:
        risk.append(
            {
                "title": "近窗大宗成交天数/金额偏高",
                "detail": f"总成交额约 {bamt}，{tdays} 个有数据日有成交。",
            }
        )

    if isinstance(sw60, dict) and sw60.get("excess_60d") is not None:
        ex = float(sw60["excess_60d"])
        pos.append(
            {
                "title": f"近 60 交易日相对申万L1 行业指数超额 {ex:+.2f} 百分点",
                "detail": f"个股60日≈{sw60.get('ret_stock_60d')}%, 行业指数60日≈{sw60.get('ret_index_60d')}% ；为相对β/风格描述，**非**确定性α。",
            }
        )

    sfr = pipeline_result.get("share_float_window_raw_rows")
    float_rows: List[Dict[str, Any]] = sfr if isinstance(sfr, list) else []
    over: List[str] = []
    top10n = _current_float_holder_names(holder_debug, p_curr)
    if float_rows:
        fn = _names_in_share_float_rows(float_rows)
        over = _fuzzy_name_overlap(fn, top10n)
    if over:
        risk.append(
            {
                "title": "解禁股东名称与「当前期流通十大」在字面上有重叠(粗筛)",
                "detail": "；".join(over) + "。最终以公告中股东全称为准。",
            }
        )

    sector_note = CAVEAT_INDUSTRY
    if isinstance(sw60, dict) and sw60.get("excess_60d") is not None:
        sector_note = (
            f"已用申万L1: 超额(60交易日)={sw60.get('excess_60d')}% ；"
            f"全市场行业口径见 sw_daily 与成分表，仍非投资建议。"
        )
    r20, r60 = snap.get("ret_20d"), snap.get("ret_60d")
    rtxt = (
        f"个股 ret_20d={r20}%, ret_60d={r60}%。"
        if (r20 is not None or r60 is not None)
        else "个股 20/60 日收益缺失(日线可能未拉齐或区间不足)。"
    )

    actions: List[str] = [
        "在**同花顺/交易所**核对最新前十大与公告(口径与时效优于 Tushare 滞后)",
        "对减持、定向增发、战配解禁、大宗的**公告**逐条对照时间窗",
    ]
    if tier in ("high", "mid") or (bd is not None and bd > 1.0):
        actions.append("综合解禁压力与大宗/股东变动后再做排雷/观察池，避免单标签决策")

    narrative = (
        f"【数据时效】{CAVEAT_TUSHARE_FRESH} "
        f"【价格】{rtxt} "
        f"【行业相对强度/板块β】{sector_note} "
        f"【计划交叉】流通两期 diff 在 diff_float；"
        f"近半年大宗见 block_trade；"
        f"近半年解禁窗行数 {float_w.get('rows_in_window', 0)}。 "
    )
    if not pos and not risk:
        narrative += " 当前规则下未产生强多/强空子信号(多为中性/数据不全/规则未覆盖终端已披露者)。"

    sa = StructuredAnalysis(
        caveats=caveats,
        plan_coverage=pl_cov,
        positive_signals=pos,
        risk_signals=risk,
        overlap_unlock_with_float_top10=over,
        sector_beta_note=sector_note,
        recommended_actions=actions,
        narrative_zh=_compress_ws(narrative),
        tushare_vs_terminal_note=CAVEAT_TUSHARE_FRESH,
    )
    return sa.to_dict()


def _compress_ws(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()
