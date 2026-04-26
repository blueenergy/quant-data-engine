"""
示例：展示「大模型在已有抓取数据上」可产出何种层次的分析（模拟文本，非投资建议）。

运行::

  cd quant-data-engine && python examples/institutional_llm_analysis_sample.py

实际接入时，可将 ``run_one(..., include_analysis=True)`` 的整段 JSON（或只含
snapshot + analysis + diff_float）作为用户消息，配合项目里的 LLM 网关调用即可。
"""
from __future__ import annotations

import json
from textwrap import dedent

# 与 InstitutionalPipeline 输出结构对齐的**虚构**样例（便于你核对字段含义）
SAMPLE_INPUT: dict = {
    "ok": True,
    "ts_code": "300667.SZ",
    "as_of": "2026-04-25",
    "periods": ["20260331", "20251231", "20250930", "20250630"],
    "diff_float": {
        "period_prev": "20251231",
        "period_curr": "20260331",
        "new_holders": [
            {
                "norm": "MorganStanley",
                "holder_name": "MORGAN STANLEY & CO. INTERNATIONAL PLC",
                "hold_amount": 180000.0,
                "hold_ratio": 0.42,
            }
        ],
        "exited_holders": [],
        "new_intl": [
            {
                "norm": "MorganStanley",
                "holder_name": "MORGAN STANLEY & CO. INTERNATIONAL PLC",
                "hold_amount": 180000.0,
                "hold_ratio": 0.42,
            }
        ],
        "increased": [],
        "decreased": [],
    },
    "float_window": {"risk_tier": "mid", "rows_in_window": 2},
    "block_trade": {
        "avg_discount_pct": 2.8,
        "trade_days": 6,
        "total_amount": 1.2e8,
    },
    "snapshot": {
        "ts_code": "000000.SZ",
        "as_of": "20260425",
        "score_opportunity": 4.5,
        "score_risk": 3.2,
        "score_composite": 5.2,
        "tags": ["intl_new_float", "mild_momentum_20_60", "block_discount"],
        "new_intl_count": 1,
        "ret_20d": 1.1,
        "ret_60d": -0.4,
    },
    "analysis": {
        "tushare_vs_terminal_note": "（与引擎内 CAVEAT 同义，略）",
        "plan_coverage": {
            "industry_vs_stock": "未: 需行业/板块指数收益(…)",
        },
        "narrative_zh": "（引擎生成的一段摘要，略）",
        "positive_signals": [
            {
                "title": "国际机构(规则归一)在流通十大两期 diff 中呈新进",
                "detail": "new_intl_count=1；需对照终端/公告。",
            }
        ],
        "risk_signals": [
            {
                "title": "未来约 6 个月有解禁风险分档: mid",
                "detail": "以占总/流通股本与股东性质为准。",
            }
        ],
        "recommended_actions": [
            "在终端(同花顺)核对**最新**前十大与公告",
        ],
    },
}

# 模拟大模型在「已读上表 JSON + 非投资建议约束」下写的解读（**虚构场景**）
SAMPLE_LLM_NARRATIVE = dedent(
    """
    【总览（假设基于上送 JSON，非真实股票）】
    在报告期 20251231 → 20260331 的流通前十大中，出现一家归一化为「MorganStanley」的
    新进入股东，这是本用例中**最强的信号**来源；但 Tushare 的股东名与公告日往往
    晚于同花顺 F10，建议以交易所披露与最新终端前十大**交叉验证**持股数量与是否仍为战略/被动配置。

    【价格与「滞涨」】
    20 日约 +1.1%、60 日约 -0.4%，整体落在温和区间，与「机构布局期、股价未大涨」的叙事
    相容，但**不能**单独作为买点依据——本例未接行业/板块指数，**无法**讨论相对行业的 α。

    【流动性与行为】
    近窗大宗有平均正折价、且有一定成交规模时，多解读为卖方让利或承接分歧；应结合
    减持/大宗细节公告，而不是仅凭折价作空。

    【解禁】
    若未来 6 个月存在「mid」档解禁压力，应核对解禁股东性质（战投/首发前限售/员工持股等）
    与流通市值占比；若与当前流通股东名在字面上重叠，要警惕筹码「从限售进入流通」后的行为，
    仍以公司公告为准。

    【本例结论姿态】
    更偏**观察池**：有外资席位新进 + 价格未急涨，但需用终端/公告**确认时效与动机**；
    解禁与大宗为风险侧约束，**综合后再决策**。以上不构成任何投资建议。
    """
).strip()


SUGGESTED_SYSTEM_PROMPT = dedent(
    """
    你是熟悉 A 股股东披露与流动性的研究助理。用户会粘贴一段 JSON，来自本仓库
    quant-data_engine 的 institutional 管道（含 snapshot、diff_float、block_trade、
    float_window、analysis）。请用中文分节输出：总览、股东与机构行为、价格与行业β(若
    缺失则说明无法判断)、大宗与减持含义、解禁风险、可执行检查清单(终端/公告)。
    若 JSON 中 analysis.tushare_vs_terminal_note 存在，需在开头说明数据可能滞后于
    同花顺。禁止荐股、禁止给出目标价，结尾声明非投资建议。
    """
).strip()


def main() -> None:
    print("=" * 72)
    print("一、可喂给大模型的「已抓取数据」示例 JSON（结构对齐 run_one 输出，标的为虚构）\n")
    print(json.dumps(SAMPLE_INPUT, ensure_ascii=False, indent=2))
    print("\n" + "=" * 72)
    print("二、以下为「大模型体」的模拟分析示例（同一段 JSON 的解读示范，**非任何真实标的**）\n")
    print(SAMPLE_LLM_NARRATIVE)
    print("\n" + "=" * 72)
    print("三、可拷贝到任意聊天补全/本地 LLM 的**系统提示** 草稿\n")
    print(SUGGESTED_SYSTEM_PROMPT)
    print("=" * 72)


if __name__ == "__main__":
    main()
