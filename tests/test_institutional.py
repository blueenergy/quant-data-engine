"""机构深度：报告期、归一、diff、解禁窗、大宗统计、综合分。"""
from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from quant_data_engine.institutional.holder_diff import diff_top10_periods
from quant_data_engine.institutional.name_normalize import normalize_institution_key
from quant_data_engine.institutional.float_risk import share_float_next_window
from quant_data_engine.institutional.block_trade_risk import block_trade_window_stats
from quant_data_engine.institutional.scoring import build_institutional_snapshot
from quant_data_engine.institutional.report_periods import last_n_completed_quarter_ends
from quant_data_engine.institutional.analytical_report import build_structured_analysis
from quant_data_engine.institutional.holder_diff import Top10DiffResult
from quant_data_engine.institutional.float_risk import ShareFloatWindowResult
from quant_data_engine.institutional.block_trade_risk import BlockTradeWindowStats


def test_last_n_quarter_ends():
    # 2 月属 Q1：最近完整季为上一自然年 Q4
    ends = last_n_completed_quarter_ends(4, date(2025, 2, 20))
    assert len(ends) == 4
    assert ends[0] == "20241231"
    assert "20240930" in ends
    # 4 月属 Q2：最近完整季为当年 Q1 末
    assert last_n_completed_quarter_ends(1, date(2025, 4, 20))[0] == "20250331"


def test_normalize_morgan():
    assert normalize_institution_key("MORGAN STANLEY HONG KONG") == "MorganStanley"
    assert normalize_institution_key("瑞银") == "UBS"
    assert normalize_institution_key("某地方私募") == "OTHER"
    assert normalize_institution_key("花旗银行") == "Citi"
    assert normalize_institution_key("STANDARD CHARTERED PLC") == "StandardChartered"
    assert normalize_institution_key("BNP PARIBAS") == "BNPParibas"


def test_diff_top10():
    prev = pd.DataFrame(
        {
            "holder_name": ["A", "B"],
            "hold_amount": [1e6, 2e6],
            "end_date": [20240930, 20240930],
        }
    )
    curr = pd.DataFrame(
        {
            "holder_name": ["B", "MORGAN STANLEY", "C"],
            "hold_amount": [2.1e6, 1e5, 3e6],
            "end_date": [20241231, 20241231, 20241231],
        }
    )
    d = diff_top10_periods(prev, curr, "20240930", "20241231")
    assert d.period_curr == "20241231"
    assert any(x.get("norm") == "MorganStanley" for x in d.new_intl)
    assert len(d.exited_holders) == 1


def test_share_float_window():
    df = pd.DataFrame(
        {
            "float_date": [20250115, 20251201],
            "float_share": [1e6, 2e6],
            "float_ratio": [1.0, 6.0],
        }
    )
    r = share_float_next_window(df, as_of=date(2024, 12, 1), months=6)
    assert r.risk_tier in ("low", "mid", "high", "unknown")


def test_block_stats_discount():
    blk = pd.DataFrame(
        {
            "trade_date": [20250110, 20250111],
            "price": [9.0, 8.0],
            "amount": [1e6, 2e6],
            "vol": [1e5, 2e5],
        }
    )
    dly = pd.DataFrame(
        {
            "trade_date": [20250110, 20250111],
            "close": [10.0, 10.0],
        }
    )
    s = block_trade_window_stats(
        blk, dly, as_of=date(2025, 1, 15), lookback_days=30
    )
    assert s.avg_discount_pct is not None
    assert s.avg_discount_pct > 0


def test_structured_analysis_minimal():
    r = {
        "snapshot": {
            "new_intl_count": 0,
            "float_risk_tier": "low",
            "tags": [],
            "ret_20d": 1.0,
            "ret_60d": -1.0,
        },
        "diff_float": {},
        "float_window": {"risk_tier": "low", "rows_in_window": 0},
        "block_trade": {"total_amount": 0, "trade_days": 0},
        "periods": ["20241231", "20240930"],
        "holder_debug": {
            "by_period": {
                "20241231": [{"holder_name": "A", "norm": "OTHER"}],
            }
        },
        "share_float_window_raw_rows": [],
    }
    a = build_structured_analysis(r)
    assert "narrative_zh" in a
    assert "plan_coverage" in a
    assert "Tushare" in " ".join(a.get("caveats", []))
    assert a.get("tushare_vs_terminal_note")


def test_build_snapshot():
    d = Top10DiffResult(period_prev="a", period_curr="b", new_intl=[{"norm": "UBS"}])
    fr = ShareFloatWindowResult(
        window_start="1", window_end="2", rows=[], total_float_shares=None,
        max_ratio_to_total=1.0, max_ratio_to_float=None, risk_tier="low",
    )
    bt = BlockTradeWindowStats(
        start_date="20240101", end_date="20241231", trade_days=0, total_amount=0.0,
        total_volume=0.0, avg_discount_pct=None, discount_days_ratio=None, risk_note="",
    )
    daily = pd.DataFrame(
        {
            "trade_date": [20240101, 20240201, 20240301, 20240401, 20240501],
            "close": [10.0, 10.0, 10.0, 10.0, 10.0],
        }
    )
    s = build_institutional_snapshot("000001.SZ", date(2024, 5, 2), fr, bt, d, daily=daily)
    assert 0 <= s.score_composite <= 10
    assert s.new_intl_count == 1
