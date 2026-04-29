"""Unit tests for theme_lag_validation_logic (spec §4.1 / §5)."""
from __future__ import annotations

from quant_data_engine.tasks.theme_lag_validation_logic import (
    SymbolValidationResult,
    aggregate_mvp,
    trading_slots_after_anchor,
    validate_one_symbol_stream,
)


def test_trading_slots_after_anchor():
    days = ["20250101", "20250102", "20250103", "20250106", "20250107"]
    assert trading_slots_after_anchor(days, "20250101", 2) == ["20250102", "20250103"]
    assert trading_slots_after_anchor(days, "20250102", 10) == ["20250103", "20250106", "20250107"]


def test_take_profit_first_bar():
    p0 = 10.0
    slots = ["d1", "d2", "d3"]

    def gc(d: str):
        return {"d1": 10.31, "d2": 10.0, "d3": 10.0}.get(d)

    r = validate_one_symbol_stream("X", p0, slots, gc)
    assert r.outcome == "take_profit"
    assert r.exit_trade_date == "d1"
    assert abs(r.return_pct - 0.031) < 1e-9


def test_expire_third_valid_bar():
    p0 = 10.0
    slots = ["d1", "d2", "d3", "d4"]

    def gc(d: str):
        return {"d1": 10.1, "d2": 10.15, "d3": 10.2, "d4": 11.0}.get(d)

    r = validate_one_symbol_stream("X", p0, slots, gc)
    assert r.outcome == "expire"
    assert r.exit_trade_date == "d3"
    assert r.exit_price == 10.2


def test_insufficient_before_three_valid():
    p0 = 10.0
    slots = ["d1", "d2"]  # only 2 slots

    def gc(d: str):
        return {"d1": 10.1, "d2": 10.15}.get(d)

    r = validate_one_symbol_stream("X", p0, slots, gc)
    assert r.outcome == "insufficient_data"


def test_stop_loss():
    p0 = 10.0
    slots = ["d1", "d2", "d3"]

    def gc(d: str):
        return {"d1": 9.6, "d2": 9.0, "d3": 9.0}.get(d)

    r = validate_one_symbol_stream("X", p0, slots, gc)
    assert r.outcome == "stop_loss"
    assert r.exit_trade_date == "d1"


def test_aggregate_mvp():
    rs = [
        SymbolValidationResult("a", 10, "take_profit", "d1", 10.3, 0.03, []),
        SymbolValidationResult("b", 10, "stop_loss", "d1", 9.6, -0.04, []),
        SymbolValidationResult("c", 10, "insufficient_data", None, None, None, []),
    ]
    agg = aggregate_mvp(rs)
    assert agg["denominator"] == 2
    assert agg["insufficient_data_count"] == 1
    assert agg["take_profit_count"] == 1
    assert agg["stop_loss_count"] == 1
    assert agg["hit_rate_take_profit"] == 0.5
