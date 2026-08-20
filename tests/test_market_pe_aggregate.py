from types import SimpleNamespace

from quant_data_engine.tasks.run_market_pe_aggregate import (
    ALLA_CODE,
    aggregate_all_a_pe,
    is_sh_sz_a,
    resolve_dates,
)


def test_is_sh_sz_a_excludes_beijing_and_indexes():
    assert is_sh_sz_a("000001.SZ")
    assert is_sh_sz_a("688981.SH")
    assert not is_sh_sz_a("430047.BJ")
    assert not is_sh_sz_a("000985.CSI")
    assert not is_sh_sz_a("")


def test_aggregate_all_a_pe_uses_harmonic_not_arithmetic():
    rows = [
        {"ts_code": "600000.SH", "pe_ttm": 10.0, "total_mv": 100.0},
        {"ts_code": "000001.SZ", "pe_ttm": 30.0, "total_mv": 300.0},
        {"ts_code": "430047.BJ", "pe_ttm": 5.0, "total_mv": 1000.0},
        {"ts_code": "000002.SZ", "pe_ttm": -8.0, "total_mv": 50.0},
        {"ts_code": "000003.SZ", "pe_ttm": None, "total_mv": 40.0},
        {"ts_code": "000004.SZ", "pe_ttm": float("nan"), "total_mv": 20.0},
    ]
    doc = aggregate_all_a_pe(rows, "20260819")
    assert doc is not None
    assert doc["ts_code"] == ALLA_CODE
    assert doc["n_universe"] == 5
    assert doc["n_used"] == 2
    assert doc["n_loss"] == 1
    assert doc["n_missing"] == 2
    # Σ mv / Σ (mv/pe) = 400 / (10 + 10) = 20; arithmetic cap-weight would be 25
    assert doc["pe_ttm"] == 20.0
    assert doc["total_mv"] == 400.0


def test_aggregate_all_a_pe_empty_universe_returns_none():
    assert aggregate_all_a_pe([{"ts_code": "430047.BJ", "pe_ttm": 8.0, "total_mv": 1.0}], "20260819") is None
    assert aggregate_all_a_pe([], "20260819") is None


def test_resolve_dates_weekdays_only():
    args = SimpleNamespace(date=None, start="20260814", end="20260817", recent_days=5)
    # Fri 14, skip Sat 15 Sun 16, Mon 17
    assert resolve_dates(args) == ["20260814", "20260817"]
    assert resolve_dates(SimpleNamespace(date="20260819", start=None, end=None, recent_days=5)) == ["20260819"]
