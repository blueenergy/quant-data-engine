from types import SimpleNamespace

import mongomock

from quant_data_engine.tasks.run_market_pe_aggregate import (
    ALLA_CODE,
    MIN_PE_COVERAGE,
    aggregate_all_a_pe,
    is_sh_sz_a,
    resolve_dates,
    run,
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


def _rows(day: str, n_used: int, n_missing: int):
    rows = []
    for i in range(n_used):
        rows.append({"ts_code": f"{600000 + i}.SH", "pe_ttm": 15.0, "total_mv": 100.0})
    for i in range(n_missing):
        rows.append({"ts_code": f"{1 + i:06d}.SZ", "pe_ttm": None, "total_mv": 50.0})
    return rows


def _seed_source(coll, day: str, n_used: int, n_missing: int):
    coll.insert_many([dict(row, trade_date=day) for row in _rows(day, n_used, n_missing)])


def test_run_flags_date_with_zero_pe_coverage():
    """2026-10-21 事故：源表估值字段全空时，必须报 degraded 而不是静默写 null 行。"""
    db = mongomock.MongoClient().db
    _seed_source(db["financial_daily_basic"], "20261021", n_used=0, n_missing=500)

    summary = run(["20261021"], db["financial_daily_basic"], db["market_pe_daily"], dry_run=True, verbose=False)

    assert summary["degraded"] == 1
    assert summary["degraded_dates"] == ["20261021"]
    assert summary["min_coverage"] == 0.0
    assert summary["ok"] == 1


def test_run_flags_coverage_below_threshold():
    db = mongomock.MongoClient().db
    # 40% 可用样本 < MIN_PE_COVERAGE
    _seed_source(db["financial_daily_basic"], "20261022", n_used=int(500 * 0.4), n_missing=300)

    summary = run(["20261022"], db["financial_daily_basic"], db["market_pe_daily"], dry_run=True, verbose=False)

    assert summary["degraded"] == 1
    assert summary["min_coverage"] < MIN_PE_COVERAGE


def test_run_healthy_date_is_not_degraded():
    db = mongomock.MongoClient().db
    _seed_source(db["financial_daily_basic"], "20261117", n_used=3600, n_missing=1600)

    summary = run(["20261117"], db["financial_daily_basic"], db["market_pe_daily"], dry_run=True, verbose=False)

    assert summary["degraded"] == 0
    assert summary["degraded_dates"] == []
    assert summary["ok"] == 1


def test_run_still_writes_degraded_row_for_self_healing():
    """degraded 只告警，不跳过写入：源表补回后重跑会 upsert 成实数。"""
    db = mongomock.MongoClient().db
    _seed_source(db["financial_daily_basic"], "20261021", n_used=0, n_missing=500)

    class _StubCollection:
        def __init__(self):
            self.ops = []

        def bulk_write(self, ops, ordered=False):
            self.ops.extend(ops)
            return SimpleNamespace(upserted_count=len(ops), modified_count=0)

    dest = _StubCollection()
    summary = run(["20261021"], db["financial_daily_basic"], dest, dry_run=False, verbose=False)

    assert summary["degraded"] == 1
    assert summary["records_changed"] == 1
    (op,) = dest.ops
    assert op._filter == {"ts_code": ALLA_CODE, "trade_date": "20261021"}
    assert op._doc["$set"]["pe_ttm"] is None
