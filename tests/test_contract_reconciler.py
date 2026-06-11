"""Unit tests for the data-contract reconciler (Phase 1: stock_daily).

Pure judgment helpers are tested directly; the orchestrator is tested with
fake adapter / mongo / state-repo / notifier so no Tushare or MongoDB is needed.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Set

import pandas as pd
import pytest

from quant_data_engine.contracts.models import OnViolation, parse_contract
from quant_data_engine.contracts.reconciler import (
    ContractReconciler,
    chunk_missing_days,
    compress_missing_ranges,
    evaluate_coverage,
    judge_violations,
    mature_quarter_ends,
    period_disclosure_deadline,
    quarter_ends_in_range,
    topological_order,
)


def _catalog_entry(**contract_overrides: Any) -> Dict[str, Any]:
    contract: Dict[str, Any] = {
        "source": "tushare",
        "storage": {
            "database": "quant_data",
            "collection": "volume_price",
            "date_field": "trade_date",
            "symbol_field": "symbol",
        },
        "window": {"start": "20240101", "end": "rolling"},
        "sla": {
            "max_lag_trading_days": 1,
            "min_coverage_pct": 99.0,
            "min_symbols_latest_day": 0,
        },
        "on_violation": {
            "auto_repair": True,
            "repair_task": "run_stock_daily",
            "repair_mode": "by-date",
            "max_repair_window_days": 30,
        },
        "alert": {"channels": ["email", "data_pulse"], "on": ["repair_failed", "still_violated"]},
    }
    contract.update(contract_overrides)
    return {"id": "stock_daily", "label": "股票日线", "contract": contract}


# ── parsing ───────────────────────────────────────────────────────────────────
def test_parse_contract_full():
    c = parse_contract(_catalog_entry())
    assert c is not None
    assert c.id == "stock_daily"
    assert c.storage.collection == "volume_price"
    assert c.window.start == "20240101"
    assert c.end_is_rolling is True
    assert c.sla.max_lag_trading_days == 1
    assert c.on_violation.repair_task == "run_stock_daily"
    assert "email" in c.alert.channels


def test_parse_contract_none_when_no_block():
    assert parse_contract({"id": "x", "freshness": {}}) is None


def test_parse_contract_storage_falls_back_to_freshness():
    entry = {
        "id": "stock_daily",
        "freshness": {"database": "quant_data", "collection": "volume_price", "field": "trade_date"},
        "contract": {"window": {"start": "20240101"}},
    }
    c = parse_contract(entry)
    assert c.storage.collection == "volume_price"
    assert c.storage.date_field == "trade_date"


def test_parse_contract_requires_start():
    with pytest.raises(ValueError):
        parse_contract({"id": "x", "contract": {"storage": {"collection": "c"}}})


# ── repair argv templates ────────────────────────────────────────────────────────
def test_resolved_args_explicit_template():
    ov = OnViolation(repair_args=["--core", "--start", "{start}", "--end", "{end}"])
    assert ov.resolved_args("20240101", "20240131") == [
        "--core", "--start", "20240101", "--end", "20240131",
    ]


def test_resolved_args_mode_fallback():
    ov = OnViolation(repair_mode="by-date")
    assert ov.resolved_args("20240101", "20240131") == [
        "--mode", "by-date", "--start", "20240101", "--end", "20240131",
    ]


def test_resolved_args_default_fallback():
    ov = OnViolation()
    assert ov.resolved_args("20240101", "20240131") == [
        "--start", "20240101", "--end", "20240131",
    ]


def test_parse_contract_reads_repair_args():
    c = parse_contract(_catalog_entry(on_violation={
        "auto_repair": True,
        "repair_task": "run_etf_daily",
        "repair_args": ["--all", "--start", "{start}", "--end", "{end}"],
        "max_repair_window_days": 60,
    }))
    assert c.on_violation.resolved_args("20070104", "20070301") == [
        "--all", "--start", "20070104", "--end", "20070301",
    ]
    assert c.on_violation.max_repair_window_days == 60


# ── pure coverage math ──────────────────────────────────────────────────────────
def test_compress_missing_ranges_groups_by_sequence():
    expected = ["20240101", "20240102", "20240103", "20240104", "20240105"]
    missing = {"20240102", "20240103", "20240105"}
    assert compress_missing_ranges(expected, missing) == [
        ("20240102", "20240103"),
        ("20240105", "20240105"),
    ]


def test_evaluate_coverage_full():
    expected = ["20240101", "20240102", "20240103"]
    cov = evaluate_coverage(expected, set(expected))
    assert cov.coverage_pct == 100.0
    assert cov.lag_trading_days == 0
    assert cov.missing_days == 0
    assert cov.latest_actual == "20240103"


def test_evaluate_coverage_with_gap_and_lag():
    expected = ["20240101", "20240102", "20240103", "20240104"]
    actual = {"20240101", "20240102"}  # missing last two => lag 2
    cov = evaluate_coverage(expected, actual)
    assert cov.present_days == 2
    assert cov.missing_days == 2
    assert cov.lag_trading_days == 2
    assert cov.latest_actual == "20240102"
    assert cov.latest_expected == "20240104"
    assert cov.coverage_pct == 50.0


def test_evaluate_coverage_empty_actual():
    expected = ["20240101", "20240102"]
    cov = evaluate_coverage(expected, set())
    assert cov.lag_trading_days == 2
    assert cov.latest_actual is None
    assert cov.coverage_pct == 0.0


# ── judgment ────────────────────────────────────────────────────────────────────
def test_judge_no_violation_when_healthy():
    c = parse_contract(_catalog_entry())
    expected = ["20240101", "20240102", "20240103"]
    cov = evaluate_coverage(expected, set(expected))
    assert judge_violations(cov, c) == []


def test_judge_flags_lag_and_coverage():
    c = parse_contract(_catalog_entry())
    expected = ["20240101", "20240102", "20240103", "20240104"]
    cov = evaluate_coverage(expected, {"20240101"})
    reasons = judge_violations(cov, c)
    assert any("freshness lag" in r for r in reasons)
    assert any("coverage" in r for r in reasons)


def test_judge_flags_min_symbols():
    c = parse_contract(_catalog_entry(sla={
        "max_lag_trading_days": 99, "min_coverage_pct": 0.0, "min_symbols_latest_day": 4000,
    }))
    expected = ["20240101"]
    cov = evaluate_coverage(expected, set(expected))
    assert judge_violations(cov, c, symbols_latest_day=10) != []
    assert judge_violations(cov, c, symbols_latest_day=5000) == []


# ── chunking ─────────────────────────────────────────────────────────────────────
def test_chunk_missing_days_respects_window():
    missing = ["20240101", "20240102", "20240201", "20240202"]
    chunks = chunk_missing_days(missing, max_window_days=30)
    assert chunks == [("20240101", "20240102"), ("20240201", "20240202")]


def test_chunk_missing_days_single_window():
    missing = ["20240101", "20240105", "20240110"]
    assert chunk_missing_days(missing, max_window_days=30) == [("20240101", "20240110")]


def test_chunk_missing_days_empty():
    assert chunk_missing_days([], 30) == []


# ── orchestrator with fakes ──────────────────────────────────────────────────────
class _FakeAdapter:
    def __init__(self, open_days: List[str]):
        self._open_days = open_days

    def fetch_trade_cal(self, start_date=None, end_date=None):
        rows = [
            {"cal_date": d, "is_open": 1}
            for d in self._open_days
            if (start_date is None or d >= start_date) and (end_date is None or d <= end_date)
        ]
        return pd.DataFrame(rows, columns=["cal_date", "is_open"])


class _FakeStateRepo:
    def __init__(self):
        self.snapshots: Dict[str, Dict[str, Any]] = {}
        self.alerts: List[str] = []
        self._last_alert: Dict[str, Any] = {}

    def upsert_snapshot(self, contract_id, snapshot):
        self.snapshots[contract_id] = snapshot
        return True

    def mark_alerted(self, contract_id):
        self.alerts.append(contract_id)

    def last_alert_at(self, contract_id):
        return self._last_alert.get(contract_id)


class _FakeNotifier:
    def __init__(self):
        self.sent: List[Dict[str, Any]] = []

    def notify(self, channels, subject, summary):
        self.sent.append({"channels": channels, "subject": subject, "summary": summary})
        return {c: "sent" for c in channels}


class _FakeReconciler(ContractReconciler):
    """Override Mongo-backed data access with in-memory sets."""

    def __init__(self, *, open_days, actual, symbols=None, repair_fills=None,
                 repair_should_fail=False, present_periods=None, repair_period_fills=None, **kw):
        from quant_data_engine.config.settings import Settings

        settings = Settings(
            mongo_uri="mongodb://x", mongo_db="finance", data_db="quant_data",
            realtime_db="quant_realtime", tushare_token="t", rate_limit_per_sec=4.0,
            job_status_sync_uri=None,
        )
        super().__init__(
            settings,
            adapter=_FakeAdapter(open_days),
            state_repo=_FakeStateRepo(),
            notifier=_FakeNotifier(),
            **kw,
        )
        self._actual: Set[str] = set(actual)
        self._symbols = symbols
        self._repair_fills = repair_fills or []
        self._repair_should_fail = repair_should_fail
        self._present_periods = set(present_periods) if present_periods is not None else None
        self._repair_period_fills = repair_period_fills or []
        self.repair_calls: List = []

    def actual_days(self, contract, start, end):
        return {d for d in self._actual if start <= d <= end}

    def _period_present(self, contract, period):
        return period in (self._present_periods or set())

    def symbols_on_day(self, contract, day):
        return self._symbols

    def repair(self, contract, cov):
        self.repair_calls.append([(s, e) for s, e in cov.missing_ranges])
        if self._repair_should_fail:
            return {"attempted": True, "chunks": 1, "succeeded": 0, "failed": 1, "errors": ["boom"]}
        self._actual |= set(self._repair_fills)
        if self._present_periods is not None:
            self._present_periods |= set(self._repair_period_fills)
        return {"attempted": True, "chunks": 1, "succeeded": 1, "failed": 0, "errors": []}


def _contract():
    return parse_contract(_catalog_entry())


def test_reconcile_ok_when_complete():
    days = ["20240101", "20240102", "20240103"]
    r = _FakeReconciler(open_days=days, actual=days)
    res = r.reconcile(_contract(), apply_repair=False, as_of="20240103")
    assert res.status == "ok"
    assert res.violations == []
    assert r.state_repo.snapshots["stock_daily"]["status"] == "ok"


def test_reconcile_violated_dry_run_no_repair():
    days = ["20240101", "20240102", "20240103"]
    r = _FakeReconciler(open_days=days, actual=["20240101"])
    res = r.reconcile(_contract(), apply_repair=False, as_of="20240103")
    assert res.status == "violated"
    assert r.repair_calls == []          # dry-run: no repair
    assert r.notifier.sent               # still_violated alert fired
    assert "stock_daily" in r.state_repo.alerts


def test_reconcile_apply_repairs_and_recovers():
    days = ["20240101", "20240102", "20240103"]
    r = _FakeReconciler(
        open_days=days, actual=["20240101"],
        repair_fills=["20240102", "20240103"],
    )
    res = r.reconcile(_contract(), apply_repair=True, as_of="20240103")
    assert r.repair_calls               # repair attempted
    assert res.status == "repaired"
    assert res.violations == []


def test_reconcile_unknown_without_calendar():
    r = _FakeReconciler(open_days=[], actual=[])
    res = r.reconcile(_contract(), apply_repair=False, as_of="20240103")
    assert res.status == "unknown"
    assert r.state_repo.snapshots["stock_daily"]["status"] == "unknown"


def test_reconcile_alert_cooldown_suppresses():
    from datetime import datetime

    days = ["20240101", "20240102", "20240103"]
    r = _FakeReconciler(open_days=days, actual=["20240101"])
    r.state_repo._last_alert["stock_daily"] = datetime.utcnow()  # recent alert
    res = r.reconcile(_contract(), apply_repair=False, as_of="20240103")
    assert res.status == "violated"
    assert res.alerted is False
    assert r.notifier.sent == []


# A 100-day window with a single mid-window gap => 99% coverage, lag 0.
# That is within the 99% SLA tolerance => no violation, but still a real gap.
def _days_100():
    from datetime import date, timedelta

    base = date(2024, 1, 1)
    return [(base + timedelta(days=i)).strftime("%Y%m%d") for i in range(100)]


def test_reconcile_subtolerance_gap_is_ok_and_dryrun_skips_repair():
    days = _days_100()
    gap = days[49]
    actual = [d for d in days if d != gap]
    r = _FakeReconciler(open_days=days, actual=actual)
    res = r.reconcile(_contract(), apply_repair=False, as_of=days[-1])
    assert res.status == "ok"               # 99% within tolerance
    assert res.violations == []
    assert res.coverage.missing_days == 1   # gap detected but tolerated
    assert r.repair_calls == []             # dry-run never repairs
    assert r.notifier.sent == []


def test_reconcile_apply_repairs_subtolerance_gap_without_alert():
    days = _days_100()
    gap = days[49]
    actual = [d for d in days if d != gap]
    r = _FakeReconciler(open_days=days, actual=actual, repair_fills=[gap])
    res = r.reconcile(_contract(), apply_repair=True, as_of=days[-1])
    assert r.repair_calls                    # decoupled: repairs even without violation
    assert res.status == "repaired"
    assert res.coverage.missing_days == 0
    assert res.alerted is False              # no SLA breach => no alert
    assert r.notifier.sent == []


def test_reconcile_repair_failure_alerts_even_without_violation():
    days = _days_100()
    gap = days[49]
    actual = [d for d in days if d != gap]
    r = _FakeReconciler(open_days=days, actual=actual, repair_should_fail=True)
    res = r.reconcile(_contract(), apply_repair=True, as_of=days[-1])
    assert res.status == "failed"            # repair failed though SLA tolerated
    assert res.violations == []
    assert res.alerted is True               # repair_failed channel fires
    assert r.notifier.sent


# ── report-period model (financial_sync) ─────────────────────────────────────────
def test_quarter_ends_in_range():
    assert quarter_ends_in_range("20240101", "20241231") == [
        "20240331", "20240630", "20240930", "20241231",
    ]
    assert quarter_ends_in_range("20240401", "20240930") == ["20240630", "20240930"]


def test_period_disclosure_deadline():
    assert period_disclosure_deadline("20240331") == "20240430"
    assert period_disclosure_deadline("20240630") == "20240831"
    assert period_disclosure_deadline("20240930") == "20241031"
    assert period_disclosure_deadline("20241231") == "20250430"   # annual -> next year


def test_mature_quarter_ends_excludes_in_progress():
    # As of 2024-12-31, annual 20241231 (deadline 2025-04-30) is NOT yet mature.
    mature = mature_quarter_ends("20240101", "20241231")
    assert mature == ["20240331", "20240630", "20240930"]
    # As of 2024-07-15, H1 (deadline 2024-08-31) not mature yet.
    assert mature_quarter_ends("20240101", "20240715") == ["20240331"]


def _report_entry(**contract_overrides):
    contract = {
        "source": "tushare",
        "storage": {"database": "quant_data", "collection": "financial_income",
                    "date_field": "end_date", "symbol_field": "ts_code"},
        "window": {"start": "20240101", "end": "rolling"},
        "completeness": {
            "kind": "report_periods", "per_period_min_coverage_pct": 80.0,
            "universe": {"database": "quant_data", "collection": "stock_info",
                         "match_key": "ts_code", "list_date_field": "list_date"},
        },
        "sla": {"max_lag_trading_days": 1, "min_coverage_pct": 99.0, "min_symbols_latest_day": 0},
        "on_violation": {
            "auto_repair": True, "repair_task": "run_financial_sync",
            "repair_args": ["--all", "--period-range", "{start}:{end}"],
            "max_repair_window_days": 730,
        },
        "alert": {"channels": ["email", "data_pulse"], "on": ["repair_failed", "still_violated"]},
    }
    contract.update(contract_overrides)
    return {"id": "financial_sync", "label": "财务数据", "contract": contract}


def test_parse_report_contract():
    c = parse_contract(_report_entry())
    assert c.completeness.kind == "report_periods"
    assert c.completeness.universe.collection == "stock_info"
    assert c.on_violation.resolved_args("20180331", "20191231") == [
        "--all", "--period-range", "20180331:20191231",
    ]


# Mature periods as of 2024-12-31 are the three 2024 quarter-ends.
_MATURE_2024 = ["20240331", "20240630", "20240930"]


def test_reconcile_report_periods_ok():
    r = _FakeReconciler(open_days=[], actual=[], present_periods=set(_MATURE_2024))
    res = r.reconcile(parse_contract(_report_entry()), apply_repair=False, as_of="20241231")
    assert res.status == "ok"
    assert res.coverage.expected_days == 3
    assert res.coverage.missing_days == 0


def test_reconcile_report_periods_missing_is_violated_dryrun():
    r = _FakeReconciler(open_days=[], actual=[], present_periods={"20240331", "20240930"})
    res = r.reconcile(parse_contract(_report_entry()), apply_repair=False, as_of="20241231")
    assert res.status == "violated"          # 2/3 = 66.7% < 99%
    assert "20240630" in res.coverage.missing_sorted
    assert r.repair_calls == []              # dry-run


def test_reconcile_report_periods_apply_repairs():
    r = _FakeReconciler(
        open_days=[], actual=[], present_periods={"20240331", "20240930"},
        repair_period_fills=["20240630"],
    )
    res = r.reconcile(parse_contract(_report_entry()), apply_repair=True, as_of="20241231")
    assert r.repair_calls
    assert res.status == "repaired"
    assert res.coverage.missing_days == 0


# ── run-recency model (earnings_sync) ─────────────────────────────────────────────
def _run_recency_entry(**contract_overrides):
    contract = {
        "source": "tushare",
        "storage": {"database": "quant_data", "collection": "earnings_forecast",
                    "date_field": "ann_date", "symbol_field": "ts_code"},
        "window": {"start": "20180101", "end": "rolling"},
        "completeness": {"kind": "run_recency"},
        "sla": {"max_run_lag_days": 3},
        "on_violation": {"auto_repair": True, "repair_task": "run_earnings_sync",
                         "repair_args": ["--days", "7"], "max_repair_window_days": 30},
        "alert": {"channels": ["email"], "on": ["repair_failed", "still_violated"]},
    }
    contract.update(contract_overrides)
    return {"id": "earnings_sync", "label": "业绩数据", "contract": contract}


def test_parse_run_recency_contract():
    c = parse_contract(_run_recency_entry())
    assert c.completeness.kind == "run_recency"
    assert c.sla.max_run_lag_days == 3
    assert c.on_violation.resolved_args("x", "y") == ["--days", "7"]


class _RunRecencyReconciler(_FakeReconciler):
    """Fake that serves a synthetic latest-run doc and records repair runs."""

    def __init__(self, *, run_doc, repair_makes_fresh=True, repair_should_fail=False):
        super().__init__(open_days=[], actual=[], repair_should_fail=repair_should_fail)
        self._run_doc = run_doc
        self._repair_makes_fresh = repair_makes_fresh
        self.repair_runs = 0

    def _latest_run_doc(self, contract):
        return self._run_doc

    def _run_repair_task(self, contract, start, end):
        self.repair_runs += 1
        if self._repair_should_fail:
            return False, "boom"
        if self._repair_makes_fresh:
            from datetime import datetime
            self._run_doc = {"status": "complete", "date": "20260611",
                             "updated_at": datetime.utcnow()}
        return True, "ok"


def _doc(days_ago, status="complete"):
    from datetime import datetime, timedelta
    return {"status": status, "date": "20260605",
            "updated_at": datetime.utcnow() - timedelta(days=days_ago)}


def test_run_recency_ok_when_recent():
    r = _RunRecencyReconciler(run_doc=_doc(1))
    res = r.reconcile(parse_contract(_run_recency_entry()), apply_repair=False)
    assert res.status == "ok"
    assert res.extra["run_lag_days"] == 1


def test_run_recency_violated_when_stale_dryrun():
    r = _RunRecencyReconciler(run_doc=_doc(5))
    res = r.reconcile(parse_contract(_run_recency_entry()), apply_repair=False)
    assert res.status == "violated"
    assert r.repair_runs == 0
    assert r.notifier.sent           # still_violated alert


def test_run_recency_apply_reruns_and_recovers():
    r = _RunRecencyReconciler(run_doc=_doc(5), repair_makes_fresh=True)
    res = r.reconcile(parse_contract(_run_recency_entry()), apply_repair=True)
    assert r.repair_runs == 1
    assert res.status == "repaired"
    assert res.extra["run_lag_days"] == 0


def test_run_recency_no_record_is_violated():
    r = _RunRecencyReconciler(run_doc=None)
    res = r.reconcile(parse_contract(_run_recency_entry()), apply_repair=False)
    assert res.status == "violated"
    assert any("no run record" in v for v in res.violations)


# ── snapshot model (reference data, e.g. stock_info) ─────────────────────────
def _snapshot_entry(min_docs: int = 4000) -> Dict[str, Any]:
    contract = {
        "source": "tushare",
        "storage": {"database": "quant_data", "collection": "stock_info"},
        "window": {"start": "20100101", "end": "rolling"},
        "completeness": {"kind": "snapshot", "min_docs": min_docs},
        "sla": {},
        "on_violation": {
            "auto_repair": True,
            "repair_task": "run_stock_info_sync",
            "repair_args": [],
        },
        "alert": {"channels": ["email"], "on": ["repair_failed", "still_violated"]},
    }
    return {"id": "stock_info", "label": "股票基础信息", "contract": contract}


def test_parse_snapshot_contract_empty_repair_args():
    c = parse_contract(_snapshot_entry())
    assert c.completeness.kind == "snapshot"
    assert c.completeness.min_docs == 4000
    # Explicit empty list must mean "no args" (not the start/end fallback).
    assert c.on_violation.repair_args == []
    assert c.on_violation.resolved_args("20100101", "20260101") == []


def test_omitted_repair_args_falls_back_to_mode():
    ov = OnViolation(repair_task="run_x", repair_mode="by-date")
    assert ov.repair_args is None
    assert ov.resolved_args("20240101", "20240201") == [
        "--mode", "by-date", "--start", "20240101", "--end", "20240201",
    ]


class _SnapshotReconciler(_FakeReconciler):
    """Fake that serves a synthetic collection document count."""

    def __init__(self, *, count, repair_to=None, repair_should_fail=False):
        super().__init__(open_days=[], actual=[], repair_should_fail=repair_should_fail)
        self._count = count
        self._repair_to = repair_to
        self.repair_runs = 0

    def _snapshot_count(self, contract):
        return self._count

    def _run_repair_task(self, contract, start, end):
        self.repair_runs += 1
        if self._repair_should_fail:
            return False, "boom"
        if self._repair_to is not None:
            self._count = self._repair_to
        return True, "ok"


def test_snapshot_ok_when_full():
    r = _SnapshotReconciler(count=5500)
    res = r.reconcile(parse_contract(_snapshot_entry()), apply_repair=False)
    assert res.status == "ok"
    assert res.extra["doc_count"] == 5500
    assert r.repair_runs == 0


def test_snapshot_violated_dryrun_empty_collection():
    r = _SnapshotReconciler(count=0)
    res = r.reconcile(parse_contract(_snapshot_entry()), apply_repair=False)
    assert res.status == "violated"
    assert r.repair_runs == 0
    assert r.notifier.sent           # still_violated alert

def test_snapshot_cold_start_repairs_and_recovers():
    # Fresh system: empty collection -> repair fills it -> ok (the north star).
    r = _SnapshotReconciler(count=0, repair_to=5519)
    res = r.reconcile(parse_contract(_snapshot_entry()), apply_repair=True)
    assert r.repair_runs == 1
    assert res.status == "repaired"
    assert res.extra["doc_count"] == 5519


def test_snapshot_repair_failure_is_failed():
    r = _SnapshotReconciler(count=0, repair_should_fail=True)
    res = r.reconcile(parse_contract(_snapshot_entry()), apply_repair=True)
    assert res.status == "failed"
    assert r.notifier.sent           # repair_failed alert


# ── dependency-ordered bootstrap ─────────────────────────────────────────────
def test_topological_order_reference_data_first():
    stock_info = parse_contract(_snapshot_entry())
    financial = parse_contract(_report_entry(depends_on=["stock_info"]))
    daily = parse_contract(_catalog_entry())  # stock_daily, no deps
    ordered = topological_order([financial, daily, stock_info])
    ids = [c.id for c in ordered]
    assert ids.index("stock_info") < ids.index(financial.id)


def test_topological_order_tolerates_unknown_and_cycles():
    a_entry = _snapshot_entry()
    a_entry["id"] = "a"
    a_entry["contract"]["depends_on"] = ["missing", "b"]
    b_entry = _snapshot_entry()
    b_entry["id"] = "b"
    b_entry["contract"]["depends_on"] = ["a"]   # cycle a<->b
    a, b = parse_contract(a_entry), parse_contract(b_entry)
    ordered = topological_order([a, b])
    assert {c.id for c in ordered} == {"a", "b"}   # no crash, all present
