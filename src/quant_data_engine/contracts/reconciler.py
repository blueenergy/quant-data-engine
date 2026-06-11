"""ContractReconciler — compare desired vs actual coverage and repair.

Pure judgment helpers (``compress_missing_ranges``, ``evaluate_coverage``,
``judge_violations``) are separated from all I/O so they can be unit-tested
without Mongo or Tushare. ``ContractReconciler`` wires those helpers to the
trading calendar, MongoDB, repair subprocesses, the state repository, and the
notifier.
"""
from __future__ import annotations

import logging
import os
import subprocess
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

from quant_data_engine.config.settings import Settings, get_settings
from quant_data_engine.contracts.models import Contract

_log = logging.getLogger(__name__)

BEIJING = timezone(timedelta(hours=8))

# src/ directory so subprocess `python -m quant_data_engine...` resolves even
# when the package is not pip-installed in the subprocess environment.
_SRC_DIR = Path(__file__).resolve().parents[2]


# ── Pure judgment helpers ─────────────────────────────────────────────────────
def compress_missing_ranges(
    expected_sorted: Sequence[str], missing: Set[str]
) -> List[Tuple[str, str]]:
    """Group missing days into runs that are adjacent in the *expected* sequence.

    Adjacency is defined by the trading-day sequence (so a weekend/holiday gap
    between two missing trading days does not split the range).
    """
    ranges: List[Tuple[str, str]] = []
    run: List[str] = []
    for d in expected_sorted:
        if d in missing:
            run.append(d)
        elif run:
            ranges.append((run[0], run[-1]))
            run = []
    if run:
        ranges.append((run[0], run[-1]))
    return ranges


@dataclass
class CoverageEval:
    expected_days: int
    present_days: int
    missing_days: int
    coverage_pct: float
    latest_expected: Optional[str]
    latest_actual: Optional[str]
    lag_trading_days: int
    missing_ranges: List[Tuple[str, str]] = field(default_factory=list)
    missing_sorted: List[str] = field(default_factory=list)


def evaluate_coverage(
    expected_sorted: Sequence[str], actual_days: Iterable[str]
) -> CoverageEval:
    """Compute coverage/freshness metrics from expected vs actual trading days."""
    expected_list = list(expected_sorted)
    actual_set = set(actual_days)
    expected_set = set(expected_list)

    present = [d for d in expected_list if d in actual_set]
    missing = [d for d in expected_list if d not in actual_set]
    missing_set = set(missing)

    n_expected = len(expected_list)
    coverage_pct = 100.0 * len(present) / n_expected if n_expected else 100.0
    latest_expected = expected_list[-1] if expected_list else None
    latest_actual = present[-1] if present else None

    if not expected_list:
        lag = 0
    elif latest_actual is None:
        lag = n_expected
    else:
        lag = sum(1 for d in expected_list if d > latest_actual)

    return CoverageEval(
        expected_days=n_expected,
        present_days=len(present),
        missing_days=len(missing),
        coverage_pct=round(coverage_pct, 4),
        latest_expected=latest_expected,
        latest_actual=latest_actual,
        lag_trading_days=lag,
        missing_ranges=compress_missing_ranges(expected_list, missing_set),
        missing_sorted=missing,
    )


def judge_violations(
    cov: CoverageEval,
    contract: Contract,
    symbols_latest_day: Optional[int] = None,
) -> List[str]:
    """Return a list of human-readable SLA violation reasons (empty = healthy)."""
    sla = contract.sla
    reasons: List[str] = []
    if cov.lag_trading_days > sla.max_lag_trading_days:
        reasons.append(
            f"freshness lag {cov.lag_trading_days} trading day(s) "
            f"> max {sla.max_lag_trading_days} "
            f"(latest_actual={cov.latest_actual}, latest_expected={cov.latest_expected})"
        )
    if cov.coverage_pct < sla.min_coverage_pct:
        reasons.append(
            f"coverage {cov.coverage_pct:.2f}% < min {sla.min_coverage_pct:.2f}% "
            f"({cov.missing_days} of {cov.expected_days} trading days missing)"
        )
    if (
        sla.min_symbols_latest_day > 0
        and symbols_latest_day is not None
        and symbols_latest_day < sla.min_symbols_latest_day
    ):
        reasons.append(
            f"latest-day symbols {symbols_latest_day} < min {sla.min_symbols_latest_day} "
            f"(day={cov.latest_actual})"
        )
    return reasons


def chunk_missing_days(missing_sorted: Sequence[str], max_window_days: int) -> List[Tuple[str, str]]:
    """Split missing days into (start, end) chunks bounded by a calendar span.

    Each chunk spans at most ``max_window_days`` calendar days so a single
    repair invocation never fetches an unbounded range (rate-limit guard).
    """
    if not missing_sorted:
        return []
    max_window_days = max(int(max_window_days), 1)
    chunks: List[Tuple[str, str]] = []
    days = list(missing_sorted)
    chunk_start = days[0]
    prev = days[0]
    for d in days[1:]:
        span = (datetime.strptime(d, "%Y%m%d") - datetime.strptime(chunk_start, "%Y%m%d")).days
        if span >= max_window_days:
            chunks.append((chunk_start, prev))
            chunk_start = d
        prev = d
    chunks.append((chunk_start, prev))
    return chunks


# ── Reconcile result ──────────────────────────────────────────────────────────
@dataclass
class ReconcileResult:
    contract_id: str
    status: str                       # ok | violated | repaired | failed | unknown
    coverage: Optional[CoverageEval]
    symbols_latest_day: Optional[int]
    violations: List[str]
    repair: Optional[Dict[str, Any]]
    alerted: bool
    note: str = ""

    def to_snapshot(self, label: str) -> Dict[str, Any]:
        cov = self.coverage
        snap: Dict[str, Any] = {
            "label": label,
            "status": self.status,
            "violations": self.violations,
            "symbols_latest_day": self.symbols_latest_day,
            "repair": self.repair,
            "note": self.note,
        }
        if cov is not None:
            snap.update(
                coverage_pct=cov.coverage_pct,
                lag_trading_days=cov.lag_trading_days,
                expected_days=cov.expected_days,
                present_days=cov.present_days,
                missing_days=cov.missing_days,
                latest_actual=cov.latest_actual,
                latest_expected=cov.latest_expected,
                missing_ranges=[list(r) for r in cov.missing_ranges],
            )
        return snap


# ── Orchestrator ──────────────────────────────────────────────────────────────
class ContractReconciler:
    def __init__(
        self,
        settings: Optional[Settings] = None,
        *,
        adapter: Any = None,
        mongo_client: Any = None,
        state_repo: Any = None,
        notifier: Any = None,
        alert_cooldown_min: Optional[int] = None,
    ) -> None:
        self.settings = settings or get_settings()
        self._adapter = adapter
        self._mongo = mongo_client
        self._state_repo = state_repo
        self._notifier = notifier
        if alert_cooldown_min is None:
            alert_cooldown_min = int(os.getenv("DATA_CONTRACT_ALERT_COOLDOWN_MIN", "180"))
        self.alert_cooldown = timedelta(minutes=max(alert_cooldown_min, 0))

    # -- lazy deps ----------------------------------------------------------
    @property
    def adapter(self):
        if self._adapter is None:
            from quant_data_engine.adapters.stock_adapter import StockAdapter

            self._adapter = StockAdapter(
                self.settings.tushare_token,
                rate_limit_per_sec=self.settings.rate_limit_per_sec,
            )
        return self._adapter

    @property
    def mongo(self):
        if self._mongo is None:
            from pymongo import MongoClient

            self._mongo = MongoClient(self.settings.mongo_uri)
        return self._mongo

    @property
    def state_repo(self):
        if self._state_repo is None:
            from quant_data_engine.contracts.state_repository import (
                DataContractStateRepository,
            )

            self._state_repo = DataContractStateRepository(
                self.settings.mongo_uri, self.settings.mongo_db
            )
        return self._state_repo

    @property
    def notifier(self):
        if self._notifier is None:
            from quant_data_engine.contracts.notifier import ContractNotifier

            self._notifier = ContractNotifier()
        return self._notifier

    # -- data access --------------------------------------------------------
    def _effective_end(self, contract: Contract, as_of: Optional[str]) -> str:
        today = as_of or datetime.now(BEIJING).strftime("%Y%m%d")
        if contract.end_is_rolling:
            return today
        return min(contract.window.end, today)

    def expected_trading_days(self, contract: Contract, as_of: Optional[str]) -> List[str]:
        end = self._effective_end(contract, as_of)
        df = self.adapter.fetch_trade_cal(start_date=contract.window.start, end_date=end)
        if df is None or getattr(df, "empty", True):
            return []
        open_days = df[df["is_open"] == 1]["cal_date"].astype(str).str.replace("-", "", regex=False)
        return sorted(d for d in open_days.tolist() if contract.window.start <= d <= end)

    def actual_days(self, contract: Contract, start: str, end: str) -> Set[str]:
        coll = self.mongo[contract.storage.database][contract.storage.collection]
        field = contract.storage.date_field
        values = coll.distinct(field, {field: {"$gte": start, "$lte": end}})
        return {str(v) for v in values if v is not None}

    def symbols_on_day(self, contract: Contract, day: str) -> Optional[int]:
        if not day:
            return None
        coll = self.mongo[contract.storage.database][contract.storage.collection]
        try:
            vals = coll.distinct(
                contract.storage.symbol_field, {contract.storage.date_field: day}
            )
            return len(vals)
        except Exception as e:  # pragma: no cover - defensive
            _log.warning("symbols_on_day failed for %s: %s", day, e)
            return None

    # -- repair -------------------------------------------------------------
    def _run_repair_task(self, contract: Contract, start: str, end: str) -> Tuple[bool, str]:
        module = f"quant_data_engine.tasks.{contract.on_violation.repair_task}"
        cmd = [
            sys.executable, "-m", module,
            "--mode", contract.on_violation.repair_mode,
            "--start", start, "--end", end,
        ]
        env = dict(os.environ)
        env["PYTHONPATH"] = str(_SRC_DIR) + os.pathsep + env.get("PYTHONPATH", "")
        _log.info("[%s] repair: %s", contract.id, " ".join(cmd))
        try:
            proc = subprocess.run(
                cmd, env=env, capture_output=True, text=True, timeout=
                float(os.getenv("DATA_CONTRACT_REPAIR_TIMEOUT", "7200")),
            )
        except Exception as e:  # pragma: no cover - subprocess env dependent
            return False, f"subprocess error: {e}"
        if proc.returncode != 0:
            tail = (proc.stderr or proc.stdout or "")[-1500:]
            return False, f"exit {proc.returncode}: {tail}"
        return True, "ok"

    def repair(self, contract: Contract, cov: CoverageEval) -> Dict[str, Any]:
        chunks = chunk_missing_days(
            cov.missing_sorted, contract.on_violation.max_repair_window_days
        )
        result: Dict[str, Any] = {
            "attempted": True,
            "chunks": len(chunks),
            "succeeded": 0,
            "failed": 0,
            "errors": [],
        }
        if not contract.on_violation.repair_task:
            result.update(attempted=False, errors=["no repair_task configured"])
            return result
        for start, end in chunks:
            ok, msg = self._run_repair_task(contract, start, end)
            if ok:
                result["succeeded"] += 1
            else:
                result["failed"] += 1
                result["errors"].append(f"{start}-{end}: {msg}")
        return result

    # -- alert --------------------------------------------------------------
    def _should_alert(self, contract: Contract, result: ReconcileResult) -> bool:
        triggers = set(contract.alert.on)
        if "repair_failed" in triggers and result.repair and result.repair.get("failed"):
            return True
        # Use the residual violations (not the status string) so a
        # violated+repair-failed run still triggers the still_violated channel.
        if "still_violated" in triggers and result.violations:
            return True
        return False

    def _alert_on_cooldown(self, contract_id: str) -> bool:
        last = self.state_repo.last_alert_at(contract_id)
        if not last:
            return False
        return (datetime.utcnow() - last) < self.alert_cooldown

    # -- main ---------------------------------------------------------------
    def reconcile(
        self,
        contract: Contract,
        *,
        apply_repair: bool = False,
        allow_alert: bool = True,
        as_of: Optional[str] = None,
    ) -> ReconcileResult:
        expected = self.expected_trading_days(contract, as_of)
        if not expected:
            result = ReconcileResult(
                contract_id=contract.id, status="unknown", coverage=None,
                symbols_latest_day=None, violations=[], repair=None, alerted=False,
                note="no trading calendar available (token/network?)",
            )
            self._persist(contract, result)
            return result

        start, end = expected[0], expected[-1]
        actual = self.actual_days(contract, start, end)
        cov = evaluate_coverage(expected, actual)
        symbols_latest = (
            self.symbols_on_day(contract, cov.latest_actual)
            if contract.sla.min_symbols_latest_day > 0 else None
        )
        violations = judge_violations(cov, contract, symbols_latest)

        repair_info: Optional[Dict[str, Any]] = None
        # Decoupled policy: repair ANY detected gap when --apply (idempotent
        # upsert), regardless of whether the SLA tolerance is breached. Alerting
        # stays gated on SLA violation / repair failure (see _should_alert).
        if cov.missing_days > 0 and apply_repair and contract.on_violation.auto_repair:
            repair_info = self.repair(contract, cov)
            # Verify: recompute after repair.
            actual2 = self.actual_days(contract, start, end)
            cov = evaluate_coverage(expected, actual2)
            symbols_latest = (
                self.symbols_on_day(contract, cov.latest_actual)
                if contract.sla.min_symbols_latest_day > 0 else None
            )
            violations = judge_violations(cov, contract, symbols_latest)

        repair_failed = bool(repair_info and repair_info.get("failed"))
        if violations:
            status = "failed" if repair_failed else "violated"
        elif repair_info and repair_info.get("attempted"):
            status = "failed" if repair_failed else "repaired"
        else:
            status = "ok"

        result = ReconcileResult(
            contract_id=contract.id, status=status, coverage=cov,
            symbols_latest_day=symbols_latest, violations=violations,
            repair=repair_info, alerted=False,
        )

        # Alert (respect cooldown), then persist.
        if allow_alert and self._should_alert(contract, result):
            if not self._alert_on_cooldown(contract.id):
                self._send_alert(contract, result)
                result.alerted = True
        self._persist(contract, result)
        if result.alerted:
            self.state_repo.mark_alerted(contract.id)
        return result

    def _send_alert(self, contract: Contract, result: ReconcileResult) -> None:
        cov = result.coverage
        subject = f"[数据合约告警] {contract.label} ({contract.id}) — {result.status}"
        summary: Dict[str, Any] = {
            "contract": contract.id,
            "status": result.status,
            "violations": "; ".join(result.violations) or "-",
        }
        if cov is not None:
            summary.update(
                coverage_pct=f"{cov.coverage_pct:.2f}%",
                lag_trading_days=cov.lag_trading_days,
                latest_actual=cov.latest_actual,
                latest_expected=cov.latest_expected,
                missing_days=cov.missing_days,
            )
        if result.repair:
            summary["repair"] = (
                f"chunks={result.repair.get('chunks')} "
                f"ok={result.repair.get('succeeded')} "
                f"failed={result.repair.get('failed')}"
            )
        try:
            self.notifier.notify(contract.alert.channels, subject, summary)
        except Exception as e:  # pragma: no cover - defensive
            _log.error("alert dispatch failed: %s", e)

    def _persist(self, contract: Contract, result: ReconcileResult) -> None:
        try:
            self.state_repo.upsert_snapshot(contract.id, result.to_snapshot(contract.label))
        except Exception as e:  # pragma: no cover - defensive
            _log.warning("data_contract_state upsert failed: %s", e)


__all__ = [
    "BEIJING",
    "CoverageEval",
    "ReconcileResult",
    "ContractReconciler",
    "compress_missing_ranges",
    "evaluate_coverage",
    "judge_violations",
    "chunk_missing_days",
]
