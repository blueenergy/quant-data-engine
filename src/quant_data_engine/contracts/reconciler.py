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


# ── Report-period helpers (pure) ──────────────────────────────────────────────
_QUARTER_ENDS = ("0331", "0630", "0930", "1231")


def quarter_ends_in_range(start: str, end: str) -> List[str]:
    """All A-share report quarter-ends (0331/0630/0930/1231) in [start, end]."""
    out: List[str] = []
    for y in range(int(start[:4]), int(end[:4]) + 1):
        for md in _QUARTER_ENDS:
            d = f"{y}{md}"
            if start <= d <= end:
                out.append(d)
    return out


def period_disclosure_deadline(period: str) -> str:
    """Latest A-share disclosure deadline (YYYYMMDD) for a quarter-end period.

    Q1→Apr 30 same year, H1→Aug 31, Q3→Oct 31, annual→Apr 30 next year.
    """
    y = int(period[:4])
    md = period[4:8]
    return {
        "0331": f"{y}0430",
        "0630": f"{y}0831",
        "0930": f"{y}1031",
        "1231": f"{y + 1}0430",
    }[md]


def mature_quarter_ends(start: str, as_of: str) -> List[str]:
    """Quarter-ends in [start, as_of] whose disclosure deadline has passed.

    Avoids flagging an in-progress period (e.g. H1 in July) as missing.
    """
    return [
        p for p in quarter_ends_in_range(start, as_of)
        if period_disclosure_deadline(p) <= as_of
    ]


def topological_order(contracts: List["Contract"]) -> List["Contract"]:
    """Order contracts so each comes after its contract-level depends_on.

    Reference data (e.g. stock_info) reconciles before datasets that need it
    (e.g. financial_sync's per-period universe). Unknown deps and cycles are
    tolerated (skipped) so a bad edge never blocks the whole bootstrap.
    """
    by_id = {c.id: c for c in contracts}
    state: Dict[str, int] = {}   # 0 = visiting, 1 = done
    order: List["Contract"] = []

    def visit(c: "Contract") -> None:
        st = state.get(c.id)
        if st is not None:
            return  # done, or currently visiting (cycle) — skip
        state[c.id] = 0
        for dep in c.depends_on:
            d = by_id.get(dep)
            if d is not None:
                visit(d)
        state[c.id] = 1
        order.append(c)

    for c in contracts:
        visit(c)
    return order


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
    extra: Dict[str, Any] = field(default_factory=dict)   # model-specific metrics
    # How many reconciles in a row ended in "repaired". One is routine; a long
    # streak means the pipeline that should deliver this data is broken and the
    # safety net has quietly been standing in for it.
    consecutive_repairs: int = 0

    def to_snapshot(self, label: str) -> Dict[str, Any]:
        cov = self.coverage
        snap: Dict[str, Any] = {
            "label": label,
            "status": self.status,
            "violations": self.violations,
            "symbols_latest_day": self.symbols_latest_day,
            "repair": self.repair,
            "consecutive_repairs": self.consecutive_repairs,
            "note": self.note,
        }
        snap.update(self.extra)
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
        repair_streak_alert: Optional[int] = None,
    ) -> None:
        self.settings = settings or get_settings()
        self._adapter = adapter
        self._mongo = mongo_client
        self._state_repo = state_repo
        self._notifier = notifier
        if alert_cooldown_min is None:
            alert_cooldown_min = int(os.getenv("DATA_CONTRACT_ALERT_COOLDOWN_MIN", "180"))
        self.alert_cooldown = timedelta(minutes=max(alert_cooldown_min, 0))
        if repair_streak_alert is None:
            repair_streak_alert = int(os.getenv("DATA_CONTRACT_REPAIR_STREAK_ALERT", "3"))
        self.repair_streak_alert = repair_streak_alert

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
        query: Dict[str, Any] = {field: {"$gte": start, "$lte": end}}
        query.update(contract.storage.source_filter())
        values = coll.distinct(field, query)
        return {str(v) for v in values if v is not None}

    # -- completeness strategy ----------------------------------------------
    def _expected_units(self, contract: Contract, as_of: Optional[str]) -> List[str]:
        if contract.completeness.kind == "report_periods":
            end = self._effective_end(contract, as_of)
            return mature_quarter_ends(contract.window.start, end)
        return self.expected_trading_days(contract, as_of)

    def _present_units(self, contract: Contract, expected: List[str]) -> Set[str]:
        if not expected:
            return set()
        if contract.completeness.kind == "report_periods":
            return {p for p in expected if self._period_present(contract, p)}
        return self.actual_days(contract, expected[0], expected[-1])

    def _period_present(self, contract: Contract, period: str) -> bool:
        """A report period is present if its row count meets the coverage bar.

        Coverage is measured against the per-period universe (e.g. stocks
        listed by the period). Delisted issuers only inflate the numerator, so
        the ratio is robust as the listed count grows over the years.
        """
        coll = self.mongo[contract.storage.database][contract.storage.collection]
        period_query: Dict[str, Any] = {contract.storage.date_field: period}
        period_query.update(contract.storage.source_filter())
        codes = len(coll.distinct(contract.storage.symbol_field, period_query))
        uni = contract.completeness.universe
        if uni is None:
            return codes > 0
        ucoll = self.mongo[uni.database][uni.collection]
        universe = ucoll.count_documents({uni.list_date_field: {"$lte": period}})
        if universe <= 0:
            return codes > 0
        return (codes / universe) >= (contract.completeness.per_period_min_coverage_pct / 100.0)

    def symbols_on_day(self, contract: Contract, day: str) -> Optional[int]:
        if not day:
            return None
        coll = self.mongo[contract.storage.database][contract.storage.collection]
        try:
            day_query: Dict[str, Any] = {contract.storage.date_field: day}
            day_query.update(contract.storage.source_filter())
            vals = coll.distinct(contract.storage.symbol_field, day_query)
            return len(vals)
        except Exception as e:  # pragma: no cover - defensive
            _log.warning("symbols_on_day failed for %s: %s", day, e)
            return None

    # -- repair -------------------------------------------------------------
    def _run_repair_task(self, contract: Contract, start: str, end: str) -> Tuple[bool, str]:
        module = f"quant_data_engine.tasks.{contract.on_violation.repair_task}"
        cmd = [sys.executable, "-m", module] + contract.on_violation.resolved_args(start, end)
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
        # A successful repair used to be silent, which is how a two-month
        # dispatcher outage stayed invisible: the nightly safety net refilled
        # the gap every day and the contract kept reporting healthy. One repair
        # is routine, a streak is a broken upstream.
        if (
            self.repair_streak_alert > 0
            and result.status == "repaired"
            and result.consecutive_repairs >= self.repair_streak_alert
        ):
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
        if contract.completeness.kind == "run_recency":
            return self._reconcile_run_recency(
                contract, apply_repair=apply_repair, allow_alert=allow_alert, as_of=as_of
            )
        if contract.completeness.kind == "snapshot":
            return self._reconcile_snapshot(
                contract, apply_repair=apply_repair, allow_alert=allow_alert
            )

        expected = self._expected_units(contract, as_of)
        if not expected:
            result = ReconcileResult(
                contract_id=contract.id, status="unknown", coverage=None,
                symbols_latest_day=None, violations=[], repair=None, alerted=False,
                note="no expected units (calendar/periods unavailable — token/network?)",
            )
            self._persist(contract, result)
            return result

        present = self._present_units(contract, expected)
        cov = evaluate_coverage(expected, present)
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
            present = self._present_units(contract, expected)
            cov = evaluate_coverage(expected, present)
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
        return self._finalize(contract, result, allow_alert)

    def _repair_streak(self, contract_id: str, status: str) -> int:
        if status != "repaired":
            return 0
        try:
            previous = self.state_repo.repair_streak(contract_id)
        except Exception as e:  # pragma: no cover - defensive
            _log.warning("could not read repair streak for %s: %s", contract_id, e)
            previous = 0
        return previous + 1

    def _finalize(self, contract: Contract, result: ReconcileResult, allow_alert: bool) -> ReconcileResult:
        """Shared tail: alert (respecting cooldown), persist, mark_alerted."""
        result.consecutive_repairs = self._repair_streak(contract.id, result.status)
        if result.consecutive_repairs:
            _log.warning(
                "[%s] data was missing and the safety net backfilled it "
                "(%d reconcile(s) in a row) — the owning pipeline is not delivering",
                contract.id,
                result.consecutive_repairs,
            )
        if allow_alert and self._should_alert(contract, result):
            if not self._alert_on_cooldown(contract.id):
                self._send_alert(contract, result)
                result.alerted = True
        self._persist(contract, result)
        if result.alerted:
            self.state_repo.mark_alerted(contract.id)
        return result

    # -- run_recency model --------------------------------------------------
    def _latest_run_doc(self, contract: Contract) -> Optional[Dict[str, Any]]:
        db = self.mongo[self.settings.mongo_db]
        keys = [contract.id]
        if contract.job_status_key and contract.job_status_key not in keys:
            keys.append(contract.job_status_key)
        doc = db["pipeline_tasks"].find_one(
            {"domain": "data_collection", "task_type": {"$in": keys}}, sort=[("date", -1)]
        )
        if not doc:
            doc = db["job_status"].find_one({"job_type": {"$in": keys}}, sort=[("date", -1)])
        return doc

    def _evaluate_run_recency(
        self, contract: Contract, doc: Optional[Dict[str, Any]]
    ) -> Tuple[List[str], Dict[str, Any]]:
        sla = contract.sla
        now = datetime.utcnow()
        if not doc:
            return (["no run record found"], {"run_lag_days": None, "last_run_status": None,
                                              "last_run_date": None, "last_updated_at": None})
        updated_at = doc.get("updated_at") or doc.get("completed_at")
        status_ = doc.get("status")
        extra: Dict[str, Any] = {
            "run_lag_days": None,
            "last_run_status": status_,
            "last_run_date": doc.get("date"),
            "last_updated_at": updated_at,
        }
        violations: List[str] = []
        if not isinstance(updated_at, datetime):
            violations.append("run record has no updated_at timestamp")
        else:
            lag_days = (now - updated_at).days
            extra["run_lag_days"] = lag_days
            if lag_days > sla.max_run_lag_days:
                violations.append(
                    f"last run {lag_days}d ago > max {sla.max_run_lag_days}d "
                    f"(date={doc.get('date')})"
                )
        if status_ == "failed":
            violations.append("last run status=failed")

        # "The job ran" and "the data moved" are different claims. hk_hold was
        # 133 days stale while every run reported complete, and this model only
        # ever checked the former.
        if sla.max_data_lag_days > 0:
            latest_data = self._latest_stored_date(contract)
            extra["latest_data_date"] = latest_data
            if latest_data is None:
                violations.append("no dated rows in storage collection")
            else:
                data_lag = (now - latest_data).days
                extra["data_lag_days"] = data_lag
                if data_lag > sla.max_data_lag_days:
                    violations.append(
                        f"stored data {data_lag}d old > max {sla.max_data_lag_days}d "
                        f"(latest={latest_data:%Y%m%d}) despite the job reporting success"
                    )
        return violations, extra

    def _latest_stored_date(self, contract: Contract) -> Optional[datetime]:
        """Newest value of the contract's date field, as a datetime."""
        coll = self.mongo[contract.storage.database][contract.storage.collection]
        field = contract.storage.date_field
        doc = coll.find_one({field: {"$ne": None}}, {field: 1}, sort=[(field, -1)])
        if not doc:
            return None
        value = doc.get(field)
        if isinstance(value, datetime):
            return value.replace(tzinfo=None)
        text = str(value)
        for fmt in ("%Y%m%d", "%Y-%m-%d"):
            try:
                return datetime.strptime(text[:10], fmt)
            except ValueError:
                continue
        _log.warning(
            "[%s] cannot read %s=%r as a date; skipping data freshness check",
            contract.id, field, value,
        )
        return None

    def _reconcile_run_recency(
        self,
        contract: Contract,
        *,
        apply_repair: bool,
        allow_alert: bool,
        as_of: Optional[str],
    ) -> ReconcileResult:
        doc = self._latest_run_doc(contract)
        violations, extra = self._evaluate_run_recency(contract, doc)

        repair_info: Optional[Dict[str, Any]] = None
        if violations and apply_repair and contract.on_violation.auto_repair:
            ok, msg = self._run_repair_task(contract, "", "")
            repair_info = {
                "attempted": True, "chunks": 1,
                "succeeded": 1 if ok else 0, "failed": 0 if ok else 1,
                "errors": [] if ok else [msg],
            }
            doc = self._latest_run_doc(contract)
            violations, extra = self._evaluate_run_recency(contract, doc)

        repair_failed = bool(repair_info and repair_info.get("failed"))
        if violations:
            status = "failed" if repair_failed else "violated"
        elif repair_info and repair_info.get("attempted"):
            status = "failed" if repair_failed else "repaired"
        else:
            status = "ok"

        result = ReconcileResult(
            contract_id=contract.id, status=status, coverage=None,
            symbols_latest_day=None, violations=violations,
            repair=repair_info, alerted=False, extra=extra,
        )
        return self._finalize(contract, result, allow_alert)

    # -- snapshot model (reference data, e.g. stock_info) -------------------
    def _snapshot_count(self, contract: Contract) -> int:
        coll = self.mongo[contract.storage.database][contract.storage.collection]
        return coll.count_documents({})

    def _reconcile_snapshot(
        self, contract: Contract, *, apply_repair: bool, allow_alert: bool
    ) -> ReconcileResult:
        min_docs = contract.completeness.min_docs
        count = self._snapshot_count(contract)
        violations = [] if count >= min_docs else [f"doc count {count} < min {min_docs}"]
        extra: Dict[str, Any] = {"doc_count": count, "min_docs": min_docs}

        repair_info: Optional[Dict[str, Any]] = None
        if violations and apply_repair and contract.on_violation.auto_repair:
            ok, msg = self._run_repair_task(contract, "", "")
            repair_info = {
                "attempted": True, "chunks": 1,
                "succeeded": 1 if ok else 0, "failed": 0 if ok else 1,
                "errors": [] if ok else [msg],
            }
            count = self._snapshot_count(contract)
            extra["doc_count"] = count
            violations = [] if count >= min_docs else [f"doc count {count} < min {min_docs}"]

        repair_failed = bool(repair_info and repair_info.get("failed"))
        if violations:
            status = "failed" if repair_failed else "violated"
        elif repair_info and repair_info.get("attempted"):
            status = "failed" if repair_failed else "repaired"
        else:
            status = "ok"

        result = ReconcileResult(
            contract_id=contract.id, status=status, coverage=None,
            symbols_latest_day=None, violations=violations,
            repair=repair_info, alerted=False, extra=extra,
        )
        return self._finalize(contract, result, allow_alert)

    def _send_alert(self, contract: Contract, result: ReconcileResult) -> None:
        cov = result.coverage
        subject = f"[数据合约告警] {contract.label} ({contract.id}) — {result.status}"
        summary: Dict[str, Any] = {
            "contract": contract.id,
            "status": result.status,
            "violations": "; ".join(result.violations) or "-",
        }
        if result.consecutive_repairs:
            summary["consecutive_repairs"] = result.consecutive_repairs
        if cov is not None:
            summary.update(
                coverage_pct=f"{cov.coverage_pct:.2f}%",
                lag_trading_days=cov.lag_trading_days,
                latest_actual=cov.latest_actual,
                latest_expected=cov.latest_expected,
                missing_days=cov.missing_days,
            )
        for k, v in result.extra.items():
            summary.setdefault(k, v)
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
    "quarter_ends_in_range",
    "period_disclosure_deadline",
    "mature_quarter_ends",
]
