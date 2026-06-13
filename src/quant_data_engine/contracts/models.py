"""Dataclasses describing a data contract, parsed from the catalog YAML.

A contract entry lives under ``contract:`` of a pipeline in
``data_pipelines_catalog.yaml``. Everything is optional with safe defaults so
existing catalog entries without a ``contract`` block are simply ignored by the
reconciler.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class StorageSpec:
    database: str
    collection: str
    date_field: str = "trade_date"
    symbol_field: str = "symbol"


@dataclass
class Window:
    start: str                 # YYYYMMDD inclusive
    end: str = "rolling"       # "rolling" => latest trading day, else YYYYMMDD


@dataclass
class SLA:
    max_lag_trading_days: int = 1
    min_coverage_pct: float = 99.0
    min_symbols_latest_day: int = 0   # 0 disables the per-day symbol-count check
    max_run_lag_days: int = 2         # run_recency: max calendar days since last run


@dataclass
class OnViolation:
    auto_repair: bool = False
    repair_task: Optional[str] = None      # e.g. run_stock_daily
    # Explicit argv template for the repair task, with {start}/{end} tokens.
    # None => derive from repair_mode (back-compat). An explicit [] means the
    # task takes no date args (e.g. run_stock_info_sync snapshot refresh).
    repair_args: Optional[List[str]] = None
    repair_mode: Optional[str] = None      # e.g. by-date (stock_daily)
    max_repair_window_days: int = 30

    def resolved_args(self, start: str, end: str) -> List[str]:
        """Return the concrete CLI args for one repair invocation.

        Precedence: explicit repair_args (incl. empty) > repair_mode-derived >
        start/end only. Each catalog dataset can therefore declare exactly how
        its task is called (e.g. index_daily uses --core, etf_daily uses --all,
        stock_info uses no args).
        """
        if self.repair_args is not None:
            template = self.repair_args
        elif self.repair_mode:
            template = ["--mode", self.repair_mode, "--start", "{start}", "--end", "{end}"]
        else:
            template = ["--start", "{start}", "--end", "{end}"]
        return [str(a).format(start=start, end=end) for a in template]


@dataclass
class UniverseSpec:
    """Per-period universe for report_periods coverage (e.g. listed stocks)."""
    database: str
    collection: str
    match_key: str = "ts_code"
    list_date_field: str = "list_date"   # universe = list_date <= period


@dataclass
class Completeness:
    """How a contract's expected units and per-unit presence are determined.

    kind:
      - calendar_trading_days (default): units are trading days from the
        exchange calendar; a day is present if any row exists for it.
      - report_periods: units are mature quarter-ends (disclosure deadline
        passed); a period is present if its row count meets the per-period
        coverage threshold against the universe.
    """
    kind: str = "calendar_trading_days"
    per_period_min_coverage_pct: float = 80.0
    universe: Optional[UniverseSpec] = None
    min_docs: int = 0                       # snapshot: collection present if count >= min_docs


@dataclass
class AlertSpec:
    channels: List[str] = field(default_factory=list)   # email | data_pulse | webhook
    on: List[str] = field(default_factory=list)         # repair_failed | still_violated | source_down


@dataclass
class Contract:
    id: str
    label: str
    source: str
    storage: StorageSpec
    window: Window
    sla: SLA
    on_violation: OnViolation
    alert: AlertSpec
    completeness: Completeness = field(default_factory=Completeness)
    # Contract-level bootstrap dependencies (other contract ids that must be
    # reconciled first, e.g. financial_sync depends on stock_info universe).
    # Distinct from the catalog pipeline-level depends_on (daily DAG gate).
    depends_on: List[str] = field(default_factory=list)
    # Runtime status can be written under a legacy job key that differs from
    # the contract id (for example institutional_deep_sync -> institutional_deep).
    job_status_key: str = ""

    @property
    def end_is_rolling(self) -> bool:
        return str(self.window.end).strip().lower() == "rolling"


def _as_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _as_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def parse_contract(entry: Dict[str, Any]) -> Optional[Contract]:
    """Parse one catalog pipeline entry into a Contract, or None if no contract.

    ``entry`` is the dict for a single pipeline from the catalog ``pipelines``
    list. Returns None when the entry has no ``contract`` block so callers can
    skip non-contract pipelines gracefully.
    """
    raw = entry.get("contract")
    if not isinstance(raw, dict):
        return None

    pid = entry.get("id")
    if not pid:
        return None

    storage_raw = raw.get("storage") or {}
    # Fall back to the freshness block for storage location when omitted.
    fresh = entry.get("freshness") or {}
    storage = StorageSpec(
        database=storage_raw.get("database") or fresh.get("database") or "quant_data",
        collection=storage_raw.get("collection") or fresh.get("collection") or "",
        date_field=storage_raw.get("date_field") or fresh.get("field") or "trade_date",
        symbol_field=storage_raw.get("symbol_field") or "symbol",
    )
    if not storage.collection:
        raise ValueError(f"contract {pid!r}: storage.collection is required")

    window_raw = raw.get("window") or {}
    start = window_raw.get("start")
    if not start:
        raise ValueError(f"contract {pid!r}: window.start is required")
    window = Window(start=str(start), end=str(window_raw.get("end", "rolling")))

    sla_raw = raw.get("sla") or {}
    sla = SLA(
        max_lag_trading_days=_as_int(sla_raw.get("max_lag_trading_days"), 1),
        min_coverage_pct=_as_float(sla_raw.get("min_coverage_pct"), 99.0),
        min_symbols_latest_day=_as_int(sla_raw.get("min_symbols_latest_day"), 0),
        max_run_lag_days=_as_int(sla_raw.get("max_run_lag_days"), 2),
    )

    ov_raw = raw.get("on_violation") or {}
    repair_args_raw = ov_raw.get("repair_args")
    repair_args = (
        [str(a) for a in repair_args_raw] if isinstance(repair_args_raw, list) else None
    )
    on_violation = OnViolation(
        auto_repair=bool(ov_raw.get("auto_repair", False)),
        repair_task=ov_raw.get("repair_task"),
        repair_args=repair_args,
        repair_mode=ov_raw.get("repair_mode"),
        max_repair_window_days=_as_int(ov_raw.get("max_repair_window_days"), 30),
    )

    alert_raw = raw.get("alert") or {}
    # NOTE: an unquoted YAML key `on` is parsed as the boolean True (YAML 1.1),
    # so accept both "on" and True for robustness against catalog edits.
    alert_on = alert_raw.get("on")
    if alert_on is None:
        alert_on = alert_raw.get(True)
    alert = AlertSpec(
        channels=list(alert_raw.get("channels") or []),
        on=list(alert_on or []),
    )

    comp_raw = raw.get("completeness") or {}
    universe = None
    uni_raw = comp_raw.get("universe")
    if isinstance(uni_raw, dict) and uni_raw.get("collection"):
        universe = UniverseSpec(
            database=uni_raw.get("database") or storage.database,
            collection=uni_raw["collection"],
            match_key=uni_raw.get("match_key") or "ts_code",
            list_date_field=uni_raw.get("list_date_field") or "list_date",
        )
    completeness = Completeness(
        kind=str(comp_raw.get("kind") or "calendar_trading_days"),
        per_period_min_coverage_pct=_as_float(comp_raw.get("per_period_min_coverage_pct"), 80.0),
        universe=universe,
        min_docs=_as_int(comp_raw.get("min_docs"), 0),
    )

    return Contract(
        id=str(pid),
        label=str(entry.get("label") or pid),
        source=str(raw.get("source") or "tushare"),
        storage=storage,
        window=window,
        sla=sla,
        on_violation=on_violation,
        alert=alert,
        completeness=completeness,
        depends_on=[str(d) for d in (raw.get("depends_on") or [])],
        job_status_key=str(entry.get("job_status_key") or pid),
    )


__all__ = [
    "StorageSpec",
    "Window",
    "SLA",
    "OnViolation",
    "AlertSpec",
    "UniverseSpec",
    "Completeness",
    "Contract",
    "parse_contract",
]
