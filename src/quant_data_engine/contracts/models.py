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


@dataclass
class OnViolation:
    auto_repair: bool = False
    repair_task: Optional[str] = None      # e.g. run_stock_daily
    # Explicit argv template for the repair task, with {start}/{end} tokens.
    # When empty, a template is derived from repair_mode (back-compat).
    repair_args: List[str] = field(default_factory=list)
    repair_mode: Optional[str] = None      # e.g. by-date (stock_daily)
    max_repair_window_days: int = 30

    def resolved_args(self, start: str, end: str) -> List[str]:
        """Return the concrete CLI args for one repair invocation.

        Precedence: explicit repair_args > repair_mode-derived > start/end only.
        Each catalog dataset can therefore declare exactly how its task is
        called (e.g. index_daily uses --core, etf_daily uses --all).
        """
        if self.repair_args:
            template = self.repair_args
        elif self.repair_mode:
            template = ["--mode", self.repair_mode, "--start", "{start}", "--end", "{end}"]
        else:
            template = ["--start", "{start}", "--end", "{end}"]
        return [str(a).format(start=start, end=end) for a in template]


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
    )

    ov_raw = raw.get("on_violation") or {}
    on_violation = OnViolation(
        auto_repair=bool(ov_raw.get("auto_repair", False)),
        repair_task=ov_raw.get("repair_task"),
        repair_args=[str(a) for a in (ov_raw.get("repair_args") or [])],
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

    return Contract(
        id=str(pid),
        label=str(entry.get("label") or pid),
        source=str(raw.get("source") or "tushare"),
        storage=storage,
        window=window,
        sla=sla,
        on_violation=on_violation,
        alert=alert,
    )


__all__ = [
    "StorageSpec",
    "Window",
    "SLA",
    "OnViolation",
    "AlertSpec",
    "Contract",
    "parse_contract",
]
