"""run_data_contract_reconcile.py — declarative data-contract reconciler.

Reads the desired-state ``contract`` block from data_pipelines_catalog.yaml,
compares it against actual coverage in MongoDB, writes a health snapshot to
``finance.data_contract_state`` (consumed by the data-pulse UI), and alerts on
SLA violations.

Default is DRY-RUN (judge + persist + alert, NO auto-repair). Pass --apply to
let the reconciler backfill missing data via the contract's repair_task.

Usage:
  # Phase 1: judge stock_daily, write state, alert — no backfill (safe)
  python -m quant_data_engine.tasks.run_data_contract_reconcile --contract stock_daily

  # Enable auto-repair (backfills gaps via run_stock_daily, then re-verifies)
  python -m quant_data_engine.tasks.run_data_contract_reconcile --contract stock_daily --apply

  # Suppress alerts (useful while validating thresholds)
  python -m quant_data_engine.tasks.run_data_contract_reconcile --contract stock_daily --no-alert

Exit codes: 0 = ok/repaired, 2 = violated/failed, 3 = unknown (no calendar),
4 = contract not found.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

import yaml

from quant_data_engine.config.settings import get_settings
from quant_data_engine.contracts.models import parse_contract
from quant_data_engine.contracts.reconciler import ContractReconciler

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
_log = logging.getLogger("contract_reconcile")

CATALOG_ENV_VAR = "DATA_PIPELINES_CATALOG"
# quant-data-engine root (…/tasks/ -> package -> src -> repo root)
_REPO_ROOT = Path(__file__).resolve().parents[3]


def _find_catalog_path(override: Optional[str]) -> Path:
    if override:
        return Path(override)
    env_path = os.getenv(CATALOG_ENV_VAR, "").strip()
    candidates = [
        Path(env_path) if env_path else None,
        _REPO_ROOT / "config" / "data_pipelines_catalog.yaml",
        _REPO_ROOT.parent / "quantFinance" / "config" / "data_pipelines_catalog.yaml",
    ]
    for c in candidates:
        if c and c.exists():
            return c
    raise FileNotFoundError("Cannot locate data_pipelines_catalog.yaml")


def _load_pipelines(path: Optional[str]) -> List[Dict[str, Any]]:
    p = _find_catalog_path(path)
    with open(p) as f:
        data = yaml.safe_load(f)
    pipelines = data.get("pipelines") if isinstance(data, dict) else data
    if isinstance(pipelines, dict):
        pipelines = list(pipelines.values())
    return [e for e in (pipelines or []) if isinstance(e, dict) and e.get("id")]


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Declarative data-contract reconciler")
    p.add_argument("--contract", default="stock_daily", help="Contract id (default: stock_daily)")
    p.add_argument("--catalog", help="Override catalog YAML path")
    p.add_argument("--apply", action="store_true",
                   help="Enable auto-repair (backfill). Default is dry-run judgment only.")
    p.add_argument("--no-alert", action="store_true", help="Do not send alerts")
    p.add_argument("--as-of", dest="as_of", help="Override business date YYYYMMDD (default: today Beijing)")
    p.add_argument("--json", action="store_true", help="Print result as JSON")
    return p.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    settings = get_settings()

    pipelines = _load_pipelines(args.catalog)
    entry = next((e for e in pipelines if e.get("id") == args.contract), None)
    if entry is None:
        _log.error("contract id %r not found in catalog", args.contract)
        return 4
    contract = parse_contract(entry)
    if contract is None:
        _log.error("pipeline %r has no `contract` block", args.contract)
        return 4

    reconciler = ContractReconciler(settings)
    result = reconciler.reconcile(
        contract,
        apply_repair=args.apply,
        allow_alert=not args.no_alert,
        as_of=args.as_of,
    )

    snapshot = result.to_snapshot(contract.label)
    if args.json:
        print(json.dumps(snapshot, ensure_ascii=False, indent=2, default=str))
    else:
        cov = result.coverage
        print(f"📋 Contract: {contract.id} ({contract.label})")
        print(f"   Mode: {'APPLY (auto-repair)' if args.apply else 'DRY-RUN (judge only)'}")
        print(f"   Status: {result.status}")
        if cov is not None:
            print(f"   Coverage: {cov.coverage_pct:.2f}%  "
                  f"({cov.present_days}/{cov.expected_days} trading days)")
            print(f"   Freshness: lag={cov.lag_trading_days}  "
                  f"latest_actual={cov.latest_actual}  latest_expected={cov.latest_expected}")
            if cov.missing_ranges:
                preview = ", ".join(f"{s}~{e}" for s, e in cov.missing_ranges[:5])
                more = "..." if len(cov.missing_ranges) > 5 else ""
                print(f"   Missing ranges ({len(cov.missing_ranges)}): {preview}{more}")
        if result.symbols_latest_day is not None:
            print(f"   Symbols latest day: {result.symbols_latest_day}")
        if "run_lag_days" in result.extra:
            print(f"   Run recency: lag={result.extra.get('run_lag_days')}d  "
                  f"last_run_date={result.extra.get('last_run_date')}  "
                  f"last_status={result.extra.get('last_run_status')}")
        if result.violations:
            print("   Violations:")
            for v in result.violations:
                print(f"     - {v}")
        if result.repair:
            print(f"   Repair: chunks={result.repair.get('chunks')} "
                  f"ok={result.repair.get('succeeded')} failed={result.repair.get('failed')}")
            for err in (result.repair.get("errors") or [])[:5]:
                print(f"     ! {err}")
        print(f"   Alerted: {result.alerted}")
        if result.note:
            print(f"   Note: {result.note}")

    if result.status in ("ok", "repaired"):
        return 0
    if result.status == "unknown":
        return 3
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
