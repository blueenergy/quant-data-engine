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

  # Reconcile EVERY contract in the catalog, dependency-ordered (judge only)
  python -m quant_data_engine.tasks.run_data_contract_reconcile --all

  # Cold-start bootstrap: backfill all contracts to spec, retry up to N passes.
  # Reference data (stock_info) is reconciled before datasets that need it.
  # On success a marker is written to data_contract_state/__bootstrap__.
  python -m quant_data_engine.tasks.run_data_contract_reconcile --bootstrap

  # O(1) deploy gate (no reconcile): exit 0 if bootstrapped, 10 if not.
  python -m quant_data_engine.tasks.run_data_contract_reconcile --check-bootstrap

Exit codes: 0 = ok/repaired, 2 = violated/failed, 3 = unknown (no calendar),
4 = contract not found, 10 = bootstrap marker absent (--check-bootstrap only).
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
from quant_data_engine.contracts.models import Contract, parse_contract
from quant_data_engine.contracts.reconciler import (
    ContractReconciler,
    ReconcileResult,
    topological_order,
)

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
    # Preferred single source of truth: the shared library's packaged catalog
    # (stock_data_access also honors the DATA_PIPELINES_CATALOG env override).
    try:
        from stock_data_access import catalog_path as _lib_catalog_path
    except ModuleNotFoundError:
        local_src = _REPO_ROOT / "data-access-lib" / "src"
        if local_src.is_dir() and str(local_src) not in sys.path:
            sys.path.insert(0, str(local_src))
        try:
            from stock_data_access import catalog_path as _lib_catalog_path
        except Exception:
            _lib_catalog_path = None
    except Exception:
        _lib_catalog_path = None

    if _lib_catalog_path is not None:
        try:
            return _lib_catalog_path()
        except FileNotFoundError:
            pass
        except Exception:
            pass

    env_path = os.getenv(CATALOG_ENV_VAR, "").strip()
    if env_path:
        p = Path(env_path)
        if p.exists():
            return p
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
    p.add_argument("--all", action="store_true", dest="run_all",
                   help="Reconcile every contract in the catalog (dependency-ordered)")
    p.add_argument("--bootstrap", action="store_true",
                   help="Cold-start: implies --all --apply, retries up to --max-passes")
    p.add_argument("--max-passes", type=int, default=1, dest="max_passes",
                   help="With --all/--bootstrap, repeat passes until all green (default: 1; bootstrap: 8)")
    p.add_argument("--check-bootstrap", action="store_true", dest="check_bootstrap",
                   help="O(1) gate: exit 0 if bootstrap marker present, 10 if absent (run nothing else)")
    p.add_argument("--catalog", help="Override catalog YAML path")
    p.add_argument("--apply", action="store_true",
                   help="Enable auto-repair (backfill). Default is dry-run judgment only.")
    p.add_argument("--no-alert", action="store_true", help="Do not send alerts")
    p.add_argument("--as-of", dest="as_of", help="Override business date YYYYMMDD (default: today Beijing)")
    p.add_argument("--json", action="store_true", help="Print result as JSON")
    return p.parse_args(argv)


def _print_result(contract: Contract, result: ReconcileResult, apply: bool) -> None:
    cov = result.coverage
    print(f"📋 Contract: {contract.id} ({contract.label})")
    print(f"   Mode: {'APPLY (auto-repair)' if apply else 'DRY-RUN (judge only)'}")
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
    if "doc_count" in result.extra:
        print(f"   Snapshot: docs={result.extra.get('doc_count')} "
              f"min={result.extra.get('min_docs')}")
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
    if result.consecutive_repairs:
        suffix = "" if apply else "  (dry run — not modified)"
        print(f"   Repair streak: {result.consecutive_repairs}{suffix}")
    print(f"   Alerted: {result.alerted}")
    if result.note:
        print(f"   Note: {result.note}")


def _exit_code_for(statuses: List[str]) -> int:
    if any(s in ("violated", "failed") for s in statuses):
        return 2
    if any(s == "unknown" for s in statuses):
        return 3
    return 0


def _run_all(
    reconciler: ContractReconciler,
    contracts: List[Contract],
    *,
    apply: bool,
    allow_alert: bool,
    as_of: Optional[str],
    max_passes: int,
    as_json: bool,
) -> int:
    ordered = topological_order(contracts)
    if not as_json:
        chain = " -> ".join(c.id for c in ordered)
        print(f"🔗 Reconcile order ({len(ordered)}): {chain}")
        print(f"   Mode: {'APPLY (auto-repair)' if apply else 'DRY-RUN (judge only)'}  "
              f"max_passes={max_passes}\n")

    results: Dict[str, ReconcileResult] = {}
    passes_run = 0
    for p in range(1, max_passes + 1):
        passes_run = p
        if not as_json and max_passes > 1:
            print(f"── Pass {p}/{max_passes} ──")
        for c in ordered:
            res = reconciler.reconcile(
                c, apply_repair=apply, allow_alert=allow_alert, as_of=as_of
            )
            results[c.id] = res
            if not as_json:
                _print_result(c, res, apply)
                print()
        if all(r.status in ("ok", "repaired") for r in results.values()):
            break

    statuses = [results[c.id].status for c in ordered]
    if as_json:
        payload = {
            "passes_run": passes_run,
            "results": {
                c.id: results[c.id].to_snapshot(c.label) for c in ordered
            },
        }
        print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
    else:
        print(f"📊 Summary (after {passes_run} pass{'es' if passes_run != 1 else ''}):")
        for c in ordered:
            r = results[c.id]
            print(f"   {r.status:9s} {c.id}")
    return _exit_code_for(statuses)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    settings = get_settings()

    # O(1) deploy gate: report whether a full bootstrap has ever been confirmed.
    # start.sh uses exit 10 (absent) to decide whether to run --bootstrap.
    if args.check_bootstrap:
        done = ContractReconciler(settings).state_repo.bootstrap_done()
        print(f"bootstrap_done={done}")
        return 0 if done else 10

    # --bootstrap is a cold-start convenience: reconcile everything with repair,
    # retrying a few passes so transient failures self-heal.
    if args.bootstrap:
        args.run_all = True
        args.apply = True
        if args.max_passes <= 1:
            args.max_passes = 8

    pipelines = _load_pipelines(args.catalog)

    if args.run_all:
        contracts = [c for c in (parse_contract(e) for e in pipelines) if c is not None]
        if not contracts:
            _log.error("no contracts found in catalog")
            return 4
        reconciler = ContractReconciler(settings)
        rc = _run_all(
            reconciler, contracts,
            apply=args.apply,
            allow_alert=not args.no_alert,
            as_of=args.as_of,
            max_passes=max(1, args.max_passes),
            as_json=args.json,
        )
        # Record the marker only when a bootstrap run confirms every contract
        # satisfied. Plain --all (judge) never claims the system is bootstrapped.
        if args.bootstrap and rc == 0:
            reconciler.state_repo.mark_bootstrap_done([c.id for c in contracts])
            if not args.json:
                print("✅ bootstrap marker recorded (data_contract_state/__bootstrap__)")
        return rc

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

    if args.json:
        print(json.dumps(result.to_snapshot(contract.label),
                         ensure_ascii=False, indent=2, default=str))
    else:
        _print_result(contract, result, args.apply)

    return _exit_code_for([result.status])


if __name__ == "__main__":
    raise SystemExit(main())
