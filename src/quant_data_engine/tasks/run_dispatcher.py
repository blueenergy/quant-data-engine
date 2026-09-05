"""run_dispatcher.py — Pipeline cascade dispatcher (dry-run safe).

Polls pipeline_tasks for the current business date and triggers any
catalog entry whose upstream depends_on are all complete AND whose
trigger_on_complete flag is True.

Usage (dry-run, just logs what would fire — no subprocess):
    python -m quant_data_engine.tasks.run_dispatcher --dry-run

Usage (live, actually calls daily_market_sync.sh <id>):
    python -m quant_data_engine.tasks.run_dispatcher

Options:
    --dry-run           Log intended triggers without executing (default: True)
    --date YYYYMMDD     Override business date (default: today Beijing time)
    --poll-interval N   Seconds between poll cycles (default: 60)
    --once              Run one poll cycle then exit (useful for testing)
    --catalog PATH      Override catalog YAML path

Design notes:
  - Reads data_pipelines_catalog.yaml for dependency graph and trigger flags
  - Queries finance.pipeline_tasks for today's status of each pipeline
  - Uses find_one_and_update with {status: "pending"} sentinel to atomically
    claim a dispatch slot, preventing double-fire if multiple instances run
  - Calls daily_market_sync.sh <shell_id> via subprocess in live mode
  - Each dispatched run is recorded back to pipeline_tasks with status=pending
    immediately; the actual task will overwrite it with complete/failed
  - Runs indefinitely until all trigger_on_complete pipelines complete or fail
    (exit on SIGINT / SIGTERM)
"""
from __future__ import annotations

import argparse
import logging
import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from pymongo import MongoClient

from quant_data_engine.config.settings import get_settings

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
_log = logging.getLogger("dispatcher")

# ── Constants ─────────────────────────────────────────────────────────────────
BEIJING = timezone(timedelta(hours=8))
DATA_COLLECTION_DOMAIN = "data_collection"
SCRIPT_DIR = Path(__file__).parent.parent.parent.parent  # quant-data-engine root (src/ layout)
CATALOG_ENV_VAR = "DATA_PIPELINES_CATALOG"
SYNC_SCRIPT_ENV_VAR = "DAILY_MARKET_SYNC_SCRIPT"


def _sync_script_candidates() -> List[Path]:
    override = os.getenv(SYNC_SCRIPT_ENV_VAR, "").strip()
    if override:
        return [Path(override)]
    return [
        SCRIPT_DIR / "daily_market_sync.sh",  # dev tree: repo root beside src/
        Path("/app/daily_market_sync.sh"),  # container image
    ]


def _resolve_sync_script() -> Path:
    """Locate daily_market_sync.sh in both the dev tree and an installed layout.

    Walking four levels up from __file__ only reaches the repo root while the
    package lives in `src/`. Once pip installs it the `src` level disappears and
    the same walk lands in `/usr/local/lib/python3.x`, so every dispatch died
    with exit 127 for six trading days while still reporting success.
    """
    candidates = _sync_script_candidates()
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    return candidates[0]


SYNC_SCRIPT = _resolve_sync_script()

CORE_DISPATCH_CATALOG: List[Dict[str, Any]] = [
    {
        "id": "stock_daily",
        "depends_on": ["tushare_daily_ready"],
        "trigger_on_complete": True,
    },
    {
        "id": "index_daily",
        "depends_on": ["tushare_daily_ready"],
        "trigger_on_complete": True,
    },
    {
        "id": "etf_daily",
        "depends_on": ["tushare_daily_ready"],
        "trigger_on_complete": True,
    },
    {
        "id": "fut_daily",
        "depends_on": ["tushare_daily_ready"],
        "trigger_on_complete": True,
    },
    {
        "id": "moneyflow",
        "depends_on": ["tushare_daily_ready"],
        "trigger_on_complete": True,
    },
    {
        "id": "market_sentiment",
        "depends_on": ["stock_daily"],
        "trigger_on_complete": True,
    },
]

# Map catalog pipeline id → shell argument for daily_market_sync.sh
# Only needed when catalog id differs from the case label in the shell script.
SHELL_ID_MAP: Dict[str, str] = {
    # L0 — Tushare-dependent (triggered after tushare_daily_ready)
    "stock_info":             "stockinfo",
    "stock_daily":            "stock",
    "index_daily":            "index",
    "index_constituents":     "index_constituents",
    "etf_daily":              "etf",
    "fut_daily":              "fut",
    "financial_sync":         "financial",
    "moneyflow":              "moneyflow",
    "nineturn_sync":          "nineturn",
    "earnings_sync":          "earnings",
    # L1 / L2 / L3
    "market_sentiment":               "sentiment",
    "theme_lag_candidates":           "theme_lag",
    "theme_lag_candidate_validation": "theme_lag",
    "risk_scan_a":                    "risk_scan_a",
    "stock_composite_scoring":        "stock_composite_scoring",
    "limit_up_sector_batch":          "limit_up_sector_batch",
    "hk_basic":                       "hk_basic",
    "hk_tradecal":                    "hk_tradecal",
    "hk_daily":                       "hk_daily",
    "us_basic":                       "us_basic",
    "us_tradecal":                    "us_tradecal",
}


# ── Catalog helpers ───────────────────────────────────────────────────────────
def _find_catalog_path(override: Optional[str] = None) -> Path:
    if override:
        return Path(override)
    # Preferred single source of truth: the shared library's packaged catalog
    # (stock_data_access also honors the DATA_PIPELINES_CATALOG env override).
    try:
        from stock_data_access import catalog_path as _lib_catalog_path
    except ModuleNotFoundError:
        local_src = SCRIPT_DIR / "data-access-lib" / "src"
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
    raise FileNotFoundError(
        "Cannot locate data_pipelines_catalog.yaml"
    )


def load_catalog(path: Optional[str] = None) -> List[Dict[str, Any]]:
    try:
        p = _find_catalog_path(path)
    except FileNotFoundError:
        if path:
            raise
        _log.warning(
            "Cannot locate data_pipelines_catalog.yaml; using built-in core dispatch catalog."
        )
        return [entry.copy() for entry in CORE_DISPATCH_CATALOG]
    with open(p) as f:
        data = yaml.safe_load(f)
    pipelines = data.get("pipelines") or data
    if isinstance(pipelines, dict):
        pipelines = list(pipelines.values())
    return [e for e in pipelines if isinstance(e, dict) and e.get("id")]


# ── MongoDB helpers ───────────────────────────────────────────────────────────
def _get_pipeline_statuses(coll, date_str: str) -> Dict[str, str]:
    """Return {task_type: status} for all data_collection docs on given date."""
    docs = coll.find(
        {"domain": DATA_COLLECTION_DOMAIN, "date": date_str},
        {"task_type": 1, "status": 1, "_id": 0},
    )
    return {d["task_type"]: d.get("status", "unknown") for d in docs}


def _claim_dispatch(coll, pipeline_id: str, date_str: str) -> bool:
    """Atomically insert a 'dispatching' sentinel; returns True if we won the race."""
    now = datetime.utcnow()
    result = coll.find_one_and_update(
        {
            "domain": DATA_COLLECTION_DOMAIN,
            "task_type": pipeline_id,
            "date": date_str,
            # Only claim if no doc exists yet (insert via upsert with this filter)
        },
        {
            "$setOnInsert": {
                "domain": DATA_COLLECTION_DOMAIN,
                "task_type": pipeline_id,
                "date": date_str,
                "status": "dispatching",
                "completed_at": now,
                "updated_at": now,
                "payload": {"triggered_by": "dispatcher"},
            }
        },
        upsert=True,
        return_document=False,  # returns original — None means it was inserted (we won)
    )
    return result is None  # None → doc didn't exist before → we inserted → we won


def _mark_dispatch_failed(coll, pipeline_id: str, date_str: str, error: str) -> None:
    now = datetime.utcnow()
    coll.update_one(
        {"domain": DATA_COLLECTION_DOMAIN, "task_type": pipeline_id, "date": date_str},
        {"$set": {"status": "failed", "error": error[:2000], "updated_at": now}},
    )


def _mark_stock_daily_realtime_skip(coll, date_str: str, reason: str, payload: Dict[str, Any]) -> None:
    """Mark stock_daily as dependency-complete while preserving the skip reason.

    Downstream pipelines depend on stock_daily being complete. A true `skipped`
    status would block them, so this writes status=complete with an explicit
    skipped_by_realtime_daily_k flag.
    """
    now = datetime.utcnow()
    coll.update_one(
        {"domain": DATA_COLLECTION_DOMAIN, "task_type": "stock_daily", "date": date_str},
        {
            "$set": {
                "domain": DATA_COLLECTION_DOMAIN,
                "task_type": "stock_daily",
                "date": date_str,
                "status": "complete",
                "completed_at": now,
                "updated_at": now,
                "payload": {
                    **payload,
                    "skipped_by_realtime_daily_k": True,
                    "skip_reason": reason,
                    "triggered_by": "dispatcher",
                },
            }
        },
        upsert=True,
    )
    coll.database["job_status"].replace_one(
        {"job_type": "stock_daily", "date": date_str},
        {
            "job_type": "stock_daily",
            "date": date_str,
            "status": "complete",
            "completed_at": now,
            "updated_at": now,
            "records_changed": 0,
            "skipped_by_realtime_daily_k": True,
            "skip_reason": reason,
            **payload,
        },
        upsert=True,
    )


def _recent_realtime_equivalence_passed(coll, date_str: str, required_days: int) -> bool:
    if required_days <= 0:
        return True
    docs = list(
        coll.find(
            {
                "domain": DATA_COLLECTION_DOMAIN,
                "task_type": "a_daily_realtime_eod_compare",
                "date": {"$lt": date_str},
                "status": "complete",
                "payload.equivalence_passed": True,
            },
            {"date": 1, "_id": 0},
        )
        .sort("date", -1)
        .limit(required_days)
    )
    return len(docs) >= required_days


def _should_skip_stock_daily_with_realtime(coll, date_str: str, settings: Any) -> tuple[bool, str, Dict[str, Any]]:
    if not getattr(settings, "enable_realtime_daily_k", False):
        return False, "ENABLE_REALTIME_DAILY_K is false", {}
    if getattr(settings, "realtime_daily_k_shadow_mode", True):
        return False, "REALTIME_DAILY_K_SHADOW_MODE is true", {}
    if not getattr(settings, "realtime_daily_k_skip_stock_daily", False):
        return False, "REALTIME_DAILY_K_SKIP_STOCK_DAILY is false", {}

    doc = coll.find_one(
        {
            "domain": DATA_COLLECTION_DOMAIN,
            "task_type": "a_daily_realtime_sync",
            "date": date_str,
        },
        {"_id": 0},
    )
    if not doc:
        return False, "no a_daily_realtime_sync status for date", {}
    payload = doc.get("payload") or {}
    if doc.get("status") != "complete" or not payload.get("quality_passed"):
        return False, "realtime daily K not complete/quality_passed", payload

    required_days = int(getattr(settings, "realtime_daily_k_equivalence_days", 3) or 0)
    if not _recent_realtime_equivalence_passed(coll, date_str, required_days):
        return False, f"recent equivalence checks < {required_days}", payload

    return True, "realtime daily K complete, healthy, and equivalence gate passed", payload


# ── Dispatch logic ────────────────────────────────────────────────────────────
def _all_deps_complete(statuses: Dict[str, str], deps: List[str]) -> bool:
    return all(statuses.get(d) == "complete" for d in deps)


def _any_dep_failed(statuses: Dict[str, str], deps: List[str]) -> bool:
    return any(statuses.get(d) == "failed" for d in deps)


def _is_terminal(status: Optional[str]) -> bool:
    return status in ("complete", "failed", "skipped", "dispatching")


def dispatch_once(
    coll,
    catalog: List[Dict[str, Any]],
    date_str: str,
    dry_run: bool,
    settings: Any = None,
) -> Dict[str, int]:
    """One poll cycle. Returns counts of {fired, failed, skipped, waiting, already_done}."""
    statuses = _get_pipeline_statuses(coll, date_str)
    counters = {"fired": 0, "failed": 0, "skipped": 0, "waiting": 0, "already_done": 0}

    for entry in catalog:
        pid = entry.get("id", "")
        if not entry.get("trigger_on_complete", False):
            continue  # cron-managed, not our job

        deps: List[str] = list(entry.get("depends_on") or [])
        current_status = statuses.get(pid)

        if _is_terminal(current_status):
            counters["already_done"] += 1
            continue

        if _any_dep_failed(statuses, deps):
            _log.warning(
                "[%s] SKIP — upstream dep failed: %s",
                pid,
                [d for d in deps if statuses.get(d) == "failed"],
            )
            counters["skipped"] += 1
            continue

        if not _all_deps_complete(statuses, deps):
            missing = [d for d in deps if statuses.get(d) != "complete"]
            _log.info("[%s] WAITING — deps not complete yet: %s", pid, missing)
            counters["waiting"] += 1
            continue

        if pid == "stock_daily" and settings is not None:
            should_skip, reason, payload = _should_skip_stock_daily_with_realtime(coll, date_str, settings)
            if should_skip:
                if dry_run:
                    _log.info("[DRY-RUN] would mark stock_daily complete via realtime daily K: %s", reason)
                else:
                    _mark_stock_daily_realtime_skip(coll, date_str, reason, payload)
                    _log.info("[stock_daily] completed via realtime daily K skip: %s", reason)
                counters["skipped"] += 1
                continue
            if getattr(settings, "enable_realtime_daily_k", False):
                _log.info("[stock_daily] realtime daily K skip not active: %s", reason)

        # All deps complete — time to fire
        shell_arg = SHELL_ID_MAP.get(pid, pid)
        if dry_run:
            _log.info(
                "[DRY-RUN] would fire: daily_market_sync.sh %s  (deps: %s)",
                shell_arg,
                deps,
            )
            counters["fired"] += 1
            continue

        # Live mode: claim the slot first
        won = _claim_dispatch(coll, pid, date_str)
        if not won:
            _log.info("[%s] already claimed by another dispatcher instance, skipping", pid)
            counters["already_done"] += 1
            continue

        _log.info("[%s] FIRING: %s %s", pid, SYNC_SCRIPT, shell_arg)
        try:
            result = subprocess.run(
                ["bash", str(SYNC_SCRIPT), shell_arg],
                timeout=3600,
                capture_output=False,
            )
            if result.returncode != 0:
                raise RuntimeError(f"exit code {result.returncode}")
            _log.info("[%s] completed successfully", pid)
            counters["fired"] += 1
        except Exception as exc:
            err = str(exc)
            # Counted separately from `skipped`: a task that blew up is not the
            # same as one we deliberately did not run, and only the former
            # should turn into a non-zero exit for the cron wrapper.
            _log.error("[%s] FAILED: %s", pid, err)
            _mark_dispatch_failed(coll, pid, date_str, err)
            counters["failed"] += 1

    return counters


def _all_settled(
    coll, catalog: List[Dict[str, Any]], date_str: str
) -> bool:
    """True when every trigger_on_complete pipeline is in a terminal state."""
    statuses = _get_pipeline_statuses(coll, date_str)
    for entry in catalog:
        if not entry.get("trigger_on_complete", False):
            continue
        if not _is_terminal(statuses.get(entry["id"])):
            return False
    return True


# ── Main ──────────────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(description="Pipeline cascade dispatcher")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=True,
        help="Log intended triggers without executing (default: True)",
    )
    parser.add_argument(
        "--live",
        action="store_true",
        default=False,
        help="Disable dry-run and actually execute dispatched tasks",
    )
    parser.add_argument("--date", default=None, help="Override business date YYYYMMDD")
    parser.add_argument(
        "--poll-interval",
        type=int,
        default=60,
        help="Seconds between poll cycles (default: 60)",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        default=False,
        help="Run one poll cycle then exit",
    )
    parser.add_argument("--catalog", default=None, help="Override catalog YAML path")
    args = parser.parse_args()

    dry_run = not args.live  # --live disables dry-run

    # Fail on start rather than once per fired pipeline: a missing script used to
    # surface only as `exit code 127` inside a cycle that still exited 0.
    if not dry_run and not SYNC_SCRIPT.is_file():
        _log.error(
            "daily_market_sync.sh not found at %s (looked in: %s). "
            "Set %s to its absolute path.",
            SYNC_SCRIPT,
            ", ".join(str(c) for c in _sync_script_candidates()),
            SYNC_SCRIPT_ENV_VAR,
        )
        sys.exit(1)

    settings = get_settings()
    client = MongoClient(settings.mongo_uri)
    coll = client[settings.mongo_db]["pipeline_tasks"]

    catalog = load_catalog(args.catalog)
    trigger_entries = [e for e in catalog if e.get("trigger_on_complete")]

    if not trigger_entries:
        _log.warning("No pipelines with trigger_on_complete=true found in catalog — nothing to dispatch.")
        return

    date_str = args.date or datetime.now(BEIJING).strftime("%Y%m%d")
    mode_label = "DRY-RUN" if dry_run else "LIVE"
    _log.info(
        "Dispatcher starting [%s] — date=%s, %d trigger-eligible pipelines, poll_interval=%ds",
        mode_label, date_str, len(trigger_entries), args.poll_interval,
    )
    for e in trigger_entries:
        _log.info("  trigger: %-40s  depends_on: %s", e["id"], e.get("depends_on", []))

    # Graceful shutdown on SIGINT / SIGTERM
    _stop = False
    def _handle_signal(sig, frame):
        nonlocal _stop
        _log.info("Received signal %s, shutting down after current cycle…", sig)
        _stop = True
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    total_failed = 0

    while not _stop:
        try:
            counts = dispatch_once(coll, catalog, date_str, dry_run, settings=settings)
            total_failed += counts["failed"]
            _log.info(
                "Cycle done — fired=%d failed=%d waiting=%d skipped=%d already_done=%d",
                counts["fired"], counts["failed"], counts["waiting"],
                counts["skipped"], counts["already_done"],
            )
            if args.once:
                break
            if not dry_run and _all_settled(coll, catalog, date_str):
                _log.info("All trigger_on_complete pipelines settled for %s — dispatcher exiting.", date_str)
                break
        except Exception as exc:
            total_failed += 1
            _log.exception("Unexpected error in poll cycle: %s", exc)

        if _stop:
            break
        time.sleep(args.poll_interval)

    client.close()
    _log.info("Dispatcher stopped.")

    # The cron wrapper only sees the exit code, so a cycle that failed to
    # dispatch must not look like a clean run.
    if total_failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
