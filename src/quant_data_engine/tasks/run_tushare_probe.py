"""run_tushare_probe.py — Tushare daily-data availability probe.

Polls multiple Tushare interfaces until ALL are confirmed available for today,
then writes pipeline_tasks(tushare_daily_ready, today, status=complete).
The dispatcher picks this up and fires all Tushare-dependent L0 pipelines.

Usage (cron, start right after market close):
    python -m quant_data_engine.tasks.run_tushare_probe

Options:
    --date YYYYMMDD   Override business date (default: today Beijing time)
    --interval N      Seconds between probe rounds (default: 300 = 5 min)
    --timeout N       Max seconds to wait before giving up (default: 21600 = 6 h)

Probes (each round, ALL must pass before gate opens):
    stock      pro.daily(ts_code=000001.SZ)        → stock_daily / stock_info gate
    index      pro.index_daily(ts_code=000300.SH)  → index_daily gate
    etf        pro.fund_daily(ts_code=510300.SH)   → etf_daily gate
    moneyflow  pro.moneyflow(ts_code=000001.SZ)    → moneyflow gate

financial_sync / earnings_sync are period-based (not day-aligned) so not probed;
they are triggered by tushare_daily_ready the same as others and run independently.

Each probe is a single well-known symbol + single-day query (minimal points cost).
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple

from quant_data_engine.adapters.tushare_adapter import TushareAdapter
from quant_data_engine.config.settings import get_settings
from quant_data_engine.utils.ingestion_job import record_ingestion_run

BEIJING = timezone(timedelta(hours=8))
PIPELINE_ID = "tushare_daily_ready"

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
_log = logging.getLogger("tushare_probe")


# ---------------------------------------------------------------------------
# Individual probe functions — return True if interface has today’s data
# ---------------------------------------------------------------------------

def _probe_stock(pro, date_str: str) -> bool:
    try:
        df = pro.daily(ts_code="000001.SZ", start_date=date_str, end_date=date_str)
        return df is not None and len(df) > 0
    except Exception as exc:
        _log.debug("stock probe error: %s", exc)
        return False


def _probe_index(pro, date_str: str) -> bool:
    try:
        df = pro.index_daily(ts_code="000300.SH", start_date=date_str, end_date=date_str)
        return df is not None and len(df) > 0
    except Exception as exc:
        _log.debug("index probe error: %s", exc)
        return False


def _probe_etf(pro, date_str: str) -> bool:
    try:
        df = pro.fund_daily(ts_code="510300.SH", start_date=date_str, end_date=date_str)
        return df is not None and len(df) > 0
    except Exception as exc:
        _log.debug("etf probe error: %s", exc)
        return False


def _probe_moneyflow(pro, date_str: str) -> bool:
    try:
        df = pro.moneyflow(ts_code="000001.SZ", start_date=date_str, end_date=date_str)
        return df is not None and len(df) > 0
    except Exception as exc:
        _log.debug("moneyflow probe error: %s", exc)
        return False


# ALL probes must pass before the gate opens
PROBES: List[Tuple[str, object]] = [
    ("stock",     _probe_stock),
    ("index",     _probe_index),
    ("etf",       _probe_etf),
    ("moneyflow", _probe_moneyflow),
]


def run_all_probes(adapter: TushareAdapter, date_str: str) -> Dict[str, bool]:
    """Run all probes with 1s gap; return {name: passed}."""
    results: Dict[str, bool] = {}
    pro = adapter._pro  # type: ignore[attr-defined]
    for name, fn in PROBES:
        results[name] = fn(pro, date_str)  # type: ignore[operator]
        time.sleep(1)
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Tushare multi-interface daily-data probe")
    parser.add_argument("--date", default=None, help="Business date YYYYMMDD (default: today BJ)")
    parser.add_argument("--interval", type=int, default=300, help="Seconds between rounds (default: 300)")
    parser.add_argument("--timeout", type=int, default=21600, help="Max wait seconds (default: 21600 = 6h)")
    args = parser.parse_args()

    settings = get_settings()
    date_str = args.date or datetime.now(BEIJING).strftime("%Y%m%d")

    _log.info(
        "Tushare multi-probe starting — date=%s probes=[%s] interval=%ds timeout=%ds",
        date_str, ", ".join(n for n, _ in PROBES), args.interval, args.timeout,
    )

    if not settings.tushare_token:
        _log.error("TUSHARE_TOKEN not set — cannot probe")
        record_ingestion_run(
            settings, PIPELINE_ID, date_str,
            status="failed", error="TUSHARE_TOKEN not configured",
        )
        sys.exit(1)

    adapter = TushareAdapter(settings.tushare_token, rate_limit_per_sec=1.0)

    deadline = time.monotonic() + args.timeout
    attempt = 0

    while time.monotonic() < deadline:
        attempt += 1
        _log.info("Round %d — probing %d interfaces …", attempt, len(PROBES))

        results = run_all_probes(adapter, date_str)
        passed = [n for n, ok in results.items() if ok]
        waiting = [n for n, ok in results.items() if not ok]

        if waiting:
            _log.info("  ✓ ready: %s", passed or "none")
            _log.info("  ⏳ waiting: %s", waiting)
        else:
            _log.info("  ✓ ALL probes passed: %s", passed)
            record_ingestion_run(
                settings, PIPELINE_ID, date_str,
                status="complete", records_changed=len(PROBES),
            )
            _log.info("pipeline_tasks(%s, %s) → complete", PIPELINE_ID, date_str)
            sys.exit(0)

        remaining = int(deadline - time.monotonic())
        if remaining <= 0:
            break
        wait = min(args.interval, remaining)
        _log.info("  Next round in %ds (deadline in %ds)", wait, remaining)
        time.sleep(wait)

    # Timeout
    results = run_all_probes(adapter, date_str)
    waiting = [n for n, ok in results.items() if not ok]
    err = f"Tushare probe timed out after {args.timeout}s ({attempt} rounds); still waiting: {waiting}"
    _log.error(err)
    record_ingestion_run(
        settings, PIPELINE_ID, date_str,
        status="failed", error=err[:2000],
    )
    sys.exit(1)


if __name__ == "__main__":
    main()
