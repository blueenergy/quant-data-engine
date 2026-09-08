"""Background monitor: detect watchlist ``minute_bars`` gaps and trigger backfill.

Runs on Windows next to the quote Big QMT instance. Polls Mongo for universe
symbols below coverage thresholds, acquires a singleton lock, then backfills
gap symbols via quote ZMQ (``get_market_data_ex`` + ``download_history_data``).

Usage::

    # Loop every 30 minutes (default thresholds for 60m G/S warmup)
    python -m quant_data_engine.tasks.run_watchlist_minute_gap_monitor \\
        --quote-account-id 62129066 --outside-session-only

    # Single scan + backfill if needed, then exit
    python -m quant_data_engine.tasks.run_watchlist_minute_gap_monitor --once

    # Dry run: print gaps only
    python -m quant_data_engine.tasks.run_watchlist_minute_gap_monitor --once --dry-run
"""
from __future__ import annotations

import argparse
import os
import socket
import sys
import time
from datetime import datetime
from zoneinfo import ZoneInfo

from pymongo import MongoClient

from quant_data_engine.bridges.miniqmt.runner import CN_TZ, in_cn_stock_session
from quant_data_engine.config.settings import get_settings
from quant_data_engine.tasks.run_backfill_watchlist_minute_bars import (
    BackfillOptions,
    filter_gap_symbols,
    print_verify_report,
    record_job_status,
    resolve_date_range,
    resolve_symbols,
    run_backfill_download,
)
from quant_data_engine.tasks.watchlist_minute_backfill import (
    acquire_backfill_lock,
    release_backfill_lock,
)
from quant_data_engine.tasks.watchlist_minute_bigqmt import add_backfill_source_arguments


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--interval",
        type=int,
        default=1800,
        help="Seconds between scans when not using --once (default: 1800)",
    )
    parser.add_argument("--once", action="store_true", help="Run one scan/backfill cycle and exit")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Detect and print gaps only; do not download",
    )
    parser.add_argument(
        "--outside-session-only",
        action="store_true",
        help="Skip backfill while CN stock session is open (still scans)",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=50,
        help="Calendar days of history to download for gap symbols (default: 50)",
    )
    parser.add_argument(
        "--min-bars-per-symbol",
        type=int,
        default=6000,
        help="Gap threshold: minimum 1m bars (default: 6000 ≈ 25 sessions)",
    )
    parser.add_argument(
        "--min-trading-days",
        type=int,
        default=25,
        help="Gap threshold: minimum distinct trading days (default: 25 for 60m G/S)",
    )
    parser.add_argument(
        "--max-wait",
        type=int,
        default=120,
        help="Seconds to wait per symbol for history download (default: 120)",
    )
    add_backfill_source_arguments(parser)
    parser.add_argument(
        "--lock-ttl",
        type=int,
        default=7200,
        help="Singleton lock TTL seconds (default: 7200)",
    )
    parser.add_argument("--start", help="Optional fixed start date YYYYMMDD for backfill")
    parser.add_argument("--end", help="Optional fixed end date YYYYMMDD for backfill")
    return parser.parse_args()


def _holder_id() -> str:
    host = socket.gethostname()
    pid = os.getpid()
    return f"gap_monitor_{host}_{pid}"


def run_cycle(args: argparse.Namespace) -> int:
    settings = get_settings()
    client = MongoClient(settings.mongo_uri)
    holder = _holder_id()
    try:
        ns = argparse.Namespace(symbols=None)
        symbols = resolve_symbols(settings, client, ns)
        if not symbols:
            print("no symbols in watchlist universe")
            return 1

        target_db = settings.data_db
        collection = client[target_db]["minute_bars"]
        gap_symbols, report = filter_gap_symbols(
            collection,
            symbols,
            min_bars_per_symbol=args.min_bars_per_symbol,
            min_trading_days=args.min_trading_days,
        )
        print(
            f"scan: universe={len(symbols)} gaps={len(gap_symbols)} "
            f"(min_bars={args.min_bars_per_symbol}, min_days={args.min_trading_days})"
        )
        if gap_symbols:
            print("gap symbols:")
            for symbol in gap_symbols:
                print(symbol)
        else:
            print_verify_report(report)
            return 0

        if args.dry_run:
            print_verify_report(report)
            return 2

        if args.outside_session_only and in_cn_stock_session(datetime.now(CN_TZ)):
            print("skip backfill: CN stock session is open (--outside-session-only)")
            return 2

        if not acquire_backfill_lock(
            client,
            settings.mongo_db,
            holder,
            ttl_seconds=args.lock_ttl,
        ):
            print("skip backfill: another holder owns the minute backfill lock")
            return 2

        try:
            backfill_args = argparse.Namespace(
                start=args.start,
                end=args.end,
                lookback_days=args.lookback_days,
            )
            start, end = resolve_date_range(backfill_args)
            options = BackfillOptions(
                start=start,
                end=end,
                max_wait=args.max_wait,
                target_db=target_db,
                min_bars_per_symbol=args.min_bars_per_symbol,
                min_trading_days=args.min_trading_days,
                record_job_status=True,
                source=args.source,
                quote_rpc_address=args.quote_rpc_address,
                quote_account_id=args.quote_account_id,
                config_path=args.config,
                rpc_timeout_seconds=args.quote_rpc_timeout,
                chunk_days=args.chunk_days,
            )
            failed = run_backfill_download(
                collection,
                gap_symbols,
                settings=settings,
                options=options,
            )
            post_report = filter_gap_symbols(
                collection,
                symbols,
                min_bars_per_symbol=args.min_bars_per_symbol,
                min_trading_days=args.min_trading_days,
            )[1]
            print_verify_report(post_report)
            record_job_status(
                settings,
                status="complete" if post_report.passed else "partial",
                symbols=symbols,
                gap_symbols=gap_symbols,
                failed_symbols=failed,
                verify_passed=post_report.passed,
            )
            return 0 if post_report.passed else 2
        finally:
            release_backfill_lock(client, settings.mongo_db, holder)
    finally:
        client.close()


def main() -> int:
    args = parse_args()
    if args.once:
        return run_cycle(args)

    print(f"gap monitor started (interval={args.interval}s)")
    while True:
        started = datetime.now().isoformat(timespec="seconds")
        print(f"\n=== cycle {started} ===")
        try:
            exit_code = run_cycle(args)
            print(f"cycle finished exit={exit_code}")
        except KeyboardInterrupt:
            print("stopped")
            return 0
        except Exception as exc:
            print(f"cycle error: {exc}")
        time.sleep(max(60, args.interval))


if __name__ == "__main__":
    sys.exit(main())
