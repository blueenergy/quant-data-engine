"""One-shot backfill: watchlist universe → miniQMT history → ``quant_data.minute_bars``.

Resolves symbols from Mongo (watchlists, strategies, portfolio plans, paper
positions), downloads minute history via miniQMT, and upserts directly into the
canonical ``minute_bars`` collection (default ``quant_data.minute_bars``).

Requires Windows + miniQMT with ``xtquant`` (9/18 deadline). After miniQMT
retires, use big QMT local 1m or a future RPC adapter.

Usage::

    python -m quant_data_engine.tasks.run_backfill_watchlist_minute_bars \\
        --start 20250701 --end 20250907

    python -m quant_data_engine.tasks.run_backfill_watchlist_minute_bars --dry-run
    python -m quant_data_engine.tasks.run_backfill_watchlist_minute_bars --verify-only
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime

from pymongo import MongoClient

from quant_data_engine.adapters.mini_qmt_adapter import MiniQMTAdapter
from quant_data_engine.config.settings import get_settings
from quant_data_engine.pipelines.mini_qmt_pipeline import MiniQMTPipeline
from quant_data_engine.repositories.minute_repository import MinuteRepository
from quant_data_engine.tasks.watchlist_minute_backfill import (
    default_date_range,
    resolve_watchlist_symbols,
    verify_minute_bars,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", help="Start date YYYYMMDD (default: today - 40 days)")
    parser.add_argument("--end", help="End date YYYYMMDD (default: today)")
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=40,
        help="When --start omitted, calendar days before --end (default: 40)",
    )
    parser.add_argument(
        "--period",
        default="1m",
        choices=["1m", "5m", "15m", "30m", "1h", "1d"],
        help="miniQMT history period (default: 1m)",
    )
    parser.add_argument(
        "--max-wait",
        type=int,
        default=120,
        help="Seconds to wait per symbol for miniQMT local data (default: 120)",
    )
    parser.add_argument(
        "--target-db",
        help="Mongo database for minute bars (default: DATA_DB_NAME / quant_data)",
    )
    parser.add_argument(
        "--target-collection",
        default="minute_bars",
        help="Target collection (default: minute_bars)",
    )
    parser.add_argument(
        "--min-bars-per-symbol",
        type=int,
        default=240,
        help="Verify threshold: minimum bars per symbol (default: 240 ≈ one session)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only resolve and print the watchlist universe; do not download",
    )
    parser.add_argument(
        "--verify-only",
        action="store_true",
        help="Skip download; verify existing target collection only",
    )
    parser.add_argument(
        "--no-verify",
        action="store_true",
        help="Skip post-download verification",
    )
    return parser.parse_args()


def _print_verify_report(report) -> None:
    print("symbol\tbars\t1970_bad\tmin_trade_date\tmax_trade_date\tok")
    for row in report.results:
        print(
            f"{row.symbol}\t{row.bar_count}\t{row.bad_1970_count}\t"
            f"{row.min_trade_date or '-'}\t{row.max_trade_date or '-'}\t{row.ok}"
        )
    if report.passed:
        print(f"verify: PASS (min_bars_per_symbol={report.min_bars_per_symbol})")
    else:
        empty = ", ".join(report.empty_symbols) or "(none empty, below min threshold)"
        print(
            f"verify: FAIL (min_bars_per_symbol={report.min_bars_per_symbol}; "
            f"empty={empty})"
        )


def main() -> int:
    args = parse_args()
    settings = get_settings()
    client = MongoClient(settings.mongo_uri)
    try:
        symbols = resolve_watchlist_symbols(settings, client)
        if not symbols:
            print("no symbols resolved from watchlist universe")
            return 1

        print(f"universe: {len(symbols)} symbols")
        for symbol in symbols:
            print(symbol)

        if args.dry_run:
            return 0

        target_db = args.target_db or settings.data_db
        collection = client[target_db][args.target_collection]

        if args.verify_only:
            report = verify_minute_bars(
                collection,
                symbols,
                min_bars_per_symbol=args.min_bars_per_symbol,
            )
            _print_verify_report(report)
            return 0 if report.passed else 2

        if args.end:
            end = args.end
            start = args.start or default_date_range(lookback_days=args.lookback_days)[0]
        elif args.start:
            start = args.start
            end = datetime.today().strftime("%Y%m%d")
        else:
            start, end = default_date_range(lookback_days=args.lookback_days)

        print(f"download: {len(symbols)} symbols {start}..{end} period={args.period}")
        print(f"target: {target_db}.{args.target_collection}")

        adapter = MiniQMTAdapter()
        pipeline = MiniQMTPipeline(
            adapter=adapter,
            repository=MinuteRepository(
                settings.mongo_uri,
                target_db,
                collection=args.target_collection,
            ),
        )

        failed: list[str] = []
        for index, symbol in enumerate(symbols, start=1):
            print(f"[{index}/{len(symbols)}] {symbol}")
            before = collection.count_documents({"symbol": symbol})
            pipeline.run(
                code=symbol,
                start_date=start,
                end_date=end,
                period=args.period,
                max_wait=args.max_wait,
            )
            after = collection.count_documents({"symbol": symbol})
            if after <= before:
                failed.append(symbol)
                print(f"  warning: no new bars for {symbol} (count {before} -> {after})")

        if failed:
            print(f"download warnings: {len(failed)} symbols with no new bars: {', '.join(failed)}")

        if args.no_verify:
            return 0

        report = verify_minute_bars(
            collection,
            symbols,
            min_bars_per_symbol=args.min_bars_per_symbol,
        )
        _print_verify_report(report)
        return 0 if report.passed else 2
    finally:
        client.close()


if __name__ == "__main__":
    sys.exit(main())
