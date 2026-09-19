"""One-shot backfill: watchlist universe → quote Big QMT → ``quant_data.minute_bars``.

Resolves symbols from Mongo (watchlists, strategies, portfolio plans, paper
positions), reads 1m via quote ZMQ ``get_market_data_ex``, downloads missing
history with ``download_history_data``, and upserts into ``minute_bars``.

Default ``--source bigqmt_rpc`` (path B timestamps). ``--source miniqmt`` is the
pre-9/18 local ``xtquant`` path.

Usage::

    python -m quant_data_engine.tasks.run_backfill_watchlist_minute_bars \\
        --start 20250701 --end 20250907 --quote-account-id 62129066

    python -m quant_data_engine.tasks.run_backfill_watchlist_minute_bars --dry-run
    python -m quant_data_engine.tasks.run_backfill_watchlist_minute_bars --verify-only
    python -m quant_data_engine.tasks.run_backfill_watchlist_minute_bars \\
        --only-gap --min-bars-per-symbol 6000 --min-trading-days 25
"""
from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from datetime import datetime
from typing import Sequence

from pymongo import MongoClient
from pymongo.collection import Collection

from quant_data_engine.config.settings import Settings, get_settings
from quant_data_engine.repositories.job_status_repository import JobStatusRepository
from quant_data_engine.tasks.watchlist_minute_backfill import (
    JOB_TYPE_WATCHLIST_MINUTE_BACKFILL,
    BackfillVerifyReport,
    default_date_range,
    resolve_watchlist_symbols,
    verify_minute_bars,
)
from quant_data_engine.tasks.watchlist_minute_bigqmt import (
    SOURCE_BIGQMT_RPC,
    SOURCE_MINIQMT,
    add_backfill_source_arguments,
    run_bigqmt_backfill_download,
)


@dataclass(frozen=True)
class BackfillOptions:
    start: str
    end: str
    period: str = "1m"
    max_wait: int = 120
    target_db: str = "quant_data"
    target_collection: str = "minute_bars"
    min_bars_per_symbol: int = 240
    min_trading_days: int = 0
    record_job_status: bool = False
    source: str = SOURCE_BIGQMT_RPC
    quote_rpc_address: str | None = None
    quote_account_id: str | None = None
    config_path: str | None = None
    rpc_timeout_seconds: float | None = None
    chunk_days: int = 5


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
        help="History period (default: 1m)",
    )
    parser.add_argument(
        "--max-wait",
        type=int,
        default=120,
        help="Seconds to wait per symbol for history download (default: 120)",
    )
    add_backfill_source_arguments(parser)
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
        "--min-trading-days",
        type=int,
        default=0,
        help="Verify threshold: minimum distinct trading days (0=disabled; 25 for 60m G/S)",
    )
    parser.add_argument(
        "--symbols",
        nargs="+",
        help="Explicit symbol list (default: resolve watchlist universe from Mongo)",
    )
    parser.add_argument(
        "--only-gap",
        action="store_true",
        help="Download only symbols that fail coverage verification",
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
    parser.add_argument(
        "--record-job-status",
        action="store_true",
        help="Write run summary to finance.job_status",
    )
    return parser.parse_args()


def resolve_date_range(args: argparse.Namespace) -> tuple[str, str]:
    if args.end:
        end = args.end
        start = args.start or default_date_range(lookback_days=args.lookback_days)[0]
    elif args.start:
        start = args.start
        end = datetime.today().strftime("%Y%m%d")
    else:
        start, end = default_date_range(lookback_days=args.lookback_days)
    return start, end


def resolve_symbols(settings: Settings, mongo_client: MongoClient, args: argparse.Namespace) -> list[str]:
    if args.symbols:
        return sorted({str(symbol).strip() for symbol in args.symbols if str(symbol).strip()})
    return resolve_watchlist_symbols(settings, mongo_client)


def filter_gap_symbols(
    collection: Collection,
    symbols: Sequence[str],
    *,
    min_bars_per_symbol: int,
    min_trading_days: int,
) -> tuple[list[str], BackfillVerifyReport]:
    report = verify_minute_bars(
        collection,
        symbols,
        min_bars_per_symbol=min_bars_per_symbol,
        min_trading_days=min_trading_days,
    )
    return list(report.gap_symbols), report


def print_verify_report(report: BackfillVerifyReport) -> None:
    print("symbol\tbars\tdays\t1970_bad\tfiller_days\tmin_trade_date\tmax_trade_date\tok")
    for row in report.results:
        ok = row.meets_criteria(
            min_bars_per_symbol=report.min_bars_per_symbol,
            min_trading_days=report.min_trading_days,
        )
        print(
            f"{row.symbol}\t{row.bar_count}\t{row.trading_days}\t{row.bad_1970_count}\t"
            f"{row.filler_days}\t"
            f"{row.min_trade_date or '-'}\t{row.max_trade_date or '-'}\t{ok}"
        )
    if report.passed:
        print(
            f"verify: PASS (min_bars_per_symbol={report.min_bars_per_symbol}; "
            f"min_trading_days={report.min_trading_days})"
        )
    else:
        gaps = ", ".join(report.gap_symbols) or "(none)"
        print(
            f"verify: FAIL (min_bars_per_symbol={report.min_bars_per_symbol}; "
            f"min_trading_days={report.min_trading_days}; gaps={gaps})"
        )


def run_backfill_download(
    collection: Collection,
    symbols: Sequence[str],
    *,
    settings: Settings,
    options: BackfillOptions,
    rpc_client=None,
) -> list[str]:
    """Download and upsert minute bars for ``symbols``. Returns symbols with no new bars."""
    if not symbols:
        return []

    print(
        f"download: {len(symbols)} symbols {options.start}..{options.end} "
        f"period={options.period} source={options.source}"
    )
    print(f"target: {options.target_db}.{options.target_collection}")

    source = (options.source or SOURCE_BIGQMT_RPC).strip().lower()
    if source == SOURCE_MINIQMT:
        failed = _run_miniqmt_backfill_download(collection, symbols, settings=settings, options=options)
    elif source == SOURCE_BIGQMT_RPC:
        failed = run_bigqmt_backfill_download(
            collection,
            symbols,
            settings=settings,
            start=options.start,
            end=options.end,
            period=options.period,
            target_db=options.target_db,
            target_collection=options.target_collection,
            rpc_timeout_seconds=options.rpc_timeout_seconds,
            download_timeout_seconds=float(options.max_wait),
            chunk_days=options.chunk_days,
            quote_rpc_address=options.quote_rpc_address,
            quote_account_id=options.quote_account_id,
            config_path=options.config_path,
            rpc_client=rpc_client,
        )
    else:
        raise ValueError(f"unsupported backfill source: {options.source}")

    if failed:
        print(f"download warnings: {len(failed)} symbols with no new bars: {', '.join(failed)}")
    return failed


def _run_miniqmt_backfill_download(
    collection: Collection,
    symbols: Sequence[str],
    *,
    settings: Settings,
    options: BackfillOptions,
) -> list[str]:
    from quant_data_engine.adapters.mini_qmt_adapter import MiniQMTAdapter
    from quant_data_engine.pipelines.mini_qmt_pipeline import MiniQMTPipeline
    from quant_data_engine.repositories.minute_repository import MinuteRepository

    adapter = MiniQMTAdapter()
    pipeline = MiniQMTPipeline(
        adapter=adapter,
        repository=MinuteRepository(
            settings.mongo_uri,
            options.target_db,
            collection=options.target_collection,
        ),
    )
    failed: list[str] = []
    for index, symbol in enumerate(symbols, start=1):
        print(f"[{index}/{len(symbols)}] {symbol}")
        before = collection.count_documents({"symbol": symbol})
        pipeline.run(
            code=symbol,
            start_date=options.start,
            end_date=options.end,
            period=options.period,
            max_wait=options.max_wait,
        )
        after = collection.count_documents({"symbol": symbol})
        if after <= before:
            failed.append(symbol)
            print(f"  warning: no new bars for {symbol} (count {before} -> {after})")
    return failed


def record_job_status(
    settings: Settings,
    *,
    status: str,
    symbols: Sequence[str],
    gap_symbols: Sequence[str],
    failed_symbols: Sequence[str],
    verify_passed: bool | None,
    error: str | None = None,
) -> None:
    repo = JobStatusRepository(settings.mongo_uri, settings.mongo_db)
    today = datetime.today().strftime("%Y%m%d")
    repo.record(
        JOB_TYPE_WATCHLIST_MINUTE_BACKFILL,
        today,
        {
            "status": status,
            "symbol_count": len(symbols),
            "gap_count": len(gap_symbols),
            "gap_symbols": list(gap_symbols)[:200],
            "failed_symbols": list(failed_symbols)[:200],
            "verify_passed": verify_passed,
        },
        status=status,
        error=error,
        sync_uri=settings.job_status_sync_uri,
    )


def main() -> int:
    args = parse_args()
    settings = get_settings()
    client = MongoClient(settings.mongo_uri)
    try:
        symbols = resolve_symbols(settings, client, args)
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
                min_trading_days=args.min_trading_days,
            )
            print_verify_report(report)
            return 0 if report.passed else 2

        download_symbols = list(symbols)
        pre_report: BackfillVerifyReport | None = None
        if args.only_gap:
            download_symbols, pre_report = filter_gap_symbols(
                collection,
                symbols,
                min_bars_per_symbol=args.min_bars_per_symbol,
                min_trading_days=args.min_trading_days,
            )
            print(f"only-gap: {len(download_symbols)} symbols need backfill")
            if not download_symbols:
                print("no gaps; skipping download")
                if pre_report is not None:
                    print_verify_report(pre_report)
                return 0

        start, end = resolve_date_range(args)
        options = BackfillOptions(
            start=start,
            end=end,
            period=args.period,
            max_wait=args.max_wait,
            target_db=target_db,
            target_collection=args.target_collection,
            min_bars_per_symbol=args.min_bars_per_symbol,
            min_trading_days=args.min_trading_days,
            record_job_status=args.record_job_status,
            source=args.source,
            quote_rpc_address=args.quote_rpc_address,
            quote_account_id=args.quote_account_id,
            config_path=args.config,
            rpc_timeout_seconds=args.quote_rpc_timeout,
            chunk_days=args.chunk_days,
        )
        failed = run_backfill_download(collection, download_symbols, settings=settings, options=options)

        if args.no_verify:
            if args.record_job_status:
                record_job_status(
                    settings,
                    status="complete",
                    symbols=symbols,
                    gap_symbols=download_symbols,
                    failed_symbols=failed,
                    verify_passed=None,
                )
            return 0

        report = verify_minute_bars(
            collection,
            symbols,
            min_bars_per_symbol=args.min_bars_per_symbol,
            min_trading_days=args.min_trading_days,
        )
        print_verify_report(report)

        if args.record_job_status:
            record_job_status(
                settings,
                status="complete" if report.passed else "partial",
                symbols=symbols,
                gap_symbols=report.gap_symbols,
                failed_symbols=failed,
                verify_passed=report.passed,
            )
        return 0 if report.passed else 2
    finally:
        client.close()


if __name__ == "__main__":
    sys.exit(main())
