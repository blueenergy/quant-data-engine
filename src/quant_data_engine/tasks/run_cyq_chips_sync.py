"""CLI task to synchronize Tushare cyq_chips for watchlist + position symbols.

The universe is dynamic: every ``finance.user_watchlists`` document's
``symbols`` array plus every distinct ``finance.trader_positions.symbol``,
filtered to A-share stocks (ETFs / HK / funds are skipped because Tushare
``cyq_chips`` does not cover them).

Usage:
  python -m quant_data_engine.tasks.run_cyq_chips_sync --days 10
  python -m quant_data_engine.tasks.run_cyq_chips_sync --start 20261101 --end 20261113
  python -m quant_data_engine.tasks.run_cyq_chips_sync --symbols 600114.SH,000338.SZ
"""
from __future__ import annotations

import argparse
import os
import re
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from pymongo import MongoClient

from quant_data_engine.adapters.stock_adapter import StockAdapter
from quant_data_engine.config.settings import get_settings
from quant_data_engine.pipelines.cyq_chips_pipeline import CyqChipsPipeline
from quant_data_engine.repositories.cyq_chips_repository import CyqChipsRepository
from quant_data_engine.utils.ingestion_job import record_ingestion_run

# Tushare's cyq_chips caps a single call at 6,000 rows; at ~110 bins/day
# 40 calendar days (~27 trading days) is comfortably below the cap.
MAX_SEGMENT_DAYS = 40

A_SHARE_PREFIXES = ("60", "68", "00", "30", "43", "83", "87", "92")
A_SHARE_PATTERN = re.compile(r"^(\d{6})\.(SH|SZ|BJ)$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Tushare cyq_chips synchronization")
    parser.add_argument(
        "--days",
        type=int,
        default=10,
        help="Lookback calendar days ending at --end (default: 10; the task is idempotent)",
    )
    parser.add_argument("--start", type=str, help="Start date YYYYMMDD (overrides --days)")
    parser.add_argument("--end", type=str, help="End date YYYYMMDD (default: today)")
    parser.add_argument(
        "--symbols",
        type=str,
        help="Comma-separated ts_codes (overrides the watchlist+positions universe)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Show universe and range")
    parser.add_argument("--verbose", action="store_true", help="Print per-symbol results")
    return parser.parse_args()


def _is_a_share(symbol: str) -> bool:
    text = str(symbol or "").strip().upper()
    match = A_SHARE_PATTERN.match(text)
    if not match:
        return False
    return match.group(1)[:2] in A_SHARE_PREFIXES


def resolve_universe(settings) -> list[str]:
    """Watchlist + position symbols, de-duplicated and A-share filtered."""
    client = MongoClient(settings.mongo_uri)
    db = client[settings.mongo_db]
    symbols: set[str] = set()
    for doc in db["user_watchlists"].find({}, {"symbols": 1}):
        for symbol in doc.get("symbols") or []:
            symbols.add(str(symbol))
    for symbol in db["trader_positions"].distinct("symbol"):
        symbols.add(str(symbol))
    return sorted(symbol for symbol in symbols if _is_a_share(symbol))


def _segments(start_date: str, end_date: str, max_days: int) -> list[tuple[str, str]]:
    start = datetime.strptime(start_date, "%Y%m%d")
    end = datetime.strptime(end_date, "%Y%m%d")
    out: list[tuple[str, str]] = []
    while start <= end:
        seg_end = min(start + timedelta(days=max_days - 1), end)
        out.append((start.strftime("%Y%m%d"), seg_end.strftime("%Y%m%d")))
        start = seg_end + timedelta(days=1)
    return out


def _resolve_range(args: argparse.Namespace) -> tuple[str, str]:
    end_date = args.end or datetime.today().strftime("%Y%m%d")
    if args.start:
        start_date = args.start
    else:
        start_date = (
            datetime.strptime(end_date, "%Y%m%d") - timedelta(days=max(args.days, 1))
        ).strftime("%Y%m%d")
    return start_date, end_date


def main() -> None:
    args = parse_args()
    settings = get_settings()

    if args.symbols:
        ts_codes = sorted(
            code.strip().upper()
            for code in args.symbols.split(",")
            if code.strip()
        )
        if not ts_codes:
            print("No valid symbols supplied")
            return
    else:
        ts_codes = resolve_universe(settings)

    start_date, end_date = _resolve_range(args)
    print("Chip distribution detail (cyq_chips) synchronization")
    print(f"   Universe: {len(ts_codes)} symbols")
    print(f"   Date range: {start_date} -> {end_date}")

    if not ts_codes:
        print("Universe is empty (no watchlist / position A-shares); skipping")
        record_ingestion_run(
            settings, "cyq_chips_sync", end_date, status="skipped", error="Empty universe"
        )
        return

    if args.dry_run:
        print(f"Dry-run: {ts_codes}")
        return

    adapter = StockAdapter(
        settings.tushare_token, rate_limit_per_sec=settings.rate_limit_per_sec
    )
    repository = CyqChipsRepository(settings.mongo_uri, settings.data_db)
    pipeline = CyqChipsPipeline(adapter=adapter, repository=repository)

    total_records = 0
    total_issues = 0
    failed_symbols: list[str] = []
    try:
        for seg_start, seg_end in _segments(start_date, end_date, MAX_SEGMENT_DAYS):
            result = pipeline.run_by_symbols(ts_codes, seg_start, seg_end)
            total_records += int(result.get("total_records") or 0)
            total_issues += int(result.get("total_issues") or 0)
            if args.verbose:
                for item in result.get("results") or []:
                    if item.issues:
                        failed_symbols.append(item.ts_code)
                        print(f"   [warn] {item.ts_code}: {item.issues}")
        repository.update_last_sync_date(end_date)
    except Exception as exc:
        record_ingestion_run(
            settings, "cyq_chips_sync", end_date, status="failed", error=str(exc)
        )
        raise

    print("Synchronization completed:")
    print(f"   Records changed: {total_records}")
    print(f"   Issues: {total_issues}")
    if failed_symbols:
        print(f"   Failed symbols: {failed_symbols[:20]}")

    record_ingestion_run(
        settings,
        "cyq_chips_sync",
        end_date,
        records_changed=total_records,
        mode="by_symbol",
        universe_size=len(ts_codes),
    )


if __name__ == "__main__":
    main()
