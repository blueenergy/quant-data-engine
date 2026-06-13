"""CLI task to synchronize Tushare stk_nineturn records.

Usage:
  python -m quant_data_engine.tasks.run_nineturn_sync --days 1
  python -m quant_data_engine.tasks.run_nineturn_sync --start 20260601 --end 20260612
  python -m quant_data_engine.tasks.run_nineturn_sync --ts-code 000001.SZ --start 20260601 --end 20260612
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from quant_data_engine.adapters.stock_adapter import StockAdapter
from quant_data_engine.config.settings import get_settings
from quant_data_engine.pipelines.nineturn_pipeline import NineturnPipeline
from quant_data_engine.repositories.nineturn_repository import NineturnRepository
from quant_data_engine.utils.ingestion_job import record_ingestion_run


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Tushare stk_nineturn synchronization")
    date_group = parser.add_mutually_exclusive_group()
    date_group.add_argument("--start", type=str, help="Start date YYYYMMDD")
    date_group.add_argument("--auto", action="store_true", help="Auto-resume from last sync date")
    date_group.add_argument("--days", type=int, help="Sync recent N trading days")
    parser.add_argument("--end", type=str, help="End date YYYYMMDD (default: today)")
    parser.add_argument("--freq", default="daily", help="Tushare frequency, default daily")
    parser.add_argument("--ts-code", default="", help="Optional comma-separated ts_code list for by-stock sync")
    parser.add_argument("--dry-run", action="store_true", help="Show target range without writing")
    parser.add_argument("--verbose", action="store_true", help="Verbose output")
    return parser.parse_args()


def get_trading_days(adapter: StockAdapter, start_date: str, end_date: str) -> list[str]:
    df = adapter.fetch_trade_cal(start_date=start_date, end_date=end_date)
    if df is None or df.empty:
        return []
    days = df[df["is_open"] == 1]["cal_date"].astype(str).str.replace("-", "", regex=False).tolist()
    return sorted(days)


def _resolve_date_range(args: argparse.Namespace, repository: NineturnRepository) -> tuple[str, str]:
    today = datetime.today().strftime("%Y%m%d")
    end_date = args.end or today
    if args.auto:
        last_date = repository.get_last_sync_date(freq=args.freq)
        if last_date:
            start_date = (datetime.strptime(last_date, "%Y%m%d") + timedelta(days=1)).strftime("%Y%m%d")
            print(f"Auto-resume mode: last stk_nineturn {args.freq} sync date was {last_date}")
        else:
            start_date = (datetime.today() - timedelta(days=14)).strftime("%Y%m%d")
            print("No stk_nineturn sync state found, defaulting to 14 days lookback")
    elif args.days:
        start_date = (datetime.today() - timedelta(days=args.days * 2)).strftime("%Y%m%d")
    elif args.start:
        start_date = args.start
    else:
        start_date = (datetime.today() - timedelta(days=1)).strftime("%Y%m%d")
    return start_date, end_date


def main() -> None:
    args = parse_args()
    settings = get_settings()
    today = datetime.today().strftime("%Y%m%d")

    adapter = StockAdapter(settings.tushare_token, rate_limit_per_sec=settings.rate_limit_per_sec)
    repository = NineturnRepository(settings.mongo_uri, settings.data_db)
    pipeline = NineturnPipeline(adapter=adapter, repository=repository)

    start_date, end_date = _resolve_date_range(args, repository)
    if start_date > end_date:
        print(f"Invalid date range: {start_date} > {end_date}")
        record_ingestion_run(settings, "nineturn_sync", today, status="skipped", error="Invalid date range")
        return

    ts_codes = [item.strip() for item in args.ts_code.split(",") if item.strip()]
    if args.days and not ts_codes:
        lookback_start = start_date
        trading_days = get_trading_days(adapter, lookback_start, end_date)
        if not trading_days:
            print("No trading days found in lookback period")
            record_ingestion_run(settings, "nineturn_sync", end_date, status="skipped", error="No trading days")
            return
        trading_days = trading_days[-args.days:]
        start_date, end_date = trading_days[0], trading_days[-1]
    else:
        trading_days = get_trading_days(adapter, start_date, end_date)

    print("Nine Turn (stk_nineturn) synchronization")
    print(f"   Date range: {start_date} -> {end_date}")
    print(f"   Frequency: {args.freq}")
    print(f"   Mode: {'by-stock' if ts_codes else 'by-date'}")
    if ts_codes:
        print(f"   Stocks: {len(ts_codes)}")
    else:
        print(f"   Trading days: {len(trading_days)}")

    if args.dry_run:
        print("Dry-run mode: no writes")
        return

    try:
        if ts_codes:
            result = pipeline.run_by_stock(ts_codes, start_date, end_date, freq=args.freq)
        else:
            if not trading_days:
                print(f"No trading days found in range {start_date} -> {end_date}")
                record_ingestion_run(settings, "nineturn_sync", end_date, status="skipped", error="No trading days")
                return
            result = pipeline.run_by_date(trading_days, freq=args.freq)
    except Exception as exc:
        record_ingestion_run(settings, "nineturn_sync", end_date, status="failed", error=str(exc))
        raise

    print("Synchronization completed:")
    print(f"   Total records: {result.get('total_records', 0)}")
    print(f"   Issues: {result.get('total_issues', 0)}")
    if args.verbose and result.get("total_issues"):
        print(f"   Details: {result}")

    record_ingestion_run(
        settings,
        "nineturn_sync",
        end_date,
        records_changed=int(result.get("total_records") or 0),
        freq=args.freq,
        mode="by_stock" if ts_codes else "by_date",
    )


if __name__ == "__main__":
    main()
