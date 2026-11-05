"""CLI task to synchronize Tushare dragon-tiger / hot-money tables.

Collections written to ``quant_data``:

- ``top_list`` 龙虎榜每日明细
- ``top_inst`` 龙虎榜机构/营业部成交明细
- ``hm_list``  游资名录 (snapshot, no trade_date)

Usage:
  python -m quant_data_engine.tasks.run_dragon_tiger_sync --days 1
  python -m quant_data_engine.tasks.run_dragon_tiger_sync --type hm_list
  python -m quant_data_engine.tasks.run_dragon_tiger_sync --start 20150105 --end 20150131
  python -m quant_data_engine.tasks.run_dragon_tiger_sync --auto --type daily
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from quant_data_engine.adapters.stock_adapter import StockAdapter
from quant_data_engine.config.settings import get_settings
from quant_data_engine.pipelines.dragon_tiger_pipeline import DragonTigerPipeline
from quant_data_engine.repositories.dragon_tiger_repository import DragonTigerRepository
from quant_data_engine.utils.ingestion_job import record_ingestion_run

DAILY_KINDS = ("top_list", "top_inst")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Tushare dragon-tiger synchronization")
    date_group = parser.add_mutually_exclusive_group()
    date_group.add_argument("--start", type=str, help="Start date YYYYMMDD")
    date_group.add_argument("--auto", action="store_true", help="Auto-resume from last sync date")
    date_group.add_argument("--days", type=int, help="Sync recent N trading days")
    parser.add_argument("--end", type=str, help="End date YYYYMMDD (default: today)")
    parser.add_argument(
        "--type",
        default="all",
        choices=["all", "daily", "top_list", "top_inst", "hm_list"],
        help="Which table(s) to sync (default: all)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Show target range without writing")
    parser.add_argument("--verbose", action="store_true", help="Verbose output")
    return parser.parse_args()


def get_trading_days(adapter: StockAdapter, start_date: str, end_date: str) -> list[str]:
    df = adapter.fetch_trade_cal(start_date=start_date, end_date=end_date)
    if df is None or df.empty:
        return []
    days = df[df["is_open"] == 1]["cal_date"].astype(str).str.replace("-", "", regex=False).tolist()
    return sorted(days)


def _resolve_date_range(
    args: argparse.Namespace,
    repository: DragonTigerRepository,
) -> tuple[str, str]:
    today = datetime.today().strftime("%Y%m%d")
    end_date = args.end or today
    if args.auto:
        candidates = [
            date
            for date in (
                repository.get_last_sync_date(kind) for kind in DAILY_KINDS
            )
            if date
        ]
        if candidates:
            last_date = min(candidates)  # resume from the least-advanced table
            start_date = (datetime.strptime(last_date, "%Y%m%d") + timedelta(days=1)).strftime("%Y%m%d")
            print(f"Auto-resume mode: last dragon-tiger sync date was {last_date}")
        else:
            start_date = (datetime.today() - timedelta(days=14)).strftime("%Y%m%d")
            print("No dragon-tiger sync state found, defaulting to 14 days lookback")
    elif args.days:
        start_date = (datetime.today() - timedelta(days=args.days * 2)).strftime("%Y%m%d")
    elif args.start:
        start_date = args.start
    else:
        start_date = (datetime.today() - timedelta(days=1)).strftime("%Y%m%d")
    return start_date, end_date


def _record(settings, job_type: str, date_str: str, status: str, error=None, records=None) -> None:
    record_ingestion_run(
        settings,
        job_type,
        date_str,
        status=status,
        error=error,
        records_changed=records,
    )


def main() -> None:
    args = parse_args()
    settings = get_settings()
    today = datetime.today().strftime("%Y%m%d")

    adapter = StockAdapter(settings.tushare_token, rate_limit_per_sec=settings.rate_limit_per_sec)
    repository = DragonTigerRepository(settings.mongo_uri, settings.data_db)
    pipeline = DragonTigerPipeline(adapter=adapter, repository=repository)

    kinds = DAILY_KINDS if args.type in ("all", "daily") else ()
    run_hm = args.type in ("all", "hm_list")

    trading_days: list[str] = []
    if kinds:
        start_date, end_date = _resolve_date_range(args, repository)
        if start_date > end_date:
            print(f"Invalid date range: {start_date} > {end_date}")
            _record(settings, "dragon_tiger_sync", today, "skipped", "Invalid date range")
            return
        trading_days = get_trading_days(adapter, start_date, end_date)
        if args.days and trading_days:
            trading_days = trading_days[-args.days:]
        if not trading_days:
            print(f"No trading days found in range {start_date} -> {end_date}")
            _record(settings, "dragon_tiger_sync", end_date, "skipped", "No trading days")
            return
        start_date, end_date = trading_days[0], trading_days[-1]

    print("Dragon-tiger (龙虎榜) synchronization")
    print(f"   Type: {args.type}")
    if kinds:
        print(f"   Date range: {trading_days[0]} -> {trading_days[-1]}")
        print(f"   Trading days: {len(trading_days)}")
    if args.dry_run:
        print("Dry-run mode: no writes")
        return

    if kinds:
        try:
            summary = pipeline.run_by_date(trading_days, kinds=kinds)
        except Exception as exc:
            _record(settings, "dragon_tiger_sync", end_date, "failed", str(exc))
            raise
        records = int(summary["top_list_rows"]) + int(summary["top_inst_rows"])
        status = "complete" if not summary["failed_dates"] else "failed"
        error = None
        if summary["failed_dates"]:
            preview = "; ".join(summary["issues"][:5])
            error = f"{summary['failed_dates']}/{summary['total_dates']} dates failed: {preview}"
        _record(settings, "dragon_tiger_sync", end_date, status, error, records)
        print(
            f"   top_list rows={summary['top_list_rows']} "
            f"top_inst rows={summary['top_inst_rows']} "
            f"failed_dates={summary['failed_dates']}"
        )
        if summary["issues"]:
            print(f"   issues: {'; '.join(summary['issues'][:5])}")

    if run_hm:
        try:
            hm = pipeline.run_hm_list()
        except Exception as exc:
            _record(settings, "hm_list_sync", today, "failed", str(exc))
            raise
        if hm["issues"]:
            _record(settings, "hm_list_sync", today, "failed", "; ".join(hm["issues"]))
        else:
            _record(settings, "hm_list_sync", today, "complete", records=hm["total_records"])
        print(f"   hm_list rows={hm['total_records']}")

    print("Synchronization completed")


if __name__ == "__main__":
    main()
