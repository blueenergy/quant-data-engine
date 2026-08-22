"""CLI task to sync Tushare stock adj_factor into Mongo ``stock_adj_factor``.

This task intentionally keeps ``volume_price`` as raw, unadjusted prices and
stores adjustment factors separately. Downstream scoring/backtests can compute
hfq/qfq prices at read time.
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from quant_data_engine.adapters.stock_adapter import StockAdapter
from quant_data_engine.config.settings import get_settings
from quant_data_engine.pipelines.stock_daily_pipeline import StockDailyPipeline
from quant_data_engine.repositories.stock_repository import StockRepository
from quant_data_engine.revision_ledger import STOCK_ADJ_FACTOR_SOURCE, revision_ledger_job
from quant_data_engine.utils.ingestion_job import record_ingestion_run


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Sync Tushare stock adj_factor into Mongo stock_adj_factor"
    )
    p.add_argument(
        "--mode",
        choices=["by-date", "by-stock"],
        default="by-date",
        help="Collection mode: by-date for all-market snapshots, by-stock for symbol ranges",
    )
    p.add_argument("--codes", nargs="+", help="Stock ts_codes, e.g. 000001.SZ 000002.SZ")
    p.add_argument("--all", action="store_true", help="Collect all listed stocks in by-stock mode")

    date_group = p.add_mutually_exclusive_group()
    date_group.add_argument("--start", type=str, help="Start date YYYYMMDD")
    date_group.add_argument("--days", type=int, help="Number of recent trading days to sync")
    p.add_argument("--end", type=str, help="End date YYYYMMDD (default: today)")

    p.add_argument("--dry-run", action="store_true", help="Show what would be collected without writing")
    p.add_argument("--verbose", action="store_true", help="Verbose output")
    return p.parse_args()


def get_trading_days(adapter: StockAdapter, start_date: str, end_date: str) -> list[str]:
    df = adapter.fetch_trade_cal(start_date=start_date, end_date=end_date)
    if df is None or df.empty:
        return []
    days = df[df["is_open"] == 1]["cal_date"].str.replace("-", "").tolist()
    return sorted(days)


def resolve_date_range(adapter: StockAdapter, args: argparse.Namespace) -> tuple[str, str]:
    today = datetime.today().strftime("%Y%m%d")
    end_date = args.end or today
    if args.days:
        lookback_start = (datetime.today() - timedelta(days=args.days * 2)).strftime("%Y%m%d")
        days = get_trading_days(adapter, lookback_start, end_date)
        if not days:
            raise RuntimeError("No trading days found in lookback period")
        tail = days[-args.days :]
        return tail[0], tail[-1]
    if args.start:
        return args.start, end_date
    return (datetime.today() - timedelta(days=1)).strftime("%Y%m%d"), end_date


def main() -> int:
    args = parse_args()
    settings = get_settings()
    adapter = StockAdapter(settings.tushare_token, rate_limit_per_sec=settings.rate_limit_per_sec)

    try:
        start_date, end_date = resolve_date_range(adapter, args)
    except Exception as exc:
        print(f"❌ {exc}")
        record_ingestion_run(
            settings,
            "stock_adj_factor",
            datetime.today().strftime("%Y%m%d"),
            status="skipped",
            error=str(exc),
        )
        return 1

    print("🟡 Stock Adjustment Factor Sync")
    print(f"   Mode: {args.mode}")
    print(f"   Date range: {start_date} -> {end_date}")

    if args.mode == "by-date":
        trading_days = get_trading_days(adapter, start_date, end_date)
        if not trading_days:
            print("⚠️  No trading days found in range")
            record_ingestion_run(
                settings,
                "stock_adj_factor",
                end_date,
                status="skipped",
                error="No trading days in range",
            )
            return 0
        print(f"   Trading days: {len(trading_days)}")
        if args.dry_run:
            print(f"💡 Dry-run: would collect adj_factor for {len(trading_days)} trading days")
            print(f"   Days: {', '.join(trading_days[:5])}{'...' if len(trading_days) > 5 else ''}")
            return 0

        repository = StockRepository(settings.mongo_uri, settings.data_db)
        pipeline = StockDailyPipeline(adapter=adapter, repository=repository)
        try:
            with revision_ledger_job(
                STOCK_ADJ_FACTOR_SOURCE,
                "stock_adj_factor_sync",
                settings.mongo_uri,
            ):
                result = pipeline.run_by_date(
                    trading_days,
                    include_daily=False,
                    include_basic=False,
                    include_adj_factor=True,
                    progress=True,
                )
        except Exception as exc:
            record_ingestion_run(settings, "stock_adj_factor", end_date, status="failed", error=str(exc))
            raise

    else:
        if args.codes:
            ts_codes = args.codes
        elif args.all:
            df = adapter.fetch_stock_basic(list_status="L")
            if df is None or df.empty:
                print("❌ Failed to fetch stock list")
                record_ingestion_run(
                    settings,
                    "stock_adj_factor",
                    end_date,
                    status="failed",
                    error="Failed to fetch stock list",
                )
                return 1
            ts_codes = df["ts_code"].tolist()
        else:
            print("❌ Must specify --codes or --all for by-stock mode")
            record_ingestion_run(
                settings,
                "stock_adj_factor",
                end_date,
                status="skipped",
                error="Missing --codes/--all",
            )
            return 1

        print(f"   Stocks: {len(ts_codes)}")
        if args.dry_run:
            print(f"💡 Dry-run: would collect adj_factor for {len(ts_codes)} stocks")
            print(f"   Codes: {', '.join(ts_codes[:5])}{'...' if len(ts_codes) > 5 else ''}")
            return 0

        repository = StockRepository(settings.mongo_uri, settings.data_db)
        pipeline = StockDailyPipeline(adapter=adapter, repository=repository)
        try:
            with revision_ledger_job(
                STOCK_ADJ_FACTOR_SOURCE,
                "stock_adj_factor_sync",
                settings.mongo_uri,
            ):
                result = pipeline.run_by_stock(
                    ts_codes,
                    start_date,
                    end_date,
                    include_daily=False,
                    include_basic=False,
                    include_adj_factor=True,
                    progress=True,
                )
        except Exception as exc:
            record_ingestion_run(settings, "stock_adj_factor", end_date, status="failed", error=str(exc))
            raise

    records_changed = int(result.get("total_adj_factor_records") or 0)
    print("\n✅ Stock adj_factor sync completed:")
    print(f"   Items processed: {result['total_processed']}")
    print(f"   Adj factor records: {records_changed}")
    print(f"   Issues: {result['total_issues']}")
    record_ingestion_run(settings, "stock_adj_factor", end_date, records_changed=records_changed)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
