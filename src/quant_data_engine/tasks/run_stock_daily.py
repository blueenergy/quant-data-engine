"""CLI task to run stock daily data collection (OHLCV + basic indicators).

Supports two modes:
  1. By-stock mode: Iterate through stocks, fetch data for date range
  2. By-date mode: Iterate through dates, fetch all stocks (bulk mode, recommended)

Environment:
  MONGO_URI / MONGO_DB / TUSHARE_TOKEN via settings
  
Usage:
  # Bulk mode by date (recommended for efficiency)
  python -m quant_data_engine.tasks.run_stock_daily --mode by-date --start 20240101 --end 20240110
  
  # By stock mode
  python -m quant_data_engine.tasks.run_stock_daily --mode by-stock --codes 000001.SZ 000002.SZ --start 20240101 --end 20240110
  
  # Auto-detect trade days and sync
  python -m quant_data_engine.tasks.run_stock_daily --mode by-date --days 5

  # Backfill only daily_basic indicators
  python -m quant_data_engine.tasks.run_stock_daily --mode by-date --start 20240101 --end 20240131 --basic-only
"""
from __future__ import annotations
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
import argparse
from datetime import datetime, timedelta
from quant_data_engine.config.settings import get_settings
from quant_data_engine.adapters.stock_adapter import StockAdapter
from quant_data_engine.repositories.stock_repository import StockRepository
from quant_data_engine.pipelines.stock_daily_pipeline import StockDailyPipeline
from quant_data_engine.utils.ingestion_job import record_ingestion_run


def parse_args():
    p = argparse.ArgumentParser(description="Run stock daily data collection")
    p.add_argument("--mode", choices=["by-stock", "by-date"], default="by-date",
                   help="Collection mode: by-stock or by-date (default: by-date)")
    
    # Stock selection
    p.add_argument("--codes", nargs="+", help="Stock ts_codes (e.g., 000001.SZ 000002.SZ)")
    p.add_argument("--all", action="store_true", help="Collect all listed stocks")
    
    # Date range
    date_group = p.add_mutually_exclusive_group()
    date_group.add_argument("--start", type=str, help="Start date YYYYMMDD")
    date_group.add_argument("--days", type=int, help="Number of recent trading days to sync")
    p.add_argument("--end", type=str, help="End date YYYYMMDD (default: today)")
    
    # Options
    p.add_argument("--basic-only", action="store_true",
                   help="Only collect daily_basic indicators; skip OHLCV volume_price writes")
    p.add_argument("--dry-run", action="store_true", help="Show what would be collected without writing")
    p.add_argument("--verbose", action="store_true", help="Verbose output")
    
    return p.parse_args()


def get_trading_days(adapter, start_date: str, end_date: str):
    """Get list of trading days in the given range, chronologically sorted."""
    df = adapter.fetch_trade_cal(start_date=start_date, end_date=end_date)
    if df is None or df.empty:
        return []
    days = df[df['is_open'] == 1]['cal_date'].str.replace('-', '').tolist()
    return sorted(days)


def main():
    args = parse_args()
    settings = get_settings()
    
    # Initialize components
    adapter = StockAdapter(settings.tushare_token, rate_limit_per_sec=settings.rate_limit_per_sec)
    repository = StockRepository(settings.mongo_uri, settings.data_db)
    pipeline = StockDailyPipeline(adapter=adapter, repository=repository)
    
    # Determine date range
    today = datetime.today().strftime("%Y%m%d")
    end_date = args.end or today
    
    if args.days:
        # Calculate start date based on recent trading days
        lookback_start = (datetime.today() - timedelta(days=args.days * 2)).strftime("%Y%m%d")
        trading_days = get_trading_days(adapter, lookback_start, end_date)
        if not trading_days:
            print(f"❌ No trading days found in lookback period")
            record_ingestion_run(
                settings, "stock_daily", today, status="skipped", error="No trading days in lookback",
            )
            return
        # Take last N days from sorted list
        trading_days = trading_days[-args.days:]
        start_date = trading_days[0]
        end_date = trading_days[-1]
    elif args.start:
        start_date = args.start
    else:
        # Default: last trading day
        start_date = (datetime.today() - timedelta(days=1)).strftime("%Y%m%d")
    
    print(f"🟡 Stock Daily Data Collection")
    print(f"   Mode: {args.mode}")
    print(f"   Date range: {start_date} -> {end_date}")
    print(f"   Scope: {'daily_basic only' if args.basic_only else 'OHLCV + daily_basic'}")
    
    if args.mode == "by-date":
        # Get trading days in range
        trading_days = get_trading_days(adapter, start_date, end_date)
        if not trading_days:
            print(f"⚠️  No trading days found in range")
            record_ingestion_run(
                settings, "stock_daily", end_date, status="skipped", error="No trading days in range",
            )
            return
        
        print(f"   Trading days: {len(trading_days)}")
        
        if args.dry_run:
            print(f"💡 Dry-run mode: would collect {len(trading_days)} trading days")
            print(f"   Scope: {'daily_basic only' if args.basic_only else 'OHLCV + daily_basic'}")
            print(f"   Days: {', '.join(trading_days[:5])}{'...' if len(trading_days) > 5 else ''}")
            return
        
        try:
            result = pipeline.run_by_date(
                trading_days,
                include_daily=not args.basic_only,
                progress=True,
            )
        except Exception as e:
            record_ingestion_run(settings, "stock_daily", end_date, status="failed", error=str(e))
            raise
        
        print(f"\n✅ Collection completed:")
        print(f"   Dates processed: {result['total_processed']}")
        print(f"   Daily records: {result['total_daily_records']}")
        print(f"   Basic records: {result['total_basic_records']}")
        print(f"   Issues: {result['total_issues']}")
        rc = int(result.get("total_daily_records") or 0) + int(result.get("total_basic_records") or 0)
        record_ingestion_run(settings, "stock_daily", end_date, records_changed=rc)
        
    elif args.mode == "by-stock":
        # Get stock codes
        if args.codes:
            ts_codes = args.codes
        elif args.all:
            df = adapter.fetch_stock_basic(list_status='L')
            if df is None or df.empty:
                print("❌ Failed to fetch stock list")
                record_ingestion_run(
                    settings, "stock_daily", end_date, status="failed", error="Failed to fetch stock list",
                )
                return
            ts_codes = df['ts_code'].tolist()
        else:
            print("❌ Must specify --codes or --all for by-stock mode")
            record_ingestion_run(
                settings, "stock_daily", end_date, status="skipped", error="Missing --codes or --all",
            )
            return
        
        print(f"   Stocks: {len(ts_codes)}")
        
        if args.dry_run:
            print(f"💡 Dry-run mode: would collect {len(ts_codes)} stocks")
            print(f"   Scope: {'daily_basic only' if args.basic_only else 'OHLCV + daily_basic'}")
            print(f"   Codes: {', '.join(ts_codes[:5])}{'...' if len(ts_codes) > 5 else ''}")
            return
        
        try:
            result = pipeline.run_by_stock(
                ts_codes,
                start_date,
                end_date,
                include_daily=not args.basic_only,
                progress=True,
            )
        except Exception as e:
            record_ingestion_run(settings, "stock_daily", end_date, status="failed", error=str(e))
            raise
        
        print(f"\n✅ Collection completed:")
        print(f"   Stocks processed: {result['total_processed']}")
        print(f"   Daily records: {result['total_daily_records']}")
        print(f"   Basic records: {result['total_basic_records']}")
        print(f"   Issues: {result['total_issues']}")
        rc = int(result.get("total_daily_records") or 0) + int(result.get("total_basic_records") or 0)
        record_ingestion_run(settings, "stock_daily", end_date, records_changed=rc)


if __name__ == "__main__":
    main()
