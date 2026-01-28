"""CLI task to run stock daily data collection (OHLCV + basic indicators).

Supports two modes:
  1. By-stock mode: Iterate through stocks, fetch data for date range
  2. By-date mode: Iterate through dates, fetch all stocks (bulk mode, recommended)

Environment:
  MONGO_URL / MONGO_DB / TUSHARE_TOKEN via settings
  
Usage:
  # Bulk mode by date (recommended for efficiency)
  python -m quant_data_engine.tasks.run_stock_daily --mode by-date --start 20240101 --end 20240110
  
  # By stock mode
  python -m quant_data_engine.tasks.run_stock_daily --mode by-stock --codes 000001.SZ 000002.SZ --start 20240101 --end 20240110
  
  # Auto-detect trade days and sync
  python -m quant_data_engine.tasks.run_stock_daily --mode by-date --days 5
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
    p.add_argument("--dry-run", action="store_true", help="Show what would be collected without writing")
    p.add_argument("--verbose", action="store_true", help="Verbose output")
    
    return p.parse_args()


def get_trading_days(adapter, start_date: str, end_date: str):
    """Get list of trading days in the given range."""
    df = adapter.fetch_trade_cal(start_date=start_date, end_date=end_date)
    if df is None or df.empty:
        return []
    return df[df['is_open'] == 1]['cal_date'].str.replace('-', '').tolist()


def main():
    args = parse_args()
    settings = get_settings()
    
    # Initialize components
    adapter = StockAdapter(settings.tushare_token, rate_limit_per_sec=settings.rate_limit_per_sec)
    repository = StockRepository(settings.mongo_uri, settings.mongo_db)
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
            return
        # Take last N days
        trading_days = trading_days[:args.days]
        start_date = trading_days[-1]
        end_date = trading_days[0]
    elif args.start:
        start_date = args.start
    else:
        # Default: last trading day
        start_date = (datetime.today() - timedelta(days=1)).strftime("%Y%m%d")
    
    print(f"🟡 Stock Daily Data Collection")
    print(f"   Mode: {args.mode}")
    print(f"   Date range: {start_date} -> {end_date}")
    
    if args.mode == "by-date":
        # Get trading days in range
        trading_days = get_trading_days(adapter, start_date, end_date)
        if not trading_days:
            print(f"⚠️  No trading days found in range")
            return
        
        print(f"   Trading days: {len(trading_days)}")
        
        if args.dry_run:
            print(f"💡 Dry-run mode: would collect {len(trading_days)} trading days")
            print(f"   Days: {', '.join(trading_days[:5])}{'...' if len(trading_days) > 5 else ''}")
            return
        
        # Run pipeline by date
        result = pipeline.run_by_date(trading_days)
        
        print(f"\n✅ Collection completed:")
        print(f"   Dates processed: {result['total_processed']}")
        print(f"   Daily records: {result['total_daily_records']}")
        print(f"   Basic records: {result['total_basic_records']}")
        print(f"   Issues: {result['total_issues']}")
        
    elif args.mode == "by-stock":
        # Get stock codes
        if args.codes:
            ts_codes = args.codes
        elif args.all:
            df = adapter.fetch_stock_basic(list_status='L')
            if df is None or df.empty:
                print("❌ Failed to fetch stock list")
                return
            ts_codes = df['ts_code'].tolist()
        else:
            print("❌ Must specify --codes or --all for by-stock mode")
            return
        
        print(f"   Stocks: {len(ts_codes)}")
        
        if args.dry_run:
            print(f"💡 Dry-run mode: would collect {len(ts_codes)} stocks")
            print(f"   Codes: {', '.join(ts_codes[:5])}{'...' if len(ts_codes) > 5 else ''}")
            return
        
        # Run pipeline by stock
        result = pipeline.run_by_stock(ts_codes, start_date, end_date)
        
        print(f"\n✅ Collection completed:")
        print(f"   Stocks processed: {result['total_processed']}")
        print(f"   Daily records: {result['total_daily_records']}")
        print(f"   Basic records: {result['total_basic_records']}")
        print(f"   Issues: {result['total_issues']}")


if __name__ == "__main__":
    main()
