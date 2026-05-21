"""CLI task to run money flow data synchronization.

Collects money flow data from Tushare for all stocks by date.

Environment:
  MONGO_URI / MONGO_DB / TUSHARE_TOKEN via settings
  
Usage:
  # Sync specific date range
  python -m quant_data_engine.tasks.run_moneyflow_sync --start 20240101 --end 20240110
  
  # Auto-resume from last sync date
  python -m quant_data_engine.tasks.run_moneyflow_sync --auto
  
  # Sync recent trading days
  python -m quant_data_engine.tasks.run_moneyflow_sync --days 5
"""
from __future__ import annotations
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
import argparse
from datetime import datetime, timedelta
from pymongo import MongoClient
from quant_data_engine.config.settings import get_settings
from quant_data_engine.adapters.stock_adapter import StockAdapter
from quant_data_engine.repositories.moneyflow_repository import MoneyFlowRepository
from quant_data_engine.pipelines.moneyflow_pipeline import MoneyFlowPipeline
from quant_data_engine.utils.ingestion_job import record_ingestion_run


def parse_args():
    p = argparse.ArgumentParser(description="Run money flow data synchronization")
    
    # Date selection
    date_group = p.add_mutually_exclusive_group()
    date_group.add_argument("--start", type=str, help="Start date YYYYMMDD")
    date_group.add_argument("--auto", action="store_true", help="Auto-resume from last sync date")
    date_group.add_argument("--days", type=int, help="Sync recent N trading days")
    
    p.add_argument("--end", type=str, help="End date YYYYMMDD (default: today)")
    
    # Options
    p.add_argument("--dry-run", action="store_true", help="Show what would be synced without writing")
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
    repository = MoneyFlowRepository(settings.mongo_uri, settings.data_db)
    
    pipeline = MoneyFlowPipeline(adapter=adapter, repository=repository)
    
    # Determine date range
    today = datetime.today().strftime("%Y%m%d")
    end_date = args.end or today
    
    if args.auto:
        # Resume from last sync date
        last_date = repository.get_last_sync_date()
        if last_date:
            # Start from next day
            last_dt = datetime.strptime(last_date, "%Y%m%d")
            start_date = (last_dt + timedelta(days=1)).strftime("%Y%m%d")
            print(f"📅 Auto-resume mode: last sync date was {last_date}")
        else:
            # Default: last 14 days
            start_date = (datetime.today() - timedelta(days=14)).strftime("%Y%m%d")
            print(f"📅 No last sync date found, defaulting to 14 days lookback")
    elif args.days:
        # Calculate start date based on recent trading days
        lookback_start = (datetime.today() - timedelta(days=args.days * 2)).strftime("%Y%m%d")
        trading_days = get_trading_days(adapter, lookback_start, end_date)
        if not trading_days:
            print(f"❌ No trading days found in lookback period")
            record_ingestion_run(
                settings, "moneyflow", end_date, status="skipped", error="No trading days in lookback",
            )
            return
        # Take last N days
        trading_days = trading_days[-args.days:]
        start_date = trading_days[0]
        end_date = trading_days[-1]
    elif args.start:
        start_date = args.start
    else:
        # Default: yesterday
        start_date = (datetime.today() - timedelta(days=1)).strftime("%Y%m%d")
    
    # Validate date range
    if start_date > end_date:
        print(f"❌ Invalid date range: {start_date} > {end_date}")
        record_ingestion_run(
            settings, "moneyflow", today, status="skipped", error="Invalid date range",
        )
        return
    
    # Get trading days
    trading_days = get_trading_days(adapter, start_date, end_date)
    if not trading_days:
        print(f"⚠️  No trading days found in range {start_date} -> {end_date}")
        record_ingestion_run(
            settings, "moneyflow", end_date, status="skipped", error="No trading days in range",
        )
        return
    
    print(f"🟡 Money Flow Data Synchronization")
    print(f"   Date range: {start_date} -> {end_date}")
    print(f"   Trading days: {len(trading_days)}")
    
    if args.dry_run:
        print(f"💡 Dry-run mode: would sync {len(trading_days)} trading days")
        print(f"   Days: {', '.join(trading_days[:5])}{'...' if len(trading_days) > 5 else ''}")
        return
    
    try:
        result = pipeline.run_by_date(trading_days)
    except Exception as e:
        record_ingestion_run(settings, "moneyflow", end_date, status="failed", error=str(e))
        raise
    
    print(f"\n✅ Synchronization completed:")
    print(f"   Dates processed: {result['total_dates']}")
    print(f"   Success: {result['success']}")
    print(f"   Failed: {result['failed']}")
    print(f"   Total records: {result['total_records']}")
    print(f"   Issues: {result['total_issues']}")
    
    # Show some errors if any
    if result['total_issues'] > 0 and args.verbose:
        failed_results = [r for r in result['results'] if r.issues]
        print(f"\n⚠️  Failed dates:")
        for r in failed_results[:10]:
            print(f"   {r.trade_date}: {', '.join(r.issues)}")
    record_ingestion_run(
        settings,
        "moneyflow",
        end_date,
        records_changed=int(result.get("total_records") or 0),
    )


if __name__ == "__main__":
    main()
