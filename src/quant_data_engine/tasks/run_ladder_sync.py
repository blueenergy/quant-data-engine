"""CLI task to run limit-up ladder data synchronization.

Collects daily limit-up ladder data from 东方财富.

Environment:
  MONGO_URI / MONGO_DB via settings
  
Usage:
  # Sync today's data
  python -m quant_data_engine.tasks.run_ladder_sync
  
  # Sync specific date
  python -m quant_data_engine.tasks.run_ladder_sync --date 20260209
  
  # Sync date range
  python -m quant_data_engine.tasks.run_ladder_sync --start 20260201 --end 20260209
  
  # Quick mode (skip sector fetching for faster execution)
  python -m quant_data_engine.tasks.run_ladder_sync --quick
"""
from __future__ import annotations
import os
import sys
import time
import signal
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
import argparse
import logging
from datetime import datetime, timedelta

from quant_data_engine.config.settings import get_settings
from quant_data_engine.repositories.ladder_repository import LadderRepository
from quant_data_engine.pipelines.ladder_pipeline import LadderPipeline, EastMoneyAPI
from quant_data_engine.utils.ingestion_job import record_ingestion_run

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def parse_args():
    p = argparse.ArgumentParser(description="Run limit-up ladder data synchronization")
    
    # Date selection
    date_group = p.add_mutually_exclusive_group()
    date_group.add_argument("--date", type=str, help="Single date to sync (YYYYMMDD)")
    date_group.add_argument("--start", type=str, help="Start date for range sync (YYYYMMDD)")
    date_group.add_argument("--auto", action="store_true", help="Auto-resume from last sync date")
    
    p.add_argument("--end", type=str, help="End date for range sync (YYYYMMDD, default: today)")
    
    # Options
    p.add_argument("--quick", action="store_true", help="Quick mode: skip sector fetching")
    p.add_argument("--source", default="eastmoney", choices=["eastmoney", "tushare"], help="Data source (eastmoney=real-time/tushare=T+1)")
    p.add_argument("--loop", action="store_true", help="Run in continuous loop mode during trading hours")
    p.add_argument("--interval", type=int, default=60, help="Loop interval in seconds (default: 60)")
    p.add_argument("--dry-run", action="store_true", help="Show what would be synced")
    p.add_argument("--verbose", action="store_true", help="Verbose output")
    
    return p.parse_args()


def get_trading_days_simple(start_date: str, end_date: str) -> list:
    """Get approximate trading days (skips weekends)."""
    start_dt = datetime.strptime(start_date, "%Y%m%d")
    end_dt = datetime.strptime(end_date, "%Y%m%d")
    
    days = []
    current = start_dt
    while current <= end_dt:
        if current.weekday() < 5:  # Monday=0, Friday=4
            days.append(current.strftime("%Y%m%d"))
        current += timedelta(days=1)
    
    return days


def is_trading_time() -> bool:
    """Check if current time is within trading hours (09:25 - 15:00 plus buffer)."""
    now = datetime.now()
    if now.weekday() >= 5: # Weekend
        return False
        
    t = now.time()
    start = datetime.strptime("09:20", "%H:%M").time()
    end = datetime.strptime("15:05", "%H:%M").time()
    
    # Simple check: 9:20 - 15:05
    # Detailed check: 9:20-11:30, 13:00-15:00
    # For simplicity, we just keep it running during lunch break too, it's fine.
    return start <= t <= end


def main_loop(args, pipeline, settings):
    """Run pipeline in a loop."""
    logger.info(f"Starting loop mode (Interval: {args.interval}s, Source: {args.source})")
    
    # Handle exit signals
    def signal_handler(sig, frame):
        logger.info("Stopping loop mode...")
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    while True:
        if is_trading_time():
            try:
                today = datetime.now().strftime("%Y%m%d")
                
                # Validation: Check if market is open
                if args.source == "eastmoney":
                    latest_date = EastMoneyAPI.get_latest_market_date()
                    if latest_date and latest_date != today:
                        if datetime.now().second < 5: # Log intermittently
                            logger.warning(f"Market closed (Latest: {latest_date}), skipping sync for {today}")
                        time.sleep(args.interval)
                        continue

                logger.info(f"🔄 Syncing data for {today}...")
                
                result = pipeline.run(today, fetch_sectors=not args.quick, source=args.source)
                ld_result = pipeline.run_limit_down(today, fetch_sectors=not args.quick, source=args.source)
                
                logger.info(f"   Stocks: Up:{result.total_stocks} Down:{ld_result.total_stocks}, Sentiment: {result.sentiment_score}")
                rc = int(result.saved_stocks or 0) + int(ld_result.saved_stocks or 0)
                record_ingestion_run(
                    settings,
                    "ladder_sync",
                    today,
                    records_changed=rc,
                    total_up=result.total_stocks,
                    total_down=ld_result.total_stocks,
                    mode="loop",
                )
            except Exception as e:
                logger.error(f"Error in loop: {e}")
                try:
                    record_ingestion_run(
                        settings,
                        "ladder_sync",
                        datetime.now().strftime("%Y%m%d"),
                        status="failed",
                        error=str(e),
                        mode="loop",
                    )
                except Exception:
                    pass
        else:
            if datetime.now().second == 0: # Log once per minute during off-hours
                logger.debug("Waiting for trading hours...")
                
        time.sleep(args.interval)


def main():
    args = parse_args()
    settings = get_settings()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Initialize components
    repository = LadderRepository(settings.mongo_uri, settings.data_db)
    pipeline = LadderPipeline(repository=repository)
    
    # Determine date(s) to process
    today = datetime.today().strftime("%Y%m%d")
    
    if args.date:
        dates = [args.date]
        print(f"📅 Single date mode: {args.date}")
    elif args.start:
        end_date = args.end or today
        dates = get_trading_days_simple(args.start, end_date)
        print(f"📅 Date range mode: {args.start} -> {end_date}")
    elif args.auto:
        last_date = repository.get_last_sync_date()
        if last_date:
            last_dt = datetime.strptime(last_date, "%Y%m%d")
            start_date = (last_dt + timedelta(days=1)).strftime("%Y%m%d")
            print(f"📅 Auto-resume: last sync was {last_date}")
        else:
            # Default: last 5 trading days
            start_date = (datetime.today() - timedelta(days=7)).strftime("%Y%m%d")
            print(f"📅 No previous sync found, defaulting to 7-day lookback")
        dates = get_trading_days_simple(start_date, today)
    else:
        # Default: today
        dates = [today]
        print(f"📅 Default mode: syncing today ({today})")
        
    # Check loop mode
    if args.loop:
        main_loop(args, pipeline, settings)
        return
    
    if not dates:
        print("⚠️  No dates to sync")
        record_ingestion_run(
            settings, "ladder_sync", today, status="skipped", error="No dates to sync",
        )
        return
    
    print(f"🟡 Limit-Up Ladder Synchronization")
    print(f"   Source: {args.source}")
    print(f"   Dates to process: {len(dates)}")
    print(f"   Quick mode: {'Yes' if args.quick else 'No'}")
    
    if args.dry_run:
        print(f"\n💡 Dry-run mode:")
        for d in dates:
            print(f"   Would sync: {d}")
        return
    
    job_date_key = dates[-1]
    # Run pipeline for each date
    total_stocks = 0
    total_saved = 0
    total_issues = 0
    
    ladder_failed = False
    for date in dates:
        print(f"\n🔄 Processing {date}...")
        
        # Validation: Check if market is open for today
        if args.source == "eastmoney" and date == datetime.now().strftime("%Y%m%d"):
            latest_date = EastMoneyAPI.get_latest_market_date()
            if latest_date and latest_date != date:
                # Unless --dry-run is on? No, even dry run should reflect reality.
                # But maybe user wants to force it? No, stale data is bad.
                print(f"⚠️  Market seems closed! Latest trade date: {latest_date}, requested: {date}. Skipping.")
                continue

        try:
            result = pipeline.run(date, fetch_sectors=not args.quick, source=args.source)
            ld_result = pipeline.run_limit_down(date, fetch_sectors=not args.quick, source=args.source)
            
            total_stocks += result.total_stocks
            total_saved += result.saved_stocks
            total_issues += len(result.issues) + len(ld_result.issues)
            
            status = "✅" if result.total_stocks > 0 else "⚠️"
            print(f"   {status} Limit-Up: {result.total_stocks}, Limit-Down: {ld_result.total_stocks}, "
                  f"sentiment: {result.sentiment_score}")
            
            if result.issues and args.verbose:
                for issue in result.issues[:5]:
                    print(f"      ⚠️ {issue}")
                    
        except Exception as e:
            logger.error(f"Failed to process {date}: {e}")
            total_issues += 1
            ladder_failed = True
    
    print(f"\n✅ Synchronization completed:")
    print(f"   Dates processed: {len(dates)}")
    print(f"   Total stocks: {total_stocks}")
    print(f"   Total saved: {total_saved}")
    print(f"   Issues: {total_issues}")
    record_ingestion_run(
        settings,
        "ladder_sync",
        job_date_key,
        status="failed" if ladder_failed else "complete",
        records_changed=total_saved,
        total_stocks=total_stocks,
        issues=total_issues,
    )


if __name__ == "__main__":
    main()
