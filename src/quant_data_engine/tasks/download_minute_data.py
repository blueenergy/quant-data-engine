"""CLI task to download minute-level data using miniQMT adapter.

This script provides command-line interface for downloading historical minute-level
data from miniQMT and storing it in the database through the quant_data_engine pipeline.

Features:
  - Download data for single or multiple symbols
  - Configurable date ranges
  - Support for different time frequencies (1m, 5m, 15m, etc.)

Environment:
  MONGO_URI / MONGO_DB as usual via settings.
  Requires miniQMT with xtquant installed.
"""

from __future__ import annotations
import sys
import os
import argparse
from datetime import datetime, timedelta

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from quant_data_engine import create_mini_qmt_pipeline, get_settings
from quant_data_engine.tasks.continuous_market_minute_sync import get_all_a_shares


def parse_args():
    p = argparse.ArgumentParser(description="Download minute-level data using miniQMT")
    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument("--symbols", nargs="+", 
                       help="Stock symbols e.g. 300722.SZ 000001.SZ")
    group.add_argument("--all-a-shares", action="store_true",
                       help="Download minute data for all A-share symbols from stock_info")
    p.add_argument("--start", type=str, default=None, 
                   help="Start date YYYYMMDD (default: 30 days ago)")
    p.add_argument("--end", type=str, default=None, 
                   help="End date YYYYMMDD (default: today)")
    p.add_argument("--period", type=str, default="1m", 
                   choices=["1m", "5m", "15m", "30m", "1h", "1d"],
                   help="Data frequency (default: 1m)")
    p.add_argument("--max-wait", type=int, default=60,
                   help="Max seconds to wait for data download (default: 60)")
    p.add_argument("--batch", action="store_true",
                   help="Use batch download mode")
    return p.parse_args()


def main():
    args = parse_args()
    settings = get_settings()

    # Determine symbol list
    if args.all_a_shares:
        print("📊 Loading all A-share symbols from stock_info...")
        symbols = get_all_a_shares()
    else:
        symbols = args.symbols or []

    if not symbols:
        print("❌ No symbols provided or loaded. Exiting.")
        return

    today_dt = datetime.today()
    end = args.end or today_dt.strftime("%Y%m%d")
    start = args.start or (today_dt - timedelta(days=30)).strftime("%Y%m%d")

    print(f"🟡 Downloading minute data for {len(symbols)} symbols")
    print(f"📅 Date range: {start} to {end}")
    print(f"🕒 Frequency: {args.period}")
    
    try:
        # Create pipeline
        pipeline = create_mini_qmt_pipeline()
        
        if args.batch:
            # Batch download mode
            print("🔄 Using batch download mode")
            pipeline.run_batch(
                codes=symbols,
                start_date=start,
                end_date=end,
                period=args.period,
                max_wait=args.max_wait
            )
        else:
            # Individual download mode
            print("🔄 Using individual download mode")
            for symbol in symbols:
                print(f"📥 Processing {symbol}...")
                pipeline.run(
                    code=symbol,
                    start_date=start,
                    end_date=end,
                    period=args.period,
                    max_wait=args.max_wait
                )
        
        print("✅ Download process completed")
        print("💡 Note: Data download is asynchronous in miniQMT")
        print("   Please check the miniQMT client for download progress")
        
    except Exception as e:
        print(f"❌ Error during download: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":  # pragma: no cover
    main()