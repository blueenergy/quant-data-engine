"""Market sentiment analysis tool for both daily and minute-level K-line data.

This tool calculates the yang/yin spectrum (阳谱/阴谱) for the A-share market,
showing what percentage of stocks are trading above their moving average.

Supports both:
  - Daily data from volume_price collection
  - Minute data from minute_bars or minute_bars_external collection

Features:
  - Auto-detects data granularity (daily vs minute)
  - Configurable moving average period
  - Batch processing for efficiency
  - Writes to appropriate output collection

Usage:
    # Daily analysis for date range
    python -m quant_data_engine.tasks.analyze_market_sentiment \\
        --start 20251201 \\
        --end 20251215 \\
        --granularity daily
    
    # Minute analysis for latest 60 minutes
    python -m quant_data_engine.tasks.analyze_market_sentiment \\
        --latest 60 \\
        --granularity minute
    
    # Minute analysis for specific time range
    python -m quant_data_engine.tasks.analyze_market_sentiment \\
        --start 202512170930 \\
        --end 202512171500 \\
        --granularity minute \\
        --collection minute_bars_external
    
    # Custom MA period
    python -m quant_data_engine.tasks.analyze_market_sentiment \\
        --latest 120 \\
        --granularity minute \\
        --ma-period 10
"""

from __future__ import annotations
import sys
import os
import argparse
from datetime import datetime, timedelta
from typing import Optional

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from quant_data_engine import get_settings
from quant_data_engine.tasks.sentiment_analyzer import calculate_sentiment


def parse_args():
    p = argparse.ArgumentParser(
        description="Market sentiment analysis for daily or minute K-line data"
    )
    
    # Data granularity
    p.add_argument(
        "--granularity",
        type=str,
        required=True,
        choices=["daily", "minute"],
        help="Data granularity: daily or minute"
    )
    
    # Time range options
    time_group = p.add_mutually_exclusive_group(required=True)
    time_group.add_argument(
        "--latest",
        type=int,
        help="Analyze latest N periods (minutes for minute, days for daily)"
    )
    time_group.add_argument(
        "--start",
        type=str,
        help="Start time (YYYYMMDD for daily, YYYYMMDDHHMM for minute)"
    )
    
    p.add_argument(
        "--end",
        type=str,
        help="End time (YYYYMMDD for daily, YYYYMMDDHHMM for minute)"
    )
    
    p.add_argument(
        "--ma-period",
        type=int,
        default=5,
        help="Moving average period (default: 5)"
    )
    
    # Collection options (minute only)
    p.add_argument(
        "--collection",
        type=str,
        default=None,
        help="Source collection for minute data (default: minute_bars_external). Ignored for daily."
    )
    
    p.add_argument(
        "--output-collection",
        type=str,
        default=None,
        help="Output collection name (auto-determined if not specified)"
    )
    
    p.add_argument(
        "--min-stocks",
        type=int,
        default=100,
        help="Minimum number of stocks required for valid calculation (default: 100)"
    )
    
    return p.parse_args()


def get_time_range(args) -> tuple[str, str]:
    """Calculate start and end time based on arguments.
    
    Returns:
        (start_time, end_time) in appropriate format
    """
    if args.latest:
        if args.granularity == "minute":
            # Calculate time range for latest N minutes
            now = datetime.now()
            end_dt = now
            start_dt = now - timedelta(minutes=args.latest)
            return start_dt.strftime("%Y%m%d%H%M"), end_dt.strftime("%Y%m%d%H%M")
        else:  # daily
            # Calculate time range for latest N days
            today = datetime.now()
            end_dt = today
            start_dt = today - timedelta(days=args.latest)
            return start_dt.strftime("%Y%m%d"), end_dt.strftime("%Y%m%d")
    else:
        # Use explicit start/end
        if not args.end:
            print("❌ Error: --end is required when using --start")
            sys.exit(1)
        return args.start, args.end


def calculate_window_start(start_time: str, ma_period: int, granularity: str) -> str:
    """Calculate earlier start time needed for MA calculation (for display purposes).
    
    Args:
        start_time: Target start time
        ma_period: MA period
        granularity: "daily" or "minute"
        
    Returns:
        Earlier start time
    """
    if granularity == "minute":
        dt = datetime.strptime(start_time, "%Y%m%d%H%M")
        window_dt = dt - timedelta(minutes=ma_period * 2)
        return window_dt.strftime("%Y%m%d%H%M")
    else:  # daily
        dt = datetime.strptime(start_time, "%Y%m%d")
        window_dt = dt - timedelta(days=ma_period * 2)
        return window_dt.strftime("%Y%m%d")


def main():
    args = parse_args()
    settings = get_settings()
    
    # Determine collections based on granularity
    if args.granularity == "minute":
        source_name = args.collection or "minute_bars_external"
        output_name = args.output_collection or "market_spectrum_minute"
    else:  # daily
        source_name = "volume_price"
        output_name = args.output_collection or "market_spectrum"
    
    # Get time range
    start_time, end_time = get_time_range(args)
    
    # Calculate window start for display
    window_start_time = calculate_window_start(start_time, args.ma_period, args.granularity)
    
    print(f"\n{'='*70}")
    print(f"📊 Market Sentiment Analysis ({args.granularity.upper()})")
    print(f"{'='*70}")
    print(f"Source collection: {source_name}")
    print(f"Output collection: {output_name}")
    print(f"Time range: {start_time} → {end_time}")
    print(f"MA period: {args.ma_period} {args.granularity}s")
    print(f"Window start (for MA): {window_start_time}")
    print(f"Min stocks threshold: {args.min_stocks}")
    print(f"{'='*70}\n")
    
    # Connect to MongoDB
    from pymongo import MongoClient
    client = MongoClient(settings.MONGO_URL)
    
    # Run sentiment analysis using shared function
    result = calculate_sentiment(
        client=client,
        db_name=settings.MONGO_DB,
        source_collection_name=source_name,
        output_collection_name=output_name,
        start_time=start_time,
        end_time=end_time,
        ma_period=args.ma_period,
        granularity=args.granularity,
        min_stocks=args.min_stocks,
        verbose=True
    )
    
    client.close()
    
    # Print summary
    print(f"\n{'='*70}")
    if result["success"]:
        results = result["results"]
        print(f"✅ Analysis complete!")
        print(f"📝 Wrote {len(results)} records to {output_name}")
        
        if results:
            print(f"\n📈 Summary:")
            avg_yang = sum(r["yang_spectrum"] for r in results) / len(results)
            avg_yin = sum(r["yin_spectrum"] for r in results) / len(results)
            print(f"   Average Yang Spectrum: {avg_yang:.2%}")
            print(f"   Average Yin Spectrum: {avg_yin:.2%}")
            print(f"   Time range: {results[0]['trade_date']} → {results[-1]['trade_date']}")
            print(f"   Periods analyzed: {len(results)}")
    else:
        print(f"❌ Analysis failed: {result.get('error', 'Unknown error')}")
    
    print(f"{'='*70}\n")


if __name__ == "__main__":
    main()
