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

    # Multiple MA periods (EOD schedule default: 5,30)
    python -m quant_data_engine.tasks.analyze_market_sentiment \\
        --granularity daily --latest 1 --ma-periods 5,30
"""

from __future__ import annotations
import sys
import os
import argparse
from datetime import datetime, timedelta
from typing import List, Optional

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from quant_data_engine import get_settings
from quant_data_engine.tasks.sentiment_analyzer import calculate_sentiment, calculate_daily_realtime_sentiment
from quant_data_engine.utils.ingestion_job import record_ingestion_run


def parse_ma_periods(ma_period: int, ma_periods: Optional[str]) -> List[int]:
    """Resolve MA periods from --ma-periods CSV or single --ma-period."""
    if ma_periods:
        out: List[int] = []
        for part in str(ma_periods).split(","):
            part = part.strip()
            if not part:
                continue
            value = int(part)
            if value < 2:
                raise ValueError(f"ma_period must be >= 2, got {value}")
            if value not in out:
                out.append(value)
        if not out:
            raise ValueError("--ma-periods produced an empty list")
        return out
    return [int(ma_period)]


def parse_args():
    p = argparse.ArgumentParser(
        description="Market sentiment analysis for daily or minute K-line data"
    )
    
    # Data granularity
    p.add_argument(
        "--granularity",
        type=str,
        required=True,
        choices=["daily", "minute", "daily_realtime"],
        help="Data granularity: daily (EOD), minute, or daily_realtime (daily MA + minute close)"
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
        help="Moving average period (default: 5). Ignored when --ma-periods is set."
    )
    p.add_argument(
        "--ma-periods",
        type=str,
        default=None,
        help="Comma-separated MA periods (e.g. 5,30). Overrides --ma-period.",
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
        if args.granularity in ("minute", "daily_realtime"):
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
    if granularity in ("minute", "daily_realtime"):
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
    job_date = datetime.now().strftime("%Y%m%d")
    try:
        ma_periods = parse_ma_periods(args.ma_period, args.ma_periods)
    except ValueError as exc:
        print(f"❌ Error: {exc}")
        sys.exit(1)

    # Determine collections based on granularity
    if args.granularity == "minute":
        source_name = args.collection or "minute_bars_external"
        output_name = args.output_collection or "market_spectrum_minute"
    elif args.granularity == "daily_realtime":
        daily_source_name = "volume_price"
        minute_source_name = args.collection or "minute_bars_external"
        source_name = f"{daily_source_name} + {minute_source_name}"
        output_name = args.output_collection or "market_spectrum_minute"
    else:  # daily
        source_name = "volume_price"
        output_name = args.output_collection or "market_spectrum"

    # Get time range
    start_time, end_time = get_time_range(args)

    # Calculate window start for display (widest lookback among requested periods)
    window_start_time = calculate_window_start(start_time, max(ma_periods), args.granularity)

    print(f"\n{'='*70}")
    print(f"📊 Market Sentiment Analysis ({args.granularity.upper()})")
    print(f"{'='*70}")
    print(f"MongoDB URI: {settings.mongo_uri}")
    print(f"Database:    {settings.data_db}")
    print(f"Source collection: {source_name}")
    print(f"Output collection: {output_name}")
    print(f"Time range: {start_time} → {end_time}")
    print(f"MA periods: {ma_periods} ({args.granularity})")
    print(f"Window start (for MA): {window_start_time}")
    print(f"Min stocks threshold: {args.min_stocks}")
    print(f"{'='*70}\n")

    # Connect to MongoDB
    from pymongo import MongoClient
    client = MongoClient(settings.mongo_uri)

    n_written = 0
    errors: List[str] = []
    last_result: dict = {"success": False, "results": [], "error": "no periods"}

    for ma_period in ma_periods:
        print(f"--- MA{ma_period} ---")
        if args.granularity == "daily_realtime":
            daily_source_name = "volume_price"
            minute_source_name = args.collection or "minute_bars_external"
            result = calculate_daily_realtime_sentiment(
                client=client,
                db_name=settings.data_db,
                daily_collection_name=daily_source_name,
                minute_collection_name=minute_source_name,
                start_time=start_time,
                end_time=end_time,
                ma_period=ma_period,
                min_stocks=args.min_stocks,
                verbose=True,
            )

            if result.get("success") and result.get("results"):
                db = client[settings.data_db]
                out_col = db[output_name]
                for doc in result["results"]:
                    out_col.update_one(
                        {
                            "trade_date": doc["trade_date"],
                            "ma_period": doc["ma_period"],
                            "granularity": doc["granularity"],
                        },
                        {"$set": doc},
                        upsert=True,
                    )
        else:
            result = calculate_sentiment(
                client=client,
                db_name=settings.data_db,
                source_collection_name=source_name,
                output_collection_name=output_name,
                start_time=start_time,
                end_time=end_time,
                ma_period=ma_period,
                granularity=args.granularity,
                min_stocks=args.min_stocks,
                verbose=True,
            )

        last_result = result
        if result.get("success"):
            n_written += len(result.get("results") or [])
        else:
            errors.append(f"MA{ma_period}: {result.get('error') or 'analysis failed'}")

    client.close()

    if not errors:
        record_ingestion_run(
            settings,
            "market_sentiment",
            job_date,
            records_changed=n_written,
            granularity=args.granularity,
            ma_periods=ma_periods,
        )
        result = {**last_result, "success": True, "results": last_result.get("results") or []}
    else:
        status = "failed" if n_written == 0 else "partial"
        record_ingestion_run(
            settings,
            "market_sentiment",
            job_date,
            status=status,
            records_changed=n_written,
            error="; ".join(errors)[:2000],
            granularity=args.granularity,
            ma_periods=ma_periods,
        )
        result = {
            "success": False,
            "error": "; ".join(errors),
            "results": last_result.get("results") or [],
        }

    # Print summary
    print(f"\n{'='*70}")
    if result["success"]:
        results = result["results"]
        print(f"✅ Analysis complete!")
        print(f"📝 Wrote {n_written} records to {output_name} (ma_periods={ma_periods})")

        if results:
            print(f"\n📊 Last period sample:")
            for r in results:
                dist = r.get("distribution", {})
                lu = dist.get("limit_up", 0)
                ld = dist.get("limit_down", 0)
                print(f"   [{r['trade_date']}] Yang: {r['yang_spectrum']:.2%}, LU: {lu}, LD: {ld}")
    else:
        print(f"❌ Analysis failed: {result.get('error', 'Unknown error')}")

    print(f"{'='*70}\n")


if __name__ == "__main__":
    main()
