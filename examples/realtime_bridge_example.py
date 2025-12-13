"""Example: Using the realtime bridge programmatically.

This example shows how to use the quant_data_engine realtime bridge
in your own Python scripts.
"""

import time
import signal
import sys


def example_1_basic_usage():
    """Example 1: Basic usage with factory."""
    from quant_data_engine import create_realtime_pipeline
    
    print("=" * 80)
    print("Example 1: Basic Usage")
    print("=" * 80)
    
    # Create pipeline with manual symbols
    pipeline = create_realtime_pipeline(
        symbols=["002050.SZ", "600618.SH"],
        interval_sec=1.0,
        enable_schedule=True,
        debug=True
    )
    
    # Start collecting data
    pipeline.start()
    
    # Run for 30 seconds
    print("⏱️ Running for 30 seconds...")
    time.sleep(30)
    
    # Stop
    pipeline.stop()
    
    print(f"✅ Processed {pipeline.bars_processed} bars")


def example_2_with_database():
    """Example 2: Load symbols from database."""
    from pymongo import MongoClient
    from quant_data_engine import get_settings, create_realtime_pipeline
    
    print("=" * 80)
    print("Example 2: Database Integration")
    print("=" * 80)
    
    # Load symbols from watchlist_strategies
    settings = get_settings()
    client = MongoClient(settings.mongo_uri)
    db = client[settings.mongo_db]
    
    docs = list(db.watchlist_strategies.find({"enabled": True}))
    symbols = []
    for d in docs:
        symbol = d.get("symbol")
        if symbol:
            # Add exchange suffix
            if symbol.startswith('6'):
                symbols.append(f"{symbol}.SH")
            else:
                symbols.append(f"{symbol}.SZ")
    
    print(f"📊 Loaded {len(symbols)} symbols from database")
    print(f"Symbols: {', '.join(symbols)}")
    
    if not symbols:
        print("⚠️ No enabled symbols found")
        return
    
    # Create and start pipeline
    pipeline = create_realtime_pipeline(
        symbols=symbols,
        interval_sec=1.0,
        enable_schedule=True
    )
    
    pipeline.start()
    
    # Run until interrupted
    print("✅ Bridge running (Ctrl+C to stop)")
    try:
        signal.pause()
    except (KeyboardInterrupt, AttributeError):
        print("\n⏹️ Stopping...")
        pipeline.stop()


def example_3_custom_adapter():
    """Example 3: Using adapter and pipeline separately."""
    from quant_data_engine.adapters.mini_qmt_realtime_adapter import MiniQMTRealtimeAdapter
    from quant_data_engine.repositories.minute_repository import MinuteRepository
    from quant_data_engine.pipelines.realtime_pipeline import RealtimePipeline
    from quant_data_engine import get_settings
    
    print("=" * 80)
    print("Example 3: Custom Configuration")
    print("=" * 80)
    
    settings = get_settings()
    
    # Create adapter with custom settings
    adapter = MiniQMTRealtimeAdapter(
        symbols=["002050.SZ"],
        interval_sec=0.5,      # Poll every 0.5 seconds
        enable_schedule=False,  # Run 24/7 for testing
        debug=True
    )
    
    # Create repository
    repository = MinuteRepository(
        mongo_uri=settings.mongo_uri,
        mongo_db=settings.mongo_db
    )
    
    # Create pipeline
    pipeline = RealtimePipeline(adapter, repository)
    
    # Start
    pipeline.start()
    
    # Run for 60 seconds
    print("⏱️ Running for 60 seconds...")
    time.sleep(60)
    
    # Stop
    pipeline.stop()
    
    print(f"✅ Processed {pipeline.bars_processed} bars")


def example_4_with_scheduler():
    """Example 4: Using TradingScheduler directly."""
    from quant_data_engine.utils.trading_scheduler import TradingScheduler
    from datetime import datetime
    
    print("=" * 80)
    print("Example 4: Trading Scheduler")
    print("=" * 80)
    
    scheduler = TradingScheduler()
    
    # Check current time
    now = datetime.now()
    is_trading = scheduler.is_trading_time(now)
    
    print(f"Current time: {now.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Trading status: {'✅ OPEN' if is_trading else '❌ CLOSED'}")
    
    # Get next trading time
    next_trading = scheduler.get_next_trading_time(now)
    print(f"Next trading time: {next_trading.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Check specific times
    print("\nTesting specific times:")
    test_times = [
        datetime(2025, 12, 13, 9, 0),   # Before open
        datetime(2025, 12, 13, 10, 0),  # Morning session
        datetime(2025, 12, 13, 12, 0),  # Lunch break
        datetime(2025, 12, 13, 14, 0),  # Afternoon session
        datetime(2025, 12, 13, 16, 0),  # After close
    ]
    
    for dt in test_times:
        status = "TRADING" if scheduler.is_trading_time(dt) else "CLOSED"
        print(f"  {dt.strftime('%H:%M')} - {status}")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Realtime bridge examples")
    parser.add_argument(
        "--example",
        type=int,
        choices=[1, 2, 3, 4],
        default=1,
        help="Example number to run (1-4)"
    )
    
    args = parser.parse_args()
    
    print("\n🌉 Quant Data Engine - Realtime Bridge Examples\n")
    
    try:
        if args.example == 1:
            example_1_basic_usage()
        elif args.example == 2:
            example_2_with_database()
        elif args.example == 3:
            example_3_custom_adapter()
        elif args.example == 4:
            example_4_with_scheduler()
    except KeyboardInterrupt:
        print("\n⏹️ Interrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    print("\n✅ Example complete!")
