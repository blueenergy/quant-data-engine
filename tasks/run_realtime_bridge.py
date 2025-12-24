"""Realtime data collection task (Windows Bridge).

Continuously collects minute bar data from miniQMT and stores in MongoDB.
This is the quant_data_engine version of the Windows Bridge.

Usage:
    # Auto-load symbols from database
    python -m quant_data_engine.tasks.run_realtime_bridge --auto
    
    # Manual symbols
    python -m quant_data_engine.tasks.run_realtime_bridge --symbols 002050.SZ 600618.SH
    
    # Debug mode
    python -m quant_data_engine.tasks.run_realtime_bridge --auto --debug
    
    # 24/7 mode (no trading schedule)
    python -m quant_data_engine.tasks.run_realtime_bridge --auto --no-schedule
"""

import argparse
import signal
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from quant_data_engine.config.settings import get_settings
from quant_data_engine.adapters.mini_qmt_realtime_adapter import MiniQMTRealtimeAdapter
from quant_data_engine.repositories.minute_repository import MinuteRepository
from quant_data_engine.pipelines.realtime_pipeline import RealtimePipeline


def add_exchange_suffix(symbol: str) -> str:
    """Add exchange suffix for miniQMT.
    
    Rules:
    - 6xxxxx (Shanghai) -> .SH
    - 0xxxxx, 3xxxxx (Shenzhen) -> .SZ
    - Already has suffix -> return as-is
    
    Args:
        symbol: Stock symbol
        
    Returns:
        str: Symbol with exchange suffix
    """
    if '.' in symbol:
        return symbol  # Already has suffix
    
    if symbol.startswith('6'):
        return f"{symbol}.SH"
    elif symbol.startswith(('0', '3')):
        return f"{symbol}.SZ"
    else:
        return f"{symbol}.SZ"


def load_symbols_from_database():
    """Load symbols from both watchlist_strategies and user_watchlists.
    
    Combines:
    1. Enabled symbols from watchlist_strategies (strategy-configured stocks)
    2. All symbols from user_watchlists (user's watchlist stocks)
    
    Returns:
        list: Deduplicated list of symbols with exchange suffixes
    """
    from pymongo import MongoClient
    
    settings = get_settings()
    client = MongoClient(settings.mongo_uri)
    db = client[settings.mongo_db]
    
    symbols_set = set()  # Use set to avoid duplicates
    
    # 1. Load from watchlist_strategies (enabled strategies)
    strategy_docs = list(db.watchlist_strategies.find({"enabled": True}))
    for d in strategy_docs:
        raw_symbol = d.get("symbol")
        if isinstance(raw_symbol, str):
            symbol_with_suffix = add_exchange_suffix(raw_symbol)
            symbols_set.add(symbol_with_suffix)
    
    # 2. Load from user_watchlists (all users' watchlists)
    watchlist_docs = list(db.user_watchlists.find({}))
    for d in watchlist_docs:
        user_symbols = d.get("symbols", [])
        if isinstance(user_symbols, list):
            for raw_symbol in user_symbols:
                if isinstance(raw_symbol, str):
                    symbol_with_suffix = add_exchange_suffix(raw_symbol)
                    symbols_set.add(symbol_with_suffix)
    
    symbols = sorted(list(symbols_set))  # Sort for consistent output
    
    print(f"📊 Loaded symbols breakdown:")
    print(f"   - Strategy-enabled: {len(strategy_docs)} stocks")
    print(f"   - User watchlists: {sum(len(d.get('symbols', [])) for d in watchlist_docs)} stocks (from {len(watchlist_docs)} users)")
    print(f"   - Total unique: {len(symbols)} stocks")
    
    return symbols


def parse_args():
    """Parse command line arguments."""
    p = argparse.ArgumentParser(
        description="Run MiniQMT realtime data bridge"
    )
    p.add_argument(
        "--symbols",
        nargs="+",
        help="Stock symbols (e.g., 002050.SZ 600618.SH). Suffixes added automatically if missing."
    )
    p.add_argument(
        "--auto",
        action="store_true",
        help="Auto-load symbols from database watchlist_strategies"
    )
    p.add_argument(
        "--interval",
        type=float,
        default=0.5,  # Changed from 1.0 to 0.5 for better tick coverage
        help="Tick polling interval in seconds (default: 0.5)"
    )
    p.add_argument(
        "--no-schedule",
        action="store_true",
        help="Disable trading time scheduling (run 24/7)"
    )
    p.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug output"
    )
    return p.parse_args()


def main():
    """Main entry point."""
    args = parse_args()
    settings = get_settings()
    
    print("=" * 80)
    print("🌉 MiniQMT Realtime Bridge (quant_data_engine)")
    print("=" * 80)
    
    # Determine symbol list
    if args.auto:
        symbols = load_symbols_from_database()
        print(f"📊 Auto mode: loaded {len(symbols)} symbols from database")
    elif args.symbols:
        # Add exchange suffixes if missing
        symbols = [add_exchange_suffix(s) for s in args.symbols]
        print(f"📝 Manual mode: using {len(symbols)} symbols")
    else:
        print("❌ Error: Must specify --symbols or --auto")
        print("   Examples:")
        print("     python -m quant_data_engine.tasks.run_realtime_bridge --auto")
        print("     python -m quant_data_engine.tasks.run_realtime_bridge --symbols 002050 600618")
        sys.exit(1)
    
    if not symbols:
        print("⚠️ No symbols to monitor")
        print("   Tip: Enable symbols in MongoDB watchlist_strategies collection")
        sys.exit(0)
    
    print(f"Symbols: {', '.join(symbols)}")
    print(f"Interval: {args.interval}s")
    print(f"Schedule: {'Disabled (24/7)' if args.no_schedule else 'Enabled (9:30-15:00)'}")
    print(f"Debug: {'On' if args.debug else 'Off'}")
    print("=" * 80)
    
    try:
        # Create adapter
        adapter = MiniQMTRealtimeAdapter(
            symbols=symbols,
            interval_sec=args.interval,
            enable_schedule=not args.no_schedule,
            debug=args.debug
        )
        
        # Create repository
        repository = MinuteRepository(
            mongo_uri=settings.mongo_uri,
            mongo_db=settings.mongo_db
        )
        
        # Create pipeline
        pipeline = RealtimePipeline(
            adapter=adapter,
            repository=repository
        )
        
        # Setup signal handlers for graceful shutdown
        def signal_handler(sig, frame):
            print("\n⏹️ Stopping bridge...")
            pipeline.stop()
            print("✅ Bridge stopped gracefully")
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        # Start pipeline
        pipeline.start()
        
        print("✅ Bridge running (Ctrl+C to stop)")
        print("=" * 80)
        
        # Keep running
        try:
            signal.pause()
        except AttributeError:
            # Windows doesn't have signal.pause()
            import time
            while True:
                time.sleep(1)
        
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
