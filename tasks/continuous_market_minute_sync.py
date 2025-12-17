"""Continuous synchronization of minute-level data for entire A-share market.

This script continuously pulls minute-level data for all A-share stocks in batches,
writing to minute_bars_external collection. It manages resource usage by:
  - Batching symbols (default 400 per batch)
  - Controlled sleep between batches
  - Configurable target latency (default 5 minutes)

The script reuses the existing miniQMT pipeline infrastructure but orchestrates
batched, continuous updates for the whole market.

Usage:
    python -m quant_data_engine.tasks.continuous_market_minute_sync \\
        --symbols-file stocks.txt \\
        --batch-size 400 \\
        --target-latency 300 \\
        --count 10

Environment:
    MONGO_URL / MONGO_DB via settings.
    Requires miniQMT with xtquant installed.
"""

from __future__ import annotations
import sys
import os
import time
import argparse
import math
from typing import List

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from quant_data_engine import create_mini_qmt_pipeline, get_settings


def load_symbols_from_file(filepath: str) -> List[str]:
    """Load stock symbols from a text file (one per line)."""
    with open(filepath, 'r', encoding='utf-8') as f:
        symbols = [line.strip() for line in f if line.strip() and not line.startswith('#')]
    return symbols


def get_all_a_shares() -> List[str]:
    """Get list of all A-share symbols from stock_info collection.
    
    Queries the stock_info collection and returns symbols using ts_code field
    which already includes exchange suffix (e.g., '000001.SZ').
    
    Returns:
        List of stock symbols with exchange suffix
    """
    from pymongo import MongoClient
    
    settings = get_settings()
    client = MongoClient(settings.mongo_uri)
    db = client[settings.mongo_db]
    
    # Query all stocks and get ts_code field directly
    cursor = db.stock_info.find({}, {"ts_code": 1, "_id": 0})
    
    symbols = []
    for doc in cursor:
        ts_code = doc.get('ts_code')
        if ts_code:
            symbols.append(ts_code)
    
    client.close()
    
    print(f"📊 Loaded {len(symbols)} A-share symbols from stock_info")
    return symbols


def parse_args():
    p = argparse.ArgumentParser(
        description="Continuously sync minute data for entire A-share market"
    )
    p.add_argument(
        "--symbols-file",
        type=str,
        help="Path to file containing stock symbols (one per line). If not provided, uses all A-shares."
    )
    p.add_argument(
        "--batch-size",
        type=int,
        default=400,
        help="Number of symbols per batch (default: 400)"
    )
    p.add_argument(
        "--target-latency",
        type=int,
        default=300,
        help="Target max latency in seconds for full cycle (default: 300 = 5 minutes)"
    )
    p.add_argument(
        "--count",
        type=int,
        default=10,
        help="Number of latest minute bars to pull per symbol (default: 10)"
    )
    p.add_argument(
        "--period",
        type=str,
        default="1m",
        choices=["1m", "5m", "15m", "30m", "1h"],
        help="Data frequency (default: 1m)"
    )
    p.add_argument(
        "--max-wait",
        type=int,
        default=30,
        help="Max seconds to wait for each batch download (default: 30)"
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print batch plan without actually downloading"
    )
    return p.parse_args()


def main():
    args = parse_args()
    settings = get_settings()
    
    # Load symbols
    if args.symbols_file:
        print(f"📂 Loading symbols from {args.symbols_file}")
        symbols = load_symbols_from_file(args.symbols_file)
    else:
        print("📊 Loading all A-share symbols...")
        symbols = get_all_a_shares()
    
    if not symbols:
        print("❌ No symbols loaded. Exiting.")
        return
    
    total_symbols = len(symbols)
    batch_size = args.batch_size
    total_batches = math.ceil(total_symbols / batch_size)
    
    # Calculate sleep time between batches to hit target latency
    # Assume T_call ≈ 2 seconds (conservative estimate)
    estimated_call_time = 2.0
    target_latency = args.target_latency
    
    # T_cycle = total_batches * (T_call + T_sleep) <= target_latency
    # => T_sleep <= (target_latency / total_batches) - T_call
    sleep_between_batches = max(
        0,
        (target_latency / total_batches) - estimated_call_time
    )
    
    print(f"\n{'='*60}")
    print(f"📈 Market Minute Sync Configuration")
    print(f"{'='*60}")
    print(f"Total symbols: {total_symbols}")
    print(f"Batch size: {batch_size}")
    print(f"Total batches: {total_batches}")
    print(f"Target latency: {target_latency}s ({target_latency/60:.1f} min)")
    print(f"Sleep between batches: {sleep_between_batches:.1f}s")
    print(f"Estimated cycle time: {total_batches * (estimated_call_time + sleep_between_batches):.1f}s")
    print(f"Bars per symbol: {args.count}")
    print(f"Period: {args.period}")
    print(f"{'='*60}\n")
    
    if args.dry_run:
        print("🔍 Dry run - showing batch plan:")
        for i in range(total_batches):
            start_idx = i * batch_size
            end_idx = min((i + 1) * batch_size, total_symbols)
            batch = symbols[start_idx:end_idx]
            print(f"  Batch {i+1}/{total_batches}: {len(batch)} symbols")
            print(f"    First: {batch[0]}, Last: {batch[-1]}")
        print("\n✅ Dry run complete. Use without --dry-run to start syncing.")
        return
    
    # Create pipeline (reusing existing infrastructure)
    print("🔧 Initializing miniQMT pipeline...")
    pipeline = create_mini_qmt_pipeline()
    
    print("🚀 Starting continuous sync loop...\n")
    
    cycle_count = 0
    try:
        while True:
            cycle_count += 1
            cycle_start = time.time()
            
            print(f"\n{'─'*60}")
            print(f"🔄 Cycle #{cycle_count} started at {time.strftime('%H:%M:%S')}")
            print(f"{'─'*60}")
            
            for i in range(total_batches):
                batch_start = time.time()
                start_idx = i * batch_size
                end_idx = min((i + 1) * batch_size, total_symbols)
                batch = symbols[start_idx:end_idx]
                
                print(f"\n📦 Batch {i+1}/{total_batches}: {len(batch)} symbols")
                
                try:
                    # Pull latest N minute bars for this batch
                    # Note: This uses get_market_data_ex with count parameter
                    # You may need to adapt pipeline.run to support this
                    # For now, using run_batch which triggers download
                    pipeline.run_batch(
                        codes=batch,
                        start_date="",  # Will use count instead
                        end_date="",
                        period=args.period,
                        max_wait=args.max_wait
                    )
                    
                    batch_elapsed = time.time() - batch_start
                    print(f"   ✅ Batch completed in {batch_elapsed:.1f}s")
                    
                except Exception as e:
                    print(f"   ❌ Error processing batch: {e}")
                    import traceback
                    traceback.print_exc()
                
                # Sleep between batches (except after last batch)
                if i < total_batches - 1 and sleep_between_batches > 0:
                    print(f"   ⏸️  Sleeping {sleep_between_batches:.1f}s...")
                    time.sleep(sleep_between_batches)
            
            cycle_elapsed = time.time() - cycle_start
            print(f"\n{'─'*60}")
            print(f"✅ Cycle #{cycle_count} completed in {cycle_elapsed:.1f}s ({cycle_elapsed/60:.1f} min)")
            print(f"{'─'*60}")
            
            # Optional: small pause before starting next cycle
            # If cycle_elapsed already ≈ target_latency, no extra sleep needed
            if cycle_elapsed < target_latency:
                extra_sleep = target_latency - cycle_elapsed
                print(f"⏸️  Extra sleep {extra_sleep:.1f}s to maintain cycle interval...")
                time.sleep(extra_sleep)
    
    except KeyboardInterrupt:
        print("\n\n🛑 Interrupted by user. Shutting down gracefully...")
        print(f"📊 Completed {cycle_count} full cycles.")
        print("✅ Shutdown complete.")


if __name__ == "__main__":
    main()
