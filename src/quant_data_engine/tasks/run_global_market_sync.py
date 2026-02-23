"""Standalone script to sync global market quotes (Yahoo Finance) to MongoDB.

Usage:
    # Run once
    python -m quant_data_engine.tasks.run_global_market_sync --once
    
    # Run periodically
    python -m quant_data_engine.tasks.run_global_market_sync --interval 60
"""
from __future__ import annotations
import os
import sys
import time
import argparse
from datetime import datetime
import yfinance as yf
from pymongo import MongoClient

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from quant_data_engine.config.settings import get_settings

# 默认常见的外部宏观指标和外盘指数
DEFAULT_SYMBOLS = ["^IXIC", "^DJI", "FXI", "GC=F", "CL=F", "^RUT", "^VIX", "^N225"]

def parse_args():
    p = argparse.ArgumentParser(description="Run Global Market Quotes Sync")
    p.add_argument("--symbols", nargs="+", default=DEFAULT_SYMBOLS, help="Symbols to fetch")
    p.add_argument("--interval", type=int, default=60, help="Polling interval in seconds")
    p.add_argument("--once", action="store_true", help="Run only once and exit")
    return p.parse_args()

def fetch_quotes(symbols):
    """
    使用 yfinance 抓取实时/准实时外盘和宏观数据。
    yfinance 会自动使用系统环境变量中的 HTTP_PROXY / HTTPS_PROXY。
    """
    tickers = yf.Tickers(" ".join(symbols))
    result = {}
    
    for symbol in symbols:
        try:
            ticker = tickers.tickers[symbol]
            
            # Attempt to get fast_info first (lighter weight)
            if hasattr(ticker, 'fast_info'):
                try:
                    price = ticker.fast_info.last_price
                    prev = ticker.fast_info.previous_close
                    if price is not None and prev is not None:
                        change_pct = ((price - prev) / prev) * 100
                        result[symbol] = {
                            "price": round(price, 2),
                            "change_pct": round(change_pct, 2),
                            "timestamp": datetime.now().isoformat()
                        }
                        time.sleep(0.3)  # Anti-throttling
                        continue
                except Exception:
                    pass
            
            # Fallback to fetching 5 days history
            hist = ticker.history(period="5d")
            if len(hist) >= 2:
                price = hist['Close'].iloc[-1]
                prev = hist['Close'].iloc[-2]
                change_pct = 0.0
                if prev:
                    change_pct = ((price - prev) / prev) * 100
                result[symbol] = {
                    "price": round(price, 2),
                    "change_pct": round(change_pct, 2),
                    "timestamp": datetime.now().isoformat()
                }
            elif len(hist) == 1:
                price = hist['Close'].iloc[-1]
                result[symbol] = {
                    "price": round(price, 2),
                    "change_pct": 0.0,
                    "timestamp": datetime.now().isoformat()
                }
            time.sleep(0.3)
            
        except Exception as e:
            print(f"[GlobalMarketSync] Error fetching {symbol}: {e}")
            continue
            
    return result

def main():
    args = parse_args()
    settings = get_settings()
    
    print(f"Connecting to MongoDB at {settings.mongo_uri} (db: {settings.mongo_db})")
    try:
        client = MongoClient(settings.mongo_uri)
        db = client[settings.mongo_db]
        collection = db["global_market_quotes"]
        # Ensure index on symbol
        collection.create_index("symbol", unique=True)
    except Exception as e:
        print(f"Failed to connect to MongoDB: {e}")
        sys.exit(1)
        
    print(f"Starting Global Market Sync. Targets: {args.symbols}")
    
    while True:
        try:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Fetching quotes...")
            quotes = fetch_quotes(args.symbols)
            
            if quotes:
                # Update MongoDB
                updates = 0
                for symbol, data in quotes.items():
                    data["symbol"] = symbol
                    data["updated_at"] = datetime.now()
                    collection.update_one(
                        {"symbol": symbol},
                        {"$set": data},
                        upsert=True
                    )
                    updates += 1
                print(f"✅ Successfully updated {updates} symbols in MongoDB.")
            else:
                print("⚠️ No quotes fetched.")
                
        except Exception as e:
            print(f"❌ Error in sync loop: {e}")
            
        if args.once:
            break
            
        print(f"💤 Sleeping for {args.interval} seconds...")
        time.sleep(args.interval)

if __name__ == "__main__":
    main()
