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
TICKERS_CONFIG = {
    "indices": ["^IXIC", "^GSPC", "^DJI", "^RUT", "^VIX", "000001.SS", "FXI", "^N225"],
    "tech_giants": ["NVDA", "TSLA", "AAPL", "MSFT", "AMD", "INTC", "GOOG", "META", "AMZN"],
    "china_concepts": ["PGJ", "BABA", "PDD", "JD", "NIO", "2823.HK"],
    "commodities": ["GC=F", "SI=F", "HG=F", "CL=F", "ALI=F"],
    "crypto": ["BTC-USD", "ETH-USD"],
    "forex": ["DX-Y.NYB", "CNH=X", "CNY=X"],
    "bonds": ["^TNX", "^TYX"]
}

DEFAULT_SYMBOLS = []
for category_symbols in TICKERS_CONFIG.values():
    DEFAULT_SYMBOLS.extend(category_symbols)

def parse_args():
    p = argparse.ArgumentParser(description="Run Global Market Quotes Sync")
    p.add_argument("--symbols", nargs="+", default=DEFAULT_SYMBOLS, help="Symbols to fetch")
    p.add_argument("--interval", type=int, default=60, help="Polling interval in seconds")
    p.add_argument("--once", action="store_true", help="Run only once and exit")
    p.add_argument("--sync-uri", type=str, default=None, help="Optional secondary MongoDB URI to mirror data to (e.g., remote cloud db)")
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
    
    # Primary DB Connection
    print(f"Connecting to Primary MongoDB at {settings.mongo_uri} (db: {settings.data_db})")
    try:
        client = MongoClient(settings.mongo_uri)
        db = client[settings.data_db]
        collection = db["global_market_quotes"]
        collection.create_index("symbol", unique=True)
    except Exception as e:
        print(f"Failed to connect to Primary MongoDB: {e}")
        sys.exit(1)
        
    # Secondary DB Connection (Optional)
    sync_collection = None
    if args.sync_uri:
        print(f"Connecting to Secondary MongoDB (mirror) at {args.sync_uri} (db: {settings.data_db})")
        try:
            sync_client = MongoClient(args.sync_uri)
            sync_db = sync_client[settings.data_db]
            sync_collection = sync_db["global_market_quotes"]
            sync_collection.create_index("symbol", unique=True)
            print("✅ Secondary MongoDB connected.")
        except Exception as e:
            print(f"⚠️ Failed to connect to Secondary MongoDB: {e}. Mirroring disabled.")
            
    print(f"Starting Global Market Sync. Targets: {args.symbols}")
    
    while True:
        try:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Fetching quotes...")
            quotes = fetch_quotes(args.symbols)
            
            if quotes:
                # Update MongoDB
                primary_updates = 0
                secondary_updates = 0
                
                for symbol, data in quotes.items():
                    data["symbol"] = symbol
                    data["updated_at"] = datetime.now()
                    
                    # Write to primary
                    collection.update_one(
                        {"symbol": symbol},
                        {"$set": data},
                        upsert=True
                    )
                    primary_updates += 1
                    
                    # Write to secondary if configured
                    if sync_collection is not None:
                        try:
                            sync_collection.update_one(
                                {"symbol": symbol},
                                {"$set": data},
                                upsert=True
                            )
                            secondary_updates += 1
                        except Exception as e:
                            print(f"  [Mirror] Error updating {symbol} to secondary DB: {e}")
                            
                msg = f"✅ Success: Updated {primary_updates} symbols in Primary."
                if sync_collection is not None:
                    msg += f" Mirrored {secondary_updates} to Secondary."
                print(msg)
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
