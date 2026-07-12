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
import random
import argparse
import logging
from datetime import datetime
import yfinance as yf
from pymongo import MongoClient

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from quant_data_engine.config.settings import get_settings

logger = logging.getLogger("quant_data_engine.global_market_sync")

# 默认常见的外部宏观指标和外盘指数
TICKERS_CONFIG = {
    "indices": ["^IXIC", "^GSPC", "^DJI", "^RUT", "^VIX", "000001.SS", "FXI", "^N225"],
    "us_index_futures": ["ES=F", "YM=F", "NQ=F", "RTY=F"],
    "tech_giants": ["NVDA", "TSLA", "AAPL", "MSFT", "AMD", "INTC", "GOOG", "META", "AMZN"],
    "china_concepts": ["PGJ", "BABA", "PDD", "JD", "NIO", "2823.HK"],
    "commodities": ["GC=F", "SI=F", "HG=F", "CL=F", "ALI=F"],
    "crypto": ["BTC-USD", "ETH-USD"],
    "forex": ["DX-Y.NYB", "USDCNH=X", "CNY=X"],
    "bonds": ["^TNX", "^TYX"]
}

DEFAULT_SYMBOLS = []
for category_symbols in TICKERS_CONFIG.values():
    DEFAULT_SYMBOLS.extend(category_symbols)

FUTURES_SYMBOLS = set(TICKERS_CONFIG.get("us_index_futures", []))

# Rate-limiting knobs — tune these if Yahoo changes their threshold.
INTER_SYMBOL_DELAY_MIN = 1.5   # seconds between individual ticker requests
INTER_SYMBOL_DELAY_MAX = 3.0
INTER_BATCH_DELAY      = 5.0   # seconds between category batches
MAX_RETRIES            = 3
RETRY_BASE_DELAY       = 2.0   # exponential backoff base (2^attempt + jitter)


def _safe_float(value):
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _pick_value(mapping, *keys):
    """Pick first non-null value from dict-like object."""
    if mapping is None:
        return None
    for key in keys:
        try:
            val = mapping.get(key)
        except Exception:
            val = None
        if val is not None:
            return val
    return None


def _normalize_change_pct(raw):
    """
    Normalize percent-like fields from Yahoo:
    - Some fields may be ratio form (e.g. -0.0135 for -1.35%)
    - Some may already be percent form.
    """
    val = _safe_float(raw)
    if val is None:
        return None
    # Heuristic: if within [-2, 2], treat as ratio and convert to percent.
    if -2.0 < val < 2.0:
        return val * 100.0
    return val


def _compute_change_pct(price: float, prev: float):
    if prev is None or prev == 0:
        return None
    return ((price - prev) / prev) * 100.0


def parse_args():
    p = argparse.ArgumentParser(description="Run Global Market Quotes Sync")
    p.add_argument("--symbols", nargs="+", default=DEFAULT_SYMBOLS, help="Symbols to fetch")
    p.add_argument("--interval", type=int, default=60, help="Polling interval in seconds")
    p.add_argument("--once", action="store_true", help="Run only once and exit")
    p.add_argument("--sync-uri", type=str, default=None, help="Optional secondary MongoDB URI to mirror data to (e.g., remote cloud db)")
    return p.parse_args()

def _is_rate_limit_error(exc: Exception) -> bool:
    """Return True when the exception looks like a Yahoo 429 / rate-limit response."""
    msg = str(exc).lower()
    return any(kw in msg for kw in ("429", "too many requests", "rate limit", "rate limited"))


def _fetch_one_symbol(symbol: str, max_retries: int = MAX_RETRIES) -> dict | None:
    """Fetch a single ticker with retry + exponential backoff on 429.

    Returns the same dict as the per-symbol branch of the old ``fetch_quotes``,
    or ``None`` when all retries are exhausted.
    """
    for attempt in range(max_retries):
        try:
            ticker = yf.Ticker(symbol)

            # --- fast_info extraction (same logic as before) ---
            fi = getattr(ticker, "fast_info", None)
            fi_dict = {}
            try:
                fi_dict = dict(fi.items()) if fi is not None and hasattr(fi, "items") else {}
            except Exception:
                fi_dict = {}

            price = _safe_float(_pick_value(fi_dict, "lastPrice", "last_price"))

            if symbol in FUTURES_SYMBOLS:
                prev = _safe_float(
                    _pick_value(fi_dict, "regularMarketPreviousClose", "regular_market_previous_close",
                                "previousClose", "previous_close")
                )
            else:
                prev = _safe_float(
                    _pick_value(fi_dict, "previousClose", "previous_close",
                                "regularMarketPreviousClose", "regular_market_previous_close")
                )

            change_pct = None
            change_source = ""
            prev_source = ""

            change_raw = _pick_value(fi_dict, "regularMarketChangePercent", "regular_market_change_percent")
            change_pct = _normalize_change_pct(change_raw)
            if change_pct is not None:
                change_source = "fast_info.regularMarketChangePercent"

            if change_pct is None and price is not None and prev is not None:
                change_pct = _compute_change_pct(price, prev)
                change_source = "computed_from_fast_info"

            if prev is not None:
                prev_source = (
                    "regularMarketPreviousClose" if symbol in FUTURES_SYMBOLS else "previousClose"
                )

            # --- fallback via 1m history ---
            if price is None or change_pct is None:
                hist = ticker.history(period="2d", interval="1m", prepost=True)
                if len(hist) >= 2:
                    hist_price = _safe_float(hist["Close"].iloc[-1])
                    hist_prev = _safe_float(hist["Close"].iloc[-2])
                    if price is None:
                        price = hist_price
                    if change_pct is None and hist_price is not None and hist_prev is not None:
                        change_pct = _compute_change_pct(hist_price, hist_prev)
                        change_source = "computed_from_1m_history"
                elif len(hist) == 1 and price is None:
                    price = _safe_float(hist["Close"].iloc[-1])

            if price is not None:
                return {
                    "price": round(price, 2),
                    "change_pct": round(change_pct, 2) if change_pct is not None else 0.0,
                    "prev_close": round(prev, 2) if prev is not None else None,
                    "change_pct_source": change_source or "unknown",
                    "prev_close_source": prev_source or "unknown",
                    "timestamp": datetime.now().isoformat(),
                }
            else:
                # fast_info returned nothing useful — treat as a soft failure for this attempt
                raise ValueError(f"No price data for {symbol} (attempt {attempt + 1})")

        except Exception as exc:
            if _is_rate_limit_error(exc):
                if attempt < max_retries - 1:
                    wait = (RETRY_BASE_DELAY ** (attempt + 1)) + random.uniform(0.5, 2.0)
                    logger.warning(
                        "[GlobalMarketSync] 429 on %s — retrying in %.1fs (attempt %d/%d)",
                        symbol, wait, attempt + 1, max_retries,
                    )
                    time.sleep(wait)
                    continue
                else:
                    logger.error(
                        "[GlobalMarketSync] 429 exhausted retries for %s (%d attempts)",
                        symbol, max_retries,
                    )
            else:
                logger.error("[GlobalMarketSync] Error fetching %s: %s", symbol, exc)
            return None

    return None


def fetch_quotes(symbols):
    """
    Fetch real-time / near-real-time global quotes via individual ``yf.Ticker``
    calls with inter-symbol delay, exponential backoff on 429, and batch-aware
    pacing so Yahoo rate-limits are respected.

    Symbols are processed in the order defined by *TICKERS_CONFIG* so that
    category batches get a longer pause between them.
    """
    # Build category-ordered symbol list (same order as TICKERS_CONFIG)
    ordered_symbols = []
    batch_boundaries = []  # indices after which a category batch ends
    for category, syms in TICKERS_CONFIG.items():
        ordered_symbols.extend(syms)
        batch_boundaries.append((category, len(ordered_symbols)))

    # Only fetch what was requested (--symbols may be a subset)
    requested = set(symbols)
    ordered = [s for s in ordered_symbols if s in requested]

    result = {}
    success_count = 0
    fail_count = 0

    batch_start_idx = 0
    for category, batch_end in batch_boundaries:
        batch_syms = [s for s in ordered[batch_start_idx:batch_end] if s in requested]
        batch_start_idx = batch_end
        if not batch_syms:
            continue

        logger.info(
            "[GlobalMarketSync] Batch '%s' (%d symbols): %s",
            category, len(batch_syms), ", ".join(batch_syms),
        )

        for symbol in batch_syms:
            data = _fetch_one_symbol(symbol)
            if data is not None:
                result[symbol] = data
                success_count += 1
            else:
                fail_count += 1

            # Randomised inter-symbol delay
            delay = random.uniform(INTER_SYMBOL_DELAY_MIN, INTER_SYMBOL_DELAY_MAX)
            time.sleep(delay)

        # Longer pause between category batches (unless this is the last batch)
        if batch_syms and batch_end < len(ordered):
            logger.debug(
                "[GlobalMarketSync] Pausing %.1fs between batches...", INTER_BATCH_DELAY,
            )
            time.sleep(INTER_BATCH_DELAY)

    logger.info(
        "[GlobalMarketSync] fetch_quotes done: %d success, %d failed, total %d requested",
        success_count, fail_count, len(requested),
    )
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
