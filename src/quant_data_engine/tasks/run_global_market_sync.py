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
from urllib.parse import quote

import requests
import yfinance as yf
from pymongo import MongoClient

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from quant_data_engine.config.settings import get_settings
from quant_data_engine.utils.overseas_proxy import enable_overseas_proxy

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

# Primary path: Yahoo's spark endpoint returns price + previousClose for up to
# 20 symbols in ONE request. 39 symbols therefore cost 2 requests per cycle
# instead of ~78 (each yf.Ticker().fast_info triggers several HTTP calls).
SPARK_URL = "https://query1.finance.yahoo.com/v7/finance/spark"
SPARK_MAX_SYMBOLS = 20
SPARK_TIMEOUT = 20
SPARK_BATCH_DELAY = 0.5        # seconds between spark requests
# Yahoo answers 429 to requests without a browser-ish User-Agent.
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)

# Fallback path (per-symbol yfinance) + shared retry knobs.
INTER_SYMBOL_DELAY_MIN = 1.5   # seconds between individual ticker requests
INTER_SYMBOL_DELAY_MAX = 3.0
MAX_RETRIES            = 3
RETRY_BASE_DELAY       = 2.0   # RETRY_BASE_DELAY ^ (attempt+1) + jitter: attempt 0→~2.5s, 1→~4.5s


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


# Order matters. ``regularMarketPreviousClose`` is the official prior-session
# close. Since yfinance 1.x, ``FastInfo.previous_close`` is derived from 1h
# prepost bars (last print of the previous day, after-hours included), so it
# must never be preferred — it flips the sign of the daily change on names
# that trade extended hours.
PREV_CLOSE_KEYS = (
    "regularMarketPreviousClose",
    "regular_market_previous_close",
    "previousClose",
    "previous_close",
)


def _pick_prev_close(fi_dict):
    """Return ``(value, key)`` for the official previous close, or ``(None, "")``."""
    for key in PREV_CLOSE_KEYS:
        val = _safe_float(fi_dict.get(key))
        if val is not None:
            return val, key
    return None, ""


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


def _quote_from_spark_meta(meta: dict) -> dict | None:
    """Build the stored quote document from a spark ``meta`` block."""
    price = _safe_float(meta.get("regularMarketPrice"))
    if price is None:
        return None
    prev = _safe_float(meta.get("previousClose"))
    change_pct = _compute_change_pct(price, prev) if prev else None
    return {
        "price": round(price, 2),
        "change_pct": round(change_pct, 2) if change_pct is not None else None,
        "prev_close": round(prev, 2) if prev is not None else None,
        "change_pct_source": "computed_from_spark" if change_pct is not None else "unknown",
        "prev_close_source": "spark.previousClose" if prev is not None else "unknown",
        "timestamp": datetime.now().isoformat(),
    }


def _fetch_spark_batch(symbols, max_retries: int = MAX_RETRIES) -> dict | None:
    """Fetch up to ``SPARK_MAX_SYMBOLS`` quotes in one request.

    Returns ``{symbol: quote}``, or ``None`` when every attempt failed (the
    caller then falls back to the per-symbol path).
    """
    url = (
        f"{SPARK_URL}?symbols={quote(','.join(symbols), safe=',=^.-')}"
        "&range=1d&interval=5m"
    )

    for attempt in range(max_retries):
        try:
            resp = requests.get(
                url, headers={"User-Agent": USER_AGENT}, timeout=SPARK_TIMEOUT
            )
            resp.raise_for_status()
            payload = resp.json()
            items = (payload.get("spark") or {}).get("result")
            if not items:
                raise RuntimeError(f"empty spark result: {str(payload)[:200]}")

            quotes = {}
            for item in items:
                responses = item.get("response") or []
                meta = responses[0].get("meta") if responses else None
                if not meta:
                    continue
                doc = _quote_from_spark_meta(meta)
                if doc is not None:
                    quotes[item.get("symbol")] = doc
            return quotes

        except Exception as exc:
            if attempt < max_retries - 1:
                wait = (RETRY_BASE_DELAY ** (attempt + 1)) + random.uniform(0.5, 2.0)
                logger.warning(
                    "[GlobalMarketSync] spark batch failed (%s) — retrying in %.1fs (attempt %d/%d)",
                    exc, wait, attempt + 1, max_retries,
                )
                time.sleep(wait)
            else:
                logger.error(
                    "[GlobalMarketSync] spark batch failed for %d symbol(s): %s",
                    len(symbols), exc,
                )
    return None


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

            prev, prev_source = _pick_prev_close(fi_dict)

            change_pct = None
            change_source = ""

            change_raw = _pick_value(fi_dict, "regularMarketChangePercent", "regular_market_change_percent")
            change_pct = _normalize_change_pct(change_raw)
            if change_pct is not None:
                change_source = "fast_info.regularMarketChangePercent"

            if change_pct is None and price is not None and prev is not None:
                change_pct = _compute_change_pct(price, prev)
                change_source = "computed_from_fast_info"

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
                    "change_pct": round(change_pct, 2) if change_pct is not None else None,
                    "prev_close": round(prev, 2) if prev is not None else None,
                    "change_pct_source": change_source or "unknown",
                    "prev_close_source": prev_source or "unknown",
                    "timestamp": datetime.now().isoformat(),
                }

            # fast_info returned nothing useful — soft failure for this attempt
            logger.debug(
                "[GlobalMarketSync] No price for %s (attempt %d/%d)",
                symbol, attempt + 1, max_retries,
            )
            return None

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
    """Fetch quotes in spark batches of 20, with a per-symbol fallback.

    Two HTTP requests cover the default 39 symbols. Any batch that fails
    after retries (or any symbol missing from a successful batch) is retried
    through the per-symbol ``_fetch_one_symbol`` path, which is slower but
    uses the chart endpoint instead of spark.
    """
    result: dict = {}
    requested = list(dict.fromkeys(symbols))  # de-dup, keep order
    fallback_syms: list[str] = []

    batches = [
        requested[i:i + SPARK_MAX_SYMBOLS]
        for i in range(0, len(requested), SPARK_MAX_SYMBOLS)
    ]

    for idx, batch in enumerate(batches):
        logger.info(
            "[GlobalMarketSync] spark batch %d/%d (%d symbols): %s",
            idx + 1, len(batches), len(batch), ", ".join(batch),
        )
        quotes = _fetch_spark_batch(batch)
        if quotes is None:
            fallback_syms.extend(batch)
        else:
            result.update(quotes)
            missing = [s for s in batch if s not in quotes]
            if missing:
                logger.warning(
                    "[GlobalMarketSync] spark missing %d symbol(s): %s",
                    len(missing), ", ".join(missing),
                )
                fallback_syms.extend(missing)
        if idx < len(batches) - 1:
            time.sleep(SPARK_BATCH_DELAY)

    if fallback_syms:
        logger.warning(
            "[GlobalMarketSync] falling back to per-symbol fetch for %d symbol(s): %s",
            len(fallback_syms), ", ".join(fallback_syms),
        )
        for symbol in fallback_syms:
            data = _fetch_one_symbol(symbol)
            if data is not None:
                result[symbol] = data
            else:
                logger.error("[GlobalMarketSync] fallback failed for %s", symbol)
            time.sleep(random.uniform(INTER_SYMBOL_DELAY_MIN, INTER_SYMBOL_DELAY_MAX))

    logger.info(
        "[GlobalMarketSync] fetch_quotes done: %d success, %d failed, total %d requested",
        len(result), len(requested) - len(result), len(requested),
    )
    return result

def main():
    args = parse_args()
    settings = get_settings()

    # Only this task (and the other overseas ones) needs the proxy; the
    # container no longer sets HTTP(S)_PROXY globally.
    enable_overseas_proxy()
    
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
