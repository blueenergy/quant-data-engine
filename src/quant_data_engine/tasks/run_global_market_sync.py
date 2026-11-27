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
import math
from datetime import datetime
from urllib.parse import quote

import requests
import yfinance as yf
from pymongo import MongoClient

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from quant_data_engine.config.settings import get_settings
from quant_data_engine.utils.global_session_bar import (
    compose_forming_bar,
    futures_night_bar,
    parse_chart_daily,
    session_bar_is_stale,
    session_state,
)
from quant_data_engine.utils.eastmoney_quote import fetch_eastmoney_quotes
from quant_data_engine.utils.overseas_proxy import enable_overseas_proxy

logger = logging.getLogger("quant_data_engine.global_market_sync")

# 默认常见的外部宏观指标和外盘指数
TICKERS_CONFIG = {
    "indices": ["^IXIC", "^GSPC", "^DJI", "^RUT", "^VIX", "000001.SS", "FXI", "^N225", "^HSI"],
    "us_index_futures": ["ES=F", "YM=F", "NQ=F", "RTY=F"],
    "tech_giants": ["NVDA", "TSLA", "AAPL", "MSFT", "AMD", "INTC", "GOOG", "META", "AMZN"],
    "china_concepts": ["PGJ", "BABA", "PDD", "JD", "NIO", "2823.HK"],
    # 港股/亚洲隔夜：恒指/日经已在 indices，这里补亚洲其余风向标。
    # HSTECH.HK 是恒生科技（港股科技，与A股科技联动最直接）；
    # ^TWII 台湾加权与 ^KS11 KOSPI 是全球半导体周期的另外两个读数。
    "hk_asia": ["HSTECH.HK", "^HSCE", "^KS11", "^TWII"],
    # 半导体龙头个股：SMH/SOXX 只有板块均值，台积电/ASML/美光/博通/ARM
    # 才是A股半导体开盘真正盯的隔夜锚点。
    "semis_leaders": ["TSM", "ASML", "MU", "AVGO", "ARM"],
    # 主题科技：ANT 是 Anthropic（2026-11-19 纳斯达克上市，真正的 AI 模型公司）；
    # 其余是各自主题的 ETF，**不是 AI 公司**——AIQ 是宽口径 AI 主题、
    # CHAT 是生成式 AI、BOTZ 以工业机器人为重、ARKW 是宽口径颠覆性科技。
    # 实测（n=500）：控制 QQQ 后这批标的对 A 股次日无增量解释力（偏相关 <= 0.09），
    # 只当隔夜情绪背景，不作为板块信号。
    "thematic_tech": ["ANT", "AIQ", "CHAT", "BOTZ", "ARKW"],
    # 企业软件：ORCL/CRM/PLTR/NOW/SNOW 都是有 AI 产品线的软件/云公司，
    # **不是 AI 原生公司**；与 Anthropic 的合作关系因公司而异，不逐一断言。
    "enterprise_software": ["ORCL", "CRM", "PLTR", "NOW", "SNOW"],
    "commodities": ["GC=F", "SI=F", "HG=F", "CL=F", "BZ=F", "ALI=F"],
    "crypto": ["BTC-USD", "ETH-USD"],
    "forex": ["DX-Y.NYB", "USDCNH=X", "CNY=X", "USDJPY=X"],
    "bonds": ["^TNX", "^TYX"],
    # Every symbol the K-line chart offers needs a live quote, otherwise it
    # can never show the session that is still open.  These are the ones the
    # daily sync writes bars for but the quote sync did not cover.
    "kline_broad_etfs": ["SPY", "QQQ", "XLK", "ARKK"],
    "kline_sector_etfs": ["XLE", "XLF", "XLV", "XLI", "XLU", "XLRE"],
    "kline_semis": ["SOXX", "SMH", "^SOX"],
    "kline_country_etfs": ["EWJ", "EWG", "EWY"],
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

# The chart endpoint carries what spark lacks: the session's opening print and
# Yahoo's own date label for the daily bar.  One symbol per request, so it is
# only called when a symbol actually starts a new session.
CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart"
CHART_TIMEOUT = 20

# symbol -> {date, open, session_start, local_date, fetched_at}
_SESSION_BAR_CACHE: dict = {}

# Fallback path (per-symbol yfinance) + shared retry knobs.
INTER_SYMBOL_DELAY_MIN = 1.5   # seconds between individual ticker requests
INTER_SYMBOL_DELAY_MAX = 3.0
MAX_RETRIES            = 3
RETRY_BASE_DELAY       = 2.0   # RETRY_BASE_DELAY ^ (attempt+1) + jitter: attempt 0→~2.5s, 1→~4.5s


def _safe_float(value):
    try:
        if value is None:
            return None
        f = float(value)
        return f if math.isfinite(f) else None
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


def _resolve_change_pct(meta: dict, price, prev):
    """Pick the percentage change to store, and say where it came from.

    Yahoo's own ``regularMarketChangePercent`` wins over the value computed
    from ``previousClose``.  spark's ``previousClose`` lags a **full session**
    for markets whose session has already closed on the current day — the
    Asian indices (``^HSI`` / ``^HSCE`` / ``^TWII`` and ``000001.SS``).  The
    computed percentage was then wrong for all four, and for ``^TWII`` even
    had the wrong sign (+3.76% against a real -4.80%).  Measured against the
    chart daily bars, Yahoo's own value matched the true daily move for 71 of
    the 77 tracked symbols; the remaining six are futures (settlement price
    baseline, unchanged by this) and crypto (where Yahoo's own value is the
    UTC-day baseline).
    """
    own = _safe_float(meta.get("regularMarketChangePercent"))
    computed = _compute_change_pct(price, prev)
    if own is None:
        return computed, "computed_from_spark"
    # Guard against a fraction-form value (1.5 meaning 150%): if scaling by
    # 100 fits the computed value better, Yahoo must have returned a ratio.
    if (
        computed is not None
        and abs(own - computed) > 0.5
        and abs(own * 100.0 - computed) < abs(own - computed)
    ):
        own = own * 100.0
    return own, "spark.regularMarketChangePercent"


def _reconcile_prev_close(price, prev, change_pct):
    """Return ``(prev_close, source)`` consistent with the stored percentage.

    ``prev_close`` feeds the forming bar, so it has to be the close the
    percentage was measured against.  Where spark's ``previousClose`` lags a
    session the two disagree, and the price/percentage pair is the
    authoritative one — so the previous close is re-derived from it.
    """
    if price is None or change_pct is None or change_pct <= -100:
        return prev, ("spark.previousClose" if prev is not None else "unknown")
    implied = price / (1.0 + change_pct / 100.0)
    if prev is None or abs(implied - prev) > abs(implied) * 0.001:
        return implied, "derived_from_change_pct"
    return prev, "spark.previousClose"


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


def _quote_from_spark_meta(meta: dict, bars: list | None = None) -> dict | None:
    """Build the stored quote document from a spark ``meta`` block."""
    price = _safe_float(meta.get("regularMarketPrice"))
    if price is None:
        return None
    prev = _safe_float(meta.get("previousClose"))
    change_pct, change_pct_source = _resolve_change_pct(meta, price, prev)
    prev, prev_close_source = _reconcile_prev_close(price, prev, change_pct)
    return {
        "price": round(price, 2),
        "change_pct": round(change_pct, 2) if change_pct is not None else None,
        "prev_close": round(prev, 2) if prev is not None else None,
        "change_pct_source": change_pct_source if change_pct is not None else "unknown",
        "prev_close_source": prev_close_source,
        "timestamp": datetime.now().isoformat(),
        # Raw meta + 5m closes, consumed by the forming-bar pass and dropped
        # before the document is written to Mongo.
        "_meta": meta,
        "_bars": bars or [],
    }


def _extract_spark_bars(response: dict) -> list:
    """Pull ``[(timestamp, close), ...]`` out of a spark response.

    Spark carries only closes for its 5-minute bars (open/high/low are null),
    which is enough to aggregate a futures night session.
    """
    timestamps = response.get("timestamp") or []
    quote = ((response.get("indicators") or {}).get("quote") or [{}])[0] or {}
    closes = quote.get("close") or []
    return [(ts, close) for ts, close in zip(timestamps, closes)]


def _fetch_chart_daily(symbol: str) -> dict | None:
    """Fetch Yahoo's current daily bar for one symbol (one request)."""
    url = f"{CHART_URL}/{quote(symbol, safe='')}?interval=1d&range=1d&includePrePost=false"
    try:
        resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=CHART_TIMEOUT)
        resp.raise_for_status()
        payload = resp.json()
    except Exception as exc:
        logger.warning("[GlobalMarketSync] chart daily failed for %s: %s", symbol, exc)
        return None
    results = ((payload or {}).get("chart") or {}).get("result") or []
    if not results:
        return None
    return parse_chart_daily(results[0])


def _ensure_session_bar(symbol: str, session: dict) -> dict | None:
    """Return the session bar (date + open), fetching it when the cache is stale."""
    cache = _SESSION_BAR_CACHE.get(symbol)
    now = time.time()
    if not session_bar_is_stale(cache, session, now):
        return cache
    bar = _fetch_chart_daily(symbol)
    if bar is None:
        # Keep whatever we had — a failed refresh must not drop a good bar.
        return cache if cache and cache.get("date") else None
    cache = {
        "date": bar["date"],
        "open": bar.get("open"),
        "session_start": session.get("session_start"),
        "local_date": session.get("local_date"),
        "fetched_at": now,
    }
    _SESSION_BAR_CACHE[symbol] = cache
    return cache


def _apply_eastmoney_quotes(result: dict) -> None:
    """Replace Yahoo's delayed Asian quotes with East Money's real-time ones.

    Yahoo serves the Asian indices 15-20 minutes late, so during their trading
    hours the card and the unclosed K-line bar show a stale price.  East Money
    serves the same indices live.

    The Yahoo ``meta`` block is patched rather than the finished document, so
    that everything downstream — the stored price, the percentage, and the
    forming bar's high / low / volume — is rebuilt from the same real-time
    numbers and stays internally consistent.  Daily bars are untouched: they
    come from the separate daily sync.
    """
    quotes = fetch_eastmoney_quotes(list(result.keys()))
    if not quotes:
        return

    for symbol, em in quotes.items():
        doc = result.get(symbol)
        if not doc:
            continue
        meta = doc.get("_meta")
        price = em.get("price")
        prev = em.get("prev_close")
        if price is None:
            continue

        # Captured before the patch below, so the log can show how far
        # Yahoo's delayed snapshot was from the real price.
        yahoo_price = _safe_float((meta or {}).get("regularMarketPrice"))
        yahoo_time = _safe_float((meta or {}).get("regularMarketTime"))

        if meta is not None:
            meta["regularMarketPrice"] = price
            if prev is not None:
                meta["previousClose"] = prev
            # Yahoo's own percentage is as delayed as its price, so drop it:
            # leaving it in place would silently reinstate the stale number.
            meta.pop("regularMarketChangePercent", None)
            if em.get("high") is not None:
                meta["regularMarketDayHigh"] = em["high"]
            if em.get("low") is not None:
                meta["regularMarketDayLow"] = em["low"]
            if em.get("volume") is not None:
                meta["regularMarketVolume"] = em["volume"]
            if em.get("time") is not None:
                # The real trade time, so the session check and the displayed
                # quote timestamp stop pointing at Yahoo's delayed snapshot.
                meta["regularMarketTime"] = int(em["time"])

        # Computed from East Money's own price and previous close rather than
        # taken from a field, so the two can never disagree.  No reconcile step
        # is needed here: the pair already comes from one source.
        change_pct = _compute_change_pct(price, prev)

        doc["price"] = round(price, 2)
        doc["change_pct"] = round(change_pct, 2) if change_pct is not None else None
        doc["prev_close"] = round(prev, 2) if prev is not None else None
        doc["change_pct_source"] = "eastmoney.push2" if change_pct is not None else "unknown"
        doc["prev_close_source"] = "eastmoney.f18" if prev is not None else "unknown"

        # Log how far Yahoo's delayed snapshot was from the real price.  This
        # is the evidence for the switch: during Asian hours it is ~15-20
        # minutes of movement, and at every other time it collapses to ~0.
        if yahoo_price is not None and yahoo_time is not None:
            logger.info(
                "[EastMoney] %s price=%.2f (yahoo %.2f, diff %+.2f%%) "
                "quote_age=%.0fs real_time=%.0fs",
                symbol, price, yahoo_price,
                (price - yahoo_price) / yahoo_price * 100.0 if yahoo_price else 0.0,
                time.time() - yahoo_time,
                time.time() - em["time"] if em.get("time") else float("nan"),
            )


def _attach_forming_bars(result: dict) -> None:
    """Give every quote whose market is open the bar of the session in progress.

    The daily K-line collection only ever holds closed sessions (it is written
    at 06:30 Beijing time, after the US close), so without this the chart
    stops at the previous close while the market is still moving.
    """
    for symbol, doc in result.items():
        meta = doc.pop("_meta", None)
        bars = doc.pop("_bars", None) or []
        if not meta:
            continue
        session = session_state(meta)
        if not session["in_session"]:
            continue

        # Futures: between 18:00 and midnight ET the running session already
        # belongs to the next CME trading day, while Yahoo still labels it with
        # today's date - the one the daily sync has stored as closed.
        night = futures_night_bar(symbol, meta, bars)
        if night:
            forming = compose_forming_bar(meta, night, doc.get("price"), doc.get("prev_close"))
            forming["high"] = night["high"]
            forming["low"] = night["low"]
            forming["volume"] = night["volume"]
            if forming.get("open") is not None and forming.get("close") is not None:
                doc["forming"] = forming
            continue

        bar = _ensure_session_bar(symbol, session)
        if not bar or not bar.get("date"):
            continue
        forming = compose_forming_bar(meta, bar, doc.get("price"), doc.get("prev_close"))
        if forming.get("open") is None or forming.get("close") is None:
            continue
        doc["forming"] = forming


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
                if not responses:
                    continue
                meta = responses[0].get("meta")
                if not meta:
                    continue
                doc = _quote_from_spark_meta(meta, _extract_spark_bars(responses[0]))
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

    _apply_eastmoney_quotes(result)
    _attach_forming_bars(result)

    logger.info(
        "[GlobalMarketSync] fetch_quotes done: %d success, %d failed, total %d requested, %d forming",
        len(result), len(requested) - len(result), len(requested),
        sum(1 for d in result.values() if d.get("forming")),
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
                    # A market that is no longer trading must not keep an old
                    # "forming" bar around — $set never removes a field.
                    data.setdefault("forming", None)

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
