"""Daily sync task: fetch US/global market OHLCV history via yfinance.

Writes into ``global_market_daily`` collection (quant_data DB).
Document schema::

    {
        "symbol":     "SOXX",
        "date":       "20260502",   # YYYYMMDD, aligned with A-share schema
        "open":       220.1,
        "high":       225.0,
        "low":        218.5,
        "close":      223.4,
        "volume":     1234567,
        "updated_at": ISODate
    }

Unique index: (symbol, date)

Usage::

    # Incremental daily update (default: last 5 trading days)
    python -m quant_data_engine.tasks.run_global_market_daily_sync

    # Initial back-fill (pulls 2 years of history)
    python -m quant_data_engine.tasks.run_global_market_daily_sync --backfill

    # Specific period
    python -m quant_data_engine.tasks.run_global_market_daily_sync --start 20240101 --end 20260430

    # Dry-run: fetch but do NOT write
    python -m quant_data_engine.tasks.run_global_market_daily_sync --dry-run

Environment variables (via .env or shell):
    MONGO_URI, DATA_DB_NAME
    HTTP_PROXY / HTTPS_PROXY  (set automatically in overseas deploy mode)
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import datetime, timedelta
from typing import List, Optional

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from quant_data_engine.config.settings import get_settings
from quant_data_engine.utils.ingestion_job import record_ingestion_run

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Target symbols (covers US_SECTOR_PROFILES + macro anchors + global indices)
# ---------------------------------------------------------------------------
DEFAULT_SYMBOLS: List[str] = [
    # US broad market
    "SPY", "^GSPC", "^DJI", "^RUT",
    # Nasdaq / tech
    "QQQ", "XLK", "^IXIC",
    # Semiconductor ETFs / index
    "SOXX", "SMH", "^SOX",
    # Sector ETFs
    "XLE", "XLF", "XLV", "XLI", "XLU", "XLRE",
    # Innovation / growth
    "ARKK",
    # Volatility + rates
    "^VIX", "^TNX", "^TYX",
    # USD index
    "DX-Y.NYB",
    # Global indices
    "^N225",   # Nikkei 225
    "^HSI",    # Hang Seng
    "000001.SS",  # Shanghai Composite
    "FXI",     # China Large-Cap ETF
    "EWJ",     # Japan ETF
    "EWG",     # Germany ETF
    "EWY",     # South Korea ETF
    # Commodities
    "GC=F",    # Gold futures
    "CL=F",    # Crude Oil futures
    "SI=F",    # Silver futures
    # Crypto
    "BTC-USD", "ETH-USD",
    # US index futures
    "ES=F", "NQ=F",
    # Tech giants (crowding signal)
    "NVDA", "AMD", "AAPL", "MSFT", "TSLA", "GOOG", "META", "AMZN", "INTC",
    # China concepts
    "BABA", "PDD", "JD", "NIO",
    "2823.HK",  # iShares A50 China (HK)
    "PGJ",      # Golden Dragon ETF
    # US index futures (additional)
    "YM=F",     # 道指期货
    "RTY=F",    # 罗素期货
]


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="美股/全球市场日线历史数据同步（yfinance → global_market_daily）"
    )
    p.add_argument(
        "--backfill",
        action="store_true",
        help="初始回填：拉取2年历史数据（约504个交易日）",
    )
    p.add_argument("--start", type=str, default=None, help="起始日期 YYYYMMDD")
    p.add_argument("--end",   type=str, default=None, help="结束日期 YYYYMMDD（默认今天）")
    p.add_argument(
        "--symbols",
        nargs="+",
        default=None,
        metavar="SYMBOL",
        help=f"指定品种列表（默认全部 {len(DEFAULT_SYMBOLS)} 个）",
    )
    p.add_argument(
        "--sync-uri",
        type=str,
        default=os.environ.get("SECONDARY_MONGO_URI"),
        help="可选：同步写入到远端 MongoDB URI",
    )
    p.add_argument("--sleep", type=float, default=1.5,
                   help="每个 symbol 间隔秒数，防止 429（默认 1.5s）")
    p.add_argument("--dry-run", action="store_true",
                   help="获取数据但不写入 MongoDB")
    return p.parse_args()


# ---------------------------------------------------------------------------
# Core sync logic
# ---------------------------------------------------------------------------

def _to_yf_period(start: Optional[str], end: Optional[str], backfill: bool) -> dict:
    """Build kwargs for yf.Ticker.history()."""
    if backfill:
        return {"period": "2y"}
    if start:
        end_dt = end or datetime.today().strftime("%Y-%m-%d")
        # yfinance expects YYYY-MM-DD
        start_fmt = f"{start[:4]}-{start[4:6]}-{start[6:]}"
        end_fmt   = f"{end_dt[:4]}-{end_dt[4:6]}-{end_dt[6:]}"
        return {"start": start_fmt, "end": end_fmt}
    # default: last 5 trading days for incremental update
    return {"period": "5d"}


def sync_symbol(symbol: str, yf_kwargs: dict, coll, dry_run: bool) -> int:
    """Fetch and upsert one symbol. Returns number of docs upserted."""
    try:
        import yfinance as yf
    except ImportError:
        logger.error("yfinance not installed. Run: pip install yfinance")
        return 0

    try:
        ticker = yf.Ticker(symbol)
        hist = ticker.history(**yf_kwargs, auto_adjust=True)
    except Exception as exc:
        logger.warning("Failed to fetch %s: %s", symbol, exc)
        return 0

    if hist.empty:
        logger.warning("No data returned for %s", symbol)
        return 0

    upserted = 0
    now = datetime.utcnow()
    for dt_idx, row in hist.iterrows():
        date_str = dt_idx.strftime("%Y%m%d")
        doc = {
            "symbol":     symbol,
            "date":       date_str,
            "open":       round(float(row["Open"]),   4) if row["Open"]   == row["Open"] else None,
            "high":       round(float(row["High"]),   4) if row["High"]   == row["High"] else None,
            "low":        round(float(row["Low"]),    4) if row["Low"]    == row["Low"]  else None,
            "close":      round(float(row["Close"]),  4) if row["Close"]  == row["Close"] else None,
            "volume":     int(row["Volume"]) if row["Volume"] == row["Volume"] else None,
            "updated_at": now,
        }
        if not dry_run:
            coll.update_one(
                {"symbol": symbol, "date": date_str},
                {"$set": doc},
                upsert=True,
            )
        upserted += 1

    logger.info("  %s: %d rows upserted%s", symbol, upserted, " [dry-run]" if dry_run else "")
    return upserted


def ensure_index(coll) -> None:
    """Ensure unique compound index on (symbol, date)."""
    coll.create_index(
        [("symbol", 1), ("date", 1)],
        unique=True,
        background=True,
    )


def main() -> None:
    args = parse_args()
    settings = get_settings()

    from pymongo import MongoClient
    client = MongoClient(settings.mongo_uri)
    coll = client[settings.data_db]["global_market_daily"]
    ensure_index(coll)

    # Optional secondary (mirror) collection
    sync_coll = None
    if args.sync_uri and not args.dry_run:
        try:
            sync_client = MongoClient(args.sync_uri)
            sync_coll = sync_client[settings.data_db]["global_market_daily"]
            ensure_index(sync_coll)
            logger.info("Secondary MongoDB connected for mirroring: %s", args.sync_uri[:40])
        except Exception as exc:
            logger.warning("Failed to connect secondary MongoDB, mirroring disabled: %s", exc)
            sync_coll = None

    symbols = args.symbols or DEFAULT_SYMBOLS
    yf_kwargs = _to_yf_period(args.start, args.end, args.backfill)

    logger.info(
        "run_global_market_daily_sync: symbols=%d yf_kwargs=%s dry_run=%s",
        len(symbols), yf_kwargs, args.dry_run,
    )

    total_ok = 0
    total_err = 0

    for symbol in symbols:
        logger.info("Syncing %s ...", symbol)
        n = sync_symbol(symbol, yf_kwargs, coll, args.dry_run)
        # Mirror to secondary if available
        if sync_coll is not None and n > 0:
            try:
                sync_symbol(symbol, yf_kwargs, sync_coll, False)
            except Exception as exc:
                logger.warning("Secondary mirror failed for %s: %s", symbol, exc)
        if n > 0:
            total_ok += 1
        else:
            total_err += 1
        time.sleep(args.sleep)

    client.close()

    logger.info(
        "run_global_market_daily_sync complete: ok=%d errors=%d",
        total_ok, total_err,
    )

    if not args.dry_run:
        record_ingestion_run(
            settings,
            job_type="global_market_daily_sync",
            date_str=datetime.today().strftime("%Y%m%d"),
            status="complete" if total_err == 0 else "partial",
            error=f"{total_err} symbol(s) failed" if total_err else None,
            records_changed=total_ok,
        )

    if total_err > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
