"""CLI task — sync US Treasury rates from Tushare to MongoDB.

Data written to quant_data.us_rates_daily with natural key (data_type, date):
  tycr   — nominal yield curve (1Y/2Y/3Y/5Y/7Y/10Y/20Y/30Y)
  trycr  — real yield curve / TIPS (5Y/7Y/10Y/20Y/30Y)
  tbr    — T-Bill rates (4W/13W/26W/52W)
  tltr   — long-term nominal average
  trltr  — long-term real average

All series are daily frequency; no Strategy-B skip needed.

Usage:
  # Default: recent 5 calendar days (daily cron)
  python -m quant_data_engine.tasks.run_us_rates_sync

  # Backfill ~2 months
  python -m quant_data_engine.tasks.run_us_rates_sync --backfill

  # Custom window
  python -m quant_data_engine.tasks.run_us_rates_sync --days 30

  # Dry-run
  python -m quant_data_engine.tasks.run_us_rates_sync --dry-run
"""
from __future__ import annotations

import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

import argparse
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List

from pymongo import MongoClient, ASCENDING
from pymongo.collection import Collection

from quant_data_engine.config.settings import get_settings
from quant_data_engine.adapters.tushare_adapter import TushareAdapter

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

_BACKFILL_DAYS = 60


def _df_to_dicts(df) -> List[Dict[str, Any]]:
    if df is None or df.empty:
        return []
    return [
        {k: (None if (v != v) else v) for k, v in row.items()}
        for row in df.to_dict("records")
    ]


def _upsert_rows(col: Collection, rows: List[Dict[str, Any]], filter_keys: List[str]) -> int:
    written = 0
    for row in rows:
        filt = {k: row[k] for k in filter_keys if k in row}
        if not filt:
            continue
        col.update_one(filt, {"$set": row}, upsert=True)
        written += 1
    return written


def _date_chunks(start_date: str, end_date: str, chunk_days: int = 365) -> List[tuple]:
    """Split [start_date, end_date] into at-most chunk_days-day windows (YYYYMMDD strings)."""
    from datetime import datetime, timedelta
    fmt = "%Y%m%d"
    start_dt = datetime.strptime(start_date, fmt)
    end_dt   = datetime.strptime(end_date,   fmt)
    chunks = []
    cur = start_dt
    while cur <= end_dt:
        chunk_end = min(cur + timedelta(days=chunk_days - 1), end_dt)
        chunks.append((cur.strftime(fmt), chunk_end.strftime(fmt)))
        cur = chunk_end + timedelta(days=1)
    return chunks


def sync_us_rates(
    adapter: TushareAdapter,
    col: Collection,
    start_date: str,
    end_date: str,
    dry_run: bool,
) -> Dict[str, int]:
    """Sync US Treasury rate series.

    Automatically chunks into 365-day windows to stay within Tushare's
    ~2000-row per-call limit (needed for multi-year backfills).
    """
    stats: Dict[str, int] = {}
    chunks = _date_chunks(start_date, end_date)

    fetchers = {
        "tycr":  adapter.fetch_us_tycr,
        "trycr": adapter.fetch_us_trycr,
        "tbr":   adapter.fetch_us_tbr,
        "tltr":  adapter.fetch_us_tltr,
        "trltr": adapter.fetch_us_trltr,
    }

    for dtype, fetch_fn in fetchers.items():
        total_written = 0
        for c_start, c_end in chunks:
            try:
                df = fetch_fn(start_date=c_start, end_date=c_end)
                rows = _df_to_dicts(df)
                for row in rows:
                    row["data_type"] = dtype
                if not dry_run and rows:
                    total_written += _upsert_rows(col, rows, ["data_type", "date"])
                else:
                    total_written += len(rows)
            except Exception as e:
                logger.warning(f"  [{dtype}] chunk {c_start}~{c_end} failed: {e}")
        logger.info(f"  [{dtype}] total upserted: {total_written} (chunks={len(chunks)})")
        stats[dtype] = total_written

    return stats


def parse_args():
    p = argparse.ArgumentParser(description="Sync US Treasury rates from Tushare")
    p.add_argument("--days", type=int, default=5,
                   help="Calendar days lookback for incremental sync (default: 5)")
    p.add_argument("--backfill", action="store_true",
                   help=f"Fetch full {_BACKFILL_DAYS}-day history (overrides --days)")
    p.add_argument("--dry-run", action="store_true",
                   help="Fetch data but do not write to MongoDB")
    p.add_argument("--verbose", action="store_true", help="Verbose logging")
    return p.parse_args()


def main():
    args = parse_args()
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    settings = get_settings()
    adapter = TushareAdapter(
        settings.tushare_token,
        rate_limit_per_sec=settings.rate_limit_per_sec,
    )

    client = MongoClient(settings.mongo_uri)
    data_db = client[settings.data_db]
    col = data_db["us_rates_daily"]

    if not args.dry_run:
        col.create_index(
            [("data_type", ASCENDING), ("date", ASCENDING)], unique=True, background=True
        )

    lookback = _BACKFILL_DAYS if args.backfill else args.days
    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=lookback)).strftime("%Y%m%d")

    logger.info("=== US Rates Sync ===")
    logger.info(f"  window={start_date}~{end_date}  dry_run={args.dry_run}")

    stats = sync_us_rates(adapter, col, start_date, end_date, args.dry_run)

    total = sum(stats.values())
    logger.info(f"=== Done: {total} rows upserted (dry_run={args.dry_run}) ===")
    logger.info(f"  {stats}")


if __name__ == "__main__":
    main()
