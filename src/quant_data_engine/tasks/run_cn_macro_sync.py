"""CLI task — sync China macro economic data from Tushare to MongoDB.

Data written to quant_data DB:
  cn_macro_rates     — daily:    shibor / shibor_lpr
  cn_macro_monthly   — monthly:  pmi / cpi / ppi / m / sf
  cn_macro_quarterly — quarterly: gdp

Strategy B: monthly and quarterly series are skipped when the current period
already exists in the DB, avoiding redundant Tushare calls.

Usage:
  # Default: fetch recent 3 months + recent 5 quarters (daily rates always fetched)
  python -m quant_data_engine.tasks.run_cn_macro_sync

  # Historical backfill
  python -m quant_data_engine.tasks.run_cn_macro_sync --backfill-months 24 --backfill-quarters 20

  # Dry-run
  python -m quant_data_engine.tasks.run_cn_macro_sync --dry-run
"""
from __future__ import annotations

import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

import argparse
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from pymongo import MongoClient, ASCENDING
from pymongo.collection import Collection

from quant_data_engine.config.settings import get_settings
from quant_data_engine.adapters.tushare_adapter import TushareAdapter

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


# --------------------------------------------------------------------------- #
# helpers                                                                       #
# --------------------------------------------------------------------------- #

def _months_ago(n: int, ref: Optional[datetime] = None) -> str:
    dt = ref or datetime.now()
    month = dt.month - n
    year = dt.year
    while month <= 0:
        month += 12
        year -= 1
    return f"{year}{month:02d}"


def _quarters_ago(n: int, ref: Optional[datetime] = None) -> str:
    dt = ref or datetime.now()
    q = (dt.month - 1) // 3 + 1
    year = dt.year
    q -= n
    while q <= 0:
        q += 4
        year -= 1
    return f"{year}Q{q}"


def _current_quarter(ref: Optional[datetime] = None) -> str:
    dt = ref or datetime.now()
    q = (dt.month - 1) // 3 + 1
    return f"{dt.year}Q{q}"


def _upsert_rows(col: Collection, rows: List[Dict[str, Any]], filter_keys: List[str]) -> int:
    """Bulk upsert rows using filter_keys as the natural key. Returns count written."""
    written = 0
    for row in rows:
        filt = {k: row[k] for k in filter_keys if k in row}
        if not filt:
            continue
        col.update_one(filt, {"$set": row}, upsert=True)
        written += 1
    return written


def _df_to_dicts(df) -> List[Dict[str, Any]]:
    if df is None or df.empty:
        return []
    return [
        {k: (None if (v != v) else v) for k, v in row.items()}
        for row in df.to_dict("records")
    ]


# --------------------------------------------------------------------------- #
# sync functions                                                                #
# --------------------------------------------------------------------------- #

def _date_chunks(start_date: str, end_date: str, chunk_days: int = 365) -> List[tuple]:
    """Split [start_date, end_date] into chunks of at most chunk_days calendar days.

    Returns list of (chunk_start_str, chunk_end_str) in YYYYMMDD format.
    Tushare daily macro interfaces cap at ~2000 rows per call; chunking by year
    keeps each request well within that limit.
    """
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


def sync_daily_rates(
    adapter: TushareAdapter,
    rates_col: Collection,
    days: int,
    dry_run: bool,
) -> Dict[str, int]:
    """Sync daily rate series: shibor, shibor_lpr.

    When days > 400 the date range is automatically split into 365-day chunks
    to avoid exceeding Tushare's ~2000-row per-call limit.
    """
    end_date   = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=days)).strftime("%Y%m%d")
    chunks     = _date_chunks(start_date, end_date)
    stats: Dict[str, int] = {}

    fetchers = {
        "shibor":     adapter.fetch_shibor,
        "shibor_lpr": adapter.fetch_shibor_lpr,
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
                    total_written += _upsert_rows(rates_col, rows, ["data_type", "date"])
                else:
                    total_written += len(rows)
            except Exception as e:
                logger.warning(f"  [{dtype}] chunk {c_start}~{c_end} failed: {e}")
        logger.info(f"  [{dtype}] total upserted: {total_written} (chunks={len(chunks)})")
        stats[dtype] = total_written

    return stats


def sync_monthly(
    adapter: TushareAdapter,
    monthly_col: Collection,
    backfill_months: int,
    dry_run: bool,
) -> Dict[str, int]:
    """Sync monthly series with Strategy-B skip: skip if current month already exists."""
    current_month = datetime.now().strftime("%Y%m")
    start_m = _months_ago(backfill_months)
    end_m = datetime.now().strftime("%Y%m")
    stats: Dict[str, int] = {}

    series = [
        ("pmi", lambda: adapter.fetch_cn_pmi(start_m=start_m, end_m=end_m)),
        ("cpi", lambda: adapter.fetch_cn_cpi(start_m=start_m, end_m=end_m)),
        ("ppi", lambda: adapter.fetch_cn_ppi(start_m=start_m, end_m=end_m)),
        ("m",   lambda: adapter.fetch_cn_m(start_m=start_m, end_m=end_m)),
        ("sf",  lambda: adapter.fetch_sf_month(start_m=start_m, end_m=end_m)),
    ]

    for dtype, fetch_fn in series:
        # Strategy B: skip if current month's data already in DB
        if not dry_run and monthly_col.find_one({"data_type": dtype, "month": current_month}):
            logger.info(f"  [{dtype}] {current_month} already in DB — skipping Tushare call")
            stats[dtype] = 0
            continue

        try:
            df = fetch_fn()
            rows = _df_to_dicts(df)
            for row in rows:
                row["data_type"] = dtype
            logger.info(f"  [{dtype}] fetched {len(rows)} rows")
            if not dry_run and rows:
                written = _upsert_rows(monthly_col, rows, ["data_type", "month"])
                stats[dtype] = written
            else:
                stats[dtype] = len(rows)
        except Exception as e:
            logger.warning(f"  [{dtype}] failed: {e}")
            stats[dtype] = 0

    return stats


def sync_quarterly(
    adapter: TushareAdapter,
    quarterly_col: Collection,
    backfill_quarters: int,
    dry_run: bool,
) -> Dict[str, int]:
    """Sync quarterly GDP with Strategy-B skip."""
    current_q = _current_quarter()
    start_q = _quarters_ago(backfill_quarters)
    end_q = current_q
    stats: Dict[str, int] = {}

    # Strategy B: skip if current quarter's data already in DB
    if not dry_run and quarterly_col.find_one({"data_type": "gdp", "quarter": current_q}):
        logger.info(f"  [gdp] {current_q} already in DB — skipping Tushare call")
        stats["gdp"] = 0
        return stats

    try:
        df = adapter.fetch_cn_gdp(start_q=start_q, end_q=end_q)
        rows = _df_to_dicts(df)
        for row in rows:
            row["data_type"] = "gdp"
        logger.info(f"  [gdp] fetched {len(rows)} rows")
        if not dry_run and rows:
            written = _upsert_rows(quarterly_col, rows, ["data_type", "quarter"])
            stats["gdp"] = written
        else:
            stats["gdp"] = len(rows)
    except Exception as e:
        logger.warning(f"  [gdp] failed: {e}")
        stats["gdp"] = 0

    return stats


# --------------------------------------------------------------------------- #
# argparse                                                                      #
# --------------------------------------------------------------------------- #

def parse_args():
    p = argparse.ArgumentParser(description="Sync China macro economic data from Tushare")
    p.add_argument("--backfill-months", type=int, default=3,
                   help="Months of history to fetch for monthly series (default: 3)")
    p.add_argument("--backfill-quarters", type=int, default=5,
                   help="Quarters of history to fetch for quarterly GDP (default: 5)")
    p.add_argument("--daily-days", type=int, default=10,
                   help="Calendar days to fetch for daily rate series (default: 10)")
    p.add_argument("--dry-run", action="store_true",
                   help="Fetch data but do not write to MongoDB")
    p.add_argument("--verbose", action="store_true", help="Verbose logging")
    return p.parse_args()


# --------------------------------------------------------------------------- #
# main                                                                          #
# --------------------------------------------------------------------------- #

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

    # Ensure indexes
    if not args.dry_run:
        data_db["cn_macro_rates"].create_index(
            [("data_type", ASCENDING), ("date", ASCENDING)], unique=True, background=True
        )
        data_db["cn_macro_monthly"].create_index(
            [("data_type", ASCENDING), ("month", ASCENDING)], unique=True, background=True
        )
        data_db["cn_macro_quarterly"].create_index(
            [("data_type", ASCENDING), ("quarter", ASCENDING)], unique=True, background=True
        )

    logger.info("=== CN Macro Sync ===")
    logger.info(f"  daily_days={args.daily_days}  backfill_months={args.backfill_months}"
                f"  backfill_quarters={args.backfill_quarters}  dry_run={args.dry_run}")

    logger.info("--- Daily rates (shibor / lpr / wz / gz) ---")
    rate_stats = sync_daily_rates(
        adapter, data_db["cn_macro_rates"], args.daily_days, args.dry_run
    )

    logger.info("--- Monthly series (pmi / cpi / ppi / m / sf) ---")
    monthly_stats = sync_monthly(
        adapter, data_db["cn_macro_monthly"], args.backfill_months, args.dry_run
    )

    logger.info("--- Quarterly GDP ---")
    quarterly_stats = sync_quarterly(
        adapter, data_db["cn_macro_quarterly"], args.backfill_quarters, args.dry_run
    )

    total = sum(rate_stats.values()) + sum(monthly_stats.values()) + sum(quarterly_stats.values())
    logger.info(f"=== Done: {total} rows upserted (dry_run={args.dry_run}) ===")
    logger.info(f"  rates: {rate_stats}")
    logger.info(f"  monthly: {monthly_stats}")
    logger.info(f"  quarterly: {quarterly_stats}")


if __name__ == "__main__":
    main()
