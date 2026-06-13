#!/usr/bin/env python3
"""Sync 沪深港股通持股明细 (hk_hold) by trade_date snapshot.

Full-market fetch by ``trade_date`` (北向 A 股 + 南向港股). Since 2024-08 the
northbound holding detail is published quarterly (sampled at quarter-ends), so
the default target dates are recent quarter-ends; use ``--daily`` to backfill
the pre-2024 daily era over an explicit range.

Drives the ``hk_hold_sync`` data contract (run_recency model).

Usage:
    # Contract repair invokes: --recent-quarters 2
    python -m quant_data_engine.tasks.run_hk_hold_sync --recent-quarters 2
    python -m quant_data_engine.tasks.run_hk_hold_sync --start 20240801 --end 20260331
    python -m quant_data_engine.tasks.run_hk_hold_sync --start 20230313 --end 20240801 --daily
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

import pymongo

from quant_data_engine import get_settings
from quant_data_engine.adapters.tushare_adapter import TushareAdapter
from quant_data_engine.contracts.reconciler import mature_quarter_ends, quarter_ends_in_range
from quant_data_engine.pipelines.shareholder_pipeline import ShareholderPipeline
from quant_data_engine.repositories.shareholder_repository import ShareholderRepository
from quant_data_engine.utils.ingestion_job import record_ingestion_run

logger = logging.getLogger(__name__)
JOB = "hk_hold_sync"


def parse_args():
    p = argparse.ArgumentParser(description="Sync 沪深港股通持股 by trade_date")
    p.add_argument("--start", help="Start date YYYYMMDD")
    p.add_argument("--end", help="End date YYYYMMDD (default: today)")
    p.add_argument("--recent-quarters", dest="recent_quarters", type=int, default=2,
                   help="When no range given, latest N quarter-end snapshots")
    p.add_argument("--daily", action="store_true",
                   help="Iterate every calendar day in range (pre-2024 daily era backfill)")
    return p.parse_args()


def daterange(start: str, end: str) -> list[str]:
    s = datetime.strptime(start, "%Y%m%d")
    e = datetime.strptime(end, "%Y%m%d")
    out = []
    while s <= e:
        out.append(s.strftime("%Y%m%d"))
        s += timedelta(days=1)
    return out


def resolve_dates(args) -> list[str]:
    today = datetime.now().strftime("%Y%m%d")
    if args.start:
        end = args.end or today
        return daterange(args.start, end) if args.daily else quarter_ends_in_range(args.start, end)
    mature = mature_quarter_ends("20240801", today)
    return mature[-max(args.recent_quarters, 1):]


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    args = parse_args()
    settings = get_settings()
    dates = resolve_dates(args)
    if not dates:
        logger.warning("no dates resolved; nothing to do")
        return
    logger.info("hk_hold sync %s..%s (%d dates, daily=%s)", dates[0], dates[-1], len(dates), args.daily)

    client = pymongo.MongoClient(settings.mongo_uri)
    repo = ShareholderRepository(client[settings.data_db])
    adapter = TushareAdapter(settings.tushare_token, rate_limit_per_sec=settings.rate_limit_per_sec)
    pipeline = ShareholderPipeline(adapter, repo)

    end_marker = max(dates)
    try:
        result = pipeline.run_hk_hold(dates)
    except Exception as e:
        record_ingestion_run(settings, JOB, end_marker, status="failed", error=str(e))
        raise
    logger.info("hk_hold sync done: %s", result)
    record_ingestion_run(settings, JOB, end_marker, records_changed=int(result.get("records") or 0))


if __name__ == "__main__":
    main()
