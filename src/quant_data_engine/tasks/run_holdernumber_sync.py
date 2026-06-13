#!/usr/bin/env python3
"""Sync 股东户数 (stk_holdernumber) by disclosure-date range (paginated).

Tushare returns holder-number disclosures at many dates (month-ends, quarter-ends,
special dates), and a single full-market call is capped (~5000 rows), so we page
by offset over a disclosure-date range. Drives the ``stk_holdernumber_sync`` data
contract (run_recency model).

Usage:
    # Contract repair invokes: --days 120
    python -m quant_data_engine.tasks.run_holdernumber_sync --days 120
    python -m quant_data_engine.tasks.run_holdernumber_sync --start 20180101 --end 20260601
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
from quant_data_engine.pipelines.shareholder_pipeline import ShareholderPipeline
from quant_data_engine.repositories.shareholder_repository import ShareholderRepository
from quant_data_engine.utils.ingestion_job import record_ingestion_run

logger = logging.getLogger(__name__)
JOB = "stk_holdernumber_sync"


def parse_args():
    p = argparse.ArgumentParser(description="Sync 股东户数 by disclosure-date range")
    p.add_argument("--start", help="Start disclosure date YYYYMMDD")
    p.add_argument("--end", help="End disclosure date YYYYMMDD (default: today)")
    p.add_argument("--days", type=int, default=120, help="When no range given, recent N calendar days")
    return p.parse_args()


def resolve_range(args) -> tuple[str, str]:
    today = datetime.now().strftime("%Y%m%d")
    end = args.end or today
    if args.start:
        return args.start, end
    start = (datetime.now() - timedelta(days=max(args.days, 1))).strftime("%Y%m%d")
    return start, end


def year_chunks(start: str, end: str) -> list[tuple[str, str]]:
    """Split [start, end] into per-calendar-year windows.

    Tushare caps stk_holdernumber pagination at offset ~150k, and a multi-year
    full-market sweep exceeds that, so we bound each paginated sweep to one year
    (~15-25k rows, well under the ceiling).
    """
    s = datetime.strptime(start, "%Y%m%d")
    e = datetime.strptime(end, "%Y%m%d")
    chunks: list[tuple[str, str]] = []
    y = s.year
    while datetime(y, 1, 1) <= e:
        lo = max(s, datetime(y, 1, 1)).strftime("%Y%m%d")
        hi = min(e, datetime(y, 12, 31)).strftime("%Y%m%d")
        chunks.append((lo, hi))
        y += 1
    return chunks


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    args = parse_args()
    settings = get_settings()
    start, end = resolve_range(args)
    logger.info("holdernumber sync range %s..%s", start, end)

    client = pymongo.MongoClient(settings.mongo_uri)
    repo = ShareholderRepository(client[settings.data_db])
    adapter = TushareAdapter(settings.tushare_token, rate_limit_per_sec=settings.rate_limit_per_sec)
    pipeline = ShareholderPipeline(adapter, repo)

    chunks = year_chunks(start, end)
    total = 0
    status = "success"
    try:
        for lo, hi in chunks:
            res = pipeline.run_holdernumber(lo, hi)
            total += int(res.get("records") or 0)
            if res.get("status") != "success":
                status = "partial"
    except Exception as e:
        record_ingestion_run(settings, JOB, end, status="failed", error=str(e))
        raise
    logger.info("holdernumber sync done: chunks=%d records=%d status=%s", len(chunks), total, status)
    record_ingestion_run(settings, JOB, end, status="complete" if status == "success" else "partial",
                         records_changed=total)


if __name__ == "__main__":
    main()
