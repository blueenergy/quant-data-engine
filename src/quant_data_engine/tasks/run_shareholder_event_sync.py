#!/usr/bin/env python3
"""Sync announcement-driven shareholder events from Tushare.

Covers three interfaces by ``ann_date`` (full market per day):
  - stk_holdertrade : 股东增减持
  - share_float     : 限售股解禁（前瞻 float_date）
  - repurchase      : 股票回购（含 proc 阶段）

Drives the ``shareholder_event_sync`` data contract (run_recency model).

Usage:
    # Contract repair invokes: --days 7
    python -m quant_data_engine.tasks.run_shareholder_event_sync --days 7
    python -m quant_data_engine.tasks.run_shareholder_event_sync --start 20180101 --end 20260430
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
JOB = "shareholder_event_sync"


def parse_args():
    p = argparse.ArgumentParser(description="Sync shareholder events by ann_date")
    p.add_argument("--start", help="Start ann_date YYYYMMDD")
    p.add_argument("--end", help="End ann_date YYYYMMDD (default: today)")
    p.add_argument("--days", type=int, default=7, help="When no range given, recent N calendar days")
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
        return daterange(args.start, args.end or today)
    start = (datetime.now() - timedelta(days=max(args.days, 1))).strftime("%Y%m%d")
    return daterange(start, today)


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    args = parse_args()
    settings = get_settings()
    dates = resolve_dates(args)
    if not dates:
        logger.warning("no dates resolved; nothing to do")
        return
    logger.info("shareholder event sync %s..%s (%d days)", dates[0], dates[-1], len(dates))

    client = pymongo.MongoClient(settings.mongo_uri)
    repo = ShareholderRepository(client[settings.data_db])
    adapter = TushareAdapter(settings.tushare_token, rate_limit_per_sec=settings.rate_limit_per_sec)
    pipeline = ShareholderPipeline(adapter, repo)

    end_marker = dates[-1]
    try:
        result = pipeline.run_events(dates)
    except Exception as e:
        record_ingestion_run(settings, JOB, end_marker, status="failed", error=str(e))
        raise
    logger.info("shareholder event sync done: %s", result)
    record_ingestion_run(settings, JOB, end_marker, records_changed=int(result.get("records") or 0))


if __name__ == "__main__":
    main()
