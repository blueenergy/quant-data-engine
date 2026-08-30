#!/usr/bin/env python3
"""从 fund_holdings 生成公募抄作业快照（观察名单 + 产品/股票 diff）。

用法:
  python -m quant_data_engine.tasks.run_fund_copywork_snapshot
  python -m quant_data_engine.tasks.run_fund_copywork_snapshot --period-prev 20250630 --period-curr 20251231
  python -m quant_data_engine.tasks.run_fund_copywork_snapshot --period-prev 20251231 --period-curr 20260331 --quality-mode provisional
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime

from pymongo import MongoClient

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from quant_data_engine import get_settings
from quant_data_engine.pipelines.fund_copywork_pipeline import FundCopyworkPipeline
from quant_data_engine.repositories.fund_copywork_repository import FundCopyworkRepository
from quant_data_engine.utils.ingestion_job import record_ingestion_run

logger = logging.getLogger(__name__)


def main() -> None:
    p = argparse.ArgumentParser(description="Build fund copy-homework snapshots.")
    p.add_argument("--pair-mode", default="previous_full", choices=("previous_full", "previous_quarter"))
    p.add_argument("--quality-mode", default="auto", choices=("auto", "official", "provisional"))
    p.add_argument("--period-prev", default="")
    p.add_argument("--period-curr", default="")
    p.add_argument("--no-preview", action="store_true", help="Skip Q1/immature H1 preview pairs.")
    p.add_argument("--no-persist", action="store_true")
    p.add_argument("--no-record-job", action="store_true")
    args = p.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    settings = get_settings()
    client = MongoClient(settings.mongo_uri)
    data_db = client[settings.data_db]
    repo = None if args.no_persist else FundCopyworkRepository(settings.mongo_uri, settings.data_db)
    pipeline = FundCopyworkPipeline(
        holdings_coll=data_db["fund_holdings"],
        fund_basic_coll=data_db["fund_basic"],
        snapshot_repo=repo,
        stock_info_coll=data_db["stock_info"],
        hk_stock_info_coll=data_db["hk_stock_info"],
    )
    explicit = bool(args.period_prev.strip() and args.period_curr.strip())
    if explicit:
        result = pipeline.run(
            pair_mode=args.pair_mode,
            quality_mode=args.quality_mode,
            period_prev=args.period_prev.strip() or None,
            period_curr=args.period_curr.strip() or None,
            persist=not args.no_persist,
        )
        results = [result]
    else:
        bundled = pipeline.run_default(
            pair_mode=args.pair_mode,
            persist=not args.no_persist,
            include_preview=not args.no_preview,
        )
        result = bundled
        results = bundled.get("results") or [bundled]
    for row in results:
        logger.info(
            "fund copywork snapshot: %s",
            {k: v for k, v in row.items() if k not in {"products", "stocks", "meta", "results"}},
        )
    if not result.get("ok"):
        if not args.no_record_job:
            record_ingestion_run(
                settings,
                "fund_copywork",
                datetime.now().strftime("%Y%m%d"),
                status="failed",
                error=str(result.get("error") or "snapshot build failed"),
                period_prev=result.get("period_prev"),
                period_curr=result.get("period_curr"),
            )
        sys.exit(2)
    if not args.no_record_job:
        record_ingestion_run(
            settings,
            "fund_copywork",
            datetime.now().strftime("%Y%m%d"),
            records_changed=int((result.get("n_written") or {}).get("products") or 0)
            + int((result.get("n_written") or {}).get("stocks") or 0),
            period_prev=result.get("period_prev"),
            period_curr=result.get("period_curr"),
            compare_mode=result.get("compare_mode") or (results[-1].get("compare_mode") if results else None),
        )


if __name__ == "__main__":  # pragma: no cover
    main()
