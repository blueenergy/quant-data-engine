#!/usr/bin/env python3
"""从 fund_holdings 生成主动公募库存四态快照。

依赖：fund_basic、至少两个报告期的 fund_holdings、volume_price（剥股价）。

用法:
  python -m quant_data_engine.tasks.run_fund_inventory_snapshot
  python -m quant_data_engine.tasks.run_fund_inventory_snapshot --period-prev 20251231 --period-curr 20260630
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
from quant_data_engine.pipelines.fund_inventory_pipeline import FundInventoryPipeline
from quant_data_engine.repositories.fund_inventory_repository import FundInventoryRepository
from quant_data_engine.utils.ingestion_job import record_ingestion_run

logger = logging.getLogger(__name__)


def main() -> None:
    p = argparse.ArgumentParser(description="Build active-fund inventory four-state snapshots.")
    p.add_argument("--pair-mode", default="previous_full", choices=("previous_full", "previous_quarter"))
    p.add_argument("--period-prev", default="", help="上一期 end_date YYYYMMDD")
    p.add_argument("--period-curr", default="", help="本期 end_date YYYYMMDD")
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
    repo = None if args.no_persist else FundInventoryRepository(settings.mongo_uri, settings.data_db)
    pipeline = FundInventoryPipeline(
        holdings_coll=data_db["fund_holdings"],
        fund_basic_coll=data_db["fund_basic"],
        volume_price_coll=data_db["volume_price"],
        snapshot_repo=repo,
        adj_factor_coll=data_db["stock_adj_factor"],
        stock_info_coll=data_db["stock_info"],
    )
    result = pipeline.run(
        pair_mode=args.pair_mode,
        period_prev=args.period_prev.strip() or None,
        period_curr=args.period_curr.strip() or None,
        persist=not args.no_persist,
    )
    logger.info("fund inventory snapshot: %s", {k: v for k, v in result.items() if k != "snapshots"})
    if not result.get("ok"):
        if not args.no_record_job:
            record_ingestion_run(
                settings,
                "fund_inventory",
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
            "fund_inventory",
            datetime.now().strftime("%Y%m%d"),
            records_changed=result.get("n_written") or 0,
            period_prev=result.get("period_prev"),
            period_curr=result.get("period_curr"),
            compare_mode=result.get("compare_mode"),
        )


if __name__ == "__main__":  # pragma: no cover
    main()
