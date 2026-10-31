#!/usr/bin/env python3
"""同步全市场公募基金基础信息到 quant_data.fund_basic（含场外主动基金，不仅 ETF）。

用法:
  python -m quant_data_engine.tasks.run_fund_basic_sync
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from quant_data_engine import get_settings
from quant_data_engine.adapters.tushare_adapter import TushareAdapter
from quant_data_engine.repositories.fund_basic_repository import FundBasicRepository
from quant_data_engine.fund_inventory.tushare_pages import iter_fund_basic_pages
from quant_data_engine.utils.ingestion_job import record_ingestion_run

logger = logging.getLogger(__name__)

_MARKETS = ("E", "O")
_STATUSES = ("L", "D", "I")


def main() -> None:
    p = argparse.ArgumentParser(description="Sync Tushare fund_basic (all markets) to fund_basic.")
    p.add_argument("--no-record-job", action="store_true")
    args = p.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    settings = get_settings()
    if not settings.tushare_token:
        logger.error("TUSHARE_TOKEN 未配置，退出")
        sys.exit(1)

    adapter = TushareAdapter(
        token=settings.tushare_token,
        rate_limit_per_sec=settings.rate_limit_per_sec,
    )
    repo = FundBasicRepository(settings.mongo_uri, settings.data_db)
    total_rows = 0
    total_upserts = 0
    errors = []
    successful_requests = 0
    for market in _MARKETS:
        for status in _STATUSES:
            try:
                page_count = 0
                for df in iter_fund_basic_pages(adapter, market=market, status=status):
                    page_count += 1
                    n = repo.upsert_from_dataframe(df)
                    total_rows += len(df)
                    total_upserts += n
                    logger.info(
                        "fund_basic market=%s status=%s page=%s rows=%s upserts=%s",
                        market,
                        status,
                        page_count,
                        len(df),
                        n,
                    )
            except Exception as e:
                logger.warning("fund_basic market=%s status=%s failed: %s", market, status, e)
                errors.append(f"{market}/{status}: {e}")
                continue
            successful_requests += 1

    logger.info("fund_basic 同步结束: rows=%s upserts=%s", total_rows, total_upserts)
    if errors or successful_requests == 0 or total_rows == 0:
        message = "; ".join(errors) or "fund_basic returned no rows"
        if not args.no_record_job:
            record_ingestion_run(
                settings,
                "fund_basic",
                datetime.now().strftime("%Y%m%d"),
                status="failed",
                error=message,
                records_changed=total_upserts,
            )
        raise SystemExit(2)
    if not args.no_record_job:
        record_ingestion_run(
            settings,
            "fund_basic",
            datetime.now().strftime("%Y%m%d"),
            records_changed=total_upserts,
        )


if __name__ == "__main__":  # pragma: no cover
    main()
