#!/usr/bin/env python3
"""
CLI script to run earnings forecast and express reports synchronization.

Usage:
    python -m quant_data_engine.tasks.run_earnings_sync --start YYYYMMDD --end YYYYMMDD
    # Or for a quick sync of the last N days
    python -m quant_data_engine.tasks.run_earnings_sync --days 5
"""
import argparse
import logging
from datetime import datetime, timedelta

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from quant_data_engine import get_settings
from quant_data_engine.adapters.tushare_adapter import TushareAdapter
from quant_data_engine.repositories.earnings_repository import EarningsRepository
from quant_data_engine.pipelines.earnings_pipeline import EarningsPipeline
from quant_data_engine.repositories.earnings_repository import EarningsRepository
from quant_data_engine.pipelines.earnings_pipeline import EarningsPipeline

logger = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(description="Run Earnings Forecast and Express reports synchronization.")
    parser.add_argument("--start", help="Start date (YYYYMMDD)")
    parser.add_argument("--end", help="End date (YYYYMMDD)")
    parser.add_argument("--days", type=int, default=5, help="Number of recent days to sync if start/end not provided.")
    args = parser.parse_args()

    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    settings = get_settings()

    if args.start and args.end:
        start_date = args.start
        end_date = args.end
    else:
        # Default to a recent window, since forecasts/expresses drop anytime
        end_dt = datetime.now()
        start_dt = end_dt - timedelta(days=args.days)
        start_date = start_dt.strftime("%Y%m%d")
        end_date = end_dt.strftime("%Y%m%d")

    logger.info(f"Starting Earnings Sync Task for period: {start_date} to {end_date}")

    import pymongo
    client = pymongo.MongoClient(settings.mongo_uri)
    db = client[settings.mongo_db]

    adapter = TushareAdapter(
        token=settings.tushare_token,
        rate_limit_per_sec=settings.rate_limit_per_sec
    )
    repo = EarningsRepository(db=db)
    pipeline = EarningsPipeline(tushare_adapter=adapter, repository=repo)

    result = pipeline.run(start_date=start_date, end_date=end_date)
    
    logger.info("Sync Job Completed.")
    logger.info(f"Summary: {result}")

if __name__ == "__main__":
    main()
