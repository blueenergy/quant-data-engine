"""CLI task to run industry and ETF daily synchronization."""
from __future__ import annotations
import os
import sys
import time
import signal
import logging
import argparse
from datetime import datetime, timedelta

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from quant_data_engine.config.settings import get_settings
from quant_data_engine.adapters.industry_adapter import IndustryAdapter
from quant_data_engine.repositories.industry_repository import IndustryRepository
from quant_data_engine.repositories.ladder_repository import LadderRepository
from quant_data_engine.pipelines.industry_pipeline import IndustryPipeline

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Core ETF mapping for industry analysis
CORE_ETFS = {
    "516160": "电力ETF",
    "512010": "医药ETF",
    "512660": "军工ETF",
    "512480": "半导体ETF",
    "512800": "银行ETF",
    "512760": "芯片ETF",
    "515050": "5G ETF",
    "515000": "科技ETF",
    "512690": "酒ETF",
    "512100": "证券ETF"
}

def parse_args():
    p = argparse.ArgumentParser(description="Run industry and ETF daily sync")
    p.add_argument("--start", type=str, help="Start date YYYYMMDD")
    p.add_argument("--end", type=str, help="End date YYYYMMDD")
    p.add_argument("--auto", action="store_true", help="Sync last 30 days automatically")
    p.add_argument("--aggregate", action="store_true", help="Only run aggregation")
    p.add_argument("--loop", action="store_true", help="Run in continuous loop mode")
    p.add_argument("--interval", type=int, default=3600, help="Loop interval in seconds (default: 3600)")
    return p.parse_args()

def is_post_market_time() -> bool:
    """Check if it's after 15:30 on a trading day."""
    now = datetime.now()
    if now.weekday() >= 5:
        return False
    t = now.time()
    post_market = datetime.strptime("15:30", "%H:%M").time()
    return t >= post_market

def main():
    args = parse_args()
    settings = get_settings()
    
    adapter = IndustryAdapter()
    industry_repo = IndustryRepository(settings.mongo_uri, settings.data_db)
    ladder_repo = LadderRepository(settings.mongo_uri, settings.data_db)
    pipeline = IndustryPipeline(adapter, industry_repo, ladder_repo)
    
    def run_sync():
        today = datetime.now().strftime("%Y%m%d")
        start = args.start or (datetime.now() - timedelta(days=30 if args.auto else 7)).strftime("%Y%m%d")
        end = args.end or today
        
        if not args.aggregate:
            logger.info(f"Syncing industry and ETF data from {start} to {end}")
            pipeline.sync_industries_daily(start, end)
            pipeline.sync_etfs_daily(CORE_ETFS, start, end)
        
        logger.info(f"Running aggregation for {end}")
        pipeline.run_aggregation(end)
        logger.info("Sync and aggregation complete.")

    if args.loop:
        logger.info(f"Starting resident industry sync loop (Interval: {args.interval}s)")
        
        def signal_handler(sig, frame):
            logger.info("Stopping industry sync loop...")
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        while True:
            if is_post_market_time():
                try:
                    run_sync()
                except Exception as e:
                    logger.error(f"Error in sync loop: {e}")
            else:
                logger.debug("Waiting for post-market time (15:30+)")
            
            time.sleep(args.interval)
    else:
        run_sync()

if __name__ == "__main__":
    main()
