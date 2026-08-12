"""CLI task to aggregate limit-up statistics by industry."""
from __future__ import annotations
import os
import sys
import time
import signal
import logging
import argparse
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from quant_data_engine.config.settings import get_settings
from quant_data_engine.repositories.industry_repository import IndustryRepository
from quant_data_engine.repositories.ladder_repository import LadderRepository
from quant_data_engine.pipelines.industry_pipeline import IndustryPipeline
from quant_data_engine.utils.ingestion_job import record_ingestion_run

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def parse_args():
    p = argparse.ArgumentParser(description="Aggregate limit-up statistics by industry")
    p.add_argument("--end", type=str, help="Aggregate as of this date YYYYMMDD (default: today)")
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
    
    industry_repo = IndustryRepository(settings.mongo_uri, settings.data_db)
    ladder_repo = LadderRepository(settings.mongo_uri, settings.data_db)
    pipeline = IndustryPipeline(industry_repo, ladder_repo)
    
    def run_sync() -> int:
        end = args.end or datetime.now().strftime("%Y%m%d")
        logger.info(f"Running aggregation for {end}")
        changed = pipeline.run_aggregation(end)
        logger.info(f"Aggregation complete: {changed} industry-periods")
        return changed

    if args.loop:
        logger.info(f"Starting resident industry sync loop (Interval: {args.interval}s)")
        
        def signal_handler(sig, frame):
            logger.info("Stopping industry sync loop...")
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        while True:
            if is_post_market_time():
                job_date = datetime.now().strftime("%Y%m%d")
                try:
                    changed = run_sync()
                    record_ingestion_run(settings, "industry_sync", job_date, records_changed=changed, mode="loop")
                except Exception as e:
                    logger.error(f"Error in sync loop: {e}")
                    try:
                        record_ingestion_run(
                            settings,
                            "industry_sync",
                            job_date,
                            status="failed",
                            error=str(e),
                            mode="loop",
                        )
                    except Exception:
                        pass
            else:
                logger.debug("Waiting for post-market time (15:30+)")
            
            time.sleep(args.interval)
    else:
        job_date = datetime.now().strftime("%Y%m%d")
        try:
            changed = run_sync()
        except Exception as e:
            record_ingestion_run(settings, "industry_sync", job_date, status="failed", error=str(e))
            raise
        record_ingestion_run(settings, "industry_sync", job_date, records_changed=changed)

if __name__ == "__main__":
    main()
