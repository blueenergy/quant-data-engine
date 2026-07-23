"""
Market events calendar sync — macro + A-share corporate events → MongoDB.

Usage:
    python -m quant_data_engine.tasks.run_market_events_sync
    python -m quant_data_engine.tasks.run_market_events_sync --loop --interval 21600
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import datetime

from pymongo import MongoClient

from quant_data_engine import get_settings
from quant_data_engine.market_events.sync import run_sync_cycle
from quant_data_engine.utils.ingestion_job import record_ingestion_run

logger = logging.getLogger(__name__)


def run_once() -> bool:
    settings = get_settings()
    job_date = datetime.now().strftime("%Y%m%d")
    client = MongoClient(settings.mongo_uri)
    try:
        stats = run_sync_cycle(client, settings.data_db)
        merged = int(stats.get("merged_count") or 0)
        if merged == 0:
            logger.warning("[MarketEventsSync] No events merged — recording skipped")
            record_ingestion_run(
                settings,
                "market_events",
                job_date,
                status="skipped",
                error="no events merged",
                records_changed=0,
                **stats,
            )
            return False
        record_ingestion_run(
            settings,
            "market_events",
            job_date,
            records_changed=int(stats.get("upserted") or 0),
            **stats,
        )
        return True
    except Exception as e:
        record_ingestion_run(settings, "market_events", job_date, status="failed", error=str(e))
        raise
    finally:
        client.close()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    parser = argparse.ArgumentParser(description="Market events calendar sync")
    parser.add_argument("--loop", action="store_true", help="Run in loop mode")
    parser.add_argument(
        "--interval",
        type=int,
        default=int(__import__("os").getenv("MARKET_EVENTS_SYNC_INTERVAL", "21600")),
        help="Loop interval seconds (default 21600 = 6h)",
    )
    args = parser.parse_args()

    if args.loop:
        logger.info("[MarketEventsSync] Starting loop every %ss", args.interval)
        while True:
            try:
                run_once()
            except Exception as e:
                logger.error("[MarketEventsSync] Cycle failed: %s", e, exc_info=True)
            time.sleep(args.interval)
    else:
        success = run_once()
        sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
