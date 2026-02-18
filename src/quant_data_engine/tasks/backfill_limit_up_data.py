"""
Backfill daily_limit_up data using LadderPipeline.
Iterates through a date range and runs the pipeline for each day.
"""
import os
import sys
import logging
import time
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv

# Add quant-data-engine/src to path
# File is in quant-data-engine/src/quant_data_engine/tasks/backfill_limit_up_data.py
src_dir = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(src_dir))

# Add vnpy-live-trading root to path (if needed later, but this script seems self-contained now)
# (Assuming structure: project_root/quant-data-engine/src/... and project_root/vnpy-live-trading)
project_root = src_dir.parent.parent
vnpy_path = project_root / "vnpy-live-trading"
if vnpy_path.exists():
    sys.path.append(str(vnpy_path))



from quant_data_engine.repositories.ladder_repository import LadderRepository
from quant_data_engine.pipelines.ladder_pipeline import LadderPipeline

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger("BackfillLimitUp")

from quant_data_engine.adapters.tushare_adapter import TushareLadderAdapter

def run_backfill(start_date: str, end_date: str, source: str = "eastmoney"):
    load_dotenv()
    mongo_uri = os.environ.get('MONGO_URI') or 'mongodb://localhost:27017'
    mongo_db = os.environ.get('MONGO_DB') or 'finance'
    
    repo = LadderRepository(mongo_uri, mongo_db)
    pipeline = LadderPipeline(repo)
    
    start_dt = datetime.strptime(start_date, "%Y%m%d")
    end_dt = datetime.strptime(end_date, "%Y%m%d")
    
    print(f"🚀 Starting backfill from {start_date} to {end_date} via {source}...")

    if source == "tushare":
        # Tushare Batch Mode
        token = os.environ.get("TUSHARE_TOKEN") or "8227b686d637c7688463e2608463f684826b010c733352632b682701" # Default token fallback if user provided one manually before maybe? Or just read from env.
        if not token:
            logger.error("TUSHARE_TOKEN not set!")
            return

        adapter = TushareLadderAdapter(token)
        
        current_chunk_start = start_dt
        
        while current_chunk_start <= end_dt:
            # Chunk size: 30 days (careful not to exceed Tushare row limit ~4000 rows? Limit list is small ~50-100 stocks/day)
            # 30 days * 100 stocks = 3000 rows. Should fit in limit_list_d response if limit is high enough.
            # Tushare doc says limit 5000/call usually.
            
            chunk_end = min(current_chunk_start + timedelta(days=30), end_dt)
            chunk_start_str = current_chunk_start.strftime("%Y%m%d")
            chunk_end_str = chunk_end.strftime("%Y%m%d")
            
            print(f"� Fetching batch {chunk_start_str}-{chunk_end_str}...", end=" ")
            
            try:
                raw_list = adapter.get_limit_up_stocks_range(chunk_start_str, chunk_end_str)
                print(f"Got {len(raw_list)} records.")
                
                if raw_list:
                    # Group by date
                    date_groups = {}
                    for item in raw_list:
                        d = item["trade_date"]
                        if d not in date_groups:
                            date_groups[d] = []
                        date_groups[d].append(item)
                    
                    # Convert and Process
                    sorted_dates = sorted(date_groups.keys())
                    for d_str in sorted_dates:
                        # Normalize
                        normalized_stocks = pipeline.convert_tushare_list(date_groups[d_str])
                        # Process
                        result = pipeline.process_external_data(d_str, normalized_stocks, source="tushare", fetch_sectors=False)
                        logger.info(f"  Processed {d_str}: {result.total_stocks} stocks, score: {result.sentiment_score}")
                else:
                     logger.warning("  No data in this batch.")

            except Exception as e:
                logger.error(f"  Batch failed: {e}")

            # Next chunk
            current_chunk_start = chunk_end + timedelta(days=1)
            
            # Rate limit sleep between chunks
            if current_chunk_start <= end_dt:
                print("⏳ Sleeping 20s between batches...")
                time.sleep(20)

        print("\n✅ Tushare Batch Backfill Complete.")
        return

    # Original Day-by-Day Loop (EastMoney)
    current_dt = start_dt
    days_total = (end_dt - start_dt).days + 1
    
    count = 0
    success_count = 0
    
    while current_dt <= end_dt:
        date_str = current_dt.strftime("%Y%m%d")
        
        # Simple weekend skip?
        if current_dt.weekday() >= 5:
            # logger.info(f"Skipping weekend: {date_str}")
            current_dt += timedelta(days=1)
            continue
            
        try:
            print(f"[{count+1}/{days_total}] Processing {date_str}...", end="\r")
            
            # Check if data already exists? 
            # Pipeline is idempotent (upsert), so we can just run it.
            # But query first to save time if we want to fill gaps only.
            # For now, just run it to ensure updates.
            
            result = pipeline.run(date_str, fetch_sectors=True, source=source)
            
            if result.total_stocks > 0:
                success_count += 1
                logger.debug(f"\n✅ {date_str}: {result.total_stocks} stocks, sentiment: {result.sentiment_score}")
            else:
                logger.debug(f"\n⚠️ {date_str}: No data (Holiday?)")
                
        except Exception as e:
            logger.error(f"\n❌ Error on {date_str}: {e}")
            
        current_dt += timedelta(days=1)
        count += 1
        
        # Rate limit pause
        time.sleep(0.5)
        
    print(f"\n✅ Backfill Complete. Processed {count} days, {success_count} days with data.")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    # Default to 2 years ago if start not provided
    two_years_ago = (datetime.now() - timedelta(days=365*2)).strftime("%Y%m%d")
    today = datetime.now().strftime("%Y%m%d")
    
    parser.add_argument("--start", default=two_years_ago, help="Start Date YYYYMMDD")
    parser.add_argument("--end", default=today, help="End Date YYYYMMDD")
    parser.add_argument("--source", default="eastmoney", help="Data Source (eastmoney/tushare)")
    
    args = parser.parse_args()
    
    run_backfill(args.start, args.end, args.source)
