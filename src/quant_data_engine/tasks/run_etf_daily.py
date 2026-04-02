"""CLI task to run sector/style ETF daily ingestion similar to index pipeline.

Features:
  - Provide codes explicitly OR use --core / --extended groups.
  - Upsert meta (name/type) into index_meta collection (reuse) for simplicity.
  - Record job_status for auditing.

Environment:
  MONGO_URL / MONGO_DB / TUSHARE_TOKEN via settings.
"""
from __future__ import annotations
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
import argparse
from datetime import datetime, timedelta
from quant_data_engine import create_etf_pipeline, get_settings
from quant_data_engine.repositories.meta_repository import MetaRepository
from quant_data_engine.utils.ingestion_job import record_ingestion_run
from quant_data_engine.constants.etf_codes import ETF_GROUPS, ETF_NAME_MAP

def parse_args():
    p = argparse.ArgumentParser(description="Run ETF daily ingestion pipeline")
    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument("--codes", nargs="+", help="ETF ts_codes e.g. 512880.SH 512760.SH")
    group.add_argument("--core", action="store_true", help="Use core sector ETF group")
    group.add_argument("--extended", action="store_true", help="Use extended ETF group")
    group.add_argument("--all", action="store_true", help="Sync ALL listed ETFs from Tushare fund_basic (market=E, status=L)")
    p.add_argument("--start", type=str, default=None, help="Start date YYYYMMDD (default: 1y ago)")
    p.add_argument("--end", type=str, default=None, help="End date YYYYMMDD (default: today)")
    p.add_argument("--no-write-meta", action="store_true", help="Skip writing ETF meta")
    p.add_argument("--no-record-job", action="store_true", help="Skip job_status recording")
    return p.parse_args()

def main():
    args = parse_args()
    settings = get_settings()
    today_dt = datetime.today()
    end = args.end or today_dt.strftime("%Y%m%d")
    start = args.start or (today_dt - timedelta(days=365)).strftime("%Y%m%d")

    pipeline = create_etf_pipeline()

    if args.codes:
        codes = args.codes
    elif args.core:
        codes = ETF_GROUPS["core_sectors"]
    elif args.extended:
        codes = ETF_GROUPS["extended"]
    elif args.all:
        # Dynamically discover all listed exchange-traded funds from Tushare
        print("🔍 Fetching all listed ETFs from Tushare fund_basic (market=E, status=L)...")
        df_basic = pipeline.adapter.fetch_fund_basic(market='E', status='L')
        if df_basic is None or df_basic.empty:
            print("❌ No ETF list returned from Tushare. Aborting.")
            raise SystemExit(1)
        codes = df_basic['ts_code'].dropna().tolist()
        print(f"✅ Found {len(codes)} listed ETFs from Tushare.")
    else:
        raise SystemExit("No codes group provided.")

    print(f"🟡 Running ETF daily ingestion for {len(codes)} codes")
    result = pipeline.run(codes, start_date=start, end_date=end)

    # Upsert meta (optional)
    if not args.no_write_meta:
        meta_repo = MetaRepository(settings.mongo_uri, settings.data_db)
        for c in codes:
            meta_repo.upsert(c, name=ETF_NAME_MAP.get(c), market="CN", alias=["ETF"])
        print("🟢 etf_meta upsert complete (reused index_meta collection)")
    else:
        print("⏭ Skipped meta upsert")

    # Record job status (optional)
    total_changed = sum(r.get("inserted", 0) for r in result.get("results", []))
    if not args.no_record_job:
        record_ingestion_run(
            settings,
            "etf_daily",
            today_dt.strftime("%Y%m%d"),
            codes=codes,
            records_changed=total_changed,
            start=start,
            end=end,
        )
        print("🟢 job_status recorded (etf_daily)")
    else:
        print("⏭ Skipped job_status recording")

    print(f"✅ ETF ingestion finished. Total changed: {total_changed}")
    print(result)

if __name__ == "__main__":  # pragma: no cover
    main()
