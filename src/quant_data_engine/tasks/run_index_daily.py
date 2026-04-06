"""CLI task to run index daily pipeline with optional meta & job status recording.

Features:
  - Provide codes explicitly OR use --core group.
  - Upsert index_meta (name/market) via MetaRepository.
  - Record job_status document summarizing run.

Environment:
  MONGO_URL / MONGO_DB / TUSHARE_TOKEN as usual via settings.
"""
# Add parent directory to path
from __future__ import annotations
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
import argparse
from datetime import datetime, timedelta
from quant_data_engine import create_index_pipeline, get_settings
from quant_data_engine.repositories.meta_repository import MetaRepository
from quant_data_engine.utils.ingestion_job import record_ingestion_run
from quant_data_engine.constants.index_codes import INDEX_GROUPS, INDEX_CODES

NAME_MAP = {
    INDEX_CODES["HS300"]: "沪深300",
    INDEX_CODES["SSE50"]: "上证50",
    INDEX_CODES["CSI500"]: "中证500",
    INDEX_CODES["STAR50"]: "科创50",
    INDEX_CODES["CSI1000"]: "中证1000",
    INDEX_CODES["SSE_INDEX"]: "上证指数",
    INDEX_CODES["SZ_INDEX"]: "深证成指",
    INDEX_CODES["GEM_INDEX"]: "创业板指数",
    INDEX_CODES["STAR100"]: "科创100指数",
    INDEX_CODES["CSI_A500"]: "中证A500指数",
}


def parse_args():
    p = argparse.ArgumentParser(description="Run index daily ingestion pipeline")
    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument("--codes", nargs="+", help="Index ts_codes e.g. 000300.SH 000905.SH")
    group.add_argument("--core", action="store_true", help="Use predefined core index group")
    p.add_argument("--backfill-days", type=int, default=3, help="Backfill tolerance days (currently adapter handles full range)")
    p.add_argument("--start", type=str, default=None, help="Start date YYYYMMDD (default: 1y ago)")
    p.add_argument("--end", type=str, default=None, help="End date YYYYMMDD (default: today)")
    p.add_argument("--no-write-meta", action="store_true", help="Skip writing index_meta")
    p.add_argument("--no-record-job", action="store_true", help="Skip job_status recording")
    return p.parse_args()


def main():
    args = parse_args()
    settings = get_settings()
    today_dt = datetime.today()
    end = args.end or today_dt.strftime("%Y%m%d")
    start = args.start or (today_dt - timedelta(days=365)).strftime("%Y%m%d")

    codes = args.codes if args.codes else INDEX_GROUPS["core"]
    print(f"🟡 Running index daily ingestion for {len(codes)} codes: {codes}")

    pipeline = create_index_pipeline()
    result = pipeline.run(codes, start_date=start, end_date=end, backfill_days=args.backfill_days)

    # Upsert meta (optional)
    if not args.no_write_meta:
        meta_repo = MetaRepository(settings.mongo_uri, settings.data_db)
        for c in codes:
            meta_repo.upsert(c, name=NAME_MAP.get(c), market="CN")
        print("🟢 index_meta upsert complete")
    else:
        print("⏭ Skipped meta upsert")

    # Record job status (optional)
    total_changed = sum(r.get("inserted", 0) for r in result.get("results", []))
    if not args.no_record_job:
        record_ingestion_run(
            settings,
            "index_daily",
            today_dt.strftime("%Y%m%d"),
            codes=codes,
            records_changed=total_changed,
            start=start,
            end=end,
        )
        print("🟢 job_status recorded")
    else:
        print("⏭ Skipped job_status recording")

    print(f"✅ Ingestion finished. Total changed: {total_changed}")
    print(result)


if __name__ == "__main__":  # pragma: no cover
    main()
