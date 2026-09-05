"""CLI task to sync whitelisted futures main-contract daily bars."""
from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from datetime import datetime, timedelta

from quant_data_engine import create_fut_pipeline, get_settings
from quant_data_engine.constants.fut_codes import FUT_NAME_MAP, FUT_WHITELIST_CODES
from quant_data_engine.repositories.meta_repository import MetaRepository
from quant_data_engine.utils.ingestion_job import record_ingestion_run


def parse_args():
    p = argparse.ArgumentParser(description="Run futures daily ingestion (whitelisted main contracts)")
    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument("--codes", nargs="+", help="Futures ts_codes e.g. IF.CFX AU.SHF")
    group.add_argument("--core", action="store_true", help="Use predefined whitelist (index + commodity)")
    p.add_argument("--start", type=str, default=None, help="Start date YYYYMMDD (default: 1y ago)")
    p.add_argument("--end", type=str, default=None, help="End date YYYYMMDD (default: today)")
    p.add_argument("--no-write-meta", action="store_true", help="Skip writing fut_meta")
    p.add_argument("--no-record-job", action="store_true", help="Skip job_status recording")
    return p.parse_args()


def main():
    args = parse_args()
    settings = get_settings()
    today_dt = datetime.today()
    end = args.end or today_dt.strftime("%Y%m%d")
    start = args.start or (today_dt - timedelta(days=365)).strftime("%Y%m%d")

    codes = args.codes if args.codes else list(FUT_WHITELIST_CODES)
    print(f"🟡 Running futures daily ingestion for {len(codes)} codes: {start}–{end}")

    pipeline = create_fut_pipeline()
    result = pipeline.run(codes, start_date=start, end_date=end)

    if not args.no_write_meta:
        meta_repo = MetaRepository(settings.mongo_uri, settings.data_db)
        for code in codes:
            meta_repo.upsert(code, name=FUT_NAME_MAP.get(code), market="FUT")
        print("🟢 fut_meta upsert complete")

    total_changed = sum(r.get("inserted", 0) for r in result.get("results", []))
    if not args.no_record_job:
        record_ingestion_run(
            settings,
            "fut_daily",
            today_dt.strftime("%Y%m%d"),
            codes=codes,
            records_changed=total_changed,
            start=start,
            end=end,
        )
        print("🟢 job_status recorded")

    print(f"✅ Futures ingestion finished. Total changed: {total_changed}")
    print(result)


if __name__ == "__main__":  # pragma: no cover
    main()
