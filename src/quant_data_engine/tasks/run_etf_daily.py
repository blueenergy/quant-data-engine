"""CLI task to run sector/style ETF daily ingestion similar to index pipeline.

Features:
  - Provide codes explicitly OR use --core / --extended groups.
  - ``--all``: 按 trade_date 逐日拉全市场 fund_daily / fund_adj / fund_share（不传 ts_code），
    与 etf_share_size 同一套 Tushare 语义；etf_basic 仍由 fund_basic(E,L) 一次全量写入。

Environment:
  MONGO_URL / MONGO_DB / TUSHARE_TOKEN via settings.
"""
from __future__ import annotations
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
import argparse
from datetime import datetime, timedelta

from pymongo import MongoClient

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
    group.add_argument(
        "--all",
        action="store_true",
        help="不传 ts_code：按 trade_date 逐日同步全市场日线/复权/份额；etf_basic 用 fund_basic(E,L) 一次写入",
    )
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
        print(f"🟡 ETF daily by ts_code × date: {len(codes)} codes, {start}–{end}")
        result = pipeline.run(codes, start_date=start, end_date=end)
    elif args.core:
        codes = ETF_GROUPS["core_sectors"]
        print(f"🟡 ETF daily by ts_code × date: core {len(codes)} codes, {start}–{end}")
        result = pipeline.run(codes, start_date=start, end_date=end)
    elif args.extended:
        codes = ETF_GROUPS["extended"]
        print(f"🟡 ETF daily by ts_code × date: extended {len(codes)} codes, {start}–{end}")
        result = pipeline.run(codes, start_date=start, end_date=end)
    elif args.all:
        n_days = (datetime.strptime(end, "%Y%m%d") - datetime.strptime(start, "%Y%m%d")).days + 1
        print(f"🟡 ETF daily 全市场（按 trade_date）: {start}–{end}，共 {n_days} 个自然日")
        result = pipeline.run(None, start_date=start, end_date=end)
    else:
        raise SystemExit("No codes group provided.")

    if not args.no_write_meta:
        meta_repo = MetaRepository(settings.mongo_uri, settings.data_db)
        if args.all:
            mc = MongoClient(settings.mongo_uri, serverSelectionTimeoutMS=8000)
            try:
                n = 0
                for doc in mc[settings.data_db]["etf_basic"].find({}, {"ts_code": 1, "name": 1}):
                    tc = doc.get("ts_code")
                    if not tc:
                        continue
                    tc = str(tc).strip()
                    nm = doc.get("name") or ETF_NAME_MAP.get(tc)
                    meta_repo.upsert(tc, name=nm, market="CN", alias=["ETF"])
                    n += 1
                print(f"🟢 etf_meta upsert from Mongo etf_basic: {n} rows")
            finally:
                mc.close()
        else:
            for c in codes:
                meta_repo.upsert(c, name=ETF_NAME_MAP.get(c), market="CN", alias=["ETF"])
            print("🟢 etf_meta upsert complete (reused index_meta collection)")
    else:
        print("⏭ Skipped meta upsert")

    total_changed = sum(r.get("inserted", 0) for r in result.get("results", []))
    if not args.no_record_job:
        job_kw: dict = {"start": start, "end": end, "mode": result.get("mode")}
        if result.get("mode") == "by_ts_code":
            job_kw["codes"] = codes  # type: ignore[name-defined]
        record_ingestion_run(
            settings,
            "etf_daily",
            today_dt.strftime("%Y%m%d"),
            records_changed=total_changed,
            **job_kw,
        )
        print("🟢 job_status recorded (etf_daily)")
    else:
        print("⏭ Skipped job_status recording")

    print(f"✅ ETF ingestion finished. Total changed: {total_changed}")
    print(result)


if __name__ == "__main__":  # pragma: no cover
    main()
