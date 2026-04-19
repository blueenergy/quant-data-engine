"""CLI: sync Tushare etf_share_size -> MongoDB etf_share_size (需约 8000 积分，见 Tushare 文档)."""
from __future__ import annotations
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

import argparse
from datetime import datetime, timedelta

from quant_data_engine import create_etf_share_size_pipeline, get_settings
from quant_data_engine.constants.etf_codes import ETF_GROUPS
from quant_data_engine.utils.ingestion_job import record_ingestion_run


def parse_args():
    p = argparse.ArgumentParser(description="Run Tushare etf_share_size ingestion")
    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument("--codes", nargs="+", help="ETF ts_codes e.g. 510330.SH")
    group.add_argument("--core", action="store_true", help="Core sector ETF group")
    group.add_argument("--extended", action="store_true", help="Extended ETF group")
    group.add_argument(
        "--all",
        action="store_true",
        help="不传 ts_code：按 trade_date 逐日拉取当日全市场 ETF（与 Tushare 文档一致）",
    )
    p.add_argument("--start", type=str, default=None, help="Start YYYYMMDD (default: 1y ago)")
    p.add_argument("--end", type=str, default=None, help="End YYYYMMDD (default: today)")
    p.add_argument("--no-record-job", action="store_true", help="Skip job_status recording")
    return p.parse_args()


def main():
    args = parse_args()
    settings = get_settings()
    today_dt = datetime.today()
    end = args.end or today_dt.strftime("%Y%m%d")
    start = args.start or (today_dt - timedelta(days=365)).strftime("%Y%m%d")

    pipeline = create_etf_share_size_pipeline()

    if args.codes:
        codes = args.codes
        print(f"🟡 etf_share_size by ts_code × date: {len(codes)} codes, {start}–{end}")
        result = pipeline.run(codes, start_date=start, end_date=end)
    elif args.core:
        codes = ETF_GROUPS["core_sectors"]
        print(f"🟡 etf_share_size by ts_code × date: core {len(codes)} codes, {start}–{end}")
        result = pipeline.run(codes, start_date=start, end_date=end)
    elif args.extended:
        codes = ETF_GROUPS["extended"]
        print(f"🟡 etf_share_size by ts_code × date: extended {len(codes)} codes, {start}–{end}")
        result = pipeline.run(codes, start_date=start, end_date=end)
    elif args.all:
        print(
            f"🟡 etf_share_size 全市场（不传 ts_code，按 trade_date）: {start}–{end} "
            f"共 {(datetime.strptime(end, '%Y%m%d') - datetime.strptime(start, '%Y%m%d')).days + 1} 个自然日"
        )
        result = pipeline.run(None, start_date=start, end_date=end)
    else:
        raise SystemExit("No codes group provided.")

    total_changed = sum(r.get("inserted", 0) for r in result.get("results", []))
    failed = [r for r in result.get("results", []) if r.get("issues")]
    if failed:
        print(f"⚠️ {len(failed)} 条子任务有 issues，示例: {failed[:3]}")

    if not args.no_record_job:
        job_extra: dict = {"start": start, "end": end, "mode": result.get("mode")}
        if result.get("mode") == "by_ts_code":
            job_extra["codes"] = codes  # type: ignore[name-defined]
        record_ingestion_run(
            settings,
            "etf_share_size",
            today_dt.strftime("%Y%m%d"),
            records_changed=total_changed,
            **job_extra,
        )
        print("🟢 job_status recorded (etf_share_size)")
    else:
        print("⏭ Skipped job_status recording")

    print(f"✅ etf_share_size finished. Total upsert touches: {total_changed}")
    print(result)


if __name__ == "__main__":
    main()
