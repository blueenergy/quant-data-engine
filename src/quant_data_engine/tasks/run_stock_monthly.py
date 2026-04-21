"""Sync A-share monthly bars (Tushare ``monthly``) into ``quant_data.stock_monthly``.

Bulk mode: ``--trade-date`` or ``--recent-months`` (completed calendar months before anchor month).

Examples:
  python -m quant_data_engine.tasks.run_stock_monthly --trade-date 20250331
  python -m quant_data_engine.tasks.run_stock_monthly --end 20250401 --recent-months 1
  python -m quant_data_engine.tasks.run_stock_monthly --ts-code 000001.SZ --start 20200101 --end 20250430
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from quant_data_engine.adapters.stock_adapter import StockAdapter
from quant_data_engine.config.settings import get_settings
from quant_data_engine.pipelines.stock_wm_bars_pipeline import StockWmBarsPipeline
from quant_data_engine.repositories.stock_repository import StockRepository
from quant_data_engine.utils.ingestion_job import record_ingestion_run
from quant_data_engine.utils.stock_wm_calendar import last_n_completed_calendar_month_end_trade_dates


def parse_args():
    p = argparse.ArgumentParser(description="Stock monthly bars → MongoDB stock_monthly")
    p.add_argument("--trade-date", type=str, default=None, help="Single month-end trade date YYYYMMDD (full market)")
    p.add_argument("--end", type=str, default=None, help="Anchor YYYYMMDD: months completed *before* this day’s month")
    p.add_argument(
        "--recent-months",
        type=int,
        default=0,
        help="If >0 and --trade-date omitted, sync last N completed calendar month-ends before anchor month",
    )
    p.add_argument("--ts-code", type=str, default=None, help="Optional: single stock with --start/--end-range")
    p.add_argument("--start", type=str, default=None, help="With --ts-code")
    p.add_argument("--end-range", dest="end_range", type=str, default=None, help="With --ts-code")
    p.add_argument("--dry-run", action="store_true")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    settings = get_settings()
    today = datetime.today().strftime("%Y%m%d")
    end_raw = (args.end or today).replace("-", "")[:8]
    end_anchor = end_raw if end_raw <= today else today
    if end_raw != end_anchor:
        print(f"⚠️  --end {end_raw} 晚于本机今天 {today}，已改用 {end_anchor} 作为锚点。")

    adapter = StockAdapter(settings.tushare_token, rate_limit_per_sec=settings.rate_limit_per_sec)
    repository = StockRepository(settings.mongo_uri, settings.data_db)
    pipeline = StockWmBarsPipeline(adapter=adapter, repository=repository)

    dates: list[str] = []
    if args.trade_date:
        dates = [args.trade_date.replace("-", "")[:8]]
    elif args.recent_months and args.recent_months > 0:
        dates = last_n_completed_calendar_month_end_trade_dates(adapter, end_anchor, args.recent_months)
    elif args.ts_code and args.start and args.end_range:
        if args.dry_run:
            print(f"[dry-run] {args.ts_code} {args.start}–{args.end_range}")
            return
        r = pipeline.ingest_monthly_ts_range(args.ts_code.upper(), args.start, args.end_range)
        record_ingestion_run(
            settings,
            "stock_monthly",
            args.end_range,
            records_changed=r.get("count"),
            mode="ts_range",
            ts_code=args.ts_code,
        )
        print(r)
        return
    else:
        dates = last_n_completed_calendar_month_end_trade_dates(adapter, end_anchor, 1)

    if not dates:
        print("❌ No monthly trade_date to sync")
        record_ingestion_run(settings, "stock_monthly", end_anchor, status="skipped", error="no_trade_dates")
        return

    if args.dry_run:
        print("[dry-run] trade_dates:", dates)
        return

    total = 0
    for td in dates:
        print(f"🟡 monthly trade_date={td} …")
        r = pipeline.ingest_monthly_trade_date(td)
        print(r)
        total += int(r.get("count") or 0)

    record_ingestion_run(
        settings,
        "stock_monthly",
        dates[-1],
        records_changed=total,
        trade_dates=dates,
    )


if __name__ == "__main__":
    main()
