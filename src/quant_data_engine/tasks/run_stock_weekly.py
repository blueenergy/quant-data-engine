"""Sync A-share weekly bars (Tushare ``weekly``) into ``quant_data.stock_weekly``.

Bulk mode: ``--trade-date`` or ``--recent-weeks`` (uses SSE calendar + ISO weeks).

Examples:
  python -m quant_data_engine.tasks.run_stock_weekly --trade-date 20250411
  python -m quant_data_engine.tasks.run_stock_weekly --end 20250418 --recent-weeks 2
  python -m quant_data_engine.tasks.run_stock_weekly --ts-code 000001.SZ --start 20200101 --end-range 20250418
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
from quant_data_engine.utils.stock_wm_calendar import last_n_weekly_bar_trade_dates


def parse_args():
    p = argparse.ArgumentParser(description="Stock weekly bars → MongoDB stock_weekly")
    p.add_argument("--trade-date", type=str, default=None, help="Single bar end date YYYYMMDD (full market)")
    p.add_argument("--end", type=str, default=None, help="Anchor date YYYYMMDD (default: today)")
    p.add_argument(
        "--recent-weeks",
        type=int,
        default=0,
        help="If >0 and --trade-date omitted, sync last N ISO week-end trade dates on or before --end",
    )
    p.add_argument("--ts-code", type=str, default=None, help="Optional: single stock backfill with --start/--end")
    p.add_argument("--start", type=str, default=None, help="With --ts-code: range start YYYYMMDD")
    p.add_argument("--end-range", dest="end_range", type=str, default=None, help="With --ts-code: range end YYYYMMDD")
    p.add_argument("--dry-run", action="store_true", help="Print dates only, no API/Mongo writes")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    settings = get_settings()
    today = datetime.today().strftime("%Y%m%d")
    end_raw = (args.end or today).replace("-", "")[:8]
    # Tushare weekly(trade_date=…) has no data for future anchors; YYYYMMDD lexicographic min is safe here.
    end_anchor = end_raw if end_raw <= today else today
    if end_raw != end_anchor:
        print(f"⚠️  --end {end_raw} 晚于本机今天 {today}，已改用 {end_anchor} 作为锚点（避免接口全空）。")

    adapter = StockAdapter(settings.tushare_token, rate_limit_per_sec=settings.rate_limit_per_sec)
    repository = StockRepository(settings.mongo_uri, settings.data_db)
    pipeline = StockWmBarsPipeline(adapter=adapter, repository=repository)

    dates: list[str] = []
    if args.trade_date:
        dates = [args.trade_date.replace("-", "")[:8]]
    elif args.recent_weeks and args.recent_weeks > 0:
        dates = last_n_weekly_bar_trade_dates(adapter, end_anchor, args.recent_weeks)
    elif args.ts_code and args.start and args.end_range:
        if args.dry_run:
            print(f"[dry-run] would sync {args.ts_code} {args.start}–{args.end_range}")
            return
        r = pipeline.ingest_weekly_ts_range(args.ts_code.upper(), args.start, args.end_range)
        record_ingestion_run(
            settings,
            "stock_weekly",
            args.end_range,
            records_changed=r.get("count"),
            mode="ts_range",
            ts_code=args.ts_code,
        )
        print(r)
        return
    else:
        # default: last 1 weekly bar ending on or before anchor
        dates = last_n_weekly_bar_trade_dates(adapter, end_anchor, 1)

    if not dates:
        print("❌ No weekly trade_date to sync (calendar empty?)")
        record_ingestion_run(settings, "stock_weekly", end_anchor, status="skipped", error="no_trade_dates")
        return

    if args.dry_run:
        print("[dry-run] trade_dates:", dates)
        return

    total = 0
    for td in dates:
        print(f"🟡 weekly trade_date={td} …")
        r = pipeline.ingest_weekly_trade_date(td)
        print(r)
        total += int(r.get("count") or 0)

    record_ingestion_run(
        settings,
        "stock_weekly",
        dates[-1],
        records_changed=total,
        trade_dates=dates,
    )


if __name__ == "__main__":
    main()
