"""P5.2: Re-backfill ``volume_price`` with raw (unadjusted) OHLCV from Tushare ``pro.daily``.

Historical bulk loads used forward-adjusted prices; daily incremental writes were already
raw. This task overwrites 2016→present by-date so read-side hfq/qfq joins are consistent.

Checkpoint key ``volume_price_raw_backfill`` in ``stock_sync_meta`` supports ``--resume``.

Usage::

  # Dry-run the full 2016→today range
  python -m quant_data_engine.tasks.run_volume_price_backfill --dry-run

  # Backfill and checkpoint after each trading day
  python -m quant_data_engine.tasks.run_volume_price_backfill --start 20160101

  # Resume after interruption
  python -m quant_data_engine.tasks.run_volume_price_backfill --resume

  # Pilot a short window (still writes OHLCV only by default)
  python -m quant_data_engine.tasks.run_volume_price_backfill --start 20240101 --end 20240131
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime
from typing import List, Optional

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from quant_data_engine.adapters.stock_adapter import StockAdapter
from quant_data_engine.config.settings import get_settings
from quant_data_engine.pipelines.stock_daily_pipeline import StockDailyPipeline
from quant_data_engine.repositories.stock_repository import StockRepository
from quant_data_engine.revision_ledger import VOLUME_PRICE_SOURCE, revision_ledger_job
from quant_data_engine.utils.ingestion_job import record_ingestion_run

DEFAULT_BACKFILL_START = "20160101"
VOLUME_PRICE_RAW_BACKFILL_META_KEY = "volume_price_raw_backfill"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Re-backfill volume_price with raw OHLCV (P5.2 migration)"
    )
    p.add_argument(
        "--start",
        type=str,
        default=DEFAULT_BACKFILL_START,
        help=f"Start trade date YYYYMMDD (default: {DEFAULT_BACKFILL_START})",
    )
    p.add_argument("--end", type=str, help="End trade date YYYYMMDD (default: today)")
    p.add_argument(
        "--resume",
        action="store_true",
        help="Continue from the day after the last checkpoint in stock_sync_meta",
    )
    p.add_argument(
        "--include-basic",
        action="store_true",
        help="Also refresh financial_daily_basic (default: OHLCV only for speed)",
    )
    p.add_argument("--dry-run", action="store_true", help="Print plan without writing")
    p.add_argument("--verbose", action="store_true", help="Verbose output")
    return p.parse_args()


def get_trading_days(adapter: StockAdapter, start_date: str, end_date: str) -> List[str]:
    df = adapter.fetch_trade_cal(start_date=start_date, end_date=end_date)
    if df is None or df.empty:
        return []
    days = df[df["is_open"] == 1]["cal_date"].str.replace("-", "").tolist()
    return sorted(days)


def filter_days_after_checkpoint(trading_days: List[str], checkpoint: Optional[str]) -> List[str]:
    if not checkpoint or len(checkpoint) != 8:
        return trading_days
    return [d for d in trading_days if d > checkpoint]


def main() -> int:
    args = parse_args()
    settings = get_settings()
    adapter = StockAdapter(settings.tushare_token, rate_limit_per_sec=settings.rate_limit_per_sec)
    repository = StockRepository(settings.mongo_uri, settings.data_db)
    pipeline = StockDailyPipeline(adapter=adapter, repository=repository)

    end_date = args.end or datetime.today().strftime("%Y%m%d")
    start_date = args.start or DEFAULT_BACKFILL_START
    trading_days = get_trading_days(adapter, start_date, end_date)
    if not trading_days:
        print(f"⚠️  No trading days in {start_date} -> {end_date}")
        record_ingestion_run(
            settings,
            "volume_price_raw_backfill",
            end_date,
            status="skipped",
            error="No trading days in range",
        )
        return 0

    checkpoint = repository.get_meta(VOLUME_PRICE_RAW_BACKFILL_META_KEY) if args.resume else None
    if args.resume and checkpoint:
        before = len(trading_days)
        trading_days = filter_days_after_checkpoint(trading_days, checkpoint)
        if not trading_days:
            print(f"✅ Already complete through checkpoint {checkpoint}")
            return 0
        print(f"   Resume: skip {before - len(trading_days)} days through {checkpoint}")
        print(f"   Continue from {trading_days[0]}")

    print("🟡 Volume Price Raw Backfill (P5.2)")
    print(f"   Range: {trading_days[0]} -> {trading_days[-1]} ({len(trading_days)} trading days)")
    print(f"   Scope: {'OHLCV + daily_basic' if args.include_basic else 'OHLCV only (raw pro.daily)'}")

    if args.dry_run:
        print(f"💡 Dry-run: would process {len(trading_days)} trading days")
        head = ", ".join(trading_days[:5])
        tail = f", ... {trading_days[-1]}" if len(trading_days) > 5 else ""
        print(f"   Days: {head}{tail}")
        return 0

    total_daily = 0
    total_basic = 0
    total_issues = 0
    try:
        with revision_ledger_job(
            VOLUME_PRICE_SOURCE,
            "volume_price_raw_backfill",
            settings.mongo_uri,
        ):
            for idx, trade_date in enumerate(trading_days, start=1):
                result = pipeline.run_by_date(
                    [trade_date],
                    include_daily=True,
                    include_basic=args.include_basic,
                    include_adj_factor=False,
                    progress=args.verbose,
                )
                daily_n = int(result.get("total_daily_records") or 0)
                basic_n = int(result.get("total_basic_records") or 0)
                issues_n = int(result.get("total_issues") or 0)
                total_daily += daily_n
                total_basic += basic_n
                total_issues += issues_n
                repository.set_meta(VOLUME_PRICE_RAW_BACKFILL_META_KEY, trade_date)
                if not args.verbose:
                    print(
                        f"[volume_price_backfill] {idx}/{len(trading_days)} {trade_date} "
                        f"daily={daily_n} basic={basic_n} issues={issues_n}",
                        flush=True,
                    )
    except Exception as exc:
        record_ingestion_run(
            settings,
            "volume_price_raw_backfill",
            end_date,
            status="failed",
            error=str(exc),
        )
        raise

    print("\n✅ Volume price raw backfill completed:")
    print(f"   Trading days processed: {len(trading_days)}")
    print(f"   Daily records touched: {total_daily}")
    print(f"   Basic records touched: {total_basic}")
    print(f"   Issues: {total_issues}")
    print(f"   Checkpoint: {trading_days[-1]}")
    record_ingestion_run(
        settings,
        "volume_price_raw_backfill",
        trading_days[-1],
        records_changed=total_daily + total_basic,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
