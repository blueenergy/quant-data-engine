"""P6 helper: validate ``volume_price`` raw backfill quality in a date range.

Checks negative closes and ``update_time`` BSON types (legacy bulk loads used datetime).

Usage::

  python -m quant_data_engine.tasks.run_volume_price_validate
  python -m quant_data_engine.tasks.run_volume_price_validate --start 20160101 --end 20251231
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from quant_data_engine.config.settings import get_settings
from quant_data_engine.repositories.stock_repository import StockRepository
from quant_data_engine.tasks.run_volume_price_backfill import DEFAULT_BACKFILL_START


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Validate volume_price raw backfill (P6 checks)")
    p.add_argument("--start", type=str, default=DEFAULT_BACKFILL_START)
    p.add_argument("--end", type=str, default="")
    p.add_argument(
        "--fail-on-negative",
        action="store_true",
        help="Exit 1 when negative close rows remain in range",
    )
    return p.parse_args()


def count_negative_closes(coll, start: str, end: str) -> int:
    return coll.count_documents(
        {
            "trade_date": {"$gte": start, "$lte": end},
            "close": {"$lte": 0},
        }
    )


def summarize_update_time_types(coll, start: str, end: str) -> dict:
    pipeline = [
        {"$match": {"trade_date": {"$gte": start, "$lte": end}}},
        {
            "$group": {
                "_id": {"$type": "$update_time"},
                "count": {"$sum": 1},
            }
        },
        {"$sort": {"count": -1}},
    ]
    rows = list(coll.aggregate(pipeline, allowDiskUse=True))
    return {str(r["_id"]): int(r["count"]) for r in rows}


def main() -> int:
    args = parse_args()
    settings = get_settings()
    repo = StockRepository(settings.mongo_uri, settings.data_db)
    end = args.end or datetime.today().strftime("%Y%m%d")
    start = args.start

    neg = count_negative_closes(repo.volume_price_coll, start, end)
    type_counts = summarize_update_time_types(repo.volume_price_coll, start, end)
    total_in_range = repo.volume_price_coll.count_documents(
        {"trade_date": {"$gte": start, "$lte": end}}
    )
    checkpoint = repo.get_meta("volume_price_raw_backfill")

    print("🟡 volume_price validation")
    print(f"   Range: {start} -> {end}")
    print(f"   Rows in range: {total_in_range:,}")
    print(f"   Negative close rows: {neg:,}")
    print(f"   update_time BSON types: {type_counts}")
    print(f"   Backfill checkpoint: {checkpoint or '(none)'}")

    ok = neg == 0
    if ok:
        print("✅ No negative closes in range")
    else:
        print("⚠️  Negative closes remain — backfill may be incomplete or pre-2016 history untouched")

    if args.fail_on_negative and not ok:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
