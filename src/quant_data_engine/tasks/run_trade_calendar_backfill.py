"""Backfill the SSE trading calendar into ``quant_data.trade_calendar``.

Stores open days only (``is_open == 1``), including future dates published by
Tushare, so downstream consumers such as quantFinance can advance trading-day
estimates (e.g. next rebalance date) beyond the last synced market bar.

Usage:
  python -m quant_data_engine.tasks.run_trade_calendar_backfill --dry-run
  python -m quant_data_engine.tasks.run_trade_calendar_backfill --start-date 20200101 --end-date 20261231 --execute
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime
from typing import Any, List

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

import tushare as ts
from pymongo import ASCENDING, MongoClient

from quant_data_engine.config.settings import get_settings


def _open_days(pro: Any, start_date: str, end_date: str) -> List[str]:
    df = pro.trade_cal(exchange="SSE", start_date=start_date, end_date=end_date, is_open="1")
    if df is None or df.empty:
        return []
    return sorted(str(x) for x in df["cal_date"].tolist())


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-date", default="20200101")
    parser.add_argument("--end-date", default="20261231")
    parser.add_argument("--execute", action="store_true", help="write to Mongo (default is dry-run)")
    args = parser.parse_args()

    settings = get_settings()
    pro = ts.pro_api(settings.tushare_token)
    days = _open_days(pro, args.start_date, args.end_date)
    first, last = (days[0], days[-1]) if days else ("-", "-")
    print(f"fetched {len(days)} open days [{first} .. {last}]")
    if not days:
        print("no calendar rows returned; aborting", file=sys.stderr)
        sys.exit(1)

    if not args.execute:
        print("[dry-run] no writes (use --execute)")
        return

    client = MongoClient(settings.mongo_uri)
    col = client[settings.data_db]["trade_calendar"]
    col.create_index([("trade_date", ASCENDING)], unique=True)
    now = datetime.utcnow()
    inserted = 0
    for day in days:
        doc = {
            "trade_date": day,
            "is_open": 1,
            "exchange": "SSE",
            "source": "tushare_trade_cal",
            "updated_at": now,
        }
        result = col.update_one({"trade_date": day}, {"$set": doc}, upsert=True)
        inserted += 1 if result.upserted_id else 0
    print(f"done: {len(days)} docs in {settings.data_db}.trade_calendar ({inserted} newly inserted)")


if __name__ == "__main__":
    main()
