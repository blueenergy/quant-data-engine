#!/usr/bin/env python3
"""CLI task to sync stock_info from Tushare bak_basic into quant_data.stock_info.

This keeps stock metadata (including ts_code) fresh for downstream services such as
earnings hunter, minute sync, and symbol-based joins.
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List

import pandas as pd
import pymongo
from pymongo import UpdateOne

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from quant_data_engine import get_settings
from quant_data_engine.adapters.stock_adapter import StockAdapter
from quant_data_engine.utils.ingestion_job import record_ingestion_run


BAK_BASIC_FIELDS = ",".join(
    [
        "trade_date",
        "ts_code",
        "name",
        "industry",
        "pe",
        "pb",
        "total_share",
        "float_share",
        "eps",
        "bvps",
        "list_date",
        "undp",
        "per_undp",
        "rev_yoy",
        "profit_yoy",
        "gpr",
        "npr",
        "holder_num",
        "total_mv",
        "circ_mv",
    ]
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync stock_info into quant_data.stock_info")
    parser.add_argument("--trade-date", help="Target trade date YYYYMMDD. If omitted, auto-detect latest open day.")
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=14,
        help="Lookback days used when auto-detecting latest open trade day (default: 14).",
    )
    return parser.parse_args()


def _to_float(v: Any) -> Optional[float]:
    if v is None or pd.isna(v):
        return None
    return float(v)


def _detect_latest_open_trade_date(adapter: StockAdapter, lookback_days: int) -> str:
    today = datetime.today()
    start = (today - timedelta(days=max(lookback_days, 3))).strftime("%Y%m%d")
    end = today.strftime("%Y%m%d")

    cal = adapter.fetch_trade_cal(exchange="SSE", start_date=start, end_date=end)
    if cal is None or cal.empty:
        return end

    open_days: List[str] = cal[cal["is_open"] == 1]["cal_date"].astype(str).str.replace("-", "").tolist()
    if not open_days:
        return end
    return sorted(open_days)[-1]


def _build_doc(row: pd.Series) -> Dict[str, Any]:
    ts_code = str(row.get("ts_code", "")).strip()
    symbol = ts_code.split(".")[0] if "." in ts_code else ts_code
    valuation_date = str(row.get("trade_date", "")).strip()

    doc = {
        "symbol": symbol,
        "ts_code": ts_code,
        "name": row.get("name", "") if pd.notna(row.get("name")) else "",
        "industry": row.get("industry", "") if pd.notna(row.get("industry")) else "",
        "pe": _to_float(row.get("pe")),
        "pb": _to_float(row.get("pb")),
        "total_share": _to_float(row.get("total_share")),
        "float_share": _to_float(row.get("float_share")),
        "eps": _to_float(row.get("eps")),
        "bvps": _to_float(row.get("bvps")),
        "list_date": str(row.get("list_date", "")).strip() if pd.notna(row.get("list_date")) else "",
        "undp": _to_float(row.get("undp")),
        "per_undp": _to_float(row.get("per_undp")),
        "rev_yoy": _to_float(row.get("rev_yoy")),
        "profit_yoy": _to_float(row.get("profit_yoy")),
        "gpr": _to_float(row.get("gpr")),
        "npr": _to_float(row.get("npr")),
        "holder_num": _to_float(row.get("holder_num")),
        "total_mv": _to_float(row.get("total_mv")),
        "circ_mv": _to_float(row.get("circ_mv")),
        "valuation_date": valuation_date,
        "valuation_update_date": valuation_date,
        "market_cap": _to_float(row.get("total_mv")),
        "circ_market_cap": _to_float(row.get("circ_mv")),
        "last_update": datetime.now(),
        "source": "tushare_bak_basic",
    }
    return doc


def main() -> None:
    args = parse_args()
    settings = get_settings()
    job_date = datetime.today().strftime("%Y%m%d")
    adapter = StockAdapter(settings.tushare_token, rate_limit_per_sec=settings.rate_limit_per_sec)

    if not adapter.enabled:
        msg = "Tushare adapter disabled (missing token or tushare package)."
        print(f"❌ {msg}")
        record_ingestion_run(settings, "stock_info_sync", job_date, status="failed", error=msg)
        return

    trade_date = args.trade_date or _detect_latest_open_trade_date(adapter, args.lookback_days)
    print(f"🟡 Stock Info Sync")
    print(f"   Target trade_date: {trade_date}")

    df = adapter.fetch_bak_basic(trade_date=trade_date, fields=BAK_BASIC_FIELDS)
    if df is None or df.empty:
        msg = f"No bak_basic data returned for trade_date={trade_date}"
        print(f"⚠️  {msg}")
        record_ingestion_run(settings, "stock_info_sync", job_date, status="skipped", error=msg)
        return

    client = pymongo.MongoClient(settings.mongo_uri)
    db = client[settings.data_db]
    coll = db["stock_info"]
    coll.create_index("symbol", background=True)
    coll.create_index("ts_code", background=True)
    coll.create_index("industry", background=True)

    ops: List[UpdateOne] = []
    for _, row in df.iterrows():
        ts_code = str(row.get("ts_code", "")).strip()
        if not ts_code:
            continue
        doc = _build_doc(row)
        symbol = doc["symbol"]
        ops.append(
            UpdateOne(
                {"$or": [{"ts_code": ts_code}, {"symbol": symbol}]},
                {"$set": doc},
                upsert=True,
            )
        )

    if not ops:
        msg = f"No valid rows to upsert for trade_date={trade_date}"
        print(f"⚠️  {msg}")
        record_ingestion_run(settings, "stock_info_sync", job_date, status="skipped", error=msg)
        return

    result = coll.bulk_write(ops, ordered=False)
    changed = int(result.upserted_count or 0) + int(result.modified_count or 0)

    print("✅ Stock info sync completed")
    print(f"   Source rows: {len(df)}")
    print(f"   Upserted: {result.upserted_count}")
    print(f"   Modified: {result.modified_count}")
    print(f"   Changed total: {changed}")

    record_ingestion_run(
        settings,
        "stock_info_sync",
        job_date,
        records_changed=changed,
        trade_date=trade_date,
        source_rows=int(len(df)),
    )


if __name__ == "__main__":
    main()
