#!/usr/bin/env python3
"""CLI task to sync stock_info from Tushare bak_basic into quant_data.stock_info.

``symbol`` and ``ts_code`` are both the Tushare ts_code (e.g. 600000.SH) so joins with
index_constituents and volume_price stay consistent. Upsert filters also match legacy
rows that only had a 6-digit ``symbol``.

bak_basic only returns names that traded on the target day, so delisted symbols are
invisible to it forever and a fresh listing that has not traded yet is missing too.
``--sync-delisted`` adds both from stock_basic, writing identity columns only.
Delisted/suspended rows are refreshed every run; still-listed ones are inserted
only when absent, since bak_basic owns their industry and valuation columns:

    python -m quant_data_engine.tasks.run_stock_info_sync --sync-delisted
    python -m quant_data_engine.tasks.run_stock_info_sync --delisted-only
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List, Sequence

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


STOCK_BASIC_FIELDS = "ts_code,symbol,name,area,industry,market,exchange,list_status,list_date,delist_date"

_INDEX_CONFLICT_CODE = 86  # IndexKeySpecsConflict

# Identity only. bak_basic drives the valuation columns for names that still
# trade; a delisted row must never reset pe/pb/total_mv on a live symbol.
IDENTITY_FIELDS = ("name", "industry", "area", "market", "exchange", "list_status", "list_date", "delist_date")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync stock_info into quant_data.stock_info")
    parser.add_argument("--trade-date", help="Target trade date YYYYMMDD. If omitted, auto-detect latest open day.")
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=14,
        help="Lookback days used when auto-detecting latest open trade day (default: 14).",
    )
    parser.add_argument(
        "--sync-delisted",
        action="store_true",
        help="Also upsert delisted/suspended names from stock_basic (identity fields only).",
    )
    parser.add_argument(
        "--delisted-only",
        action="store_true",
        help="Skip the bak_basic valuation snapshot and only backfill delisted/suspended names.",
    )
    return parser.parse_args()


def _build_identity_doc(row: pd.Series) -> Dict[str, Any]:
    """Identity-only doc for a stock_basic row, valuation columns untouched."""
    ts_code = str(row.get("ts_code", "")).strip()
    doc: Dict[str, Any] = {"symbol": ts_code, "ts_code": ts_code, "source_identity": "tushare_stock_basic"}
    for field in IDENTITY_FIELDS:
        value = row.get(field)
        doc[field] = "" if value is None or pd.isna(value) else str(value).strip()
    doc["last_identity_update"] = datetime.now()
    return doc


def _ensure_indexes(coll) -> None:
    """Create the lookup indexes, tolerating ones that already exist.

    stock_info was seeded with a sparse ``symbol_1``; asking for the same name
    with different options raises IndexKeySpecsConflict and used to abort the
    whole sync over an index that was already there.
    """
    for key in ("symbol", "ts_code", "industry"):
        try:
            coll.create_index(key, background=True)
        except pymongo.errors.OperationFailure as exc:
            if exc.code != _INDEX_CONFLICT_CODE:
                raise
            print(f"   ℹ️  index on {key} already exists with different options, keeping it")


def _identity_match(ts_code: str) -> Dict[str, Any]:
    short = ts_code.split(".", 1)[0] if "." in ts_code else ts_code
    return {"$or": [{"ts_code": ts_code}, {"symbol": ts_code}, {"symbol": short}]}


def sync_identity_names(
    adapter: StockAdapter,
    coll,
    statuses: Sequence[str],
    *,
    insert_only: bool = False,
) -> Dict[str, int]:
    """Backfill names bak_basic cannot see, from stock_basic.

    bak_basic is a snapshot of what traded on one day, so a symbol that
    delisted before the first sync never entered stock_info at all, and a
    fresh listing that has not traded yet is equally invisible. Historical
    point-in-time universes still score those names, and a missing row costs
    the industry lookup that the growth market-competitiveness submodule uses.

    ``insert_only`` writes via ``$setOnInsert`` so existing rows are left
    alone. Use it for still-listed names: bak_basic owns their industry and
    valuation columns, and rewriting them here would only add churn.
    """
    stats = {"rows": 0, "upserted": 0, "modified": 0}
    ops: List[UpdateOne] = []
    operator = "$setOnInsert" if insert_only else "$set"
    for status in statuses:
        df = adapter.fetch_stock_basic(list_status=status, fields=STOCK_BASIC_FIELDS)
        if df is None or df.empty:
            continue
        stats["rows"] += int(len(df))
        for _, row in df.iterrows():
            ts_code = str(row.get("ts_code", "")).strip()
            if not ts_code:
                continue
            ops.append(UpdateOne(_identity_match(ts_code), {operator: _build_identity_doc(row)}, upsert=True))

    if not ops:
        return stats
    result = coll.bulk_write(ops, ordered=False)
    stats["upserted"] = int(result.upserted_count or 0)
    stats["modified"] = int(result.modified_count or 0)
    return stats


def sync_delisted_names(adapter: StockAdapter, coll, statuses: Sequence[str] = ("D", "P")) -> Dict[str, int]:
    """Refresh delisted/suspended identities, then add any missing live ones."""
    stats = sync_identity_names(adapter, coll, statuses)
    listed = sync_identity_names(adapter, coll, ("L",), insert_only=True)
    return {
        "rows": stats["rows"] + listed["rows"],
        "upserted": stats["upserted"] + listed["upserted"],
        "modified": stats["modified"] + listed["modified"],
        "listed_inserted": listed["upserted"],
    }


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
    valuation_date = str(row.get("trade_date", "")).strip()

    doc = {
        "symbol": ts_code,
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
        # bak_basic only ever returns names that traded that day.
        "list_status": "L",
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

    print(f"🟡 Stock Info Sync")

    client = pymongo.MongoClient(settings.mongo_uri)
    db = client[settings.data_db]
    coll = db["stock_info"]
    _ensure_indexes(coll)

    if args.delisted_only or args.sync_delisted:
        stats = sync_delisted_names(adapter, coll)
        print(f"   stock_basic rows scanned: {stats['rows']}")
        print(f"   identity upserted / modified: {stats['upserted']} / {stats['modified']}")
        print(f"   of which missing listed names inserted: {stats.get('listed_inserted', 0)}")
        if args.delisted_only:
            # Its own job_type: this runs weekly, and stamping stock_info_sync
            # here would hide a failed daily bak_basic pass from freshness checks.
            record_ingestion_run(
                settings,
                "stock_info_delisted_sync",
                job_date,
                records_changed=stats["upserted"] + stats["modified"],
                source_rows=stats["rows"],
            )
            return

    trade_date = args.trade_date or _detect_latest_open_trade_date(adapter, args.lookback_days)
    print(f"   Target trade_date: {trade_date}")

    df = adapter.fetch_bak_basic(trade_date=trade_date, fields=BAK_BASIC_FIELDS)
    if df is None or df.empty:
        msg = f"No bak_basic data returned for trade_date={trade_date}"
        print(f"⚠️  {msg}")
        record_ingestion_run(settings, "stock_info_sync", job_date, status="skipped", error=msg)
        return

    ops: List[UpdateOne] = []
    for _, row in df.iterrows():
        ts_code = str(row.get("ts_code", "")).strip()
        if not ts_code:
            continue
        ops.append(UpdateOne(_identity_match(ts_code), {"$set": _build_doc(row)}, upsert=True))

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
