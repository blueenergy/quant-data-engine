"""Sync Tushare financing/securities lending (两融) summaries into MongoDB.

Writes per-exchange rows plus an SSE+SZSE aggregate (exchange_id=ALL) to
``quant_data.margin_market_daily``. Amounts stay in yuan as Tushare returns them.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Sequence

import pandas as pd
from pymongo import ASCENDING, MongoClient, UpdateOne
from pymongo.collection import Collection

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from quant_data_engine.adapters.tushare_adapter import TushareAdapter
from quant_data_engine.config.settings import get_settings
from quant_data_engine.utils.ingestion_job import record_ingestion_run

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

DEFAULT_START_DATE = "20100101"
COLL_NAME = "margin_market_daily"
ALL_EXCHANGE_ID = "ALL"
HEADLINE_EXCHANGES = ("SSE", "SZSE")
FETCH_EXCHANGES = ("SSE", "SZSE", "BSE")
NUMERIC_FIELDS = ("rzye", "rzmre", "rzche", "rqye", "rqmcl", "rzrqye", "rqyl")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="同步沪深两融交易汇总 margin")
    parser.add_argument("--start", default=None, help="起始日期 YYYYMMDD；指定后按区间同步")
    parser.add_argument("--end", default=datetime.today().strftime("%Y%m%d"), help="结束日期 YYYYMMDD")
    parser.add_argument("--recent-days", type=int, default=10, help="未指定 --start 时同步最近 N 个自然日")
    parser.add_argument("--dry-run", action="store_true", help="只抓取和打印统计，不写 MongoDB")
    return parser.parse_args()


def _start_date(args: argparse.Namespace) -> str:
    if args.start:
        return args.start
    return (datetime.strptime(args.end, "%Y%m%d") - timedelta(days=max(args.recent_days, 1) - 1)).strftime("%Y%m%d")


def _year_windows(start: str, end: str) -> List[tuple]:
    start_dt = datetime.strptime(start, "%Y%m%d")
    end_dt = datetime.strptime(end, "%Y%m%d")
    if start_dt > end_dt:
        return []
    windows: List[tuple] = []
    year = start_dt.year
    while year <= end_dt.year:
        win_start = max(start_dt, datetime(year, 1, 1)).strftime("%Y%m%d")
        win_end = min(end_dt, datetime(year, 12, 31)).strftime("%Y%m%d")
        windows.append((win_start, win_end))
        year += 1
    return windows


def _to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def rows_from_df(df: Optional[pd.DataFrame]) -> List[dict]:
    if df is None or df.empty:
        return []
    rows: List[dict] = []
    for raw in df.to_dict("records"):
        trade_date = raw.get("trade_date")
        exchange_id = raw.get("exchange_id")
        # pandas can turn a None/NaN cell into float('nan'); ``not nan`` is
        # False, so an explicit isna check is required to skip the row.
        if trade_date is None or pd.isna(trade_date) or exchange_id is None or pd.isna(exchange_id):
            continue
        row = {
            "trade_date": str(trade_date),
            "exchange_id": str(exchange_id),
        }
        for field in NUMERIC_FIELDS:
            row[field] = _to_float(raw.get(field))
        rows.append(row)
    return rows


def aggregate_sse_szse(rows: Sequence[dict]) -> List[dict]:
    """Sum SSE+SZSE into exchange_id=ALL. Skip dates missing either exchange."""
    by_date: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        if row.get("exchange_id") not in HEADLINE_EXCHANGES:
            continue
        trade_date = row.get("trade_date")
        if trade_date is None or pd.isna(trade_date):
            continue
        bucket = by_date.setdefault(
            str(trade_date),
            {"exchanges": set(), "sums": {field: 0.0 for field in NUMERIC_FIELDS}, "seen": {field: False for field in NUMERIC_FIELDS}},
        )
        bucket["exchanges"].add(row["exchange_id"])
        for field in NUMERIC_FIELDS:
            value = _to_float(row.get(field))
            if value is None:
                continue
            bucket["sums"][field] += value
            bucket["seen"][field] = True
    out: List[dict] = []
    for trade_date, bucket in by_date.items():
        if bucket["exchanges"] != set(HEADLINE_EXCHANGES):
            continue
        item = {"trade_date": trade_date, "exchange_id": ALL_EXCHANGE_ID}
        for field in NUMERIC_FIELDS:
            item[field] = bucket["sums"][field] if bucket["seen"][field] else None
        out.append(item)
    out.sort(key=lambda row: row["trade_date"])
    return out


def _ensure_indexes(coll: Collection) -> None:
    coll.create_index(
        [("trade_date", ASCENDING), ("exchange_id", ASCENDING)],
        unique=True,
        name="margin_market_date_exchange_uidx",
        background=True,
    )
    coll.create_index(
        [("exchange_id", ASCENDING), ("trade_date", ASCENDING)],
        name="margin_market_exchange_date_idx",
        background=True,
    )


def upsert_margin_rows(coll: Collection, rows: Iterable[dict]) -> int:
    ops = []
    now = datetime.utcnow()
    for row in rows:
        payload = dict(row)
        payload["updated_at"] = now
        ops.append(
            UpdateOne(
                {"trade_date": payload["trade_date"], "exchange_id": payload["exchange_id"]},
                {"$set": payload},
                upsert=True,
            )
        )
    if not ops:
        return 0
    result = coll.bulk_write(ops, ordered=False)
    return (result.upserted_count or 0) + (result.modified_count or 0)


def run(args: argparse.Namespace) -> dict:
    settings = get_settings()
    if not settings.tushare_token:
        raise RuntimeError("TUSHARE_TOKEN 未设置")
    adapter = TushareAdapter(settings.tushare_token, rate_limit_per_sec=settings.rate_limit_per_sec)
    start = _start_date(args)
    end = args.end
    windows = _year_windows(start, end)
    fetched_rows: List[dict] = []

    for exchange_id in FETCH_EXCHANGES:
        for win_start, win_end in windows:
            logger.info("Fetching margin: %s %s..%s", exchange_id, win_start, win_end)
            try:
                df = adapter.fetch_margin(start_date=win_start, end_date=win_end, exchange_id=exchange_id)
            except Exception as exc:
                if exchange_id == "BSE":
                    logger.warning("  skip BSE margin %s..%s: %s", win_start, win_end, exc)
                    continue
                raise
            chunk = rows_from_df(df)
            if len(chunk) >= 1000:
                logger.warning("  %s %s..%s 返回 %d 行，已达单次上限，可能仍被截断", exchange_id, win_start, win_end, len(chunk))
            fetched_rows.extend(chunk)

    deduped = {(row["trade_date"], row["exchange_id"]): row for row in fetched_rows}
    exchange_rows = list(deduped.values())
    all_rows = aggregate_sse_szse(exchange_rows)
    to_write = exchange_rows + all_rows

    if args.dry_run:
        logger.info(
            "[dry-run] exchange_rows=%d all_rows=%d span=%s..%s",
            len(exchange_rows),
            len(all_rows),
            min((r["trade_date"] for r in exchange_rows), default="-"),
            max((r["trade_date"] for r in exchange_rows), default="-"),
        )
        return {"fetched": len(exchange_rows), "written": 0, "all_days": len(all_rows), "start": start, "end": end}

    client = MongoClient(settings.mongo_uri)
    try:
        coll = client[settings.data_db][COLL_NAME]
        _ensure_indexes(coll)
        written = upsert_margin_rows(coll, to_write)
    finally:
        client.close()

    return {
        "fetched": len(exchange_rows),
        "written": written,
        "all_days": len(all_rows),
        "start": start,
        "end": end,
    }


def main() -> None:
    args = parse_args()
    settings = get_settings()
    try:
        summary = run(args)
        logger.info("run_margin_sync complete: %s", summary)
        if not args.dry_run:
            record_ingestion_run(
                settings,
                job_type="margin_sync",
                date_str=summary["end"],
                status="complete",
                records_changed=summary["written"],
                start_date=summary["start"],
                end_date=summary["end"],
            )
    except Exception as exc:
        logger.error("run_margin_sync failed: %s", exc)
        if not args.dry_run:
            record_ingestion_run(settings, "margin_sync", args.end, status="failed", error=str(exc))
        raise SystemExit(1)


if __name__ == "__main__":
    main()
