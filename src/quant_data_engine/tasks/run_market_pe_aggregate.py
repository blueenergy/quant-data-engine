"""Pre-aggregate all-A harmonic PE from financial_daily_basic.

Writes one row per trade date to ``quant_data.market_pe_daily`` with
``ts_code=ALLA.WI``. Formula is cap-weighted harmonic TTM PE of profitable
Shanghai/Shenzhen A-shares:

    pe_ttm = Σ total_mv / Σ (total_mv / pe_ttm)

This is a dashboard thermometer, not CSI All-Share (000985) official PE.
"""
from __future__ import annotations

import argparse
import logging
import math
import os
import sys
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Sequence

from pymongo import ASCENDING, MongoClient, UpdateOne
from pymongo.collection import Collection

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from quant_data_engine.config.settings import get_settings
from quant_data_engine.utils.ingestion_job import record_ingestion_run

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

JOB_TYPE = "market_pe_aggregate"
SOURCE_COLL = "financial_daily_basic"
DEST_COLL = "market_pe_daily"
ALLA_CODE = "ALLA.WI"
ALLA_NAME = "全A（自算）"
METHOD = "cap_harmonic_ttm"
UNIVERSE = "sh_sz_a"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="从 financial_daily_basic 预聚合全A谐波市盈率")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--date", default=None, help="单个交易日 YYYYMMDD")
    group.add_argument("--start", default=None, help="回填起始日期 YYYYMMDD")
    parser.add_argument("--end", default=None, help="结束日期 YYYYMMDD（默认今天）")
    parser.add_argument("--recent-days", type=int, default=5, help="未指定 --date/--start 时回看的自然日数")
    parser.add_argument("--dry-run", action="store_true", help="计算但不写 MongoDB")
    parser.add_argument("--verbose", action="store_true", help="打印每日摘要")
    return parser.parse_args()


def _to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(number) or math.isinf(number):
        return None
    return number


def is_sh_sz_a(ts_code: Any) -> bool:
    code = str(ts_code or "")
    return code.endswith(".SH") or code.endswith(".SZ")


def aggregate_all_a_pe(rows: Sequence[Dict[str, Any]], trade_date: str) -> Optional[Dict[str, Any]]:
    """Return one market_pe_daily document, or None when the universe is empty."""
    n_universe = 0
    n_used = 0
    n_loss = 0
    n_missing = 0
    sum_mv = 0.0
    sum_earnings = 0.0

    for row in rows:
        if not is_sh_sz_a(row.get("ts_code")):
            continue
        n_universe += 1
        pe_ttm = _to_float(row.get("pe_ttm"))
        total_mv = _to_float(row.get("total_mv"))
        if pe_ttm is not None and pe_ttm > 0 and total_mv is not None and total_mv > 0:
            n_used += 1
            sum_mv += total_mv
            sum_earnings += total_mv / pe_ttm
            continue
        if total_mv is not None and total_mv > 0 and pe_ttm is not None and pe_ttm <= 0:
            n_loss += 1
            continue
        n_missing += 1

    if n_universe == 0:
        return None
    pe_ttm = (sum_mv / sum_earnings) if n_used and sum_earnings > 0 else None
    return {
        "ts_code": ALLA_CODE,
        "name": ALLA_NAME,
        "trade_date": str(trade_date),
        "pe_ttm": pe_ttm,
        "total_mv": sum_mv if n_used else None,
        "n_universe": n_universe,
        "n_used": n_used,
        "n_loss": n_loss,
        "n_missing": n_missing,
        "method": METHOD,
        "universe": UNIVERSE,
    }


def _ensure_indexes(coll: Collection) -> None:
    coll.create_index(
        [("ts_code", ASCENDING), ("trade_date", ASCENDING)],
        unique=True,
        name="market_pe_code_date_uidx",
        background=True,
    )
    coll.create_index(
        [("trade_date", ASCENDING), ("ts_code", ASCENDING)],
        name="market_pe_date_code_idx",
        background=True,
    )


def upsert_pe_rows(coll: Collection, rows: Iterable[Dict[str, Any]]) -> int:
    ops = []
    now = datetime.utcnow()
    for row in rows:
        payload = dict(row)
        payload["updated_at"] = now
        ops.append(
            UpdateOne(
                {"ts_code": payload["ts_code"], "trade_date": payload["trade_date"]},
                {"$set": payload},
                upsert=True,
            )
        )
    if not ops:
        return 0
    result = coll.bulk_write(ops, ordered=False)
    return (result.upserted_count or 0) + (result.modified_count or 0)


def resolve_dates(args: argparse.Namespace) -> List[str]:
    end = args.end or datetime.today().strftime("%Y%m%d")
    if args.date:
        return [args.date]
    if args.start:
        start = args.start
    else:
        start = (
            datetime.strptime(end, "%Y%m%d") - timedelta(days=max(int(args.recent_days), 1) - 1)
        ).strftime("%Y%m%d")
    start_dt = datetime.strptime(start, "%Y%m%d")
    end_dt = datetime.strptime(end, "%Y%m%d")
    dates: List[str] = []
    cur = start_dt
    while cur <= end_dt:
        if cur.weekday() < 5:
            dates.append(cur.strftime("%Y%m%d"))
        cur += timedelta(days=1)
    return dates


def _load_day_rows(coll: Collection, trade_date: str) -> List[Dict[str, Any]]:
    return list(
        coll.find(
            {"trade_date": trade_date},
            {"_id": 0, "ts_code": 1, "pe_ttm": 1, "total_mv": 1},
        )
    )


def run(dates: Sequence[str], source: Collection, dest: Collection, *, dry_run: bool, verbose: bool) -> dict:
    ok = 0
    skipped = 0
    errors = 0
    written = 0
    computed: List[Dict[str, Any]] = []
    for trade_date in dates:
        try:
            rows = _load_day_rows(source, trade_date)
            doc = aggregate_all_a_pe(rows, trade_date)
            if doc is None:
                skipped += 1
                if verbose:
                    logger.info("Skipping %s: no SH/SZ daily_basic rows", trade_date)
                continue
            computed.append(doc)
            ok += 1
            if verbose:
                logger.info(
                    "  %s pe_ttm=%s n_used=%s n_loss=%s n_missing=%s n_universe=%s",
                    trade_date,
                    None if doc["pe_ttm"] is None else round(doc["pe_ttm"], 4),
                    doc["n_used"],
                    doc["n_loss"],
                    doc["n_missing"],
                    doc["n_universe"],
                )
        except Exception as exc:
            logger.error("market_pe_aggregate failed date=%s: %s", trade_date, exc)
            errors += 1
    if not dry_run and computed:
        written = upsert_pe_rows(dest, computed)
    return {
        "ok": ok,
        "skipped": skipped,
        "errors": errors,
        "records_changed": written,
        "start": dates[0] if dates else None,
        "end": dates[-1] if dates else None,
    }


def main() -> None:
    args = parse_args()
    settings = get_settings()
    dates = resolve_dates(args)
    client = MongoClient(settings.mongo_uri)
    try:
        db = client[settings.data_db]
        source = db[SOURCE_COLL]
        dest = db[DEST_COLL]
        if not args.dry_run:
            _ensure_indexes(dest)
        summary = run(dates, source, dest, dry_run=args.dry_run, verbose=args.verbose or args.dry_run)
    finally:
        client.close()

    logger.info("run_market_pe_aggregate finished: %s", summary)
    if summary["errors"]:
        status = "partial"
        error = f"{summary['errors']} date(s) failed"
    elif summary["ok"] == 0 and summary["skipped"]:
        status = "skipped"
        error = f"no financial_daily_basic rows for {summary['skipped']} date(s)"
        logger.warning("Computed nothing — %s", error)
    else:
        status = "complete"
        error = None

    if not args.dry_run:
        record_ingestion_run(
            settings,
            job_type=JOB_TYPE,
            date_str=dates[-1] if dates else (args.end or datetime.today().strftime("%Y%m%d")),
            status=status,
            error=error,
            records_changed=summary["records_changed"],
            start_date=summary["start"],
            end_date=summary["end"],
        )
    if summary["errors"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
