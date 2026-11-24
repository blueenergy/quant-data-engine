"""Offshore RMB daily bars from tushare into ``global_market_daily``.

Why not Yahoo: ``USDCNH=X`` (and ``CNH=X``) serve **one** bar for every range
and interval — only the live quote exists, no daily history. ``USDCNY=X`` has a
full series but that is the *onshore* rate, a different market.  tushare's
``fx_daily`` carries ``USDCNH.FXCM`` (offshore, FXCM) with real history, so the
daily series comes from there while the live quote keeps coming from Yahoo.

``run_global_market_daily_sync`` no longer lists ``USDCNH=X``: without history,
all it could write was a single snapshot-priced bar per day, which is not a
close.

Prices: tushare gives ``bid_*`` and ``ask_*``. The bid/ask **midpoint** is used
so the bar matches what Yahoo's quote represents.  ``volume`` stays 0 — FX has
no traded volume and the chart would label it as shares.

Usage::

    python -m quant_data_engine.tasks.run_fx_daily_sync            # last 5 days
    python -m quant_data_engine.tasks.run_fx_daily_sync --days 30
    python -m quant_data_engine.tasks.run_fx_daily_sync --backfill # 2 years
    python -m quant_data_engine.tasks.run_fx_daily_sync --dry-run
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from quant_data_engine.config.settings import get_settings
from quant_data_engine.utils.ingestion_job import record_ingestion_run

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# tushare ts_code -> global_market_daily symbol
FX_CODES: Dict[str, str] = {
    "USDCNH.FXCM": "USDCNH=X",
}

JOB_TYPE = "fx_daily_sync"
# tushare caps rows per response; the 2-year backfill is walked in chunks.
BACKFILL_CHUNK_DAYS = 120


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="离岸人民币日线同步（tushare fx_daily → global_market_daily）")
    p.add_argument("--days", type=int, default=5, help="增量天数（默认 5）")
    p.add_argument("--start", type=str, default=None, help="起始日期 YYYYMMDD")
    p.add_argument("--end", type=str, default=None, help="结束日期 YYYYMMDD（默认今天）")
    p.add_argument("--backfill", action="store_true", help="回填 2 年历史")
    p.add_argument("--codes", nargs="+", default=None, help=f"指定 ts_code（默认 {list(FX_CODES)}）")
    p.add_argument("--dry-run", action="store_true", help="只拉取不写库")
    return p.parse_args()


def _mid(row, side_a: str, side_b: str) -> Optional[float]:
    """Midpoint of the bid/ask pair, or ``None`` when either leg is missing."""
    a, b = row.get(side_a), row.get(side_b)
    try:
        if a is None or b is None or a != a or b != b:  # NaN check
            return None
        return round((float(a) + float(b)) / 2, 6)
    except (TypeError, ValueError):
        return None


def _date_chunks(start: str, end: str) -> List[Tuple[str, str]]:
    """Split a long range so one tushare call never trips the row cap."""
    fmt = "%Y%m%d"
    cur = datetime.strptime(start, fmt)
    stop = datetime.strptime(end, fmt)
    chunks: List[Tuple[str, str]] = []
    while cur <= stop:
        chunk_end = min(cur + timedelta(days=BACKFILL_CHUNK_DAYS - 1), stop)
        chunks.append((cur.strftime(fmt), chunk_end.strftime(fmt)))
        cur = chunk_end + timedelta(days=1)
    return chunks


def _fetch(pro, ts_code: str, start: str, end: str) -> List[dict]:
    """Fetch and de-duplicate one ts_code over a range."""
    seen: Dict[str, dict] = {}
    for chunk_start, chunk_end in _date_chunks(start, end):
        try:
            df = pro.fx_daily(ts_code=ts_code, start_date=chunk_start, end_date=chunk_end)
        except Exception as exc:
            logger.warning("fx_daily %s %s-%s failed: %s", ts_code, chunk_start, chunk_end, exc)
            continue
        if df is None or df.empty:
            continue
        for _, row in df.iterrows():
            date_str = str(row.get("trade_date") or "")
            if len(date_str) != 8:
                continue
            seen[date_str] = row.to_dict()
        logger.info("  %s %s-%s: %d 行", ts_code, chunk_start, chunk_end, len(df))
    return [seen[date_str] for date_str in sorted(seen)]


def sync_code(pro, ts_code: str, symbol: str, start: str, end: str, coll, dry_run: bool) -> int:
    rows = _fetch(pro, ts_code, start, end)
    if not rows:
        logger.warning("%s: 区间 %s-%s 无数据", symbol, start, end)
        return 0

    now = datetime.utcnow()
    written = 0
    for row in rows:
        close = _mid(row, "bid_close", "ask_close")
        if close is None:
            continue
        doc = {
            "symbol": symbol,
            "date": str(row["trade_date"]),
            "open": _mid(row, "bid_open", "ask_open"),
            "high": _mid(row, "bid_high", "ask_high"),
            "low": _mid(row, "bid_low", "ask_low"),
            "close": close,
            "volume": 0,
            "source": f"tushare:{ts_code}",
            "updated_at": now,
        }
        if not dry_run:
            coll.update_one({"symbol": symbol, "date": doc["date"]}, {"$set": doc}, upsert=True)
        written += 1

    logger.info(
        "  %s (%s): %d/%d 行写入%s", symbol, ts_code, written, len(rows), " [dry-run]" if dry_run else ""
    )
    return written


def main() -> None:
    args = parse_args()
    settings = get_settings()

    token = os.environ.get("TUSHARE_TOKEN") or getattr(settings, "tushare_token", "")
    if not token:
        logger.error("TUSHARE_TOKEN 未设置")
        sys.exit(1)

    import tushare as ts
    from pymongo import MongoClient

    pro = ts.pro_api(token)

    end = (args.end or datetime.now().strftime("%Y%m%d")).replace("-", "")
    if args.backfill:
        start = (datetime.now() - timedelta(days=730)).strftime("%Y%m%d")
    elif args.start:
        start = args.start.replace("-", "")
    else:
        start = (datetime.now() - timedelta(days=args.days)).strftime("%Y%m%d")

    codes = args.codes or list(FX_CODES)
    logger.info("run_fx_daily_sync: start=%s end=%s codes=%s dry_run=%s", start, end, codes, args.dry_run)

    client = MongoClient(settings.mongo_uri)
    coll = client[settings.data_db]["global_market_daily"]

    total = 0
    failed: List[str] = []
    for ts_code in codes:
        symbol = FX_CODES.get(ts_code)
        if not symbol:
            logger.error("未知 ts_code（未配置 symbol 映射）: %s", ts_code)
            failed.append(ts_code)
            continue
        try:
            total += sync_code(pro, ts_code, symbol, start, end, coll, args.dry_run)
        except Exception as exc:
            logger.exception("同步 %s 失败: %s", ts_code, exc)
            failed.append(ts_code)

    client.close()

    logger.info("run_fx_daily_sync complete: rows=%d errors=%d", total, len(failed))
    if failed:
        logger.error("失败的品种: %s", ", ".join(failed))

    if not args.dry_run:
        record_ingestion_run(
            settings,
            job_type=JOB_TYPE,
            date_str=end,
            status="complete" if not failed else "partial",
            error=f"{len(failed)} code(s) failed" if failed else None,
            records_changed=total,
        )

    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
