"""Sync Tushare index_dailybasic rows into quant_data.index_daily_basic."""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timedelta
from typing import Dict, List

import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from quant_data_engine.adapters.tushare_adapter import TushareAdapter
from quant_data_engine.config.settings import get_settings
from quant_data_engine.repositories.market_regime_repository import MarketRegimeRepository
from quant_data_engine.tools.risk_scanner.instruments import CAPE_INDEX_PROFILES
from quant_data_engine.utils.ingestion_job import record_ingestion_run

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

DEFAULT_START_DATE = "20100101"
INDEX_DAILYBASIC_FIELDS = "ts_code,trade_date,close,pe,pe_ttm,pb,total_mv"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="同步指数估值指标 index_dailybasic")
    parser.add_argument("--start", default=None, help="起始日期 YYYYMMDD；指定后按区间同步")
    parser.add_argument("--end", default=datetime.today().strftime("%Y%m%d"), help="结束日期 YYYYMMDD")
    parser.add_argument("--recent-days", type=int, default=5, help="未指定 --start 时同步最近 N 个自然日")
    parser.add_argument("--indices", nargs="+", default=list(CAPE_INDEX_PROFILES.keys()), help="指数 profile key 或 ts_code")
    parser.add_argument("--dry-run", action="store_true", help="只抓取和打印统计，不写 MongoDB")
    return parser.parse_args()


def _resolve_indices(values: List[str]) -> Dict[str, str]:
    resolved: Dict[str, str] = {}
    for value in values:
        if value in CAPE_INDEX_PROFILES:
            profile = CAPE_INDEX_PROFILES[value]
            resolved[profile["ts_code"]] = profile["name"]
        else:
            resolved[value] = value
    return resolved


def _start_date(args: argparse.Namespace) -> str:
    if args.start:
        return args.start
    return (datetime.strptime(args.end, "%Y%m%d") - timedelta(days=max(args.recent_days, 1) - 1)).strftime("%Y%m%d")


def _clean_rows(df: pd.DataFrame) -> List[dict]:
    if df is None or df.empty:
        return []
    rows = []
    for raw in df.to_dict("records"):
        row = {key: (None if pd.isna(value) else value) for key, value in raw.items()}
        if not row.get("ts_code") or not row.get("trade_date"):
            continue
        rows.append(row)
    return rows


def _year_windows(start: str, end: str) -> List[tuple]:
    """Split [start, end] into per-calendar-year windows.

    Tushare ``index_dailybasic`` caps a single call at ~3000 rows, so a long
    range silently drops the oldest dates. Per-year windows (~243 trading days)
    stay well under that cap.
    """
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


def run(args: argparse.Namespace) -> dict:
    settings = get_settings()
    if not settings.tushare_token:
        raise RuntimeError("TUSHARE_TOKEN 未设置")
    adapter = TushareAdapter(settings.tushare_token, rate_limit_per_sec=settings.rate_limit_per_sec)
    repo = MarketRegimeRepository(settings.mongo_uri, settings.data_db)
    start = _start_date(args)
    end = args.end
    indices = _resolve_indices(args.indices)
    windows = _year_windows(start, end)
    fetched = 0
    written = 0

    for ts_code, name in indices.items():
        index_rows: List[dict] = []
        for win_start, win_end in windows:
            logger.info("Fetching index_dailybasic: %s %s %s..%s", ts_code, name, win_start, win_end)
            df = adapter.fetch_index_dailybasic(ts_code=ts_code, start_date=win_start, end_date=win_end, fields=INDEX_DAILYBASIC_FIELDS)
            chunk = _clean_rows(df)
            if len(chunk) >= 3000:
                logger.warning("  %s %s..%s 返回 %d 行，已达单次上限，可能仍被截断", ts_code, win_start, win_end, len(chunk))
            index_rows.extend(chunk)
        # de-duplicate within this index by trade_date (overlapping window edges)
        deduped = {row["trade_date"]: row for row in index_rows}
        rows = list(deduped.values())
        fetched += len(rows)
        if args.dry_run:
            logger.info("  [dry-run] %s rows=%d span=%s..%s", ts_code, len(rows),
                        min((r["trade_date"] for r in rows), default="-"),
                        max((r["trade_date"] for r in rows), default="-"))
        else:
            written += repo.upsert_index_daily_basic(rows)

    return {"fetched": fetched, "written": written, "start": start, "end": end}


def main() -> None:
    args = parse_args()
    settings = get_settings()
    try:
        summary = run(args)
        logger.info("run_index_dailybasic_sync complete: %s", summary)
        if not args.dry_run:
            record_ingestion_run(
                settings,
                job_type="index_dailybasic",
                date_str=summary["end"],
                status="complete",
                records_changed=summary["written"],
                start_date=summary["start"],
                end_date=summary["end"],
            )
    except Exception as exc:
        logger.error("run_index_dailybasic_sync failed: %s", exc)
        if not args.dry_run:
            record_ingestion_run(settings, "index_dailybasic", args.end, status="failed", error=str(exc))
        raise SystemExit(1)


if __name__ == "__main__":
    main()