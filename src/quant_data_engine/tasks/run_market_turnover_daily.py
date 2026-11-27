"""Pre-compute market-wide turnover by board into ``market_turnover_daily``.

One row per trading day, derived from two sources:

* ``volume_price`` — end-of-day stock rows only. Intraday snapshot rows
  (``miniqmt`` / ``tushare_rt_k`` / ``eastmoney_snapshot``) are excluded, so a
  day that has not finished syncing is skipped rather than recorded as a
  half-session.
* ``etf_daily`` + ``fund_basic`` — listed funds grouped into 股票型 / 债券型 /
  货币型 / 其他. ETF is kept in its own fields, never merged into the stock
  total, because ETFs have no intraday feed and the two are compared side by
  side rather than added (see the volume-waterline design note).

Amounts are written in 亿元.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timedelta
from typing import Any, Dict, List, Mapping, Optional

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from quant_data_engine.config.settings import get_settings
from quant_data_engine.tools.market_turnover import STOCK_SNAPSHOT_SOURCES, aggregate_boards, aggregate_etf
from quant_data_engine.tools.market_turnover.repository import TurnoverRepository
from quant_data_engine.utils.ingestion_job import record_ingestion_run

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

SCHEMA_VERSION = 1


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="A股分板块成交额日频聚合（股票五板块 + ETF 三类）")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--date", default=None, help="计算日期 YYYYMMDD（默认今天）")
    group.add_argument("--start", default=None, help="回填起始日期 YYYYMMDD")
    parser.add_argument("--end", default=None, help="回填结束日期 YYYYMMDD（默认今天）")
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=3,
        help="未指定日期时重算最近 N 个工作日（幂等，用于补上 ETF 迟到的那一天）",
    )
    parser.add_argument("--dry-run", action="store_true", help="计算但不写 MongoDB")
    parser.add_argument("--verbose", action="store_true", help="打印摘要")
    return parser.parse_args()


def _date_range(start: str, end: str) -> List[str]:
    s = datetime.strptime(start, "%Y%m%d")
    e = datetime.strptime(end, "%Y%m%d")
    dates: List[str] = []
    cur = s
    while cur <= e:
        if cur.weekday() < 5:
            dates.append(cur.strftime("%Y%m%d"))
        cur += timedelta(days=1)
    return dates


def load_fund_types(db: Any) -> Dict[str, Optional[str]]:
    """Map ``ts_code`` -> Tushare fund type, used to split listed funds."""
    types: Dict[str, Optional[str]] = {}
    for doc in db["fund_basic"].find({}, {"_id": 0, "ts_code": 1, "fund_type": 1, "type": 1}):
        types[doc.get("ts_code")] = doc.get("fund_type") or doc.get("type")
    return types


def build_daily_doc(
    db: Any,
    trade_date: str,
    fund_types: Mapping[str, Optional[str]],
) -> Optional[Dict[str, Any]]:
    """Build one turnover row, or ``None`` when no end-of-day stock rows exist."""
    stock_rows = db["volume_price"].find(
        {"trade_date": trade_date, "source": {"$nin": list(STOCK_SNAPSHOT_SOURCES)}},
        {"_id": 0, "symbol": 1, "amount": 1},
    )
    stocks = aggregate_boards(stock_rows)
    if stocks["n_stocks"] == 0:
        return None

    etf_rows = db["etf_daily"].find(
        {"trade_date": trade_date}, {"_id": 0, "ts_code": 1, "amount": 1}
    )
    funds = aggregate_etf(etf_rows, fund_types)

    doc: Dict[str, Any] = {
        "trade_date": trade_date,
        "schema_version": SCHEMA_VERSION,
        "source": "derived",
        "stock_count": stocks["n_stocks"],
        "stock_total": round(stocks["total"], 2),
    }
    for key, value in stocks["amounts"].items():
        doc[key] = round(value, 2)
    for key, value in funds["amounts"].items():
        doc[f"etf_{key}"] = round(value, 2)
    doc["etf_total"] = round(funds["total"], 2)
    doc["etf_count"] = funds["n_funds"]
    # etf_daily lands around 16:11 via the dispatcher, but a late probe can push it
    # past this job. Flag it explicitly so the API shows "missing" instead of a
    # zero that reads like "nobody traded ETFs today".
    doc["etf_available"] = funds["n_funds"] > 0
    return doc


def run(
    dates: List[str],
    db: Any,
    repo: TurnoverRepository,
    *,
    fund_types: Mapping[str, Optional[str]],
    dry_run: bool,
    verbose: bool,
) -> Dict[str, int]:
    ok = 0
    errors = 0
    records = 0
    skipped = 0
    for trade_date in dates:
        try:
            doc = build_daily_doc(db, trade_date, fund_types)
            if doc is None:
                logger.info("Skipping turnover date=%s: no end-of-day volume_price rows", trade_date)
                skipped += 1
                continue
            if verbose:
                logger.info(
                    "  %s stock_total=%.1f sh=%.1f sz=%.1f cx=%.1f star=%.1f bse=%.1f | etf=%.1f (equity %.1f / bond %.1f / money %.1f)",
                    trade_date,
                    doc["stock_total"],
                    doc["sh"],
                    doc["sz"],
                    doc["cx"],
                    doc["star"],
                    doc["bse"],
                    doc["etf_total"],
                    doc["etf_equity"],
                    doc["etf_bond"],
                    doc["etf_money"],
                )
            if not dry_run:
                records += 1 if repo.upsert_daily(doc) else 0
            ok += 1
        except Exception as exc:
            logger.error("Turnover compute failed date=%s: %s", trade_date, exc)
            errors += 1
    return {"ok": ok, "skipped": skipped, "errors": errors, "records_changed": records}


def main() -> None:
    args = parse_args()
    settings = get_settings()
    today = datetime.today().strftime("%Y%m%d")
    if args.start:
        dates = _date_range(args.start, args.end or today)
    elif args.date:
        dates = [args.date]
    else:
        # Recompute a short window by default: the row is idempotent, and this is
        # what repairs a day whose ETF rows arrived after the job had run.
        lookback = max(1, args.lookback_days)
        dates = _date_range(
            (datetime.strptime(today, "%Y%m%d") - timedelta(days=lookback * 2 + 2)).strftime("%Y%m%d"),
            today,
        )[-lookback:]

    repo = TurnoverRepository(settings.mongo_uri, settings.data_db, settings.realtime_db)
    fund_types = load_fund_types(repo.data_db)
    summary = run(
        dates,
        repo.data_db,
        repo,
        fund_types=fund_types,
        dry_run=args.dry_run,
        verbose=args.verbose,
    )
    logger.info("run_market_turnover_daily finished: %s", summary)

    if summary["errors"]:
        status = "partial"
        error = f"{summary['errors']} date(s) failed"
    elif summary["ok"] == 0 and summary["skipped"]:
        status = "skipped"
        error = f"no end-of-day volume_price rows for {summary['skipped']} date(s): {', '.join(dates)}"
        logger.warning("Computed nothing — %s", error)
    else:
        status = "complete"
        error = None

    if not args.dry_run:
        record_ingestion_run(
            settings,
            job_type="market_turnover_daily",
            date_str=dates[-1],
            status=status,
            error=error,
            records_changed=summary["records_changed"],
        )
    if summary["errors"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
