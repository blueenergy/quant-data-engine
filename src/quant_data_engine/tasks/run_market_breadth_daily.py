"""Pre-compute daily A-share breadth atomic metrics into market_breadth_daily.

This task isolates the heavy ``volume_price`` scan (advances / declines /
unchanged / new-high / new-low) from the regime scan so that
``run_market_regime_scan`` can consume the pre-aggregated rows instead of
re-scanning millions of price rows on every run.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timedelta
from typing import List

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from quant_data_engine.config.settings import get_settings
from quant_data_engine.repositories.market_regime_repository import MarketRegimeRepository
from quant_data_engine.tools.risk_scanner.breadth_builder import MarketBreadthBuilder
from quant_data_engine.utils.ingestion_job import record_ingestion_run

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="A股每日广度原子指标预计算（涨跌/新高新低）")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--date", default=None, help="计算日期 YYYYMMDD（默认今天）")
    group.add_argument("--start", default=None, help="回填起始日期 YYYYMMDD")
    parser.add_argument("--end", default=None, help="回填结束日期 YYYYMMDD（默认今天）")
    parser.add_argument("--dry-run", action="store_true", help="计算但不写 MongoDB")
    parser.add_argument("--verbose", action="store_true", help="打印摘要")
    return parser.parse_args()


def _date_range(start: str, end: str) -> List[str]:
    s = datetime.strptime(start, "%Y%m%d")
    e = datetime.strptime(end, "%Y%m%d")
    dates = []
    cur = s
    while cur <= e:
        if cur.weekday() < 5:
            dates.append(cur.strftime("%Y%m%d"))
        cur += timedelta(days=1)
    return dates


def _has_volume_price_rows(repo: MarketRegimeRepository, trade_date: str) -> bool:
    return repo.db["volume_price"].find_one({"trade_date": trade_date}, {"_id": 1}) is not None


def run(dates: List[str], builder: MarketBreadthBuilder, repo: MarketRegimeRepository, *, dry_run: bool, verbose: bool) -> dict:
    ok = 0
    errors = 0
    records = 0
    skipped = 0
    for trade_date in dates:
        try:
            if not _has_volume_price_rows(repo, trade_date):
                logger.info("Skipping market breadth date=%s: no volume_price rows", trade_date)
                skipped += 1
                continue
            logger.info("Computing market breadth date=%s", trade_date)
            warnings: List[str] = []
            breadth_doc = builder.build(trade_date, warnings=warnings)
            if verbose:
                logger.info(
                    "  total=%s advances=%s declines=%s unchanged=%s new_high=%s new_low=%s warnings=%d",
                    breadth_doc.get("total_normal"),
                    breadth_doc.get("advances"),
                    breadth_doc.get("declines"),
                    breadth_doc.get("unchanged"),
                    breadth_doc.get("new_high_count"),
                    breadth_doc.get("new_low_count"),
                    len(warnings),
                )
                for warning in warnings:
                    logger.info("  warning: %s", warning)
            if not dry_run:
                records += 1 if repo.upsert_breadth(breadth_doc) else 0
            ok += 1
        except Exception as exc:
            logger.error("Market breadth compute failed date=%s: %s", trade_date, exc)
            errors += 1
    return {"ok": ok, "skipped": skipped, "errors": errors, "records_changed": records}


def main() -> None:
    args = parse_args()
    settings = get_settings()
    today = datetime.today().strftime("%Y%m%d")
    if args.start:
        dates = _date_range(args.start, args.end or today)
    else:
        dates = [args.date or today]
    repo = MarketRegimeRepository(settings.mongo_uri, settings.data_db)
    builder = MarketBreadthBuilder(repo)
    summary = run(dates, builder, repo, dry_run=args.dry_run, verbose=args.verbose)
    logger.info("run_market_breadth_daily finished: %s", summary)

    # Reporting only on `errors` let a run that computed nothing at all still be
    # recorded as complete. Every day the upstream volume_price rows landed after
    # this job ran, so it skipped, reported success, and market_breadth_daily
    # silently stopped advancing while the task log stayed green.
    if summary["errors"]:
        status = "partial"
        error = f"{summary['errors']} date(s) failed"
    elif summary["ok"] == 0 and summary["skipped"]:
        status = "skipped"
        error = f"no volume_price rows for {summary['skipped']} date(s): {', '.join(dates)}"
        logger.warning("Computed nothing — %s", error)
    else:
        status = "complete"
        error = None

    if not args.dry_run:
        record_ingestion_run(
            settings,
            job_type="market_breadth_daily",
            date_str=dates[-1],
            status=status,
            error=error,
            records_changed=summary["records_changed"],
        )
    if summary["errors"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
