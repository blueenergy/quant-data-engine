"""Run A-share market regime scan and persist breadth/CAPE/report documents."""
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
from quant_data_engine.tools.risk_scanner.regime_scanner import MarketRegimeScanner
from quant_data_engine.utils.ingestion_job import record_ingestion_run

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="A股广度与估值扫描（MCL/兴登堡/CAPE）")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--date", default=None, help="扫描日期 YYYYMMDD（默认今天）")
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


def run(dates: List[str], scanner: MarketRegimeScanner, repo: MarketRegimeRepository, *, dry_run: bool, verbose: bool) -> dict:
    ok = 0
    errors = 0
    records = 0
    skipped = 0
    for trade_date in dates:
        try:
            if not _has_volume_price_rows(repo, trade_date):
                logger.info("Skipping market regime date=%s: no volume_price rows", trade_date)
                skipped += 1
                continue
            logger.info("Scanning market regime date=%s", trade_date)
            report = scanner.scan(trade_date)
            if verbose:
                logger.info(
                    "  alert=%s cape=%d warnings=%d",
                    report.composite_alert,
                    len(report.cape),
                    len(report.warnings),
                )
                for warning in report.warnings:
                    logger.info("  warning: %s", warning)
            if not dry_run:
                if scanner.last_breadth_doc:
                    records += 1 if repo.upsert_breadth(scanner.last_breadth_doc) else 0
                records += repo.upsert_many_cape(scanner.last_cape_docs)
                records += 1 if repo.upsert_report(report) else 0
            ok += 1
        except Exception as exc:
            logger.error("Market regime scan failed date=%s: %s", trade_date, exc)
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
    scanner = MarketRegimeScanner(repo)
    summary = run(dates, scanner, repo, dry_run=args.dry_run, verbose=args.verbose)
    logger.info("run_market_regime_scan complete: %s", summary)
    if not args.dry_run:
        record_ingestion_run(
            settings,
            job_type="market_regime_scan",
            date_str=dates[-1],
            status="complete" if summary["errors"] == 0 else "partial",
            error=f"{summary['errors']} date(s) failed" if summary["errors"] else None,
            records_changed=summary["records_changed"],
        )
    if summary["errors"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()