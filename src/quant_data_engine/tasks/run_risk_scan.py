"""Daily market risk scan task.

Runs after ETF data is ready (recommended: 19:30), scans all configured
sectors, persists results to MongoDB ``risk_reports`` collection, and
records a ``job_status`` entry for monitoring.

Usage::

    # Scan today, all sectors
    python -m quant_data_engine.tasks.run_risk_scan

    # Scan a specific date, all sectors
    python -m quant_data_engine.tasks.run_risk_scan --date 20260502

    # Scan specific sectors only
    python -m quant_data_engine.tasks.run_risk_scan --sectors semicon nev robot

    # Backfill a date range
    python -m quant_data_engine.tasks.run_risk_scan --start 20260401 --end 20260502

    # Dry-run: compute but do NOT write to MongoDB
    python -m quant_data_engine.tasks.run_risk_scan --dry-run

    # Print formatted report to stdout as well
    python -m quant_data_engine.tasks.run_risk_scan --verbose

Environment variables (via .env or shell):
    TUSHARE_TOKEN, MONGO_URI, DATA_DB_NAME
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timedelta
from typing import List

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from quant_data_engine.adapters.stock_adapter import StockAdapter
from quant_data_engine.adapters.tushare_adapter import TushareAdapter
from quant_data_engine.config.settings import get_settings
from quant_data_engine.repositories.risk_report_repository import RiskReportRepository
from quant_data_engine.tools.risk_scanner import SECTOR_PROFILES, MarketRiskScanner
from quant_data_engine.utils.ingestion_job import record_ingestion_run

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

ALL_SECTORS = list(SECTOR_PROFILES.keys())


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="市场风险预警扫描（每日收盘后执行，写入 risk_reports 集合）"
    )

    date_group = p.add_mutually_exclusive_group()
    date_group.add_argument(
        "--date",
        type=str,
        default=None,
        help="扫描日期 YYYYMMDD（默认：今天）",
    )
    date_group.add_argument(
        "--start",
        type=str,
        default=None,
        help="回填起始日期 YYYYMMDD（与 --end 配合使用）",
    )

    p.add_argument(
        "--end",
        type=str,
        default=None,
        help="回填结束日期 YYYYMMDD（默认：今天）",
    )
    p.add_argument(
        "--sectors",
        nargs="+",
        choices=ALL_SECTORS,
        default=ALL_SECTORS,
        metavar="SECTOR",
        help=f"要扫描的行业，可多选（默认：全部）。可选值: {', '.join(ALL_SECTORS)}",
    )
    p.add_argument(
        "--lookback",
        type=int,
        default=252,
        metavar="DAYS",
        help="历史分位基准天数（默认：252 ≈ 1年）",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="计算但不写入 MongoDB",
    )
    p.add_argument(
        "--verbose",
        action="store_true",
        help="在终端打印格式化报告",
    )
    return p.parse_args()


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------

def _date_range(start: str, end: str) -> List[str]:
    """Return list of YYYYMMDD strings from start to end (inclusive), weekdays only."""
    s = datetime.strptime(start, "%Y%m%d")
    e = datetime.strptime(end, "%Y%m%d")
    dates = []
    cur = s
    while cur <= e:
        if cur.weekday() < 5:  # 0=Mon … 4=Fri，跳过周六(5)周日(6)
            dates.append(cur.strftime("%Y%m%d"))
        cur += timedelta(days=1)
    return dates


def run(
    dates: List[str],
    sectors: List[str],
    scanner: MarketRiskScanner,
    repo: RiskReportRepository,
    dry_run: bool,
    verbose: bool,
) -> dict:
    total_ok = 0
    total_err = 0

    for date in dates:
        for sector in sectors:
            try:
                logger.info("Scanning date=%s sector=%s", date, sector)
                report = scanner.scan(date, sector=sector)

                if verbose:
                    scanner.print_report(report)

                if not dry_run:
                    ok = repo.upsert_report(report)
                    if ok:
                        total_ok += 1
                    else:
                        total_err += 1
                else:
                    total_ok += 1
                    logger.info(
                        "  [dry-run] score=%.1f level=%s warnings=%d",
                        report.overall_risk_score,
                        report.overall_risk_level,
                        len(report.warnings),
                    )
            except Exception as exc:
                logger.error("Failed date=%s sector=%s: %s", date, sector, exc)
                total_err += 1

    return {"ok": total_ok, "errors": total_err}


def main() -> None:
    args = parse_args()
    settings = get_settings()

    if not settings.tushare_token:
        logger.error("TUSHARE_TOKEN 未设置，退出")
        sys.exit(1)

    # Resolve date(s)
    today = datetime.today().strftime("%Y%m%d")
    if args.start:
        end = args.end or today
        dates = _date_range(args.start, end)
    else:
        dates = [args.date or today]

    sectors = args.sectors

    logger.info(
        "run_risk_scan: dates=%s sectors=%s dry_run=%s",
        dates if len(dates) <= 5 else f"{dates[0]}..{dates[-1]}({len(dates)})",
        sectors,
        args.dry_run,
    )

    # Build adapters + scanner
    adapter = TushareAdapter(
        token=settings.tushare_token,
        rate_limit_per_sec=settings.rate_limit_per_sec,
    )
    stock_adapter = StockAdapter(
        token=settings.tushare_token,
        rate_limit_per_sec=settings.rate_limit_per_sec,
    )
    scanner = MarketRiskScanner(
        adapter=adapter,
        stock_adapter=stock_adapter,
        lookback_days=args.lookback,
    )
    repo = RiskReportRepository(settings.mongo_uri, settings.data_db)

    summary = run(
        dates=dates,
        sectors=sectors,
        scanner=scanner,
        repo=repo,
        dry_run=args.dry_run,
        verbose=args.verbose,
    )

    logger.info(
        "run_risk_scan complete: ok=%d errors=%d dry_run=%s",
        summary["ok"], summary["errors"], args.dry_run,
    )

    # Record job_status for the last date (daily run convention)
    if not args.dry_run:
        record_ingestion_run(
            settings,
            job_type="risk_scan",
            date_str=dates[-1],
            status="complete" if summary["errors"] == 0 else "partial",
            error=f"{summary['errors']} sector(s) failed" if summary["errors"] else None,
            records_changed=summary["ok"],
            sectors=",".join(sectors),
        )

    if summary["errors"] > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
