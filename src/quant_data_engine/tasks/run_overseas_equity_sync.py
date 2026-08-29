"""Sync Tushare HK / US equity into market-specific Mongo collections.

Usage:
  python -m quant_data_engine.tasks.run_overseas_equity_sync --market hk --dataset basic
  python -m quant_data_engine.tasks.run_overseas_equity_sync --market hk --dataset tradecal
  python -m quant_data_engine.tasks.run_overseas_equity_sync --market hk --dataset daily --days 1
  python -m quant_data_engine.tasks.run_overseas_equity_sync --market us --dataset basic

``hk_daily`` on the current token is ~1 call/minute then 1 call/hour.
Default interval between HK daily dates is 3605s. Override with
``TUSHARE_OVERSEAS_DAILY_MIN_INTERVAL_SEC``. Do not re-run ``hk_daily``
inside that window — adapter retries on 1/min wait 65s once; hourly
limits fail immediately.

US / HK ``*_daily_adj`` and US ``us_daily`` currently have no permission;
those datasets fail-soft with status=failed / reason=permission_denied.
HK daily does not call ``hk_daily_adj`` unless ``--want-adj``.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from quant_data_engine import get_settings
from quant_data_engine.adapters.tushare_adapter import TushareAdapter
from quant_data_engine.pipelines.overseas_equity_pipeline import OverseasEquityPipeline
from quant_data_engine.repositories.overseas_equity_repository import OverseasEquityRepository
from quant_data_engine.utils.ingestion_job import record_ingestion_run

logger = logging.getLogger(__name__)

DATASETS = ("basic", "tradecal", "daily")
MARKETS = ("hk", "us")
DEFAULT_CAL_START = "20200101"
HK_DAILY_DEFAULT_INTERVAL = 3605.0
HK_DAILY_COMPLETE_MIN_SYMBOLS = 1500


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Sync Tushare HK/US equity")
    p.add_argument("--market", choices=MARKETS, required=True)
    p.add_argument("--dataset", choices=DATASETS, required=True)
    p.add_argument("--start", help="Start date YYYYMMDD (tradecal/daily)")
    p.add_argument("--end", help="End date YYYYMMDD (default: today)")
    p.add_argument("--days", type=int, help="Recent N open days (daily only)")
    p.add_argument(
        "--min-interval-sec",
        type=float,
        default=None,
        help="Sleep between daily by-date calls (HK default 3605)",
    )
    p.add_argument(
        "--force",
        action="store_true",
        help="Re-fetch HK daily dates already present in Mongo",
    )
    p.add_argument(
        "--want-adj",
        action="store_true",
        help="Also fetch daily_adj (HK currently has no permission)",
    )
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--no-record-job", action="store_true")
    return p.parse_args(argv)


def _job_type(market: str, dataset: str) -> str:
    return f"{market}_{dataset}"


def _interval(market: str, dataset: str, override: float | None) -> float:
    if override is not None:
        return max(override, 0.0)
    env = os.getenv("TUSHARE_OVERSEAS_DAILY_MIN_INTERVAL_SEC")
    if env:
        try:
            return max(float(env), 0.0)
        except ValueError:
            pass
    if dataset == "daily" and market == "hk":
        return HK_DAILY_DEFAULT_INTERVAL
    return 0.0


def main(argv=None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    args = parse_args(argv)
    settings = get_settings()
    job = _job_type(args.market, args.dataset)
    today = datetime.today().strftime("%Y%m%d")
    end_date = args.end or today

    if not settings.tushare_token:
        logger.error("TUSHARE_TOKEN 未配置，退出")
        if not args.no_record_job:
            record_ingestion_run(settings, job, today, status="failed", error="TUSHARE_TOKEN missing")
        return 1

    adapter = TushareAdapter(
        token=settings.tushare_token,
        rate_limit_per_sec=settings.rate_limit_per_sec,
    )
    repo = OverseasEquityRepository(settings.mongo_uri, settings.data_db, args.market)
    interval = _interval(args.market, args.dataset, args.min_interval_sec)
    pipeline = OverseasEquityPipeline(adapter, repo, min_interval_sec=interval)

    if args.dataset == "basic":
        if args.dry_run:
            logger.info("dry-run %s basic (no writes)", args.market)
            return 0
        result = pipeline.run_basic()
        rc = int(result.get("records") or 0)
        status = result.get("status") or "success"
        if not args.no_record_job:
            record_ingestion_run(
                settings, job, today, status="complete" if status == "success" else status,
                error=result.get("error"), records_changed=rc,
            )
        logger.info("%s basic done: %s", args.market, result)
        return 0 if status in ("success", "skipped") else 1

    if args.dataset == "tradecal":
        start = args.start or DEFAULT_CAL_START
        if args.dry_run:
            logger.info("dry-run %s tradecal %s..%s", args.market, start, end_date)
            return 0
        result = pipeline.run_tradecal(start, end_date)
        rc = int(result.get("records") or 0)
        status = result.get("status") or "success"
        if not args.no_record_job:
            record_ingestion_run(
                settings, job, end_date, status="complete" if status == "success" else status,
                error=result.get("error"), records_changed=rc,
            )
        logger.info("%s tradecal done: %s", args.market, result)
        return 0 if status in ("success", "skipped") else 1

    # daily
    start = args.start
    if args.days and not start:
        lookback = (datetime.today() - timedelta(days=max(args.days * 3, 10))).strftime("%Y%m%d")
        start = lookback
    if not start:
        start = (datetime.today() - timedelta(days=3)).strftime("%Y%m%d")

    try:
        days = pipeline.open_days(start, end_date)
    except Exception as e:
        logger.error("failed to load %s tradecal: %s", args.market, e)
        if not args.no_record_job:
            record_ingestion_run(settings, job, end_date, status="failed", error=str(e))
        return 1

    if args.days:
        days = days[-args.days:]
    if not days:
        logger.warning("no open days in %s..%s", start, end_date)
        if not args.no_record_job:
            record_ingestion_run(
                settings, job, end_date, status="skipped", error="no open days",
            )
        return 0

    if args.dry_run:
        logger.info(
            "dry-run %s daily %d days %s..%s interval=%.0fs",
            args.market, len(days), days[0], days[-1], interval,
        )
        return 0

    result = pipeline.run_daily(
        days,
        want_adj=True if args.market != "hk" else bool(args.want_adj),
        skip_complete_min_symbols=(
            0 if args.force or args.market != "hk" else HK_DAILY_COMPLETE_MIN_SYMBOLS
        ),
    )
    rc = int(result.get("daily_records") or 0) + int(result.get("adj_records") or 0)
    status = result.get("status") or "success"
    job_status = "complete" if status == "success" else status
    if not args.no_record_job:
        record_ingestion_run(
            settings, job, days[-1], status=job_status,
            error=result.get("error"), records_changed=rc,
        )
    logger.info("%s daily done: %s", args.market, result)
    return 0 if status in ("success", "skipped") else 1


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
