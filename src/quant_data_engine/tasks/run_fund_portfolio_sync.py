#!/usr/bin/env python3
"""
同步 Tushare 公募基金持仓（fund_portfolio）到 **外部数据标准库**（环境变量 DATA_DB_NAME，默认 quant_data）下的 fund_holdings。

披露按公告日 ann_date 聚合；默认可按最近若干「日历日」逐日拉取公告日当天全市场披露
（适合每周跑一次，用 --ann-days 覆盖过去一周多几天防漏）。

用法:
  python -m quant_data_engine.tasks.run_fund_portfolio_sync
  python -m quant_data_engine.tasks.run_fund_portfolio_sync --ann-days 10
  python -m quant_data_engine.tasks.run_fund_portfolio_sync --ts-code 001753.OF

需要 Tushare 积分权限（见 https://tushare.pro/document/2?doc_id=121）。
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
from quant_data_engine.repositories.fund_portfolio_repository import FundPortfolioRepository
from quant_data_engine.utils.ingestion_job import record_ingestion_run

logger = logging.getLogger(__name__)


def _daterange_ann_strings(days: int) -> list[str]:
    end = datetime.now().date()
    start = end - timedelta(days=max(days - 1, 0))
    out: list[str] = []
    d = start
    while d <= end:
        out.append(d.strftime("%Y%m%d"))
        d += timedelta(days=1)
    return out


def main() -> None:
    p = argparse.ArgumentParser(description="Sync fund portfolio holdings (Tushare fund_portfolio).")
    p.add_argument(
        "--ann-days",
        type=int,
        default=14,
        help="按公告日 ann_date 逐日拉取：从今天往前共 N 个日历日（默认 14，适合每周任务）",
    )
    p.add_argument(
        "--ts-code",
        type=str,
        default="",
        help="仅同步单只基金（.OF 代码），指定后不再按 ann_date 扫日期",
    )
    p.add_argument("--no-record-job", action="store_true", help="不写 job_status / pipeline_tasks")
    args = p.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    settings = get_settings()
    if not settings.tushare_token:
        logger.error("TUSHARE_TOKEN 未配置，退出")
        sys.exit(1)

    logger.info(
        "fund_portfolio 写入外部数据 DB（DATA_DB_NAME）=%s （非 MONGO_DB=%s）",
        settings.data_db,
        settings.mongo_db,
    )

    adapter = TushareAdapter(
        token=settings.tushare_token,
        rate_limit_per_sec=settings.rate_limit_per_sec,
    )
    repo = FundPortfolioRepository(settings.mongo_uri, settings.data_db)

    total_rows = 0
    total_upserts = 0

    if args.ts_code.strip():
        logger.info("fund_portfolio 单基金同步: ts_code=%s", args.ts_code.strip())
        df = adapter.fetch_fund_portfolio(ts_code=args.ts_code.strip())
        if df is not None and not df.empty:
            n = repo.upsert_from_dataframe(df)
            total_rows += len(df)
            total_upserts += n
            logger.info("写入 fund_holdings: rows=%s upsert_ops=%s", len(df), n)
    else:
        dates = _daterange_ann_strings(args.ann_days)
        logger.info(
            "fund_portfolio 按 ann_date 逐日同步: %s 天 (%s .. %s)",
            len(dates),
            dates[0] if dates else "-",
            dates[-1] if dates else "-",
        )
        for ann in dates:
            try:
                df = adapter.fetch_fund_portfolio(ann_date=ann)
            except Exception as e:
                logger.warning("ann_date=%s 拉取失败（可忽略空日）: %s", ann, e)
                continue
            if df is None or df.empty:
                continue
            n = repo.upsert_from_dataframe(df)
            total_rows += len(df)
            total_upserts += n
            logger.info("ann_date=%s rows=%s upsert_field_ops≈%s", ann, len(df), n)

    logger.info("fund_portfolio 同步结束: total_rows=%s total_upsert_modified=%s", total_rows, total_upserts)

    if not args.no_record_job:
        today_str = datetime.now().strftime("%Y%m%d")
        record_ingestion_run(
            settings,
            "fund_portfolio",
            today_str,
            records_changed=total_upserts,
            ann_days=args.ann_days,
            ts_code=args.ts_code.strip() or None,
        )


if __name__ == "__main__":  # pragma: no cover
    main()
