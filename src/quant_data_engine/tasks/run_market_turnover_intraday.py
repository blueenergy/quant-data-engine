"""Aggregate the live full-market snapshot into per-board turnover every 2 minutes.

Reads ``quant_realtime.volume_price_realtime_latest`` (the bridge refreshes it
roughly once a minute) and appends one row per snapshot to
``quant_realtime.market_turnover_intraday``.

Why this exists: the volume-waterline panel needs today's cumulative turnover
per board at a given clock time, and the raw collection only ever holds the
latest snapshot — nothing is stored intraday. Summing 5,588 documents takes
well under a second, so a 2-minute cron is enough and no bridge change is
needed.

Only stocks are aggregated. ETFs have no intraday feed (``etf_daily`` is
Tushare T-1), so mixing them in would put two different timestamps on one line.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import datetime, time as dtime
from typing import Any, Dict, Optional

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from quant_data_engine.config.settings import get_settings
from quant_data_engine.tools.market_turnover import aggregate_boards
from quant_data_engine.tools.market_turnover.alerts import maybe_send
from quant_data_engine.tools.market_turnover.repository import TurnoverRepository

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

SCHEMA_VERSION = 1
SESSION_OPEN = dtime(9, 25)
SESSION_CLOSE = dtime(15, 5)
DEFAULT_MIN_STOCKS = 4000


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="盘中分板块成交额聚合（每 2 分钟一条快照）")
    parser.add_argument("--loop", action="store_true", help="持续运行（默认跑一次）")
    parser.add_argument("--interval", type=float, default=120.0, help="loop 模式下的间隔秒数")
    parser.add_argument("--dry-run", action="store_true", help="打印结果但不写 MongoDB")
    parser.add_argument("--ignore-session", action="store_true", help="忽略交易时段检查")
    parser.add_argument("--min-stocks", type=int, default=DEFAULT_MIN_STOCKS, help="快照股票数下限，低于此跳过")
    parser.add_argument(
        "--backfill-sector",
        type=int,
        default=0,
        metavar="DAYS",
        help="从 realtime_sector_strength 回填最近 N 个交易日的全市场合计（仅 total，无分板块）",
    )
    parser.add_argument(
        "--include-today",
        action="store_true",
        help="回填时包含当天（仅在当天确认不会再写入分板块行时使用，例如收盘后一次性补全）",
    )
    parser.add_argument("--no-alert", action="store_true", help="只聚合，不做盘中放量告警")
    parser.add_argument("--verbose", action="store_true", help="打印每条聚合结果")
    return parser.parse_args()


def _in_session(now: Optional[datetime] = None) -> bool:
    current = (now or datetime.now()).time()
    return SESSION_OPEN <= current <= SESSION_CLOSE


def run_once(
    repo: TurnoverRepository,
    *,
    dry_run: bool = False,
    ignore_session: bool = False,
    min_stocks: int = DEFAULT_MIN_STOCKS,
    verbose: bool = False,
    alert: bool = True,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Aggregate the current snapshot once. Returns a small status dict."""
    if not ignore_session and not _in_session(now):
        return {"status": "outside_session"}

    latest = repo.realtime_db["volume_price_realtime_latest"]
    head = latest.find_one({}, {"_id": 0, "snapshot_time": 1, "trade_date": 1}, sort=[("snapshot_time", -1)])
    if not head or not head.get("snapshot_time"):
        return {"status": "no_snapshot"}

    snapshot_time = str(head["snapshot_time"])
    trade_date = str(head.get("trade_date") or snapshot_time[:8])

    existing = repo.intraday_coll.find_one(
        {"trade_date": trade_date, "snapshot_time": snapshot_time}, {"_id": 0}
    )
    if existing:
        # 桥停更时快照会重复，告警仍要能基于这一条做出判断。
        alert_result = (
            maybe_send(repo, existing, now=now, dry_run=dry_run) if alert else {"status": "disabled"}
        )
        return {"status": "duplicate", "snapshot_time": snapshot_time, "alert": alert_result}

    rows = latest.find({}, {"_id": 0, "symbol": 1, "amount": 1})
    agg = aggregate_boards(rows)
    if agg["n_stocks"] < min_stocks:
        logger.warning(
            "Skipping snapshot %s: only %d stocks (min %d) — bridge likely mid-refresh",
            snapshot_time,
            agg["n_stocks"],
            min_stocks,
        )
        return {"status": "low_coverage", "snapshot_time": snapshot_time, "n_stocks": agg["n_stocks"]}

    doc: Dict[str, Any] = {
        "trade_date": trade_date,
        "snapshot_time": snapshot_time,
        "schema_version": SCHEMA_VERSION,
        "coverage": "boards",
        "source": "miniqmt",
        "n_stocks": agg["n_stocks"],
        "total": round(agg["total"], 2),
    }
    for key, value in agg["amounts"].items():
        doc[key] = round(value, 2)

    if verbose:
        logger.info(
            "%s total=%.1f sh=%.1f sz=%.1f cx=%.1f star=%.1f bse=%.1f",
            snapshot_time,
            doc["total"],
            doc["sh"],
            doc["sz"],
            doc["cx"],
            doc["star"],
            doc["bse"],
        )
    if not dry_run:
        repo.upsert_intraday(doc)
    alert_result = maybe_send(repo, doc, now=now, dry_run=dry_run) if alert else {"status": "disabled"}
    return {
        "status": "computed_dry_run" if dry_run else "written",
        "snapshot_time": snapshot_time,
        "total": doc["total"],
        "alert": alert_result,
    }


def backfill_from_sector(
    repo: TurnoverRepository, days: int, *, dry_run: bool = False, include_today: bool = False
) -> Dict[str, Any]:
    """Seed total-only intraday history from the Shenwan sector snapshots.

    ``realtime_sector_strength`` sums to the exact market-wide turnover (verified
    digit-for-digit against Tushare closes) and goes back further than this
    collection can, but it carries no board split. Those rows are marked
    ``coverage="total_only"`` so nothing downstream mistakes them for a
    board-complete snapshot — they are only used to build the time-of-day
    baseline.
    """
    rows = list(
        repo.realtime_db["realtime_sector_strength"].aggregate(
            [
                {"$match": {"level": "L1", "ma_period": 5}},
                {
                    "$group": {
                        "_id": {"d": "$trade_date", "t": "$snapshot_time"},
                        "amount_yuan": {"$sum": "$total_amount_yuan"},
                    }
                },
                {"$sort": {"_id.d": 1, "_id.t": 1}},
            ]
        )
    )
    if not rows:
        return {"status": "no_source_rows", "written": 0}

    today = datetime.now().strftime("%Y%m%d")
    # Never seed today by default: the live path writes board-complete rows for
    # it, and a total-only upsert on the same (date, snapshot_time) would replace
    # them. ``--include-today`` is for a session that is already closed.
    all_dates = sorted({row["_id"]["d"] for row in rows if include_today or row["_id"]["d"] < today})
    keep = set(all_dates[-days:]) if days > 0 else set(all_dates)
    written = 0
    for row in rows:
        trade_date = row["_id"]["d"]
        if trade_date not in keep:
            continue
        snapshot_time = row["_id"]["t"]
        doc = {
            "trade_date": trade_date,
            "snapshot_time": snapshot_time,
            "schema_version": SCHEMA_VERSION,
            "coverage": "total_only",
            "source": "sector_aggregate",
            "total": round(float(row["amount_yuan"]) / 1e8, 2),
        }
        if not dry_run:
            repo.upsert_intraday(doc)
        written += 1
    return {"status": "backfilled", "days": len(keep), "written": written}


def main() -> None:
    args = parse_args()
    settings = get_settings()
    repo = TurnoverRepository(settings.mongo_uri, settings.data_db, settings.realtime_db)

    if args.backfill_sector:
        result = backfill_from_sector(
            repo, args.backfill_sector, dry_run=args.dry_run, include_today=args.include_today
        )
        logger.info("run_market_turnover_intraday backfill: %s", result)
        return

    if not args.loop:
        result = run_once(
            repo,
            dry_run=args.dry_run,
            ignore_session=args.ignore_session,
            min_stocks=args.min_stocks,
            verbose=args.verbose,
            alert=not args.no_alert,
        )
        logger.info("run_market_turnover_intraday: %s", result)
        return

    logger.info("turnover intraday loop started (interval=%ss)", args.interval)
    while True:
        try:
            result = run_once(
                repo,
                dry_run=args.dry_run,
                ignore_session=args.ignore_session,
                min_stocks=args.min_stocks,
                verbose=args.verbose,
                alert=not args.no_alert,
            )
            if result["status"] not in ("duplicate", "outside_session"):
                logger.info("turnover intraday: %s", result)
        except Exception as exc:  # keep the loop alive across transient Mongo errors
            logger.error("turnover intraday iteration failed: %s", exc)
        time.sleep(max(5.0, args.interval))


if __name__ == "__main__":
    main()
