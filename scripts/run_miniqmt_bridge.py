#!/usr/bin/env python
"""Standalone miniQMT realtime bridge.

Run this on a Windows machine with miniQMT/xtquant installed. The script is a
thin entrypoint; bridge implementation lives in ``quant_data_engine.bridges``.
It is not a quant-data-engine container task and is not started by start.sh,
cron, or Docker Compose.
"""
from __future__ import annotations

import argparse
from dataclasses import replace
import logging
from pathlib import Path
import sys

from pymongo import MongoClient

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from quant_data_engine.bridges.miniqmt import (  # noqa: E402
    BridgeConfig,
    BridgeRunner,
    MiniQMTTickSource,
    RealtimeDailyKSink,
    SymbolUniverse,
)
from quant_data_engine.config.settings import get_settings  # noqa: E402
from quant_data_engine.repositories.stock_repository import StockRepository  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run standalone miniQMT realtime bridge")
    parser.set_defaults(
        config=None,
        once=False,
        loop=False,
        auto=None,
        ignore_session=None,
    )
    parser.add_argument("--config", help="Path to JSON config file")
    parser.add_argument("--once", action="store_true", help="Run one snapshot and exit")
    parser.add_argument("--loop", action="store_true", help="Run continuously")
    parser.add_argument("--mongo-uri", help="Primary MongoDB URI override")
    parser.add_argument(
        "--sync-uri",
        help=(
            "Optional secondary MongoDB URI for mirror writes. Mirrors volume_price and "
            "volume_price_realtime_latest data, and also mirrors this task's job_status/"
            "pipeline_tasks freshness/health to the same cluster (overriding JOB_STATUS_SYNC_URI)."
        ),
    )
    parser.add_argument("--symbols", nargs="*", help="Manual symbol override, e.g. 000001.SZ 600000.SH")
    parser.add_argument("--auto", action="store_true", help="Resolve symbols from configured database universe")
    parser.add_argument("--manual", dest="auto", action="store_false", help="Use only --symbols")
    parser.add_argument("--sinks", help="Comma-separated sinks, currently only realtime-daily-k")
    parser.add_argument("--poll-interval", type=float, help="Tick polling interval seconds")
    parser.add_argument("--daily-k-interval", type=float, help="Realtime daily-K write interval seconds")
    parser.add_argument("--refresh", type=int, help="Universe refresh interval seconds, 0 disables refresh")
    parser.add_argument("--ignore-session", action="store_true", help="Run outside CN stock trading session")
    parser.add_argument("--respect-session", dest="ignore_session", action="store_false")
    parser.add_argument("--include-watchlists", dest="include_watchlists", action="store_true", default=None)
    parser.add_argument("--no-include-watchlists", dest="include_watchlists", action="store_false")
    parser.add_argument(
        "--include-watchlist-strategies",
        dest="include_watchlist_strategies",
        action="store_true",
        default=None,
    )
    parser.add_argument(
        "--no-include-watchlist-strategies",
        dest="include_watchlist_strategies",
        action="store_false",
    )
    parser.add_argument("--include-portfolio-plans", dest="include_portfolio_plans", action="store_true", default=None)
    parser.add_argument("--no-include-portfolio-plans", dest="include_portfolio_plans", action="store_false")
    parser.add_argument("--include-paper-positions", dest="include_paper_positions", action="store_true", default=None)
    parser.add_argument("--no-include-paper-positions", dest="include_paper_positions", action="store_false")
    parser.add_argument("--plan-statuses", help="Comma-separated portfolio plan statuses to include")
    parser.add_argument("--warmup-seconds", type=float, help="Sleep after initial miniQMT subscription")
    parser.add_argument("--log-level", default="INFO", help="Python logging level")
    return parser.parse_args()


def load_config(args: argparse.Namespace) -> BridgeConfig:
    config = BridgeConfig.from_json(args.config) if args.config else BridgeConfig()
    return config.merge_cli(args)


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s [miniqmt_bridge] %(levelname)s %(message)s",
    )
    config = load_config(args)
    settings = get_settings()
    mongo_uri = config.mongo_uri or settings.mongo_uri
    settings = replace(
        settings,
        mongo_uri=mongo_uri,
        job_status_sync_uri=config.sync_uri or settings.job_status_sync_uri,
    )

    client = MongoClient(mongo_uri)
    try:
        trading_db_name = getattr(settings, "trading_db", "quant_trading")
        universe = SymbolUniverse(
            finance_db=client[settings.mongo_db],
            trading_db=client[trading_db_name],
            config=config.universe,
        )
        repo = StockRepository(
            mongo_uri,
            settings.data_db,
            realtime_db_name=settings.realtime_db,
            sync_uri=config.sync_uri,
        )
        source = MiniQMTTickSource(
            [],
            warmup_seconds=config.warmup_seconds,
        )
        sinks = [
            RealtimeDailyKSink(
                repo=repo,
                settings=settings,
                write_interval=config.daily_k_interval,
                universe_type=universe.universe_type,
            )
        ]
        runner = BridgeRunner(
            config=config,
            universe=universe,
            source=source,
            sinks=sinks,
        )
        runner.run()
    finally:
        client.close()


if __name__ == "__main__":
    main()

