#!/usr/bin/env python
"""Standalone realtime bridge (miniQMT xtdata or Big QMT ZMQ RPC).

Run on Windows. Supports:
- ``miniqmt``: local xtquant ``get_full_tick`` (legacy)
- ``bigqmt_rpc``: dual QMT instance quote terminal over ZMQ RPC

Not started by quant-data-engine Docker/cron.
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

from quant_data_engine.bridges.bigqmt import (  # noqa: E402
    BigQmtRpcTickSource,
    default_zmq_address,
    make_trading_day_checker,
)
from quant_data_engine.bridges.miniqmt import (  # noqa: E402
    BridgeConfig,
    BridgeRunner,
    MiniQMTTickSource,
    SymbolUniverse,
)
from quant_data_engine.bridges.miniqmt.constants import (  # noqa: E402
    SINK_MINUTE_BARS,
    SINK_REALTIME_DAILY_K,
    TICK_SOURCE_BIGQMT_RPC,
    TICK_SOURCE_MINIQMT,
)
from quant_data_engine.bridges.miniqmt.factory import create_sinks, daily_k_source_mode  # noqa: E402
from quant_data_engine.config.settings import get_settings  # noqa: E402
from quant_data_engine.repositories.minute_repository import MinuteRepository  # noqa: E402
from quant_data_engine.repositories.stock_repository import StockRepository  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run standalone realtime bridge (miniQMT or Big QMT RPC)")
    parser.set_defaults(
        config=None,
        once=False,
        loop=False,
        auto=None,
        ignore_session=None,
        universe_mode=None,
        shadow_mode=False,
        require_listed_from_db=False,
    )
    parser.add_argument("--config", help="Path to JSON config file")
    parser.add_argument("--once", action="store_true", help="Run one snapshot and exit")
    parser.add_argument("--loop", action="store_true", help="Run continuously")
    parser.add_argument(
        "--tick-source",
        choices=[TICK_SOURCE_MINIQMT, TICK_SOURCE_BIGQMT_RPC],
        help="miniqmt=local xtquant; bigqmt_rpc=quote instance ZMQ RPC",
    )
    parser.add_argument(
        "--quote-rpc-address",
        help="ZMQ connect address for quote instance, e.g. tcp://127.0.0.1:15626",
    )
    parser.add_argument(
        "--quote-account-id",
        help="Quote terminal account id (used to derive default ZMQ port when address omitted)",
    )
    parser.add_argument("--quote-rpc-timeout", type=float, help="Per-batch RPC timeout seconds")
    parser.add_argument(
        "--shadow",
        dest="shadow_mode",
        action="store_true",
        help="Shadow mode: write to bigqmt_shadow_snapshots only, not volume_price",
    )
    parser.add_argument("--mongo-uri", help="Primary MongoDB URI override")
    parser.add_argument(
        "--sync-uri",
        help=(
            "Optional secondary MongoDB URI for mirror writes. Mirrors volume_price and "
            "volume_price_realtime_latest data, and also mirrors this task's job_status/"
            "pipeline_tasks freshness/health to the same cluster (overriding JOB_STATUS_SYNC_URI)."
        ),
    )
    parser.add_argument(
        "--universe",
        dest="universe_mode",
        choices=["targeted", "full_market"],
        help="targeted = watchlist/plan pool; full_market = listed A-shares (Tushare rt_k replacement)",
    )
    parser.add_argument("--symbols", nargs="*", help="Manual symbol override, e.g. 000001.SZ 600000.SH")
    parser.add_argument("--auto", action="store_true", help="Resolve symbols from configured database universe")
    parser.add_argument("--manual", dest="auto", action="store_false", help="Use only --symbols")
    parser.add_argument(
        "--sinks",
        help="Comma-separated sinks: realtime-daily-k, minute-bars (minute-bars requires targeted universe)",
    )
    parser.add_argument("--poll-interval", type=float, help="Tick polling interval seconds")
    parser.add_argument(
        "--daily-k-interval",
        type=float,
        help="Realtime daily-K write interval seconds (full_market default 60; lower later if the host keeps up)",
    )
    parser.add_argument("--poll-batch-size", type=int, help="get_full_tick batch size; 0 = all symbols at once")
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
    parser.add_argument(
        "--require-listed-from-db",
        dest="require_listed_from_db",
        action="store_true",
        help="Fail when stock_info is empty (recommended for bigqmt_rpc full_market)",
    )
    parser.add_argument(
        "--market-insight-trigger-url",
        help="quant-api URL for 火眼金睛; Windows+WSL typically http://127.0.0.1:3001/api/market-insights/generate-internal",
    )
    parser.add_argument(
        "--market-insight-trigger-key",
        help="Shared secret for X-Hermes-Quant-Key (same as quant-api HERMES_QUANT_INTERNAL_KEY)",
    )
    parser.add_argument("--market-insight-trigger-timeout-sec", type=float, help="Market insight HTTP timeout")
    parser.add_argument(
        "--intraday-t0-trigger-url",
        help="quant-api URL for T+0; Windows+WSL typically http://127.0.0.1:3001/api/intraday-t0/signals/generate-all",
    )
    parser.add_argument("--intraday-t0-trigger-key", help="Shared secret for T+0 trigger; defaults to insight key/env")
    parser.add_argument("--intraday-t0-trigger-timeout-sec", type=float, help="T+0 HTTP timeout")
    parser.add_argument("--log-level", default="INFO", help="Python logging level")
    return parser.parse_args()


def load_config(args: argparse.Namespace) -> BridgeConfig:
    config = BridgeConfig.from_json(args.config) if args.config else BridgeConfig()
    return config.merge_cli(args)


def _resolve_quote_rpc_address(config: BridgeConfig) -> str:
    if config.quote_rpc_address:
        return config.quote_rpc_address
    if config.quote_account_id:
        return default_zmq_address(config.quote_account_id)
    raise ValueError("bigqmt_rpc requires quote_rpc_address or quote_account_id")


def _build_tick_source(config: BridgeConfig, symbols: list[str]):
    if config.tick_source == TICK_SOURCE_BIGQMT_RPC:
        return BigQmtRpcTickSource(
            symbols,
            connect_address=_resolve_quote_rpc_address(config),
            account_id=config.quote_account_id or "",
            poll_batch_size=config.poll_batch_size,
            rpc_timeout_seconds=config.quote_rpc_timeout_sec,
        )
    return MiniQMTTickSource(
        symbols,
        warmup_seconds=config.warmup_seconds,
        poll_batch_size=config.poll_batch_size,
    )


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s [realtime_bridge] %(levelname)s %(message)s",
    )
    config = load_config(args)
    if config.tick_source == TICK_SOURCE_BIGQMT_RPC and config.universe.is_full_market:
        if not config.require_listed_from_db:
            config = replace(config, require_listed_from_db=True)
    settings = config.overlay_settings(get_settings())
    mongo_uri = settings.mongo_uri
    full_market = config.universe.is_full_market
    source_mode = daily_k_source_mode(config, full_market=full_market)
    if full_market:
        settings = replace(settings, realtime_daily_k_source_mode=source_mode)
        logging.info(
            "full-market mode: tick_source=%s poll_interval=%.1fs daily_k_interval=%.1fs "
            "refresh=%ds poll_batch_size=%s shadow=%s",
            config.tick_source,
            config.poll_interval,
            config.daily_k_interval,
            config.refresh,
            config.poll_batch_size or "all",
            config.shadow_mode,
        )
        if config.tick_source == TICK_SOURCE_BIGQMT_RPC:
            logging.info("quote RPC address: %s", _resolve_quote_rpc_address(config))
        logging.info(
            "full-market triggers: insight=%s t0=%s",
            settings.market_insight_trigger_url or "(unset)",
            settings.intraday_t0_trigger_url or "(unset)",
        )
    elif SINK_MINUTE_BARS in config.sinks:
        logging.info(
            "watchlist minute-bars: tick_source=%s poll_interval=%.1fs write_interval=%.1fs "
            "refresh=%ds poll_batch_size=%s sinks=%s",
            config.tick_source,
            config.poll_interval,
            config.daily_k_interval,
            config.refresh,
            config.poll_batch_size or "all",
            ",".join(config.sinks),
        )
        if config.tick_source == TICK_SOURCE_BIGQMT_RPC:
            logging.info("quote RPC address: %s", _resolve_quote_rpc_address(config))

    client = MongoClient(mongo_uri)
    try:
        trading_db_name = getattr(settings, "trading_db", "quant_trading")
        data_db = client[settings.data_db]
        universe = SymbolUniverse(
            finance_db=client[settings.mongo_db],
            trading_db=client[trading_db_name],
            data_db=data_db,
            config=config.universe,
            require_listed_from_db=config.require_listed_from_db,
        )
        stock_repo = None
        minute_repo = None
        if SINK_REALTIME_DAILY_K in config.sinks:
            stock_repo = StockRepository(
                mongo_uri,
                settings.data_db,
                realtime_db_name=settings.realtime_db,
                sync_uri=config.sync_uri,
            )
        if SINK_MINUTE_BARS in config.sinks:
            minute_repo = MinuteRepository(
                mongo_uri,
                settings.data_db,
                sync_uri=config.sync_uri,
            )
        source = _build_tick_source(config, [])
        sinks = create_sinks(
            config,
            stock_repo=stock_repo,
            minute_repo=minute_repo,
            settings=settings,
            universe=universe,
            shadow_db=client[settings.realtime_db] if config.shadow_mode else None,
        )
        trading_day_checker = None
        if config.tick_source == TICK_SOURCE_BIGQMT_RPC:
            trading_day_checker = make_trading_day_checker(data_db)
        runner = BridgeRunner(
            config=config,
            universe=universe,
            source=source,
            sinks=sinks,
            trading_day_checker=trading_day_checker,
        )
        runner.run()
    finally:
        client.close()


if __name__ == "__main__":
    main()
