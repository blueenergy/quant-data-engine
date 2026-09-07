"""Assemble standalone bridge sinks from ``BridgeConfig``."""
from __future__ import annotations

from typing import Any

from .config import BridgeConfig
from .constants import (
    SINK_MINUTE_BARS,
    SINK_REALTIME_DAILY_K,
    SOURCE_MODE,
    SOURCE_MODE_BIGQMT_FULL_MARKET,
    SOURCE_MODE_BIGQMT_WATCHLIST_MINUTE,
    SOURCE_MODE_FULL_MARKET,
    SOURCE_MODE_WATCHLIST_MINUTE,
    TASK_TYPE,
    TASK_TYPE_FULL_MARKET,
    TASK_TYPE_WATCHLIST_MINUTE,
    TICK_SOURCE_BIGQMT_RPC,
)
from .sinks import RealtimeDailyKSink
from .tick_minute import MinuteBarsSink


def daily_k_source_mode(config: BridgeConfig, *, full_market: bool) -> str:
    if config.tick_source == TICK_SOURCE_BIGQMT_RPC and full_market:
        return SOURCE_MODE_BIGQMT_FULL_MARKET
    return SOURCE_MODE_FULL_MARKET if full_market else SOURCE_MODE


def minute_source_mode(config: BridgeConfig) -> str:
    if config.tick_source == TICK_SOURCE_BIGQMT_RPC:
        return SOURCE_MODE_BIGQMT_WATCHLIST_MINUTE
    return SOURCE_MODE_WATCHLIST_MINUTE


def create_sinks(
    config: BridgeConfig,
    *,
    stock_repo: Any,
    minute_repo: Any,
    settings: Any,
    universe: Any,
    shadow_db: Any = None,
) -> list[Any]:
    """Build sink instances in ``config.sinks`` order."""
    full_market = config.universe.is_full_market
    daily_mode = daily_k_source_mode(config, full_market=full_market)
    sinks: list[Any] = []
    for name in config.sinks:
        if name == SINK_REALTIME_DAILY_K:
            sink_kwargs = dict(
                repo=stock_repo,
                settings=settings,
                write_interval=config.daily_k_interval,
                universe_type=universe.universe_type,
                source_mode=daily_mode,
                task_type=TASK_TYPE_FULL_MARKET if full_market else TASK_TYPE,
                enable_derivatives=full_market and not config.shadow_mode,
            )
            if config.shadow_mode:
                if shadow_db is None:
                    raise ValueError("shadow_mode requires shadow_db")
                from quant_data_engine.bridges.bigqmt.shadow_sink import ShadowRealtimeDailyKSink

                sinks.append(ShadowRealtimeDailyKSink(shadow_db=shadow_db, **sink_kwargs))
            else:
                sinks.append(RealtimeDailyKSink(**sink_kwargs))
        elif name == SINK_MINUTE_BARS:
            sinks.append(
                MinuteBarsSink(
                    repo=minute_repo,
                    settings=settings,
                    write_interval=config.daily_k_interval,
                    universe_type=universe.universe_type,
                    source_mode=minute_source_mode(config),
                    task_type=TASK_TYPE_WATCHLIST_MINUTE,
                )
            )
        else:
            raise ValueError(f"unsupported sink: {name}")
    return sinks
