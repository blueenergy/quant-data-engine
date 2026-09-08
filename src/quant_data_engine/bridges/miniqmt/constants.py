"""Constants for the standalone miniQMT bridge."""
from __future__ import annotations

TASK_TYPE = "a_daily_realtime_targeted_sync"
TASK_TYPE_FULL_MARKET = "a_daily_realtime_sync"
TASK_TYPE_WATCHLIST_MINUTE = "watchlist_minute_realtime"
SOURCE_TAG = "miniqmt"
SOURCE_MODE = "miniqmt_targeted_daily"
SOURCE_MODE_FULL_MARKET = "miniqmt_full_market_daily"
SOURCE_MODE_BIGQMT_FULL_MARKET = "bigqmt_full_market_daily"
SOURCE_MODE_WATCHLIST_MINUTE = "miniqmt_watchlist_minute"
SOURCE_MODE_BIGQMT_WATCHLIST_MINUTE = "bigqmt_watchlist_minute"
SOURCE_MODE_BIGQMT_WATCHLIST_MINUTE_KLINE = "bigqmt_watchlist_minute_kline"
TICK_SOURCE_MINIQMT = "miniqmt"
TICK_SOURCE_BIGQMT_RPC = "bigqmt_rpc"
MINUTE_BAR_SOURCE_TICK = "tick"
MINUTE_BAR_SOURCE_QMT_1M = "qmt_1m"
DEFAULT_KLINE_COUNT = 241
UNIVERSE_TYPE_WITH_STRATEGY = "watchlist_plan_strategy"
UNIVERSE_TYPE_WITHOUT_STRATEGY = "watchlist_plan"
UNIVERSE_TYPE_FULL_MARKET = "full_market_a_share"
UNIVERSE_MODE_TARGETED = "targeted"
UNIVERSE_MODE_FULL_MARKET = "full_market"
DEFAULT_PLAN_STATUSES = ("generated", "needs_review", "approved", "executed_paper", "partially_executed")
SUPPORTED_SINKS = ("realtime-daily-k", "minute-bars")
RESERVED_SINKS = ()
SINK_REALTIME_DAILY_K = "realtime-daily-k"
SINK_MINUTE_BARS = "minute-bars"

# Targeted factory defaults (kept for backward compatibility).
TARGETED_POLL_INTERVAL = 0.5
TARGETED_DAILY_K_INTERVAL = 5.0
TARGETED_REFRESH = 60
TARGETED_POLL_BATCH_SIZE = 0
TARGETED_WARMUP_SECONDS = 0.5

# Full-market defaults: match Tushare rt_k cron (~1 snapshot/min).
# Lower daily_k_interval later if Windows miniQMT + Mongo keep up.
FULL_MARKET_POLL_INTERVAL = 2.0
FULL_MARKET_DAILY_K_INTERVAL = 60.0
FULL_MARKET_REFRESH = 3600
FULL_MARKET_POLL_BATCH_SIZE = 400
FULL_MARKET_WARMUP_SECONDS = 3.0

# Big QMT RPC client timeout when tick_source is bigqmt_rpc (keep in sync with bigqmt.constants).
DEFAULT_QUOTE_RPC_TIMEOUT_SEC = 60.0

# xtquant sector names tried when Mongo stock_info is empty.
XTDATA_A_SHARE_SECTORS = ("沪深A股", "京市A股", "北证A股")
