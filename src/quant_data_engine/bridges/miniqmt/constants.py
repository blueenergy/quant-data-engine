"""Constants for the standalone miniQMT bridge."""
from __future__ import annotations

TASK_TYPE = "a_daily_realtime_targeted_sync"
SOURCE_TAG = "miniqmt"
SOURCE_MODE = "miniqmt_targeted_daily"
UNIVERSE_TYPE_WITH_STRATEGY = "watchlist_plan_strategy"
UNIVERSE_TYPE_WITHOUT_STRATEGY = "watchlist_plan"
DEFAULT_PLAN_STATUSES = ("approved", "executed_paper", "partially_executed")
SUPPORTED_SINKS = ("realtime-daily-k",)
RESERVED_SINKS = ("minute-bars",)

