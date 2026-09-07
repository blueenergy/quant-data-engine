"""Constants for the Big QMT ZMQ RPC tick source."""
from __future__ import annotations

SOURCE_MODE_BIGQMT_FULL_MARKET = "bigqmt_full_market_daily"
SHADOW_COLLECTION = "bigqmt_shadow_snapshots"

TICK_SOURCE_MINIQMT = "miniqmt"
TICK_SOURCE_BIGQMT_RPC = "bigqmt_rpc"

DEFAULT_ZMQ_BASE_PORT = 15560
DEFAULT_QUOTE_RPC_TIMEOUT_SEC = 60.0
DEFAULT_QUOTE_RPC_POLL_BATCH_SIZE = 200
DEFAULT_QUOTE_RPC_HOST = "127.0.0.1"
