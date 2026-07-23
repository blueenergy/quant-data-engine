"""Adapters fetch raw market events from external sources."""

from quant_data_engine.market_events.adapters.cn_gsrl import fetch_cn_gsrl
from quant_data_engine.market_events.adapters.cn_report import fetch_cn_report
from quant_data_engine.market_events.adapters.cn_unlock import fetch_cn_unlock
from quant_data_engine.market_events.adapters.macro import fetch_macro_baidu, fetch_macro_ws
from quant_data_engine.market_events.adapters.seed import fetch_seed_events

__all__ = [
    "fetch_macro_ws",
    "fetch_macro_baidu",
    "fetch_cn_report",
    "fetch_cn_unlock",
    "fetch_cn_gsrl",
    "fetch_seed_events",
]
