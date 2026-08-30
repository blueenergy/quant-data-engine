"""公募抄作业：观察名单 + 两期产品/股票 diff。"""

from quant_data_engine.fund_copywork.actions import (
    ACTION_DECREASED,
    ACTION_EXITED,
    ACTION_INCREASED,
    ACTION_LABELS,
    ACTION_NEW,
    ACTION_UNCHANGED,
    classify_action,
    holding_change,
)
from quant_data_engine.fund_copywork.diff import (
    canonical_holdings,
    consensus_snapshots,
    product_snapshot,
)
from quant_data_engine.fund_copywork.watchlist import (
    WatchlistConfig,
    build_watchlist,
    load_watchlist_config,
    strategy_aum_from_rows,
)

__all__ = [
    "ACTION_DECREASED",
    "ACTION_EXITED",
    "ACTION_INCREASED",
    "ACTION_LABELS",
    "ACTION_NEW",
    "ACTION_UNCHANGED",
    "WatchlistConfig",
    "build_watchlist",
    "canonical_holdings",
    "classify_action",
    "consensus_snapshots",
    "holding_change",
    "load_watchlist_config",
    "product_snapshot",
    "strategy_aum_from_rows",
]
