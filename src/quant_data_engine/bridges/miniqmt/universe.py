"""Symbol universe resolution for the standalone miniQMT bridge."""
from __future__ import annotations

from typing import Any, Optional, Sequence

from quant_data_engine.tasks.run_watchlist_intraday_eastmoney import normalize_watchlist_symbol_to_ts_code

from .config import UniverseConfig
from .constants import UNIVERSE_TYPE_WITH_STRATEGY, UNIVERSE_TYPE_WITHOUT_STRATEGY


def normalize_symbol(raw: Any) -> Optional[str]:
    """Normalize common A-share symbols into ts_code format."""
    if not isinstance(raw, str):
        return None
    return normalize_watchlist_symbol_to_ts_code(raw)


def _collection(db: Any, name: str) -> Any:
    try:
        return db[name]
    except Exception:
        return None


class SymbolUniverse:
    """Resolve and refresh the target symbol list for the miniQMT bridge."""

    def __init__(
        self,
        *,
        finance_db: Any,
        trading_db: Any,
        config: UniverseConfig,
    ) -> None:
        self.finance_db = finance_db
        self.trading_db = trading_db
        self.config = config

    @property
    def universe_type(self) -> str:
        if self.config.auto and self.config.include_watchlist_strategies:
            return UNIVERSE_TYPE_WITH_STRATEGY
        return UNIVERSE_TYPE_WITHOUT_STRATEGY

    def resolve(self) -> list[str]:
        if not self.config.auto:
            return self._normalize_many(self.config.symbols)

        symbols: set[str] = set()
        if self.config.include_watchlists:
            symbols.update(self.load_watchlist_symbols())
        if self.config.include_watchlist_strategies:
            symbols.update(self.load_strategy_symbols())
        if self.config.include_portfolio_plans:
            symbols.update(self.load_active_plan_symbols(self.config.plan_statuses))
        if self.config.symbols:
            symbols.update(self._normalize_many(self.config.symbols))
        return sorted(symbols)

    def refresh(self) -> list[str]:
        return self.resolve()

    def load_watchlist_symbols(self) -> set[str]:
        out: set[str] = set()
        coll = _collection(self.finance_db, "user_watchlists")
        if coll is None:
            return out
        for doc in coll.find({}, {"symbols": 1}):
            for symbol in doc.get("symbols") or []:
                normalized = normalize_symbol(symbol)
                if normalized:
                    out.add(normalized)
        return out

    def load_strategy_symbols(self) -> set[str]:
        out: set[str] = set()
        coll = _collection(self.finance_db, "watchlist_strategies")
        if coll is None:
            return out
        for doc in coll.find({"enabled": True}, {"symbol": 1}):
            normalized = normalize_symbol(doc.get("symbol"))
            if normalized:
                out.add(normalized)
        return out

    def load_active_plan_symbols(self, statuses: Sequence[str]) -> set[str]:
        out: set[str] = set()
        plans = _collection(self.trading_db, "portfolio_trade_plans")
        items = _collection(self.trading_db, "portfolio_trade_plan_items")
        if plans is None or items is None:
            return out

        plan_rows = list(
            plans.find(
                {"status": {"$in": list(statuses)}},
                {"plan_id": 1, "_id": 0},
            )
        )
        plan_ids = [str(row.get("plan_id")) for row in plan_rows if row.get("plan_id")]
        if not plan_ids:
            return out

        for item in items.find({"plan_id": {"$in": plan_ids}}, {"symbol": 1, "_id": 0}):
            normalized = normalize_symbol(item.get("symbol"))
            if normalized:
                out.add(normalized)
        return out

    @staticmethod
    def _normalize_many(symbols: Sequence[str]) -> list[str]:
        out = {normalized for normalized in (normalize_symbol(symbol) for symbol in symbols) if normalized}
        return sorted(out)

