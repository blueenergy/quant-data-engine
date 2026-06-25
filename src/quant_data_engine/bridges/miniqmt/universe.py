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


def _holds_shares(value: Any) -> bool:
    """Return True when a position row reports a positive share count."""
    try:
        return float(value) > 0
    except (TypeError, ValueError):
        return False


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
        if self.config.include_paper_positions:
            symbols.update(self.load_paper_position_symbols())
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

    def load_paper_position_symbols(self) -> set[str]:
        """Collect symbols currently held in paper-trading position snapshots.

        ``portfolio_paper_positions`` keeps one document per (lineage, as_of_date).
        We pick the most recent snapshot per lineage and union the still-held
        symbols (``shares > 0`` when the field is present).
        """
        out: set[str] = set()
        coll = _collection(self.trading_db, "portfolio_paper_positions")
        if coll is None:
            return out

        projection = {
            "positions": 1,
            "as_of_date": 1,
            "plan_id": 1,
            "strategy_template_id": 1,
            "params_hash": 1,
            "user_id": 1,
            "_id": 0,
        }
        latest_by_lineage: dict[tuple, dict] = {}
        for doc in coll.find({}, projection):
            key = doc.get("plan_id") or (
                doc.get("strategy_template_id"),
                doc.get("params_hash"),
                doc.get("user_id"),
            )
            as_of = str(doc.get("as_of_date") or "")
            current = latest_by_lineage.get(key)
            if current is None or as_of >= str(current.get("as_of_date") or ""):
                latest_by_lineage[key] = doc

        for doc in latest_by_lineage.values():
            for row in doc.get("positions") or []:
                if not isinstance(row, dict):
                    continue
                if "shares" in row and not _holds_shares(row.get("shares")):
                    continue
                normalized = normalize_symbol(row.get("symbol"))
                if normalized:
                    out.add(normalized)
        return out

    @staticmethod
    def _normalize_many(symbols: Sequence[str]) -> list[str]:
        out = {normalized for normalized in (normalize_symbol(symbol) for symbol in symbols) if normalized}
        return sorted(out)

