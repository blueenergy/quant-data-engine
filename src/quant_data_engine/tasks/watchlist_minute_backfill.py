"""Watchlist + holdings minute-bar backfill helpers (miniQMT → Mongo)."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Sequence

from pymongo.collection import Collection

from quant_data_engine.config.settings import Settings


@dataclass(frozen=True)
class SymbolVerifyResult:
    symbol: str
    bar_count: int
    bad_1970_count: int
    min_trade_date: str | None
    max_trade_date: str | None

    @property
    def ok(self) -> bool:
        return self.bar_count > 0 and self.bad_1970_count == 0


@dataclass(frozen=True)
class BackfillVerifyReport:
    results: tuple[SymbolVerifyResult, ...]
    min_bars_per_symbol: int

    @property
    def passed(self) -> bool:
        if not self.results:
            return False
        return all(
            result.bar_count >= self.min_bars_per_symbol and result.bad_1970_count == 0
            for result in self.results
        )

    @property
    def empty_symbols(self) -> tuple[str, ...]:
        return tuple(result.symbol for result in self.results if result.bar_count == 0)


def default_date_range(
    *,
    end: datetime | None = None,
    lookback_days: int = 40,
) -> tuple[str, str]:
    """Return ``(start, end)`` as YYYYMMDD strings for G/S warmup windows."""
    end_dt = end or datetime.today()
    start_dt = end_dt - timedelta(days=lookback_days)
    return start_dt.strftime("%Y%m%d"), end_dt.strftime("%Y%m%d")


def resolve_watchlist_symbols(
    settings: Settings,
    mongo_client: Any,
    *,
    include_watchlists: bool = True,
    include_watchlist_strategies: bool = True,
    include_portfolio_plans: bool = True,
    include_paper_positions: bool = True,
) -> list[str]:
    """Resolve the same symbol universe as the miniQMT bridge (watchlist + holdings)."""
    from quant_data_engine.bridges.miniqmt.config import UniverseConfig
    from quant_data_engine.bridges.miniqmt.universe import SymbolUniverse

    trading_db_name = getattr(settings, "trading_db", "quant_trading")
    universe = SymbolUniverse(
        finance_db=mongo_client[settings.mongo_db],
        trading_db=mongo_client[trading_db_name],
        data_db=mongo_client[settings.data_db],
        config=UniverseConfig(
            auto=True,
            include_watchlists=include_watchlists,
            include_watchlist_strategies=include_watchlist_strategies,
            include_portfolio_plans=include_portfolio_plans,
            include_paper_positions=include_paper_positions,
        ),
    )
    return universe.resolve()


def verify_minute_bars(
    collection: Collection,
    symbols: Sequence[str],
    *,
    min_bars_per_symbol: int = 240,
) -> BackfillVerifyReport:
    """Check ``minute_bars`` coverage for each symbol."""
    results: list[SymbolVerifyResult] = []
    for symbol in symbols:
        bar_count = int(collection.count_documents({"symbol": symbol}))
        bad_1970_count = int(
            collection.count_documents({"symbol": symbol, "trade_date": {"$regex": r"^1970"}})
        )
        bounds = list(
            collection.aggregate(
                [
                    {"$match": {"symbol": symbol}},
                    {
                        "$group": {
                            "_id": None,
                            "min_td": {"$min": "$trade_date"},
                            "max_td": {"$max": "$trade_date"},
                        }
                    },
                ]
            )
        )
        min_trade_date = bounds[0]["min_td"] if bounds else None
        max_trade_date = bounds[0]["max_td"] if bounds else None
        results.append(
            SymbolVerifyResult(
                symbol=symbol,
                bar_count=bar_count,
                bad_1970_count=bad_1970_count,
                min_trade_date=min_trade_date,
                max_trade_date=max_trade_date,
            )
        )
    return BackfillVerifyReport(tuple(results), min_bars_per_symbol=min_bars_per_symbol)
