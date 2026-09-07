"""Watchlist + holdings minute-bar backfill helpers (miniQMT → Mongo)."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Sequence

from pymongo import ReturnDocument
from pymongo.collection import Collection
from pymongo.errors import DuplicateKeyError

from quant_data_engine.config.settings import Settings

JOB_TYPE_WATCHLIST_MINUTE_BACKFILL = "watchlist_minute_backfill"
LOCK_ID = "watchlist_minute_backfill"
LOCK_COLLECTION = "ingestion_locks"


@dataclass(frozen=True)
class SymbolVerifyResult:
    symbol: str
    bar_count: int
    bad_1970_count: int
    min_trade_date: str | None
    max_trade_date: str | None
    trading_days: int = 0

    def meets_criteria(self, *, min_bars_per_symbol: int, min_trading_days: int = 0) -> bool:
        if self.bad_1970_count > 0:
            return False
        if self.bar_count < min_bars_per_symbol:
            return False
        if min_trading_days > 0 and self.trading_days < min_trading_days:
            return False
        return True


@dataclass(frozen=True)
class BackfillVerifyReport:
    results: tuple[SymbolVerifyResult, ...]
    min_bars_per_symbol: int
    min_trading_days: int = 0

    @property
    def passed(self) -> bool:
        if not self.results:
            return False
        return all(
            result.meets_criteria(
                min_bars_per_symbol=self.min_bars_per_symbol,
                min_trading_days=self.min_trading_days,
            )
            for result in self.results
        )

    @property
    def empty_symbols(self) -> tuple[str, ...]:
        return tuple(result.symbol for result in self.results if result.bar_count == 0)

    @property
    def gap_symbols(self) -> tuple[str, ...]:
        return tuple(
            result.symbol
            for result in self.results
            if not result.meets_criteria(
                min_bars_per_symbol=self.min_bars_per_symbol,
                min_trading_days=self.min_trading_days,
            )
        )


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
    min_trading_days: int = 0,
) -> BackfillVerifyReport:
    """Check ``minute_bars`` coverage for each symbol."""
    symbol_list = sorted({str(symbol) for symbol in symbols if symbol})
    if not symbol_list:
        return BackfillVerifyReport(
            (),
            min_bars_per_symbol=min_bars_per_symbol,
            min_trading_days=min_trading_days,
        )

    stats_by_symbol: dict[str, dict[str, Any]] = {}
    pipeline = [
        {"$match": {"symbol": {"$in": symbol_list}}},
        {
            "$project": {
                "symbol": 1,
                "trade_date": 1,
                "day": {"$substr": ["$trade_date", 0, 8]},
                "is_1970": {
                    "$regexMatch": {"input": {"$ifNull": ["$trade_date", ""]}, "regex": r"^1970"},
                },
            }
        },
        {
            "$group": {
                "_id": "$symbol",
                "bar_count": {"$sum": 1},
                "bad_1970_count": {"$sum": {"$cond": ["$is_1970", 1, 0]}},
                "trading_days": {"$addToSet": "$day"},
                "min_trade_date": {"$min": "$trade_date"},
                "max_trade_date": {"$max": "$trade_date"},
            }
        },
    ]
    for row in collection.aggregate(pipeline):
        stats_by_symbol[str(row["_id"])] = row

    results: list[SymbolVerifyResult] = []
    for symbol in symbol_list:
        row = stats_by_symbol.get(symbol)
        if row is None:
            results.append(
                SymbolVerifyResult(
                    symbol=symbol,
                    bar_count=0,
                    bad_1970_count=0,
                    min_trade_date=None,
                    max_trade_date=None,
                    trading_days=0,
                )
            )
            continue
        trading_days = {str(day) for day in (row.get("trading_days") or []) if day}
        results.append(
            SymbolVerifyResult(
                symbol=symbol,
                bar_count=int(row.get("bar_count") or 0),
                bad_1970_count=int(row.get("bad_1970_count") or 0),
                min_trade_date=row.get("min_trade_date"),
                max_trade_date=row.get("max_trade_date"),
                trading_days=len(trading_days),
            )
        )
    return BackfillVerifyReport(
        tuple(results),
        min_bars_per_symbol=min_bars_per_symbol,
        min_trading_days=min_trading_days,
    )


def acquire_backfill_lock(
    mongo_client: Any,
    db_name: str,
    holder_id: str,
    *,
    ttl_seconds: int = 7200,
) -> bool:
    """Acquire singleton lock for watchlist minute backfill (best-effort)."""
    now = datetime.now(timezone.utc)
    expires_at = now + timedelta(seconds=ttl_seconds)
    coll = mongo_client[db_name][LOCK_COLLECTION]
    try:
        coll.create_index("lock_id", unique=True, background=True)
    except Exception:
        pass
    try:
        doc = coll.find_one_and_update(
            {
                "lock_id": LOCK_ID,
                "$or": [
                    {"expires_at": {"$lt": now}},
                    {"expires_at": None},
                    {"expires_at": {"$exists": False}},
                    {"holder_id": holder_id},
                ],
            },
            {
                "$set": {
                    "lock_id": LOCK_ID,
                    "holder_id": holder_id,
                    "expires_at": expires_at,
                    "updated_at": now,
                },
                "$setOnInsert": {"created_at": now},
            },
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
    except DuplicateKeyError:
        return False
    return bool(doc and doc.get("holder_id") == holder_id)


def release_backfill_lock(mongo_client: Any, db_name: str, holder_id: str) -> None:
    """Release lock when held by ``holder_id``."""
    coll = mongo_client[db_name][LOCK_COLLECTION]
    coll.delete_one({"lock_id": LOCK_ID, "holder_id": holder_id})
