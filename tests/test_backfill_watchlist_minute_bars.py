"""Tests for watchlist minute backfill helpers and CLI wiring."""
from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from quant_data_engine.tasks.watchlist_minute_backfill import (
    default_date_range,
    resolve_watchlist_symbols,
    verify_minute_bars,
)


class _Collection:
    def __init__(self, docs):
        self.docs = list(docs)

    def find(self, query=None, projection=None):
        rows = self._match(query or {})
        if projection:
            projected = []
            for row in rows:
                item = {key: row.get(key) for key in projection if key in row}
                projected.append(item)
            rows = projected
        return rows

    def count_documents(self, query):
        rows = self._match(query)
        return len(rows)

    def aggregate(self, pipeline):
        rows = self._match(pipeline[0]["$match"])
        if not rows:
            return []
        trade_dates = [row["trade_date"] for row in rows]
        return [{"min_td": min(trade_dates), "max_td": max(trade_dates)}]

    def _match(self, query):
        rows = self.docs
        symbol = query.get("symbol")
        if symbol:
            rows = [row for row in rows if row.get("symbol") == symbol]
        status = query.get("status")
        if isinstance(status, dict) and "$in" in status:
            allowed = set(status["$in"])
            rows = [row for row in rows if row.get("status") in allowed]
        plan_id = query.get("plan_id")
        if isinstance(plan_id, dict) and "$in" in plan_id:
            allowed = set(plan_id["$in"])
            rows = [row for row in rows if row.get("plan_id") in allowed]
        if query.get("enabled") is True:
            rows = [row for row in rows if row.get("enabled") is True]
        regex = query.get("trade_date", {})
        if isinstance(regex, dict) and "$regex" in regex:
            prefix = regex["$regex"].replace("^", "")
            rows = [row for row in rows if str(row.get("trade_date", "")).startswith(prefix)]
        return rows


class _Db:
    def __init__(self, mapping):
        self.mapping = mapping

    def __getitem__(self, name):
        return self.mapping[name]


def test_default_date_range_lookback():
    start, end = default_date_range(end=datetime(2026, 9, 7), lookback_days=40)
    assert end == "20260907"
    assert start == "20260729"


def test_resolve_watchlist_symbols_merges_sources():
    settings = MagicMock()
    settings.mongo_db = "finance"
    settings.data_db = "quant_data"
    settings.trading_db = "quant_trading"

    finance = _Db(
        {
            "user_watchlists": _Collection([{"symbols": ["000001"]}]),
            "watchlist_strategies": _Collection([{"enabled": True, "symbol": "600000.SH"}]),
        }
    )
    trading = _Db(
        {
            "portfolio_trade_plans": _Collection([{"plan_id": "p1", "status": "approved"}]),
            "portfolio_trade_plan_items": _Collection([{"plan_id": "p1", "symbol": "300750.SZ"}]),
            "portfolio_paper_positions": _Collection([]),
        }
    )
    client = MagicMock()
    client.__getitem__.side_effect = lambda name: {
        "finance": finance,
        "quant_trading": trading,
        "quant_data": _Db({}),
    }[name]

    symbols = resolve_watchlist_symbols(settings, client)
    assert symbols == ["000001.SZ", "300750.SZ", "600000.SH"]


def test_verify_minute_bars_pass_and_fail():
    collection = _Collection(
        [
            {"symbol": "000001.SZ", "trade_date": "202609070930"},
            {"symbol": "000001.SZ", "trade_date": "202609070931"},
            {"symbol": "600000.SH", "trade_date": "197001010000"},
        ]
    )
    report = verify_minute_bars(collection, ["000001.SZ", "600000.SH"], min_bars_per_symbol=2)
    assert report.results[0].bar_count == 2
    assert report.results[0].ok
    assert report.results[1].bad_1970_count == 1
    assert not report.passed
    assert report.empty_symbols == ()


def test_backfill_main_dry_run(capsys):
    settings = MagicMock()
    settings.mongo_uri = "mongodb://localhost:27017"
    settings.mongo_db = "finance"
    settings.data_db = "quant_data"
    settings.trading_db = "quant_trading"

    with (
        patch("quant_data_engine.tasks.run_backfill_watchlist_minute_bars.get_settings", return_value=settings),
        patch("quant_data_engine.tasks.run_backfill_watchlist_minute_bars.MongoClient") as mongo_cls,
        patch(
            "quant_data_engine.tasks.run_backfill_watchlist_minute_bars.resolve_watchlist_symbols",
            return_value=["000001.SZ"],
        ),
        patch("sys.argv", ["run_backfill_watchlist_minute_bars", "--dry-run"]),
    ):
        mongo_cls.return_value.close = MagicMock()
        from quant_data_engine.tasks.run_backfill_watchlist_minute_bars import main

        assert main() == 0

    captured = capsys.readouterr()
    assert "000001.SZ" in captured.out
    assert "universe: 1 symbols" in captured.out
