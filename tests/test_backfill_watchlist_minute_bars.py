"""Tests for watchlist minute backfill helpers and CLI wiring."""
from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock, patch

from quant_data_engine.tasks.watchlist_minute_backfill import (
    acquire_backfill_lock,
    default_date_range,
    release_backfill_lock,
    resolve_watchlist_symbols,
    verify_minute_bars,
)
from quant_data_engine.tasks.run_backfill_watchlist_minute_bars import (
    filter_gap_symbols,
    resolve_symbols,
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
        group_stage = next((stage["$group"] for stage in pipeline if "$group" in stage), None)
        if group_stage and "_id" in group_stage:
            grouped: dict[str, dict] = {}
            for row in rows:
                symbol = row.get("symbol")
                if not symbol:
                    continue
                bucket = grouped.setdefault(
                    symbol,
                    {
                        "bar_count": 0,
                        "bad_1970_count": 0,
                        "trading_days": set(),
                        "min_trade_date": row.get("trade_date"),
                        "max_trade_date": row.get("trade_date"),
                    },
                )
                bucket["bar_count"] += 1
                trade_date = str(row.get("trade_date") or "")
                if trade_date.startswith("1970"):
                    bucket["bad_1970_count"] += 1
                if len(trade_date) >= 8:
                    bucket["trading_days"].add(trade_date[:8])
                bucket["min_trade_date"] = min(bucket["min_trade_date"], trade_date)
                bucket["max_trade_date"] = max(bucket["max_trade_date"], trade_date)
            return [
                {
                    "_id": symbol,
                    "bar_count": data["bar_count"],
                    "bad_1970_count": data["bad_1970_count"],
                    "trading_days": sorted(data["trading_days"]),
                    "min_trade_date": data["min_trade_date"],
                    "max_trade_date": data["max_trade_date"],
                }
                for symbol, data in grouped.items()
            ]
        trade_dates = [row["trade_date"] for row in rows]
        return [{"min_td": min(trade_dates), "max_td": max(trade_dates)}]

    def find_one_and_update(self, *args, **kwargs):
        return {"lock_id": "watchlist_minute_backfill", "holder_id": "holder-a"}

    def create_index(self, *args, **kwargs):
        return "lock_id_1"

    def delete_one(self, query):
        return None

    def _match(self, query):
        rows = self.docs
        symbol = query.get("symbol")
        if symbol and not isinstance(symbol, dict):
            rows = [row for row in rows if row.get("symbol") == symbol]
        symbols = query.get("symbol")
        if isinstance(symbols, dict) and "$in" in symbols:
            allowed = set(symbols["$in"])
            rows = [row for row in rows if row.get("symbol") in allowed]
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
    assert report.results[0].trading_days == 1
    assert report.results[0].meets_criteria(min_bars_per_symbol=2)
    assert report.results[1].bad_1970_count == 1
    assert not report.passed
    assert report.empty_symbols == ()
    assert report.gap_symbols == ("600000.SH",)


def test_filter_gap_symbols():
    collection = _Collection(
        [
            {"symbol": "000001.SZ", "trade_date": "202609070930"},
            {"symbol": "000001.SZ", "trade_date": "202609070931"},
            {"symbol": "600000.SH", "trade_date": "202609070930"},
            {"symbol": "600000.SH", "trade_date": "202609070931"},
        ]
    )
    gaps, report = filter_gap_symbols(
        collection,
        ["000001.SZ", "600000.SH", "300750.SZ"],
        min_bars_per_symbol=2,
        min_trading_days=1,
    )
    assert gaps == ["300750.SZ"]
    assert report.gap_symbols == ("300750.SZ",)


def test_resolve_symbols_explicit_list():
    settings = MagicMock()
    client = MagicMock()
    args = MagicMock(symbols=["000001.SZ", "600000.SH"])
    assert resolve_symbols(settings, client, args) == ["000001.SZ", "600000.SH"]


def test_acquire_and_release_backfill_lock():
    lock_coll = _Collection([])
    db = _Db({"ingestion_locks": lock_coll})
    client = MagicMock()
    client.__getitem__.return_value = db
    assert acquire_backfill_lock(client, "finance", "holder-a", ttl_seconds=60)
    release_backfill_lock(client, "finance", "holder-a")


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


def test_backfill_only_gap_skips_when_no_gaps(capsys):
    settings = MagicMock()
    settings.mongo_uri = "mongodb://localhost:27017"
    settings.mongo_db = "finance"
    settings.data_db = "quant_data"
    settings.trading_db = "quant_trading"

    collection = _Collection(
        [
            {"symbol": "000001.SZ", "trade_date": "202609070930"},
            {"symbol": "000001.SZ", "trade_date": "202609070931"},
            {"symbol": "000001.SZ", "trade_date": "202609080930"},
        ]
    )
    mongo_cls = MagicMock()
    mongo_cls.return_value.close = MagicMock()
    mongo_cls.return_value.__getitem__.return_value.__getitem__.return_value = collection

    with (
        patch("quant_data_engine.tasks.run_backfill_watchlist_minute_bars.get_settings", return_value=settings),
        patch("quant_data_engine.tasks.run_backfill_watchlist_minute_bars.MongoClient", mongo_cls),
        patch(
            "quant_data_engine.tasks.run_backfill_watchlist_minute_bars.resolve_watchlist_symbols",
            return_value=["000001.SZ"],
        ),
        patch("quant_data_engine.tasks.run_backfill_watchlist_minute_bars.run_backfill_download") as download,
        patch(
            "sys.argv",
            [
                "run_backfill_watchlist_minute_bars",
                "--only-gap",
                "--min-bars-per-symbol",
                "2",
                "--min-trading-days",
                "1",
            ],
        ),
    ):
        from quant_data_engine.tasks.run_backfill_watchlist_minute_bars import main

        assert main() == 0
        download.assert_not_called()

    captured = capsys.readouterr()
    assert "no gaps" in captured.out


def test_gap_monitor_dry_run(capsys):
    settings = MagicMock()
    settings.mongo_uri = "mongodb://localhost:27017"
    settings.mongo_db = "finance"
    settings.data_db = "quant_data"
    settings.trading_db = "quant_trading"

    collection = _Collection([{"symbol": "000001.SZ", "trade_date": "202609070930"}])
    mongo_cls = MagicMock()
    mongo_cls.return_value.close = MagicMock()
    mongo_cls.return_value.__getitem__.side_effect = lambda name: {
        "quant_data": _Db({"minute_bars": collection}),
        "finance": _Db({"ingestion_locks": _Collection([])}),
    }[name]

    with (
        patch("quant_data_engine.tasks.run_watchlist_minute_gap_monitor.get_settings", return_value=settings),
        patch("quant_data_engine.tasks.run_watchlist_minute_gap_monitor.MongoClient", mongo_cls),
        patch(
            "quant_data_engine.tasks.run_watchlist_minute_gap_monitor.resolve_symbols",
            return_value=["000001.SZ", "600000.SH"],
        ),
        patch("sys.argv", ["run_watchlist_minute_gap_monitor", "--once", "--dry-run", "--min-bars-per-symbol", "2"]),
    ):
        from quant_data_engine.tasks.run_watchlist_minute_gap_monitor import main

        assert main() == 2

    captured = capsys.readouterr()
    assert "600000.SH" in captured.out


def test_gap_monitor_outside_session_only_skips_during_session(capsys):
    settings = MagicMock()
    settings.mongo_uri = "mongodb://localhost:27017"
    settings.mongo_db = "finance"
    settings.data_db = "quant_data"
    settings.trading_db = "quant_trading"

    collection = _Collection([{"symbol": "000001.SZ", "trade_date": "202609070930"}])
    mongo_cls = MagicMock()
    mongo_cls.return_value.close = MagicMock()
    mongo_cls.return_value.__getitem__.side_effect = lambda name: {
        "quant_data": _Db({"minute_bars": collection}),
        "finance": _Db({"ingestion_locks": _Collection([])}),
    }[name]

    with (
        patch("quant_data_engine.tasks.run_watchlist_minute_gap_monitor.get_settings", return_value=settings),
        patch("quant_data_engine.tasks.run_watchlist_minute_gap_monitor.MongoClient", mongo_cls),
        patch(
            "quant_data_engine.tasks.run_watchlist_minute_gap_monitor.resolve_symbols",
            return_value=["000001.SZ", "600000.SH"],
        ),
        patch(
            "quant_data_engine.tasks.run_watchlist_minute_gap_monitor.in_cn_stock_session",
            return_value=True,
        ) as in_session,
        patch("quant_data_engine.tasks.run_watchlist_minute_gap_monitor.run_backfill_download") as download,
        patch(
            "sys.argv",
            [
                "run_watchlist_minute_gap_monitor",
                "--once",
                "--outside-session-only",
                "--min-bars-per-symbol",
                "2",
            ],
        ),
    ):
        from quant_data_engine.tasks.run_watchlist_minute_gap_monitor import main

        assert main() == 2
        in_session.assert_called_once()
        download.assert_not_called()

    captured = capsys.readouterr()
    assert "skip backfill: CN stock session is open" in captured.out
