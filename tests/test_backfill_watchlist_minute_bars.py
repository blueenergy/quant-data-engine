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
    BackfillOptions,
    filter_gap_symbols,
    resolve_symbols,
    run_backfill_download,
)
from quant_data_engine.tasks.watchlist_minute_bigqmt import (
    SOURCE_BIGQMT_RPC,
    backfill_symbol_via_bigqmt,
    iter_date_chunks,
    resolve_quote_rpc,
    run_bigqmt_backfill_download,
)
from quant_data_engine.bridges.bigqmt.zmq_rpc_client import RpcTransportError


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


def _columnar_frame(stime: str, close: float = 10.0) -> dict:
    return {
        "__bigqmt_type__": "DataFrame",
        "columns": ["stime", "open", "high", "low", "close", "volume"],
        "records": {
            "stime": [stime],
            "open": [close],
            "high": [close],
            "low": [close],
            "close": [close],
            "volume": [100.0],
        },
    }


class _FakeRepo:
    def __init__(self):
        self.records = []

    def upsert_bar_records(self, records):
        self.records.extend(list(records))
        return len(records)


class _FakeQuoteRpc:
    def __init__(self, frames=None, frames_after_download=None, download_error=None):
        self.cache = dict(frames or {})
        self.after = dict(frames_after_download or {})
        self.download_error = download_error
        self.downloads = []
        self.downloads2 = []
        self.reads = []
        self.closed = False

    def get_market_data_ex(self, **kwargs):
        self.reads.append(kwargs)
        symbol = kwargs["stock_list"][0]
        payload = self.cache.get(symbol)
        if payload is None:
            return {}
        return {symbol: payload}

    def download_history_data(self, stock_code, **kwargs):
        if self.download_error is not None:
            raise self.download_error
        self.downloads.append((stock_code, kwargs))
        if stock_code in self.after:
            self.cache[stock_code] = self.after[stock_code]
        return True

    def download_history_data2(self, stock_list, **kwargs):
        self.downloads2.append((list(stock_list), kwargs))
        for stock_code in stock_list:
            if stock_code in self.after:
                self.cache[stock_code] = self.after[stock_code]
        return True

    def close(self):
        self.closed = True


def test_iter_date_chunks_splits_and_zero_means_whole_range():
    assert iter_date_chunks("20260901", "20260908", 5) == [
        ("20260901", "20260905"),
        ("20260906", "20260908"),
    ]
    assert iter_date_chunks("20260901", "20260908", 0) == [("20260901", "20260908")]


def test_resolve_quote_rpc_from_account_id(monkeypatch):
    monkeypatch.delenv("QUOTE_RPC_ADDRESS", raising=False)
    monkeypatch.delenv("QUOTE_ACCOUNT_ID", raising=False)
    with patch(
        "quant_data_engine.tasks.watchlist_minute_bigqmt.default_watchlist_config_path",
        return_value=None,
    ):
        address, account_id, timeout = resolve_quote_rpc(quote_account_id="62129066")
    assert account_id == "62129066"
    assert address.endswith(":15626")
    assert timeout == 60.0


def test_backfill_symbol_uses_cache_without_download():
    rpc = _FakeQuoteRpc(frames={"000001.SZ": _columnar_frame("20260908093000")})
    repo = _FakeRepo()
    written = backfill_symbol_via_bigqmt(
        rpc,
        repo,
        "000001.SZ",
        start="20260908",
        end="20260908",
        chunk_days=0,
    )
    assert written == 1
    assert rpc.downloads == []
    assert repo.records[0]["trade_date"] == "202609080930"
    assert rpc.reads[0]["start_time"] == "20260908"
    assert rpc.reads[0]["end_time"] == "20260908"
    assert rpc.reads[0]["dividend_type"] == "none"


def test_backfill_symbol_downloads_then_rereads_empty_cache():
    rpc = _FakeQuoteRpc(
        frames={},
        frames_after_download={"000001.SZ": _columnar_frame("20260908093000", 11.5)},
    )
    repo = _FakeRepo()
    written = backfill_symbol_via_bigqmt(
        rpc,
        repo,
        "000001.SZ",
        start="20260908",
        end="20260908",
        chunk_days=0,
        download_timeout_seconds=90,
    )
    assert written == 1
    assert rpc.downloads[0][0] == "000001.SZ"
    assert rpc.downloads[0][1]["start_time"] == "20260908"
    assert rpc.downloads[0][1]["timeout_seconds"] == 90
    assert repo.records[0]["close"] == 11.5


def test_backfill_symbol_falls_back_to_download_history_data2():
    rpc = _FakeQuoteRpc(
        frames={},
        frames_after_download={"000001.SZ": _columnar_frame("20260908093000")},
        download_error=RpcTransportError("rpc method is not implemented: download_history_data"),
    )
    repo = _FakeRepo()
    written = backfill_symbol_via_bigqmt(
        rpc,
        repo,
        "000001.SZ",
        start="20260908",
        end="20260908",
        chunk_days=0,
    )
    assert written == 1
    assert rpc.downloads == []
    assert rpc.downloads2[0][0] == ["000001.SZ"]


def test_backfill_symbol_still_empty_after_download():
    rpc = _FakeQuoteRpc(frames={})
    repo = _FakeRepo()
    written = backfill_symbol_via_bigqmt(
        rpc,
        repo,
        "000001.SZ",
        start="20260908",
        end="20260908",
        chunk_days=0,
    )
    assert written == 0
    assert len(rpc.downloads) == 1
    assert repo.records == []


def test_run_backfill_download_bigqmt_rpc(capsys):
    settings = MagicMock()
    settings.mongo_uri = "mongodb://localhost:27017"
    collection = _Collection([])
    rpc = _FakeQuoteRpc(frames={"000001.SZ": _columnar_frame("20260908093000")})
    options = BackfillOptions(
        start="20260908",
        end="20260908",
        source=SOURCE_BIGQMT_RPC,
        chunk_days=0,
    )
    with patch(
        "quant_data_engine.tasks.watchlist_minute_bigqmt.MinuteRepository",
        return_value=_FakeRepo(),
    ):
        failed = run_backfill_download(
            collection,
            ["000001.SZ"],
            settings=settings,
            options=options,
            rpc_client=rpc,
        )
    assert failed == []
    assert rpc.closed is False
    captured = capsys.readouterr()
    assert "source=bigqmt_rpc" in captured.out


def test_run_bigqmt_backfill_marks_failed_when_empty(capsys):
    settings = MagicMock()
    settings.mongo_uri = "mongodb://localhost:27017"
    collection = _Collection([])
    rpc = _FakeQuoteRpc(frames={})
    failed = run_bigqmt_backfill_download(
        collection,
        ["000001.SZ"],
        settings=settings,
        start="20260908",
        end="20260908",
        chunk_days=0,
        rpc_client=rpc,
        repository=_FakeRepo(),
    )
    assert failed == ["000001.SZ"]
    captured = capsys.readouterr()
    assert "Issue #95" in captured.out

