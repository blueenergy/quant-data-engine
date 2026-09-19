"""Tests for watchlist minute backfill helpers and CLI wiring."""
from __future__ import annotations

from datetime import datetime, timedelta
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
    print_verify_report,
    resolve_symbols,
    run_backfill_download,
)
from quant_data_engine.tasks.watchlist_minute_bigqmt import (
    SOURCE_BIGQMT_RPC,
    backfill_symbol_via_bigqmt,
    filter_session_chunks,
    is_filler_bar,
    iter_date_chunks,
    resolve_quote_rpc,
    run_bigqmt_backfill_download,
    split_real_days,
)
from quant_data_engine.bridges.bigqmt.zmq_rpc_client import RpcTransportError, RpcTransportTimeout


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
        group_stages = [stage["$group"] for stage in pipeline if "$group" in stage]
        if len(group_stages) >= 2:
            # Mirrors verify_minute_bars' two-stage group: (symbol, day) then
            # symbol. Reuses is_filler_bar so this fake and the Mongo `is_flat`
            # predicate cannot drift apart.
            per_day: dict[tuple[str, str], dict] = {}
            for row in rows:
                symbol = row.get("symbol")
                if not symbol:
                    continue
                trade_date = str(row.get("trade_date") or "")
                bucket = per_day.setdefault(
                    (symbol, trade_date[:8]),
                    {
                        "bars": 0,
                        "flat_bars": 0,
                        "bad_1970_bars": 0,
                        "min_trade_date": trade_date,
                        "max_trade_date": trade_date,
                    },
                )
                bucket["bars"] += 1
                if is_filler_bar(row):
                    bucket["flat_bars"] += 1
                if trade_date.startswith("1970"):
                    bucket["bad_1970_bars"] += 1
                bucket["min_trade_date"] = min(bucket["min_trade_date"], trade_date)
                bucket["max_trade_date"] = max(bucket["max_trade_date"], trade_date)

            grouped: dict[str, dict] = {}
            for (symbol, day), data in per_day.items():
                bucket = grouped.setdefault(
                    symbol,
                    {
                        "bar_count": 0,
                        "bad_1970_count": 0,
                        "trading_days": set(),
                        "filler_days": 0,
                        "min_trade_date": data["min_trade_date"],
                        "max_trade_date": data["max_trade_date"],
                    },
                )
                bucket["bar_count"] += data["bars"]
                bucket["bad_1970_count"] += data["bad_1970_bars"]
                bucket["trading_days"].add(day)
                if data["flat_bars"] == data["bars"]:
                    bucket["filler_days"] += 1
                bucket["min_trade_date"] = min(bucket["min_trade_date"], data["min_trade_date"])
                bucket["max_trade_date"] = max(bucket["max_trade_date"], data["max_trade_date"])
            return [
                {
                    "_id": symbol,
                    "bar_count": data["bar_count"],
                    "bad_1970_count": data["bad_1970_count"],
                    "trading_days": sorted(data["trading_days"]),
                    "filler_days": data["filler_days"],
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
    mongo_cls.return_value.__getitem__.return_value = _Db(
        {
            "minute_bars": collection,
            "trade_calendar": _Collection(
                [{"trade_date": "20260907"}, {"trade_date": "20260908"}]
            ),
        }
    )

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


def _filler_frame(stime: str, price: float = 35.47) -> dict:
    """What QMT returns for a symbol with no local 1m cache: flat, no volume."""
    return {
        "__bigqmt_type__": "DataFrame",
        "columns": ["stime", "open", "high", "low", "close", "volume"],
        "records": {
            "stime": [stime],
            "open": [price],
            "high": [price],
            "low": [price],
            "close": [price],
            "volume": [0.0],
        },
    }


def _frame_bars(date: str, prices: list[float], volumes: list[float]) -> dict:
    """Multi-bar frame; bar i lands on consecutive minutes from 09:30.

    Stamps must differ in HHMM — records dedupe on the normalized
    ``YYYYMMDDHHMM`` key, so coarser stamps collapse into one bar.
    """
    stamps = []
    for index in range(len(prices)):
        minute = 30 + index
        stamps.append(f"{date}{9 + minute // 60:02d}{minute % 60:02d}00")
    return {
        "__bigqmt_type__": "DataFrame",
        "columns": ["stime", "open", "high", "low", "close", "volume"],
        "records": {
            "stime": stamps,
            "open": list(prices),
            "high": list(prices),
            "low": list(prices),
            "close": list(prices),
            "volume": list(volumes),
        },
    }


def _session_stamps(day: str) -> list[str]:
    """A-share 1m session: 09:30–11:30 (121 bars) + 13:01–15:00 (120 bars)."""
    stamps = []
    for minute in range(121):
        total = 9 * 60 + 30 + minute
        stamps.append(f"{day}{total // 60:02d}{total % 60:02d}00")
    for minute in range(120):
        total = 13 * 60 + 1 + minute
        stamps.append(f"{day}{total // 60:02d}{total % 60:02d}00")
    return stamps


def _merged(*frames: dict) -> dict:
    columns = ["stime", "open", "high", "low", "close", "volume"]
    records = {column: [] for column in columns}
    for frame in frames:
        for column in columns:
            records[column].extend(frame["records"][column])
    return {"__bigqmt_type__": "DataFrame", "columns": columns, "records": records}


class _OverwritingRepo:
    """Mimics MinuteRepository: upserts keyed on (symbol, trade_date); last wins.

    The real method returns ``inserted + modified + upserted``, and $setting
    identical values reports ``modified_count == 0`` — so a re-write of
    unchanged rows counts as nothing. Mirroring that matters: it is what makes
    ``written == 0`` mean "no new data" downstream.
    """

    _FIELDS = ("open", "high", "low", "close", "volume")

    def __init__(self, existing=None):
        self.rows = {}
        for record in existing or []:
            self.rows[(str(record["symbol"]), str(record["trade_date"]))] = dict(record)

    def upsert_bar_records(self, records):
        written = 0
        for record in records:
            key = (str(record["symbol"]), str(record["trade_date"]))
            current = self.rows.get(key)
            if current is None or any(
                current.get(field) != record.get(field) for field in self._FIELDS
            ):
                written += 1
            self.rows[key] = dict(record)
        return written


def test_is_filler_bar_distinguishes_filler_from_limit_up():
    flat = {"open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0, "volume": 0.0}
    limit_up = {"open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0, "volume": 500.0}
    normal = {"open": 10.0, "high": 10.5, "low": 9.8, "close": 10.2, "volume": 500.0}
    no_volume = {"open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0}
    missing_price = {"high": 10.0, "low": 10.0, "close": 10.0, "volume": 0.0}

    assert is_filler_bar(flat) is True
    # 一字板 is flat too, but it traded — misreading it as filler would drop
    # real limit-up/limit-down days.
    assert is_filler_bar(limit_up) is False
    assert is_filler_bar(normal) is False
    assert is_filler_bar(no_volume) is True
    assert is_filler_bar(missing_price) is False


def test_split_real_days_keeps_day_with_stray_1459_bar():
    """A real day keeps its one flat bar at 14:59 and stays a 2-bar day."""
    real_day = [
        {"symbol": "000001.SZ", "trade_date": "202609070930", "open": 10.0,
         "high": 10.1, "low": 9.9, "close": 10.05, "volume": 100.0},
        {"symbol": "000001.SZ", "trade_date": "202609071459", "open": 10.05,
         "high": 10.05, "low": 10.05, "close": 10.05, "volume": 0.0},
    ]
    filler_day = [
        {"symbol": "000001.SZ", "trade_date": "202609080930", "open": 35.47,
         "high": 35.47, "low": 35.47, "close": 35.47, "volume": 0.0},
        {"symbol": "000001.SZ", "trade_date": "202609081459", "open": 35.47,
         "high": 35.47, "low": 35.47, "close": 35.47, "volume": 0.0},
    ]

    real, filler_days = split_real_days(real_day + filler_day)

    assert filler_days == ["20260908"]
    # Both of the real day's bars survive, the stray 14:59 flat bar included.
    assert [row["trade_date"] for row in real] == ["202609070930", "202609071459"]
    assert split_real_days([]) == ([], [])


def test_split_real_days_is_per_symbol_not_per_day():
    """The live sink hands one whole poll batch to a single call.

    Grouping by day alone would let one symbol's all-filler day through as soon
    as *any other* symbol in the batch traded on that day.
    """
    traded = {"symbol": "000001.SZ", "trade_date": "202609080930", "open": 10.0,
              "high": 10.1, "low": 9.9, "close": 10.05, "volume": 100.0}
    filler = {"symbol": "600961.SH", "trade_date": "202609080930", "open": 35.47,
              "high": 35.47, "low": 35.47, "close": 35.47, "volume": 0.0}

    real, filler_days = split_real_days([traded, filler])

    assert [row["symbol"] for row in real] == ["000001.SZ"]
    assert filler_days == ["20260908"]


def test_backfill_never_writes_filler_over_existing_real_bars(capsys):
    """Regression for the 2026-09-19 corruption.

    QMT answers a cache miss with placeholder bars rather than nothing. Reading
    that non-empty reply as "has data" skipped the download *and* wrote filler
    over real rows, blanking 18 real trading days of 000426.SZ.
    """
    existing = [
        {"symbol": "000426.SZ", "trade_date": "202608130930", "open": 37.88,
         "high": 37.9, "low": 37.5, "close": 37.88, "volume": 7042.0},
    ]
    rpc = _FakeQuoteRpc(frames={"000426.SZ": _filler_frame("202608130930")})
    repo = _OverwritingRepo(existing)

    written = backfill_symbol_via_bigqmt(
        rpc, repo, "000426.SZ", start="20260813", end="20260813", chunk_days=0
    )

    # Filler counts as a cache miss, so the download still fires...
    assert len(rpc.downloads) == 1
    assert written == 0
    # ...and the real row is untouched: the filler price never reached the DB.
    assert repo.rows[("000426.SZ", "202608130930")]["close"] == 37.88
    assert repo.rows[("000426.SZ", "202608130930")]["volume"] == 7042.0
    captured = capsys.readouterr()
    assert "still incomplete after download" in captured.out
    assert "1 filler days" in captured.out


def test_backfill_writes_real_data_after_filler_triggers_download():
    rpc = _FakeQuoteRpc(
        frames={"000426.SZ": _filler_frame("202608130930")},
        frames_after_download={"000426.SZ": _columnar_frame("202608130930", 40.07)},
    )
    repo = _OverwritingRepo()

    written = backfill_symbol_via_bigqmt(
        rpc, repo, "000426.SZ", start="20260813", end="20260813", chunk_days=0
    )

    assert len(rpc.downloads) == 1
    assert written == 1
    assert repo.rows[("000426.SZ", "202608130930")]["close"] == 40.07


def test_backfill_keeps_241_bar_real_day_intact():
    """Guards the day-level rule against being "simplified" to per-bar filtering.

    A real day carries one stray flat bar at 14:59 (bridge artifact). Filtering
    bars individually would leave 240 and silently break the `close` horizon,
    which consumers read as the day's last bar.
    """
    day = "20260907"
    stamps = _session_stamps(day)
    assert len(stamps) == 241
    prices = [10.0 + index / 1000 for index in range(len(stamps))]
    volumes = [100.0] * len(stamps)
    stray = stamps.index(f"{day}145900")
    volumes[stray] = 0.0

    rpc = _FakeQuoteRpc(
        frames={
            "000001.SZ": {
                "__bigqmt_type__": "DataFrame",
                "columns": ["stime", "open", "high", "low", "close", "volume"],
                "records": {
                    "stime": stamps,
                    "open": list(prices),
                    "high": list(prices),
                    "low": list(prices),
                    "close": list(prices),
                    "volume": volumes,
                },
            }
        }
    )
    repo = _OverwritingRepo()

    written = backfill_symbol_via_bigqmt(
        rpc, repo, "000001.SZ", start=day, end=day, chunk_days=0
    )

    stored = sorted(key[1] for key in repo.rows)
    assert written == 241
    assert len(stored) == 241
    # Stored keys are normalized to 12 chars (YYYYMMDDHHMM).
    # The stray 14:59 bar survives and the day still ends at 15:00.
    assert f"{day}1459" in stored
    assert repo.rows[("000001.SZ", f"{day}1459")]["volume"] == 0.0
    assert stored[-1] == f"{day}1500"
    # A real day must not trigger a download.
    assert rpc.downloads == []


def test_backfill_never_downgrades_real_rows_after_download():
    """Pass 2 must not let a worse re-read overwrite what pass 1 stored."""
    real_day, filler_day = "20260907", "20260908"
    filler_rows = _merged(
        _filler_frame(f"{real_day}093000"),
        _filler_frame(f"{real_day}093100"),
        _filler_frame(f"{filler_day}093000"),
        _filler_frame(f"{filler_day}093100"),
    )
    rpc = _FakeQuoteRpc(
        frames={
            "000001.SZ": _merged(
                _frame_bars(real_day, [10.0, 10.1], [100.0, 200.0]),
                _merged(_filler_frame(f"{filler_day}093000"), _filler_frame(f"{filler_day}093100")),
            )
        },
        # The download makes things worse: every day now reads back as filler.
        frames_after_download={"000001.SZ": filler_rows},
    )
    repo = _OverwritingRepo()

    written = backfill_symbol_via_bigqmt(
        rpc, repo, "000001.SZ", start=real_day, end=filler_day, chunk_days=1
    )

    assert len(rpc.downloads) == 1
    assert written == 2
    # The real day survived verbatim...
    assert repo.rows[("000001.SZ", f"{real_day}0930")]["close"] == 10.0
    assert repo.rows[("000001.SZ", f"{real_day}0930")]["volume"] == 100.0
    # ...and the filler day was never written.
    assert not [key for key in repo.rows if key[1].startswith(filler_day)]


def test_backfill_downloads_once_for_whole_range():
    real_day, filler_day = "20260907", "20260908"
    rpc = _FakeQuoteRpc(
        frames={
            "000001.SZ": _merged(
                _frame_bars(real_day, [10.0], [100.0]),
                _filler_frame(f"{filler_day}093000"),
            )
        },
        frames_after_download={
            "000001.SZ": _merged(
                _frame_bars(real_day, [10.0], [100.0]),
                _frame_bars(filler_day, [11.0], [300.0]),
            )
        },
    )
    repo = _OverwritingRepo()

    written = backfill_symbol_via_bigqmt(
        rpc, repo, "000001.SZ", start=real_day, end=filler_day, chunk_days=1
    )

    # One download for the whole span, not one per chunk.
    assert len(rpc.downloads) == 1
    _, kwargs = rpc.downloads[0]
    assert (kwargs["start_time"], kwargs["end_time"]) == (real_day, filler_day)
    assert written == 2
    assert repo.rows[("000001.SZ", f"{filler_day}0930")]["close"] == 11.0


def test_backfill_skips_download_when_cache_has_real_bars():
    frame = _frame_bars("20260908", [10.0, 10.1], [100.0, 200.0])
    rpc = _FakeQuoteRpc(frames={"000001.SZ": frame})
    repo = _OverwritingRepo()

    written = backfill_symbol_via_bigqmt(
        rpc, repo, "000001.SZ", start="20260908", end="20260908", chunk_days=0
    )

    assert written == 2
    assert rpc.downloads == []


def test_verify_minute_bars_flags_fully_filled_symbol():
    """A fully-filled symbol must not pass on bar_count/trading_days alone."""
    real = [
        {"symbol": "000426.SZ", "trade_date": "202609070930", "open": 10.0,
         "high": 10.1, "low": 9.9, "close": 10.05, "volume": 100.0},
    ]
    filler = [
        {"symbol": "600961.SH", "trade_date": f"2026090{day}0930", "open": 35.47,
         "high": 35.47, "low": 35.47, "close": 35.47, "volume": 0.0}
        for day in (7, 8)
    ]
    collection = _Collection(real + filler)

    report = verify_minute_bars(
        collection, ["000426.SZ", "600961.SH"], min_bars_per_symbol=1, min_trading_days=1
    )

    assert report.results[0].filler_days == 0
    assert report.results[0].meets_criteria(min_bars_per_symbol=1, min_trading_days=1)
    # Same bar_count and trading_days as the real symbol, yet it must fail.
    assert report.results[1].bar_count == 2
    assert report.results[1].trading_days == 2
    assert report.results[1].filler_days == 2
    assert not report.results[1].meets_criteria(min_bars_per_symbol=1, min_trading_days=1)
    assert report.gap_symbols == ("600961.SH",)


def test_verify_stray_flat_bar_is_not_a_filler_day():
    """The 14:59 bridge artifact must not condemn an otherwise real day."""
    collection = _Collection(
        [
            {"symbol": "000001.SZ", "trade_date": "202609070930", "open": 10.0,
             "high": 10.1, "low": 9.9, "close": 10.05, "volume": 100.0},
            {"symbol": "000001.SZ", "trade_date": "202609071459", "open": 10.05,
             "high": 10.05, "low": 10.05, "close": 10.05, "volume": 0.0},
        ]
    )
    report = verify_minute_bars(collection, ["000001.SZ"], min_bars_per_symbol=2)

    assert report.results[0].filler_days == 0
    assert report.results[0].meets_criteria(min_bars_per_symbol=2)
    assert report.passed


def test_verify_limit_up_day_is_not_a_filler_day():
    """Flat bars that traded are a 一字板, not filler — must stay a real day."""
    collection = _Collection(
        [
            {"symbol": "000001.SZ", "trade_date": "202609070930", "open": 11.0,
             "high": 11.0, "low": 11.0, "close": 11.0, "volume": 500.0},
            {"symbol": "000001.SZ", "trade_date": "202609070931", "open": 11.0,
             "high": 11.0, "low": 11.0, "close": 11.0, "volume": 300.0},
        ]
    )
    report = verify_minute_bars(collection, ["000001.SZ"], min_bars_per_symbol=2)

    assert report.results[0].filler_days == 0
    assert report.results[0].meets_criteria(min_bars_per_symbol=2)


def test_verify_zero_filled_symbol_has_no_filler_days():
    report = verify_minute_bars(_Collection([]), ["600961.SH"], min_bars_per_symbol=1)

    assert report.results[0].bar_count == 0
    assert report.results[0].filler_days == 0
    assert report.results[0].trading_days == 0
    assert report.empty_symbols == ("600961.SH",)
    assert report.gap_symbols == ("600961.SH",)


def test_verify_docs_without_ohlcv_are_not_flat():
    """Bare {symbol, trade_date} docs must not read as filler (null guard)."""
    collection = _Collection(
        [
            {"symbol": "000001.SZ", "trade_date": "202609070930"},
            {"symbol": "000001.SZ", "trade_date": "202609070931"},
        ]
    )
    report = verify_minute_bars(collection, ["000001.SZ"], min_bars_per_symbol=2)

    assert report.results[0].filler_days == 0
    assert report.results[0].meets_criteria(min_bars_per_symbol=2)


def test_print_verify_report_includes_filler_days_column(capsys):
    collection = _Collection(
        [
            {"symbol": "600961.SH", "trade_date": "202609070930", "open": 35.47,
             "high": 35.47, "low": 35.47, "close": 35.47, "volume": 0.0},
            {"symbol": "600961.SH", "trade_date": "202609070931", "open": 35.47,
             "high": 35.47, "low": 35.47, "close": 35.47, "volume": 0.0},
        ]
    )
    report = verify_minute_bars(
        collection, ["600961.SH"], min_bars_per_symbol=2, min_trading_days=1
    )

    print_verify_report(report)
    lines = capsys.readouterr().out.splitlines()
    header = lines[0].split("\t")
    row = lines[1].split("\t")

    assert "filler_days" in header
    assert row[header.index("filler_days")] == "1"
    assert row[header.index("bars")] == "2"
    assert row[-1] == "False"


def test_print_verify_report_includes_missing_days_column(capsys):
    collection = _Collection(
        [
            {"symbol": "000001.SZ", "trade_date": "202609070930", "open": 10.0,
             "high": 10.1, "low": 9.9, "close": 10.05, "volume": 100.0},
        ]
    )
    report = verify_minute_bars(
        collection,
        ["000001.SZ"],
        min_bars_per_symbol=1,
        trading_days=("20260907", "20260908"),
    )

    print_verify_report(report)
    lines = capsys.readouterr().out.splitlines()
    header = lines[0].split("\t")
    row = lines[1].split("\t")

    assert "missing_days" in header
    assert row[header.index("missing_days")] == "1"
    assert row[-1] == "False"


class _TruncatingQuoteRpc(_FakeQuoteRpc):
    """Reproduces ``get_market_data_ex``'s anchored truncation.

    The real reply is not "every bar in the window" but one contiguous run
    anchored at ``end_time`` that **stops at the first day with no cached
    bars**. So a wide window can drop real days at the front entirely — and
    with them the filler days that should have triggered a download.

    ``days`` and ``days_after_download`` are keyed by ``YYYYMMDD`` day; a
    download merges the latter in, standing in for a warmer terminal cache.
    """

    def __init__(self, days, days_after_download=None, **kwargs):
        super().__init__(**kwargs)
        self.days = dict(days)
        self.days_after = dict(days_after_download or {})

    def download_history_data(self, stock_code, **kwargs):
        self.downloads.append((stock_code, kwargs))
        self.days.update(self.days_after)
        return True

    def get_market_data_ex(self, **kwargs):
        self.reads.append(kwargs)
        symbol = kwargs["stock_list"][0]
        start = str(kwargs.get("start_time") or "")
        end = str(kwargs.get("end_time") or "")
        # Walk from end backwards; stop as soon as a day in range is missing.
        frames = []
        cursor = datetime.strptime(end, "%Y%m%d")
        start_dt = datetime.strptime(start, "%Y%m%d")
        while cursor >= start_dt:
            frame = self.days.get(cursor.strftime("%Y%m%d"))
            if frame is None:
                break
            frames.append(frame)
            cursor -= timedelta(days=1)
        if not frames:
            return {}
        return {symbol: _merged(*reversed(frames))}


def test_backfill_downloads_when_reply_truncated_before_a_gap():
    """The anchored truncation must still be detected as a cache miss.

    ``600150.SH`` in production: rows exist for 20260909/20260910 but not for
    20260908, so a wide read returns only the tail block — every returned bar
    is real, so a filler-only test sees nothing wrong, no download fires, and
    the stale days stay stale forever.
    """
    rpc = _TruncatingQuoteRpc(
        days={
            "20260907": _frame_bars("20260907", [10.0], [100.0]),
            "20260909": _frame_bars("20260909", [10.2], [120.0]),
            "20260910": _frame_bars("20260910", [10.3], [130.0]),
        },
        days_after_download={"20260908": _frame_bars("20260908", [10.15], [110.0])},
    )
    repo = _OverwritingRepo()

    written = backfill_symbol_via_bigqmt(
        rpc,
        repo,
        "600150.SH",
        start="20260907",
        end="20260910",
        chunk_days=0,
        trading_days=("20260907", "20260908", "20260909", "20260910"),
    )

    # Without the calendar: the reply holds only real bars, so nothing looks
    # like a cache miss, no download fires and 20260907/08 stay missing.
    assert len(rpc.downloads) == 1
    assert written == 4
    assert sorted(key[1][:8] for key in repo.rows) == [
        "20260907", "20260908", "20260909", "20260910",
    ]


def test_backfill_no_download_when_calendar_coverage_is_complete():
    days = {
        "20260907": _frame_bars("20260907", [10.0], [100.0]),
        "20260908": _frame_bars("20260908", [10.1], [110.0]),
    }
    rpc = _TruncatingQuoteRpc(days)
    repo = _OverwritingRepo()

    written = backfill_symbol_via_bigqmt(
        rpc,
        repo,
        "600150.SH",
        start="20260907",
        end="20260908",
        chunk_days=0,
        trading_days=("20260907", "20260908"),
    )

    assert written == 2
    assert rpc.downloads == []


def test_backfill_rereads_unflagged_chunks_after_download():
    """A partially-filled day outside ``need_download`` must still be fixed.

    Pass 1 reads a day as "real" whenever *any* of its bars is real, so a day
    that is half real and half placeholder gets its placeholder values written.
    The download is issued for the whole range, but if pass 2 only re-reads the
    flagged chunks that day is never revisited — ``600150.SH 20260907`` kept a
    stale ``close`` of 38.30 instead of 38.38.
    """
    real_day, filler_day = "20260907", "20260908"
    rpc = _FakeQuoteRpc(
        frames={
            "000001.SZ": _merged(
                _frame_bars(real_day, [10.0, 99.0], [100.0, 0.0]),
                _merged(
                    _filler_frame(f"{filler_day}093000"),
                    _filler_frame(f"{filler_day}093100"),
                ),
            )
        },
        frames_after_download={
            "000001.SZ": _merged(
                _frame_bars(real_day, [10.0, 11.5], [100.0, 200.0]),
                _frame_bars(filler_day, [12.0, 12.5], [300.0, 400.0]),
            )
        },
    )
    repo = _OverwritingRepo()

    backfill_symbol_via_bigqmt(
        rpc, repo, "000001.SZ", start=real_day, end=filler_day, chunk_days=1
    )

    # The last bar of the unflagged day was a placeholder in pass 1...
    assert repo.rows[("000001.SZ", f"{real_day}0931")]["close"] == 11.5
    # ...and pass 2 re-read *both* chunks, not just the flagged one.
    assert len(rpc.reads) == 4


def test_backfill_force_download_skips_read_pass_and_never_writes_filler():
    real_day, filler_day = "20260907", "20260908"

    class _Tracked(_FakeQuoteRpc):
        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self.events = []

        def get_market_data_ex(self, **kwargs):
            self.events.append("read")
            return super().get_market_data_ex(**kwargs)

        def download_history_data(self, stock_code, **kwargs):
            self.events.append("download")
            return super().download_history_data(stock_code, **kwargs)

    rpc = _Tracked(
        frames={
            "000001.SZ": _merged(
                _frame_bars(real_day, [10.0], [100.0]),
                _filler_frame(f"{filler_day}093000"),
            )
        }
    )
    repo = _OverwritingRepo()

    backfill_symbol_via_bigqmt(
        rpc,
        repo,
        "000001.SZ",
        start=real_day,
        end=filler_day,
        chunk_days=0,
        force_download=True,
    )

    # No read before the download: the probe pass is skipped entirely.
    assert rpc.events[0] == "download"
    assert len(rpc.downloads) == 1
    # The day-level filler rule still holds under --redownload.
    assert repo.rows[("000001.SZ", f"{real_day}0930")]["close"] == 10.0
    assert not [key for key in repo.rows if key[1].startswith(filler_day)]


def test_verify_missing_days_needs_a_calendar():
    collection = _Collection(
        [
            {"symbol": "000001.SZ", "trade_date": "202609070930", "open": 10.0,
             "high": 10.1, "low": 9.9, "close": 10.05, "volume": 100.0},
            {"symbol": "000001.SZ", "trade_date": "202609070931", "open": 10.05,
             "high": 10.2, "low": 10.0, "close": 10.1, "volume": 100.0},
        ]
    )
    calendar = ("20260907", "20260908", "20260909")

    without = verify_minute_bars(collection, ["000001.SZ"], min_bars_per_symbol=1)
    assert without.results[0].missing_days == 0
    assert without.passed

    with_calendar = verify_minute_bars(
        collection, ["000001.SZ"], min_bars_per_symbol=1, trading_days=calendar
    )
    # 2 bars over 1 day is still 2 bars: only the calendar can see the other two.
    assert with_calendar.results[0].bar_count == 2
    assert with_calendar.results[0].trading_days == 1
    assert with_calendar.results[0].missing_days == 2
    assert not with_calendar.passed
    assert with_calendar.gap_symbols == ("000001.SZ",)


def test_verify_missing_days_for_symbol_with_no_bars():
    report = verify_minute_bars(
        _Collection([]),
        ["600961.SH"],
        min_bars_per_symbol=1,
        trading_days=("20260907", "20260908"),
    )

    assert report.results[0].missing_days == 2
    assert not report.results[0].meets_criteria(min_bars_per_symbol=1)


def test_iter_date_chunks_splits_and_zero_means_whole_range():
    assert iter_date_chunks("20260901", "20260908", 5) == [
        ("20260901", "20260905"),
        ("20260906", "20260908"),
    ]
    assert iter_date_chunks("20260901", "20260908", 0) == [("20260901", "20260908")]


def test_filter_session_chunks_skips_weekend_only_days():
    chunks = iter_date_chunks("20260724", "20260727", 1)
    assert filter_session_chunks(chunks) == [
        ("20260724", "20260724"),
        ("20260727", "20260727"),
    ]


def test_backfill_skips_weekend_without_rpc():
    rpc = _FakeQuoteRpc(frames={})
    repo = _FakeRepo()
    written = backfill_symbol_via_bigqmt(
        rpc,
        repo,
        "000001.SZ",
        start="20260725",
        end="20260726",
        chunk_days=1,
    )
    assert written == 0
    assert rpc.reads == []
    assert rpc.downloads == []


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


def test_resolve_quote_rpc_ignores_live_bridge_timeout(tmp_path, monkeypatch):
    monkeypatch.delenv("QUOTE_RPC_ADDRESS", raising=False)
    monkeypatch.delenv("QUOTE_ACCOUNT_ID", raising=False)
    config = tmp_path / "watchlist_minute.json"
    config.write_text(
        '{"tick_source": "bigqmt_rpc", "quote_account_id": "62129066", '
        '"quote_rpc_timeout_sec": 15, "universe": {"mode": "targeted"}}',
        encoding="utf-8",
    )
    with patch(
        "quant_data_engine.tasks.watchlist_minute_bigqmt.default_watchlist_config_path",
        return_value=None,
    ):
        _, account_id, timeout = resolve_quote_rpc(config_path=str(config))
    assert account_id == "62129066"
    assert timeout == 60.0
    _, _, timeout_cli = resolve_quote_rpc(
        quote_account_id="62129066",
        config_path=str(config),
        rpc_timeout_seconds=90,
    )
    assert timeout_cli == 90.0


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
    assert rpc.reads[0]["fill_data"] is False


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


def test_backfill_symbol_timeout_is_cache_miss_then_download():
    class TimeoutThenData(_FakeQuoteRpc):
        def get_market_data_ex(self, **kwargs):
            self.reads.append(kwargs)
            if not self.downloads:
                raise RpcTransportTimeout("zmq rpc timeout: get_market_data_ex")
            symbol = kwargs["stock_list"][0]
            return {symbol: self.after[symbol]}

    rpc = TimeoutThenData(
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
    )
    assert written == 1
    assert len(rpc.downloads) == 1
    assert repo.records[0]["close"] == 11.5


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

