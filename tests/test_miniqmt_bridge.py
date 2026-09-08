from argparse import Namespace
from dataclasses import dataclass
from datetime import datetime
from types import SimpleNamespace
from typing import Optional
from unittest.mock import MagicMock
from zoneinfo import ZoneInfo

import pytest

from quant_data_engine.bridges.miniqmt.config import BridgeConfig, UniverseConfig
from quant_data_engine.bridges.miniqmt.runner import BridgeRunner, in_cn_stock_session
from quant_data_engine.bridges.miniqmt.sinks import RealtimeDailyKSink, tick_to_realtime_daily_record
from quant_data_engine.bridges.miniqmt.source import MiniQMTTickSource, SymbolDelta
from quant_data_engine.bridges.miniqmt.universe import SymbolUniverse


class _Collection:
    def __init__(self, docs):
        self.docs = docs

    def find(self, query=None, projection=None):
        query = query or {}
        rows = list(self.docs)
        if "enabled" in query:
            rows = [row for row in rows if row.get("enabled") == query["enabled"]]
        if "status" in query and "$in" in query["status"]:
            allowed = set(query["status"]["$in"])
            rows = [row for row in rows if row.get("status") in allowed]
        if "plan_id" in query and "$in" in query["plan_id"]:
            allowed = set(query["plan_id"]["$in"])
            rows = [row for row in rows if row.get("plan_id") in allowed]
        if projection:
            include = {key for key, enabled in projection.items() if enabled}
            return [{key: value for key, value in row.items() if key in include} for row in rows]
        return rows


class _Db:
    def __init__(self, mapping):
        self.mapping = mapping

    def __getitem__(self, name):
        return self.mapping[name]


class _XtCalendar:
    def __init__(self, dates):
        self.dates = dates

    def get_trading_dates(self, market, start, end):
        return self.dates


def test_bridge_config_loads_json_and_cli_overrides(tmp_path):
    config_path = tmp_path / "miniqmt_bridge.json"
    config_path.write_text(
        """
        {
          "mode": "loop",
          "sync_uri": "mongodb://sync",
          "daily_k_interval": 9,
          "universe": {
            "auto": true,
            "include_watchlist_strategies": false,
            "plan_statuses": ["approved"]
          }
        }
        """,
        encoding="utf-8",
    )

    cfg = BridgeConfig.from_json(config_path).merge_cli(
        Namespace(
            once=True,
            loop=False,
            sync_uri=None,
            mongo_uri=None,
            sinks=None,
            poll_interval=0.25,
            daily_k_interval=7,
            refresh=None,
            ignore_session=None,
            warmup_seconds=None,
            auto=None,
            symbols=None,
            include_watchlists=None,
            include_watchlist_strategies=True,
            include_portfolio_plans=None,
            plan_statuses=None,
        )
    )

    assert cfg.mode == "once"
    assert cfg.sync_uri == "mongodb://sync"
    assert cfg.poll_interval == 0.25
    assert cfg.daily_k_interval == 7
    assert cfg.universe.include_watchlist_strategies is True
    assert cfg.universe.plan_statuses == ("approved",)


@dataclass
class _OverlaySettings:
    mongo_uri: str
    job_status_sync_uri: Optional[str]
    market_insight_trigger_url: Optional[str]
    market_insight_trigger_key: Optional[str]
    market_insight_trigger_timeout_sec: float
    intraday_t0_trigger_url: Optional[str]
    intraday_t0_trigger_key: Optional[str]
    intraday_t0_trigger_timeout_sec: float


def test_bridge_config_loads_trigger_urls_and_overlays_settings(tmp_path):
    config_path = tmp_path / "miniqmt_bridge.json"
    config_path.write_text(
        """
        {
          "universe": {"mode": "full_market"},
          "market_insight_trigger_url": "http://127.0.0.1:3001/api/market-insights/generate-internal",
          "market_insight_trigger_key": "json-secret",
          "intraday_t0_trigger_url": "http://127.0.0.1:3001/api/intraday-t0/signals/generate-all"
        }
        """,
        encoding="utf-8",
    )
    cfg = BridgeConfig.from_json(config_path)
    assert cfg.market_insight_trigger_url.endswith("/generate-internal")
    assert cfg.market_insight_trigger_key == "json-secret"
    assert cfg.intraday_t0_trigger_url.endswith("/generate-all")

    settings = _OverlaySettings(
        mongo_uri="mongodb://env",
        job_status_sync_uri=None,
        market_insight_trigger_url="http://quant-api:3001/api/market-insights/generate-internal",
        market_insight_trigger_key="env-secret",
        market_insight_trigger_timeout_sec=5.0,
        intraday_t0_trigger_url=None,
        intraday_t0_trigger_key="env-t0",
        intraday_t0_trigger_timeout_sec=5.0,
    )
    overlaid = cfg.overlay_settings(settings)
    assert overlaid.mongo_uri == "mongodb://env"
    assert overlaid.market_insight_trigger_url.endswith("127.0.0.1:3001/api/market-insights/generate-internal")
    assert overlaid.market_insight_trigger_key == "json-secret"
    assert overlaid.intraday_t0_trigger_url.endswith("/generate-all")
    assert overlaid.intraday_t0_trigger_key == "env-t0"


def test_bridge_config_accepts_targeted_minute_sink():
    cfg = BridgeConfig.from_mapping({"sinks": ["minute-bars"]})
    assert cfg.sinks == ("minute-bars",)
    assert cfg.universe.is_full_market is False


def test_bridge_config_rejects_minute_sink_on_full_market():
    with pytest.raises(ValueError, match="minute-bars sink requires universe.mode=targeted"):
        BridgeConfig.from_mapping(
            {"sinks": ["minute-bars"], "universe": {"mode": "full_market"}}
        )


def test_bridge_config_rejects_qmt_1m_with_daily_k():
    with pytest.raises(ValueError, match="cannot share a process with realtime-daily-k"):
        BridgeConfig.from_mapping(
            {
                "sinks": ["minute-bars", "realtime-daily-k"],
                "tick_source": "bigqmt_rpc",
                "minute_bar_source": "qmt_1m",
                "quote_account_id": "62129066",
            }
        )


def test_tick_to_realtime_daily_record_maps_full_tick_fields():
    tick = {
        "lastPrice": 10.5,
        "lastClose": 10.0,
        "open": 10.1,
        "high": 10.8,
        "low": 9.9,
        "volume": 12345,
        "amount": 6789000.0,
        "transactionNum": 321,
        "askPrice": [10.51],
        "askVol": [200],
        "bidPrice": [10.49],
        "bidVol": [180],
        "time": 1781842200000,
    }

    rec = tick_to_realtime_daily_record(
        "000001.SZ",
        tick,
        "20260619",
        "202606191030",
        universe_type="watchlist_plan_strategy",
    )

    assert rec is not None
    assert rec["source"] == "miniqmt"
    assert rec["source_mode"] == "miniqmt_targeted_daily"
    assert rec["universe_type"] == "watchlist_plan_strategy"
    assert rec["open"] == 10.1
    assert rec["high"] == 10.8
    assert rec["low"] == 9.9
    assert rec["close"] == 10.5
    assert rec["pre_close"] == 10.0
    assert rec["change"] == 0.5
    assert rec["pct_chg"] == 5.0
    assert rec["volume"] == 12345
    assert rec["amount"] == 6789.0
    assert rec["realtime_amount_yuan"] == 6789000.0
    assert rec["realtime_ask_price1"] == 10.51
    assert rec["realtime_bid_volume1"] == 180


def test_tick_to_realtime_daily_record_rejects_missing_price():
    assert (
        tick_to_realtime_daily_record(
            "000001.SZ",
            {"volume": 1},
            "20260619",
            "202606191030",
            universe_type="watchlist_plan_strategy",
        )
        is None
    )


def test_symbol_universe_merges_sources_and_strategy_default():
    finance_db = _Db(
        {
            "user_watchlists": _Collection(
                [
                    {"symbols": ["000001", "600000.SH", "bad"]},
                    {"symbols": ["300750"]},
                ]
            ),
            "watchlist_strategies": _Collection(
                [
                    {"symbol": "688001", "enabled": True},
                    {"symbol": "830799", "enabled": False},
                ]
            ),
        }
    )
    trading_db = _Db(
        {
            "portfolio_trade_plans": _Collection(
                [
                    {"plan_id": "p1", "status": "approved"},
                    {"plan_id": "p2", "status": "generated"},
                    {"plan_id": "p3", "status": "executed_paper"},
                ]
            ),
            "portfolio_trade_plan_items": _Collection(
                [
                    {"plan_id": "p1", "symbol": "002050"},
                    {"plan_id": "p2", "symbol": "830799.BJ"},
                    {"plan_id": "p3", "symbol": "600000"},
                ]
            ),
        }
    )

    universe = SymbolUniverse(finance_db=finance_db, trading_db=trading_db, config=UniverseConfig())

    assert universe.resolve() == ["000001.SZ", "002050.SZ", "300750.SZ", "600000.SH", "688001.SH", "830799.BJ"]
    assert universe.universe_type == "watchlist_plan_strategy"


def test_symbol_universe_includes_latest_paper_positions():
    finance_db = _Db({"user_watchlists": _Collection([]), "watchlist_strategies": _Collection([])})
    trading_db = _Db(
        {
            "portfolio_trade_plans": _Collection([]),
            "portfolio_trade_plan_items": _Collection([]),
            "portfolio_paper_positions": _Collection(
                [
                    {
                        "plan_id": "plan-a",
                        "as_of_date": "20260101",
                        "positions": [{"symbol": "000001", "shares": 100}],
                    },
                    # Newer snapshot for plan-a supersedes the older one above.
                    {
                        "plan_id": "plan-a",
                        "as_of_date": "20260103",
                        "positions": [
                            {"symbol": "600000.SH", "shares": 200},
                            {"symbol": "300750", "shares": 0},
                        ],
                    },
                    {
                        "plan_id": "plan-b",
                        "as_of_date": "20260102",
                        "positions": [{"symbol": "688001"}],
                    },
                ]
            ),
        }
    )

    config = UniverseConfig(
        include_watchlists=False,
        include_watchlist_strategies=False,
        include_portfolio_plans=False,
        include_paper_positions=True,
    )
    universe = SymbolUniverse(finance_db=finance_db, trading_db=trading_db, config=config)

    # 000001 dropped (older plan-a snapshot), 300750 dropped (zero shares),
    # 688001 kept (no shares field => assumed held).
    assert universe.resolve() == ["600000.SH", "688001.SH"]


def test_symbol_universe_can_disable_paper_positions():
    finance_db = _Db({"user_watchlists": _Collection([]), "watchlist_strategies": _Collection([])})
    trading_db = _Db(
        {
            "portfolio_trade_plans": _Collection([]),
            "portfolio_trade_plan_items": _Collection([]),
            "portfolio_paper_positions": _Collection(
                [{"plan_id": "plan-a", "as_of_date": "20260103", "positions": [{"symbol": "600000.SH", "shares": 200}]}]
            ),
        }
    )

    config = UniverseConfig(
        include_watchlists=False,
        include_watchlist_strategies=False,
        include_portfolio_plans=False,
        include_paper_positions=False,
    )
    universe = SymbolUniverse(finance_db=finance_db, trading_db=trading_db, config=config)

    assert universe.resolve() == []


def test_symbol_universe_can_disable_strategy_tag_and_symbols():
    finance_db = _Db(
        {
            "user_watchlists": _Collection([{"symbols": ["000001"]}]),
            "watchlist_strategies": _Collection([{"symbol": "688001", "enabled": True}]),
        }
    )
    trading_db = _Db({"portfolio_trade_plans": _Collection([]), "portfolio_trade_plan_items": _Collection([])})
    config = UniverseConfig(include_watchlist_strategies=False)

    universe = SymbolUniverse(finance_db=finance_db, trading_db=trading_db, config=config)

    assert universe.resolve() == ["000001.SZ"]
    assert universe.universe_type == "watchlist_plan"


class _FakeXtData:
    def __init__(self):
        self.subscriptions = []
        self.unsubscriptions = []
        self.ticks = {"000001.SZ": {"lastPrice": 1}, "600000.SH": {"lastPrice": 2}}
        self._next_seq = 100
        self.tick_requests = []

    def subscribe_whole_quote(self, symbols):
        self.subscriptions.append(tuple(symbols))
        self._next_seq += 1
        return self._next_seq

    def get_full_tick(self, symbols):
        self.tick_requests.append(tuple(symbols))
        return {symbol: self.ticks[symbol] for symbol in symbols if symbol in self.ticks}

    def unsubscribe_quote(self, seq):
        self.unsubscriptions.append(seq)


def test_miniqmt_tick_source_subscribes_polls_updates_and_stops():
    xtdata = _FakeXtData()
    source = MiniQMTTickSource(["000001.SZ"], xtdata_module=xtdata, warmup_seconds=0, sleep_func=lambda _: None)

    source.start()
    ticks = source.poll()
    delta = source.update_symbols(["600000.SH"])
    source.stop()

    assert xtdata.subscriptions == [("000001.SZ",), ("600000.SH",)]
    assert ticks == {"000001.SZ": {"lastPrice": 1}}
    assert delta.added == ("600000.SH",)
    assert delta.removed == ("000001.SZ",)
    assert xtdata.unsubscriptions == [101, 102]


def test_realtime_daily_k_sink_writes_daily_and_latest():
    repo = MagicMock()
    repo.upsert_daily.return_value = 1
    repo.upsert_realtime_latest.return_value = 1
    sink = RealtimeDailyKSink(
        repo=repo,
        write_interval=5,
        universe_type="watchlist_plan_strategy",
        record_runs=False,
    )
    now = datetime(2026, 6, 19, 10, 30, tzinfo=ZoneInfo("Asia/Shanghai"))

    result = sink.process(
        ticks={"000001.SZ": {"lastPrice": 10.5, "lastClose": 10.0, "volume": 1}},
        symbols=["000001.SZ"],
        now=now,
        force=True,
    )

    assert result is not None
    assert result.synced_symbols == 1
    repo.upsert_daily.assert_called_once()
    repo.upsert_realtime_latest.assert_called_once()


class _FakeUniverse:
    def __init__(self, symbols):
        self.symbols = symbols
        self.universe_type = "watchlist_plan_strategy"

    def resolve(self):
        return list(self.symbols)

    def refresh(self):
        return list(self.symbols)


class _FakeSource:
    xtdata = None

    def __init__(self):
        self.started = False
        self.stopped = False
        self.symbols = []
        self.poll_count = 0

    def update_symbols(self, symbols):
        old = set(self.symbols)
        new = set(symbols)
        self.symbols = list(symbols)
        return SymbolDelta(added=tuple(sorted(new - old)), removed=tuple(sorted(old - new)))

    def start(self):
        self.started = True

    def poll(self):
        self.poll_count += 1
        return {"000001.SZ": {"lastPrice": 10.5, "lastClose": 10.0, "volume": 1}}

    def stop(self):
        self.stopped = True


def test_bridge_runner_once_dispatches_sink_and_stops_source():
    source = _FakeSource()
    sink = MagicMock()
    sink.process.return_value = SimpleNamespace(
        expected_symbols=1,
        synced_symbols=1,
        daily_changed=1,
        latest_changed=1,
    )
    cfg = BridgeConfig(mode="once", ignore_session=True, universe=UniverseConfig(auto=True))
    now = datetime(2026, 6, 19, 10, 30, tzinfo=ZoneInfo("Asia/Shanghai"))

    runner = BridgeRunner(
        config=cfg,
        universe=_FakeUniverse(["000001.SZ"]),
        source=source,
        sinks=[sink],
        clock=lambda: now,
        sleep_func=lambda _: None,
        install_signal_handlers=False,
    )
    runner.run()

    assert source.started is True
    assert source.poll_count == 1
    assert source.stopped is True
    sink.process.assert_called_once()


def test_bridge_runner_once_records_skip_outside_session():
    source = _FakeSource()
    sink = MagicMock()
    cfg = BridgeConfig(mode="once", ignore_session=False, universe=UniverseConfig(auto=True))
    # Sunday -> not a trading day, so session guard triggers.
    now = datetime(2026, 6, 21, 10, 30, tzinfo=ZoneInfo("Asia/Shanghai"))

    runner = BridgeRunner(
        config=cfg,
        universe=_FakeUniverse(["000001.SZ"]),
        source=source,
        sinks=[sink],
        clock=lambda: now,
        sleep_func=lambda _: None,
        install_signal_handlers=False,
    )
    runner.run()

    sink.process.assert_not_called()
    sink.record_skipped.assert_called_once()
    assert sink.record_skipped.call_args.kwargs["reason"] == "non-trading day"
    assert source.stopped is True


def test_in_cn_stock_session_uses_xtdata_calendar():
    tz = ZoneInfo("Asia/Shanghai")
    session_dt = datetime(2026, 6, 19, 10, 0, tzinfo=tz)

    assert in_cn_stock_session(session_dt, xtdata_module=_XtCalendar(["20260619"])) is True
    assert in_cn_stock_session(session_dt, xtdata_module=_XtCalendar([])) is False


def _cli(**overrides):
    base = dict(
        once=False,
        loop=False,
        sync_uri=None,
        mongo_uri=None,
        sinks=None,
        poll_interval=None,
        daily_k_interval=None,
        refresh=None,
        ignore_session=None,
        warmup_seconds=None,
        auto=None,
        symbols=None,
        include_watchlists=None,
        include_watchlist_strategies=None,
        include_portfolio_plans=None,
        include_paper_positions=None,
        plan_statuses=None,
        universe_mode=None,
        poll_batch_size=None,
    )
    base.update(overrides)
    return Namespace(**base)


def test_full_market_cli_applies_one_minute_defaults():
    cfg = BridgeConfig().merge_cli(_cli(loop=True, universe_mode="full_market"))

    assert cfg.universe.mode == "full_market"
    assert cfg.universe.is_full_market is True
    assert cfg.poll_interval == 2.0
    assert cfg.daily_k_interval == 60.0
    assert cfg.refresh == 3600
    assert cfg.poll_batch_size == 400
    assert cfg.warmup_seconds == 3.0


def test_full_market_cli_keeps_explicit_faster_interval():
    cfg = BridgeConfig().merge_cli(_cli(universe_mode="full_market", daily_k_interval=30, poll_interval=1))

    assert cfg.daily_k_interval == 30
    assert cfg.poll_interval == 1


def test_full_market_json_defaults_when_intervals_omitted(tmp_path):
    config_path = tmp_path / "miniqmt_bridge.json"
    config_path.write_text('{"universe": {"mode": "full_market"}}', encoding="utf-8")

    cfg = BridgeConfig.from_json(config_path)
    assert cfg.daily_k_interval == 60.0
    assert cfg.poll_batch_size == 400
    assert cfg.refresh == 3600


def test_symbol_universe_full_market_uses_stock_info():
    data_db = _Db({"stock_info": _Collection([{"ts_code": "000001.SZ"}, {"symbol": "600000.SH"}])})
    universe = SymbolUniverse(
        finance_db=_Db({}),
        trading_db=_Db({}),
        data_db=data_db,
        config=UniverseConfig(mode="full_market"),
    )

    assert universe.resolve() == ["000001.SZ", "600000.SH"]
    assert universe.universe_type == "full_market_a_share"


def test_symbol_universe_full_market_falls_back_to_xtdata_sectors():
    class _Xt:
        def get_stock_list_in_sector(self, sector):
            if sector == "沪深A股":
                return ["000001.SZ", "600000.SH"]
            if sector == "北证A股":
                return ["830799.BJ"]
            return []

    universe = SymbolUniverse(
        finance_db=_Db({}),
        trading_db=_Db({}),
        data_db=_Db({"stock_info": _Collection([])}),
        config=UniverseConfig(mode="full_market"),
        xtdata_module=_Xt(),
    )

    assert universe.resolve() == ["000001.SZ", "600000.SH", "830799.BJ"]


def test_miniqmt_tick_source_polls_in_batches():
    xtdata = _FakeXtData()
    source = MiniQMTTickSource(
        ["000001.SZ", "600000.SH"],
        xtdata_module=xtdata,
        warmup_seconds=0,
        poll_batch_size=1,
        sleep_func=lambda _: None,
    )

    source.start()
    ticks = source.poll()

    assert ticks == {"000001.SZ": {"lastPrice": 1}, "600000.SH": {"lastPrice": 2}}
    assert xtdata.tick_requests == [("000001.SZ",), ("600000.SH",)]


def test_realtime_daily_k_sink_full_market_calls_postprocess():
    repo = MagicMock()
    post = SimpleNamespace(daily_changed=7, latest_changed=7)
    postprocess = MagicMock(return_value=post)
    settings = SimpleNamespace(mongo_uri="mongodb://localhost", data_db="quant_data", realtime_db="quant_realtime")
    sink = RealtimeDailyKSink(
        repo=repo,
        settings=settings,
        write_interval=60,
        universe_type="full_market_a_share",
        source_mode="miniqmt_full_market_daily",
        task_type="a_daily_realtime_sync",
        enable_derivatives=True,
        record_runs=False,
        postprocess_func=postprocess,
    )
    now = datetime(2026, 6, 19, 10, 30, tzinfo=ZoneInfo("Asia/Shanghai"))

    result = sink.process(
        ticks={"000001.SZ": {"lastPrice": 10.5, "lastClose": 10.0, "volume": 1}},
        symbols=["000001.SZ"],
        now=now,
        force=True,
    )

    assert result is not None
    assert result.daily_changed == 7
    repo.upsert_daily.assert_not_called()
    postprocess.assert_called_once()
    assert postprocess.call_args.kwargs["task_type"] == "a_daily_realtime_sync"
    assert postprocess.call_args.kwargs["source_mode"] == "miniqmt_full_market_daily"
    assert postprocess.call_args.kwargs["records"][0]["source_mode"] == "miniqmt_full_market_daily"

