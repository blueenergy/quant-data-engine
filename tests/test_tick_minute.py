"""Tests for watchlist tick → 1m aggregation and minute-bars sink."""
from datetime import datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from quant_data_engine.bridges.miniqmt.config import BridgeConfig
from quant_data_engine.bridges.miniqmt.constants import (
    SOURCE_MODE_BIGQMT_WATCHLIST_MINUTE,
    TASK_TYPE_WATCHLIST_MINUTE,
)
from quant_data_engine.bridges.miniqmt.factory import create_sinks, minute_source_mode
from quant_data_engine.bridges.miniqmt.tick_minute import MinuteBarsSink, TickMinuteAggregator

CN_TZ = ZoneInfo("Asia/Shanghai")


def _ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def _tick(price: float, volume: int, dt: datetime) -> dict:
    return {"lastPrice": price, "volume": volume, "time": _ms(dt)}


class _Repo:
    def __init__(self):
        self.batches = []

    def upsert_bar_records(self, records):
        self.batches.append(list(records))
        return len(records)


def test_aggregator_labels_bar_at_close_of_minute_and_deltas_volume():
    agg = TickMinuteAggregator()
    t0 = datetime(2026, 9, 7, 10, 30, 15, tzinfo=CN_TZ)
    t1 = datetime(2026, 9, 7, 10, 30, 40, tzinfo=CN_TZ)
    t2 = datetime(2026, 9, 7, 10, 31, 0, 1000, tzinfo=CN_TZ)
    symbols = ["000001.SZ"]

    closed = agg.ingest({"000001.SZ": _tick(10.0, 1000, t0)}, t0, symbols)
    assert closed == []
    forming = agg.snapshot_open()
    assert forming[0]["trade_date"] == "202609071031"
    assert forming[0]["volume"] == 0

    agg.ingest({"000001.SZ": _tick(10.5, 1100, t1)}, t1, symbols)
    forming = agg.snapshot_open()
    assert forming[0]["high"] == 10.5
    assert forming[0]["volume"] == 100

    closed = agg.ingest({"000001.SZ": _tick(10.6, 1200, t2)}, t2, symbols)
    assert len(closed) == 1
    bar = closed[0]
    assert bar["symbol"] == "000001.SZ"
    assert bar["trade_date"] == "202609071031"
    assert bar["open"] == 10.0
    assert bar["high"] == 10.5
    assert bar["low"] == 10.0
    assert bar["close"] == 10.5
    assert bar["volume"] == 100


def test_minute_sink_writes_forming_then_closed_on_minute_roll():
    repo = _Repo()
    sink = MinuteBarsSink(
        repo=repo,
        write_interval=5.0,
        universe_type="watchlist_plan_strategy",
        record_runs=False,
    )
    t0 = datetime(2026, 9, 7, 10, 30, 20, tzinfo=CN_TZ)
    t1 = datetime(2026, 9, 7, 10, 31, 0, 1000, tzinfo=CN_TZ)
    ticks0 = {"000001.SZ": _tick(10.0, 100, t0)}
    first = sink.process(ticks=ticks0, symbols=["000001.SZ"], now=t0)
    assert first is not None
    assert first.daily_changed == 1
    assert repo.batches[0][0]["trade_date"] == "202609071031"

    skipped = sink.process(ticks=ticks0, symbols=["000001.SZ"], now=t0)
    assert skipped is None

    ticks1 = {"000001.SZ": _tick(10.2, 150, t1)}
    rolled = sink.process(ticks=ticks1, symbols=["000001.SZ"], now=t1, force=True)
    assert rolled is not None
    trade_dates = {row["trade_date"] for row in repo.batches[-1]}
    assert "202609071031" in trade_dates


def test_create_sinks_builds_minute_bars_for_bigqmt_targeted():
    cfg = BridgeConfig.from_mapping(
        {
            "sinks": ["minute-bars"],
            "tick_source": "bigqmt_rpc",
            "quote_account_id": "62129066",
        }
    )
    universe = SimpleNamespace(universe_type="watchlist_plan_strategy")
    sinks = create_sinks(
        cfg,
        stock_repo=None,
        minute_repo=_Repo(),
        settings=None,
        universe=universe,
    )
    assert len(sinks) == 1
    assert isinstance(sinks[0], MinuteBarsSink)
    assert minute_source_mode(cfg) == SOURCE_MODE_BIGQMT_WATCHLIST_MINUTE
    assert sinks[0].task_type == TASK_TYPE_WATCHLIST_MINUTE
