from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import mongomock
import pytest

from quant_data_engine.auction.universe import load_first_board_pool, normalize_symbol
from quant_data_engine.tasks.run_auction_live_capture import (
    AuctionLiveCapture,
    load_index_prev_close,
    load_prev_close,
)

CN_TZ = ZoneInfo("Asia/Shanghai")


def _ms(hour: int, minute: int, second: int) -> int:
    return int(datetime(2026, 11, 4, hour, minute, second, tzinfo=CN_TZ).timestamp() * 1000)


def _tick(hour: int, minute: int, second: int, price: float, volume: float, amount: float) -> dict:
    return {
        "time": _ms(hour, minute, second),
        "lastPrice": price,
        "volume": volume,
        "amount": amount,
        "bidPrice": [price - 0.01] if price else [10.0],
        "askPrice": [price + 0.01] if price else [10.02],
        "bidVol": [10],
        "askVol": [10],
        "stockStatus": 2,
    }


class FakeClock:
    def __init__(self, now: datetime) -> None:
        self.now = now

    def __call__(self) -> datetime:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += timedelta(seconds=seconds)


class FakeRpc:
    def __init__(self, ticks_by_poll: list[dict]) -> None:
        self.ticks_by_poll = ticks_by_poll
        self.calls = 0
        self.closed = False

    def get_ticks(self, codes, timeout_seconds: float = 15.0) -> dict:
        index = min(self.calls, len(self.ticks_by_poll) - 1)
        self.calls += 1
        return self.ticks_by_poll[index]

    def close(self) -> None:
        self.closed = True


def test_normalize_symbol():
    assert normalize_symbol("600114") == "600114.SH"
    assert normalize_symbol("000001") == "000001.SZ"
    assert normalize_symbol("430047") == "430047.BJ"
    assert normalize_symbol("600114.SH") == "600114.SH"


def test_load_first_board_pool_excludes_prev_day_limit_up():
    db = mongomock.MongoClient().quant_data
    db.daily_limit_up.insert_many([
        {"date": "20261103", "symbol": "600001.SH", "status": "limit_up", "streak": 1},
        {"date": "20261103", "symbol": "600002.SH", "status": "limit_up", "streak": 2},
        {"date": "20261103", "symbol": "600003.SH", "status": "limit_up", "streak": 1},
        {"date": "20261102", "symbol": "600003.SH", "status": "limit_up", "streak": 1},
    ])
    assert load_first_board_pool(db, "20261104") == ["600001.SH"]


def test_load_first_board_pool_falls_back_to_all_limit_up():
    db = mongomock.MongoClient().quant_data
    db.daily_limit_up.insert_many([
        {"date": "20261103", "symbol": "600001.SH", "status": "limit_up", "streak": 2},
        {"date": "20261103", "symbol": "600002.SH", "status": "limit_up", "streak": 3},
    ])
    assert load_first_board_pool(db, "20261104") == ["600001.SH", "600002.SH"]


def test_load_prev_close_prefers_today_pre_close_then_prior_close():
    db = mongomock.MongoClient().quant_data
    db.volume_price.insert_many([
        {"symbol": "600114.SH", "trade_date": "20261104", "pre_close": 9.5, "close": 9.6},
        {"symbol": "000001.SZ", "trade_date": "20261103", "close": 11.0},
    ])
    assert load_prev_close(db, ["600114.SH", "000001.SZ"], "20261104") == {
        "600114.SH": 9.5,
        "000001.SZ": 11.0,
    }


def test_load_index_prev_close_uses_index_and_etf_collections():
    db = mongomock.MongoClient().quant_data
    db.index_prices.insert_one({"ts_code": "000300.SH", "trade_date": "20261103", "close": 4000.0})
    db.etf_daily.insert_one({"ts_code": "510300.SH", "trade_date": "20261103", "close": 4.0})
    assert load_index_prev_close(db, ["000300.SH", "510300.SH", "399303.SZ"], "20261104") == {
        "000300.SH": 4000.0,
        "510300.SH": 4.0,
    }


def test_capture_polls_writes_snapshots_features_and_run():
    db = mongomock.MongoClient().quant_data
    ticks = [
        {"600114.SH": _tick(9, 15, 3, 0.0, 100, 100_000)},
        {"600114.SH": _tick(9, 25, 1, 10.1, 300, 300_000)},
    ]
    clock = FakeClock(datetime(2026, 11, 4, 9, 15, 0, tzinfo=CN_TZ))
    capture = AuctionLiveCapture(
        db=db,
        rpc=FakeRpc(ticks),
        symbols=["600114.SH"],
        trade_date="20261104",
        prev_close={"600114.SH": 10.0},
        avg_1m_amount={},
        index_prev_close={},
        sector_info={"600114.SH": ("801010.SI", "农林牧渔")},
        start_dt=datetime(2026, 11, 4, 9, 15, 0, tzinfo=CN_TZ),
        end_dt=datetime(2026, 11, 4, 9, 15, 5, tzinfo=CN_TZ),
        partial_dt=datetime(2026, 11, 4, 9, 15, 3, tzinfo=CN_TZ),
        poll_interval=3.0,
        clock=clock,
        sleep_func=clock.advance,
    )
    capture.run()

    snaps = list(db.auction_snapshots.find({}, {"_id": 0}).sort("ts", 1))
    assert [row["ts"] for row in snaps] == ["20261104091503.000", "20261104092501.000"]
    assert snaps[0]["volume_delta"] == 100
    assert snaps[1]["volume_delta"] == 200

    feature = db.auction_features.find_one({"symbol": "600114.SH", "trade_date": "20261104"})
    assert feature is not None
    assert feature["source"] == "qmt_tick_live"
    assert feature["partial"] is False
    assert feature["is_auction_complete"] is True
    assert feature["open_price"] == pytest.approx(10.1)
    assert feature["gap_pct"] == pytest.approx(1.0)
    assert feature["sector_l1_code"] == "801010.SI"

    run = db.auction_capture_runs.find_one({"trade_date": "20261104", "source": "qmt_tick_live"})
    assert run is not None
    assert run["status"] == "completed"
    assert run["coverage_ratio"] == pytest.approx(1.0)
    assert run["match_coverage_ratio"] == pytest.approx(1.0)
    assert run["snapshot_count"] == 2


def test_capture_keeps_richer_match_row_when_stale_zero_repeats():
    """QMT repeats 09:25:01 with volume=0 after the match frame; keep the match."""
    db = mongomock.MongoClient().quant_data
    ticks = [
        {"600114.SH": _tick(9, 15, 3, 0.0, 100, 100_000)},
        {"600114.SH": _tick(9, 25, 1, 10.1, 510, 1_050_600)},
        {"600114.SH": _tick(9, 25, 1, 10.1, 0, 0)},
    ]
    clock = FakeClock(datetime(2026, 11, 4, 9, 15, 0, tzinfo=CN_TZ))
    capture = AuctionLiveCapture(
        db=db,
        rpc=FakeRpc(ticks),
        symbols=["600114.SH"],
        trade_date="20261104",
        prev_close={"600114.SH": 10.0},
        avg_1m_amount={},
        index_prev_close={},
        sector_info={},
        start_dt=datetime(2026, 11, 4, 9, 15, 0, tzinfo=CN_TZ),
        end_dt=datetime(2026, 11, 4, 9, 15, 8, tzinfo=CN_TZ),
        partial_dt=datetime(2026, 11, 4, 9, 15, 3, tzinfo=CN_TZ),
        poll_interval=3.0,
        clock=clock,
        sleep_func=clock.advance,
    )
    capture.run()

    feature = db.auction_features.find_one({"symbol": "600114.SH", "trade_date": "20261104"})
    assert feature["volume_match_0925"] == pytest.approx(410)
    assert feature["auction_volume"] == pytest.approx(510)
    assert feature["auction_amount"] == pytest.approx(1_050_600)
    snap = db.auction_snapshots.find_one({"symbol": "600114.SH", "ts": "20261104092501.000"})
    assert snap["volume"] == pytest.approx(510)
    assert snap["volume_delta"] == pytest.approx(410)
