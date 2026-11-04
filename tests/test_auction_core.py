from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from quant_data_engine.auction.core import (
    build_features,
    hhmmss,
    phase_of,
    ref_price,
    snapshot_from_tick,
    tick_ts_ms,
    ts_from_ms,
)

CN_TZ = ZoneInfo("Asia/Shanghai")


def _ms(hour: int, minute: int, second: int, day: int = 4) -> int:
    return int(datetime(2026, 11, day, hour, minute, second, tzinfo=CN_TZ).timestamp() * 1000)


def _row(ts: str, phase: str, price: float, volume: float, amount: float) -> dict:
    return {
        "symbol": "600114.SH",
        "trade_date": ts[:8],
        "ts": ts,
        "ts_ms": 0,
        "phase": phase,
        "last_price": price,
        "ref_price": price,
        "volume": volume,
        "volume_delta": volume,
        "volume_cum": volume,
        "amount": amount,
        "amount_delta": amount,
        "amount_cum": amount,
        "volume_is_cumulative": True,
        "amount_is_cumulative": True,
        "ask_price": [price + 0.01, 0.0, 0.0, 0.0, 0.0],
        "bid_price": [price - 0.01, 0.0, 0.0, 0.0, 0.0],
        "ask_volume": [10.0, 0.0, 0.0, 0.0, 0.0],
        "bid_volume": [10.0, 0.0, 0.0, 0.0, 0.0],
    }


def test_phase_boundaries():
    assert phase_of(91500) == "0915_0920"
    assert phase_of(91959) == "0915_0920"
    assert phase_of(92000) == "0920_0925"
    assert phase_of(92459) == "0920_0925"
    assert phase_of(92500) == "match_0925"
    assert phase_of(92559) == "match_0925"
    assert phase_of(92600) == "0925_0930"
    assert phase_of(93000) == "0925_0930"
    assert phase_of(93001) is None
    assert phase_of(None) is None


def test_hhmmss_and_ts_from_ms():
    assert hhmmss("20261104091503.000") == 91503
    assert hhmmss("20261104092501") == 92501
    assert hhmmss("bad") is None
    assert ts_from_ms(_ms(9, 25, 1)) == "20261104092501.000"


def test_tick_ts_ms_supports_epoch_and_stime():
    assert tick_ts_ms({"time": _ms(9, 25, 1)}) == _ms(9, 25, 1)
    assert tick_ts_ms({"time": _ms(9, 25, 1) // 1000}) == _ms(9, 25, 1)
    assert tick_ts_ms({"time": "20261104092501"}) == _ms(9, 25, 1)
    assert tick_ts_ms({"stime": "20261104092501.000"}) == _ms(9, 25, 1)
    assert tick_ts_ms({}) is None


def test_ref_price_falls_back_to_book():
    assert ref_price({"last_price": 10.0, "bid_price": [9.0], "ask_price": [9.1]}) == 10.0
    assert ref_price({"last_price": 0.0, "bid_price": [10.0], "ask_price": [10.02]}) == pytest.approx(10.01)
    assert ref_price({"last_price": 0.0, "bid_price": [10.0], "ask_price": []}) == 10.0
    assert ref_price({"last_price": 0.0, "bid_price": [], "ask_price": []}) is None


def test_snapshot_from_tick_normalizes_and_derives_delta():
    tick = {
        "time": _ms(9, 15, 3),
        "lastPrice": 0.0,
        "volume": 100,
        "amount": 100_000,
        "bidPrice": [10.0, 9.99],
        "askPrice": [10.02, 10.03],
        "bidVol": [30, 10],
        "askVol": [20, 10],
        "stockStatus": 2,
    }
    row = snapshot_from_tick(tick, "600114.SH", "20261104")
    assert row is not None
    assert row["ts"] == "20261104091503.000"
    assert row["phase"] == "0915_0920"
    assert row["ref_price"] == pytest.approx(10.01)
    assert row["volume_delta"] == 100
    assert row["amount_delta"] == 100_000
    assert row["volume_is_cumulative"] is True
    assert row["source"] == "qmt_tick_live"

    second = snapshot_from_tick(
        {**tick, "volume": 250, "amount": 250_000},
        "600114.SH",
        "20261104",
        prev_volume=100,
        prev_amount=100_000,
    )
    assert second is not None
    assert second["volume_delta"] == 150
    assert second["amount_delta"] == 150_000


def test_snapshot_rejects_outside_window_and_wrong_date():
    tick = {"time": _ms(9, 40, 0), "lastPrice": 10.0, "volume": 1, "amount": 10}
    assert snapshot_from_tick(tick, "600114.SH", "20261104") is None
    tick = {"time": _ms(9, 15, 3), "lastPrice": 10.0, "volume": 1, "amount": 10}
    assert snapshot_from_tick(tick, "600114.SH", "20261105") is None


def test_build_features_matches_backfill_shape():
    rows = [
        _row("20261104091503.000", "0915_0920", 10.0, 100, 100_000),
        _row("20261104092003.000", "0920_0925", 10.2, 200, 200_000),
        _row("20261104092501.000", "match_0925", 10.1, 300, 300_000),
    ]
    feature = build_features(
        rows, "600114.SH", "20261104", prev_close=10.0, avg_1m_amount=100_000,
        source="qmt_tick_live", partial=False,
    )
    assert feature is not None
    assert feature["open_price"] == pytest.approx(10.1)
    assert feature["gap_pct"] == pytest.approx(1.0)
    assert feature["auction_volume"] == pytest.approx(600)
    assert feature["auction_amount"] == pytest.approx(600_000)
    assert feature["auction_amount_ratio_20d"] == pytest.approx(0.6)
    assert feature["is_auction_complete"] is True
    assert feature["feature_version"] == "auction_v1"
    assert feature["source"] == "qmt_tick_live"
    assert feature["partial"] is False


def test_build_features_without_match_is_incomplete():
    rows = [_row("20261104091503.000", "0915_0920", 10.0, 100, 100_000)]
    feature = build_features(rows, "600114.SH", "20261104", prev_close=10.0, avg_1m_amount=None)
    assert feature is not None
    assert feature["is_auction_complete"] is False
    assert feature["price_0925"] == pytest.approx(10.0)
    assert feature["source"] == "qmt_tick_backfill"
    assert "partial" not in feature
