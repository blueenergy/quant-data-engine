from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from quant_data_engine.tick.features import (
    FEATURE_VERSION,
    build_minute_features,
    parse_frames,
)

CN_TZ = ZoneInfo("Asia/Shanghai")
DATE = "20261118"
SYMBOL = "600114.SH"


def _ms(hour: int, minute: int, second: int) -> int:
    return int(datetime(2026, 11, 18, hour, minute, second, tzinfo=CN_TZ).timestamp() * 1000)


def _tick(
    hour: int,
    minute: int,
    second: int,
    *,
    last: float,
    volume: float,
    amount: float,
    txn: int,
    bid: float = 9.99,
    ask: float = 10.01,
    bid_vol: float = 100.0,
    ask_vol: float = 50.0,
) -> dict:
    return {
        "stime": f"{DATE}{hour:02d}{minute:02d}{second:02d}",
        "time": _ms(hour, minute, second),
        "lastPrice": last,
        "volume": volume,
        "amount": amount,
        "transactionNum": txn,
        "bidPrice": [bid] * 5,
        "askPrice": [ask] * 5,
        "bidVol": [bid_vol] * 5,
        "askVol": [ask_vol] * 5,
    }


def _records(ticks: list[dict]) -> dict:
    fields = [
        "stime", "time", "lastPrice", "volume", "amount", "transactionNum",
        "bidPrice", "askPrice", "bidVol", "askVol",
    ]
    return {field: [tick[field] for tick in ticks] for field in fields}


def _by_minute(rows: list[dict]) -> dict[str, dict]:
    return {row["minute"]: row for row in rows}


def test_direction_buy_when_last_above_mid():
    ticks = [
        _tick(9, 30, 0, last=10.00, volume=0, amount=0, txn=0),
        _tick(9, 30, 3, last=10.02, volume=100, amount=100_200, txn=5),
    ]
    rows = _by_minute(build_minute_features(_records(ticks), SYMBOL, DATE))
    row = rows[f"{DATE}0930"]
    assert row["volume_sum"] == 100
    assert row["buy_vol"] == 100
    assert row["sell_vol"] == 0
    assert row["net_flow_ratio"] == 1.0
    assert row["flow_uncertain_ratio"] == 0.0
    assert row["tick_count"] == 2


def test_direction_sell_when_last_below_mid():
    ticks = [
        _tick(9, 30, 0, last=10.00, volume=0, amount=0, txn=0),
        _tick(9, 30, 3, last=9.98, volume=100, amount=99_800, txn=5),
    ]
    row = _by_minute(build_minute_features(_records(ticks), SYMBOL, DATE))[f"{DATE}0930"]
    assert row["sell_vol"] == 100
    assert row["buy_vol"] == 0
    assert row["net_flow_ratio"] == -1.0


def test_direction_inherits_previous_when_price_unchanged():
    ticks = [
        _tick(9, 30, 0, last=10.00, volume=0, amount=0, txn=0),
        _tick(9, 30, 3, last=10.02, volume=100, amount=100_200, txn=5),
        _tick(9, 30, 6, last=10.00, volume=200, amount=200_400, txn=9),
    ]
    row = _by_minute(build_minute_features(_records(ticks), SYMBOL, DATE))[f"{DATE}0930"]
    assert row["buy_vol"] == 200
    assert row["sell_vol"] == 0
    # the second interval inherited the direction -> flagged as uncertain
    assert row["flow_uncertain_vol"] == 100
    assert row["flow_uncertain_ratio"] == 0.5


def test_missing_book_leaves_volume_unclassified():
    def blank(hour, minute, second, volume, amount, txn):
        tick = _tick(
            hour, minute, second, last=0.0, volume=volume, amount=amount, txn=txn
        )
        tick["bidPrice"] = []
        tick["askPrice"] = []
        tick["bidVol"] = []
        tick["askVol"] = []
        return tick

    ticks = [
        blank(9, 30, 0, 0, 0, 0),
        blank(9, 30, 3, 100, 100_200, 5),
    ]
    row = _by_minute(build_minute_features(_records(ticks), SYMBOL, DATE))[f"{DATE}0930"]
    assert row["volume_sum"] == 100
    assert row["buy_vol"] == 0 and row["sell_vol"] == 0
    assert row["flow_unclassified_vol"] == 100
    assert row["flow_unclassified_ratio"] == 1.0
    assert row["book_missing_ratio"] == 1.0


def test_book_metrics_and_vwap():
    ticks = [
        _tick(9, 30, 0, last=10.00, volume=0, amount=0, txn=0),
        _tick(9, 30, 3, last=10.02, volume=100, amount=100_200, txn=5),
    ]
    row = _by_minute(build_minute_features(_records(ticks), SYMBOL, DATE))[f"{DATE}0930"]
    # 5-level book: bid 100 x5 = 500 vs ask 50 x5 = 250
    assert row["oi5_mean"] == pytest.approx(250 / 750)
    assert row["oi1_mean"] == pytest.approx(50 / 150)
    assert row["spread_mean"] == pytest.approx(0.02 / 10.0)
    # vwap = amount / (volume lots * 100 shares)
    assert row["vwap"] == pytest.approx(100_200 / (100 * 100))
    assert row["avg_trade_size"] == pytest.approx(100_200 / 5)
    assert row["feature_version"] == FEATURE_VERSION


def test_after_hours_and_lunch_ticks_are_dropped():
    ticks = [
        _tick(11, 29, 57, last=10.00, volume=0, amount=0, txn=0),
        _tick(12, 0, 0, last=10.00, volume=10, amount=10_000, txn=1),
        _tick(15, 30, 13, last=10.00, volume=20, amount=20_000, txn=2),
    ]
    frames = parse_frames(_records(ticks), DATE)
    assert [frame.minute for frame in frames] == [f"{DATE}1129"]
    rows = build_minute_features(_records(ticks), SYMBOL, DATE)
    assert [row["minute"] for row in rows] == [f"{DATE}1129"]


def test_daily_volume_total_equals_last_cumulative():
    ticks = [
        _tick(9, 25, 1, last=0.0, volume=0, amount=0, txn=0),
        _tick(9, 30, 0, last=10.00, volume=1_598, amount=3_410_132, txn=120),
        _tick(14, 59, 57, last=10.10, volume=9_598, amount=20_000_000, txn=900),
    ]
    rows = build_minute_features(_records(ticks), SYMBOL, DATE)
    total = sum(row["volume_sum"] for row in rows)
    assert total == 9_598
    # the interval that carries the auction match is booked to the 09:30
    # bucket, the same minute the 1m bars use for it
    assert _by_minute(rows)[f"{DATE}0930"]["volume_sum"] == 1_598


def test_auction_window_is_kept_and_intraday_is_labeled_by_start():
    ticks = [
        _tick(9, 15, 1, last=0.0, volume=0, amount=0, txn=0),
        _tick(9, 30, 3, last=10.02, volume=100, amount=100_200, txn=5),
    ]
    rows = _by_minute(build_minute_features(_records(ticks), SYMBOL, DATE))
    assert f"{DATE}0915" in rows
    assert f"{DATE}0930" in rows


def test_empty_records_produce_no_rows():
    assert build_minute_features({}, SYMBOL, DATE) == []
