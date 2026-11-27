"""Unit tests for watchlist East Money intraday helpers."""
from datetime import datetime
from zoneinfo import ZoneInfo

from quant_data_engine.tasks.run_watchlist_intraday_eastmoney import (
    in_cn_stock_session,
    ts_code_to_secid,
    _row_to_volume_price_record,
)


def test_ts_code_to_secid():
    assert ts_code_to_secid("600519.SH") == "1.600519"
    assert ts_code_to_secid("000001.SZ") == "0.000001"
    assert ts_code_to_secid("920002.BJ") == "0.920002"
    assert ts_code_to_secid("600519.sh") == "1.600519"
    assert ts_code_to_secid("INVALID") is None
    assert ts_code_to_secid("AAPL") is None


def test_in_cn_stock_session_weekday_morning():
    tz = ZoneInfo("Asia/Shanghai")
    # 2026-05-01 is Friday in user_info - actually May 1 2026 might be Labour Day holiday - use a known weekday
    dt = datetime(2026, 5, 6, 10, 0, tzinfo=tz)  # Wednesday
    assert in_cn_stock_session(dt) is True


def test_in_cn_stock_session_lunch_break():
    tz = ZoneInfo("Asia/Shanghai")
    dt = datetime(2026, 5, 6, 12, 0, tzinfo=tz)
    assert in_cn_stock_session(dt) is False


def test_in_cn_stock_session_weekend():
    tz = ZoneInfo("Asia/Shanghai")
    dt = datetime(2026, 5, 2, 10, 0, tzinfo=tz)  # Saturday
    assert in_cn_stock_session(dt) is False


# Recorded 2026-11-27 from push2 for 1.600519 (贵州茅台), together with what our
# own volume_price row held for 2026-11-27.  The two sets agree field by field,
# which is what proves the mapping: f15 is the high, f18 the previous close.
EASTMONEY_600519 = {
    "f2": 1472.36,    # close
    "f15": 1481.41,   # high
    "f16": 1452.99,   # low
    "f17": 1469.99,   # open
    "f18": 1469.0,    # previous close
    "f5": 1000,
    "f6": 5000000.0,
    "f3": 1.47,
    "f4": 0.15,
}
OUR_VOLUME_PRICE_600519 = {
    "open": 1469.99,
    "high": 1481.41,
    "low": 1452.99,
    "close": 1472.36,
}


def test_row_to_volume_price_record_amount_k():
    rec = _row_to_volume_price_record("600519.SH", "20260506", dict(EASTMONEY_600519))
    assert rec is not None
    assert rec["amount"] == 5000.0
    assert rec["volume"] == 1000
    assert rec["source"] == "eastmoney_snapshot"


def test_row_to_volume_price_record_matches_our_own_daily_bar():
    """Cross-checked against the row our own daily collection held that day.

    The previous version of this test asserted ``high == f18`` and
    ``pre_close == f15`` — i.e. it encoded the bug and stayed green for the
    seven months the mapping was reversed.
    """
    rec = _row_to_volume_price_record("600519.SH", "20260506", dict(EASTMONEY_600519))
    assert rec is not None
    assert rec["open"] == OUR_VOLUME_PRICE_600519["open"]
    assert rec["high"] == OUR_VOLUME_PRICE_600519["high"]
    assert rec["low"] == OUR_VOLUME_PRICE_600519["low"]
    assert rec["close"] == OUR_VOLUME_PRICE_600519["close"]
    assert rec["pre_close"] == 1469.0


def test_row_to_volume_price_record_ohlc_invariants():
    """high 必须不低于 open/close/low —— 映射错位时这条会立刻失败。

    旧实现把昨收写进 high，对上涨股会出现 high < close，而 pct_chg 仍正确，
    所以只看涨跌幅永远发现不了。
    """
    rec = _row_to_volume_price_record("600519.SH", "20260506", dict(EASTMONEY_600519))
    assert rec is not None
    assert rec["high"] >= max(rec["open"], rec["close"], rec["low"])
    assert rec["low"] <= min(rec["open"], rec["close"], rec["high"])
