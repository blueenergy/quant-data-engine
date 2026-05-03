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


def test_row_to_volume_price_record_amount_k():
    row = {
        "f2": 10.0,
        "f3": -1.0,
        "f4": -0.1,
        "f5": 1000,
        "f6": 5000000.0,
        "f15": 10.1,
        "f16": 9.9,
        "f17": 10.05,
        "f18": 10.2,
    }
    rec = _row_to_volume_price_record("000001.SZ", "20260506", row)
    assert rec is not None
    assert rec["amount"] == 5000.0
    assert rec["volume"] == 1000
    assert rec["source"] == "eastmoney_snapshot"
    assert rec["open"] == 10.05
    assert rec["high"] == 10.2
    assert rec["low"] == 9.9
    assert rec["pre_close"] == 10.1
