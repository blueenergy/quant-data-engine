"""Unit tests for the offshore-RMB daily sync (tushare fx_daily -> daily bars)."""
from __future__ import annotations

import math

import pandas as pd

from quant_data_engine.tasks import run_fx_daily_sync as fx


class _FakePro:
    def __init__(self, df):
        self._df = df
        self.calls = []

    def fx_daily(self, ts_code, start_date, end_date):
        self.calls.append((ts_code, start_date, end_date))
        return self._df


class _FakeColl:
    def __init__(self):
        self.docs = {}

    def update_one(self, flt, update, upsert=False):
        self.docs[(flt["symbol"], flt["date"])] = update["$set"]


def _row(trade_date, bid_o, ask_o, bid_h, ask_h, bid_l, ask_l, bid_c, ask_c):
    return {
        "ts_code": "USDCNH.FXCM",
        "trade_date": trade_date,
        "bid_open": bid_o, "ask_open": ask_o,
        "bid_high": bid_h, "ask_high": ask_h,
        "bid_low": bid_l, "ask_low": ask_l,
        "bid_close": bid_c, "ask_close": ask_c,
        "tick_qty": 100,
    }


def test_mid_uses_bid_ask_midpoint():
    assert fx._mid({"bid_close": 7.0, "ask_close": 7.2}, "bid_close", "ask_close") == 7.1


def test_mid_handles_missing_and_nan():
    assert fx._mid({"bid_close": None, "ask_close": 7.2}, "bid_close", "ask_close") is None
    assert fx._mid({"bid_close": math.nan, "ask_close": 7.2}, "bid_close", "ask_close") is None
    assert fx._mid({}, "bid_close", "ask_close") is None


def test_date_chunks_split_long_ranges():
    chunks = fx._date_chunks("20241125", "20261123")
    assert chunks[0][0] == "20241125"
    assert chunks[-1][1] == "20261123"
    # every chunk is contiguous with the next
    for (_, prev_end), (next_start, _) in zip(chunks, chunks[1:]):
        assert prev_next_day(prev_end) == next_start


def prev_next_day(date_str):
    from datetime import datetime, timedelta
    return (datetime.strptime(date_str, "%Y%m%d") + timedelta(days=1)).strftime("%Y%m%d")


def test_sync_uses_midpoint_and_marks_source():
    df = pd.DataFrame([_row("20261123", 6.666, 6.667, 6.668, 6.670, 6.665, 6.666, 6.665, 6.667)])
    coll = _FakeColl()

    written = fx.sync_code(_FakePro(df), "USDCNH.FXCM", "USDCNH=X", "20261101", "20261124", coll, dry_run=False)

    assert written == 1
    doc = coll.docs[("USDCNH=X", "20261123")]
    assert doc["open"] == 6.6665      # (6.666 + 6.667) / 2
    assert doc["close"] == 6.666      # (6.665 + 6.667) / 2
    assert doc["volume"] == 0         # FX has no traded volume
    assert doc["source"] == "tushare:USDCNH.FXCM"


def test_sync_skips_rows_without_close():
    bad = _row("20261122", 1, 1, 1, 1, 1, 1, None, None)
    df = pd.DataFrame([bad, _row("20261123", 6.6, 6.6, 6.7, 6.7, 6.5, 6.5, 6.6, 6.6)])
    coll = _FakeColl()

    written = fx.sync_code(_FakePro(df), "USDCNH.FXCM", "USDCNH=X", "20261101", "20261124", coll, dry_run=False)

    assert written == 1
    assert ("USDCNH=X", "20261122") not in coll.docs


def test_dry_run_does_not_write():
    df = pd.DataFrame([_row("20261123", 6.6, 6.6, 6.7, 6.7, 6.5, 6.5, 6.6, 6.6)])
    coll = _FakeColl()

    written = fx.sync_code(_FakePro(df), "USDCNH.FXCM", "USDCNH=X", "20261101", "20261124", coll, dry_run=True)

    assert written == 1
    assert coll.docs == {}


def test_empty_response_is_not_an_error():
    coll = _FakeColl()
    assert fx.sync_code(_FakePro(pd.DataFrame()), "USDCNH.FXCM", "USDCNH=X", "20261101", "20261124", coll, False) == 0
    assert coll.docs == {}
