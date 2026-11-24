"""Unit tests for the global-market forming (in-progress) daily bar helpers."""
from __future__ import annotations

from quant_data_engine.utils.global_session_bar import (
    compose_forming_bar,
    exchange_local_date,
    parse_chart_daily,
    session_bar_is_stale,
    session_state,
)

# 2026-11-23 09:30 EST  ==  2026-11-23 14:30 UTC
SESSION_OPEN_EST = 1795444200
# 2026-11-23 16:00 EST  ==  2026-11-23 21:00 UTC
SESSION_CLOSE_EST = 1795467600
# 2026-11-23 12:05 EST  ==  2026-11-23 17:05 UTC
MID_SESSION_EST = 1795452300


def _meta(**overrides):
    meta = {
        "gmtoffset": -18000,
        "regularMarketTime": MID_SESSION_EST,
        "regularMarketPrice": 24002.21,
        "regularMarketDayHigh": 24353.65,
        "regularMarketDayLow": 23794.96,
        "regularMarketVolume": 4_963_692_000,
        "previousClose": 22617.73,
        "currentTradingPeriod": {
            "regular": {"start": SESSION_OPEN_EST, "end": SESSION_CLOSE_EST}
        },
    }
    meta.update(overrides)
    return meta


def _chart_result(ts, open_, high, low, close, volume, gmtoffset=-18000):
    return {
        "meta": {"gmtoffset": gmtoffset},
        "timestamp": [ts],
        "indicators": {
            "quote": [{
                "open": [open_],
                "high": [high],
                "low": [low],
                "close": [close],
                "volume": [volume],
            }]
        },
    }


def test_exchange_local_date_uses_yahoo_offset():
    assert exchange_local_date(SESSION_OPEN_EST, -18000) == "20261123"
    # 12:05 EST is still the same day; 17:05 UTC in Hong Kong already is not.
    assert exchange_local_date(MID_SESSION_EST, -18000) == "20261123"
    assert exchange_local_date(MID_SESSION_EST, 28800) == "20261124"
    assert exchange_local_date(None, -18000) is None


def test_session_state_in_session():
    state = session_state(_meta())
    assert state["in_session"] is True
    assert state["session_start"] == SESSION_OPEN_EST
    assert state["local_date"] == "20261123"


def test_session_state_before_open_is_not_in_session():
    # Last trade still belongs to the previous session (pre-market).
    state = session_state(_meta(regularMarketTime=SESSION_OPEN_EST - 3600))
    assert state["in_session"] is False


def test_session_state_after_close_is_not_in_session():
    state = session_state(_meta(regularMarketTime=SESSION_CLOSE_EST))
    assert state["in_session"] is False


def test_session_state_closed_market_points_at_next_session():
    # Yahoo rolls currentTradingPeriod forward once a market closes.
    state = session_state(_meta(
        regularMarketTime=SESSION_CLOSE_EST,
        currentTradingPeriod={"regular": {
            "start": SESSION_OPEN_EST + 86400,
            "end": SESSION_CLOSE_EST + 86400,
        }},
    ))
    assert state["in_session"] is False


def test_parse_chart_daily_reads_bar():
    bar = parse_chart_daily(_chart_result(
        SESSION_OPEN_EST, 24269.75, 24353.65, 23794.96, 24002.21, 4_963_692_000,
    ))
    assert bar == {
        "date": "20261123",
        "open": 24269.75,
        "high": 24353.65,
        "low": 23794.96,
        "close": 24002.21,
        "volume": 4_963_692_000,
    }


def test_parse_chart_daily_skips_bar_without_open():
    # Yahoo publishes the forming bar before its open is known.
    result = _chart_result(SESSION_OPEN_EST, None, None, None, None, None)
    result["timestamp"] = [SESSION_OPEN_EST - 86400, SESSION_OPEN_EST]
    result["indicators"]["quote"][0] = {
        "open": [23592.67, None],
        "high": [23837.76, None],
        "low": [22513.01, None],
        "close": [23879.20, None],
        "volume": [10, None],
    }
    bar = parse_chart_daily(result)
    assert bar["date"] == "20261122"


def test_compose_forming_bar_uses_live_price_and_session_range():
    bar = compose_forming_bar(
        _meta(),
        {"date": "20261123", "open": 24269.75},
        price=24002.21,
        prev_close=22617.73,
    )
    assert bar == {
        "date": "20261123",
        "open": 24269.75,
        "high": 24353.65,
        "low": 23794.96,
        "close": 24002.21,
        "volume": 4_963_692_000,
        "prev_close": 22617.73,
    }


def test_session_bar_is_stale_on_new_session():
    session = session_state(_meta())
    cache = {
        "date": "20261123",
        "open": 24269.75,
        "session_start": SESSION_OPEN_EST,
        "local_date": "20261123",
        "fetched_at": 1000.0,
    }
    assert session_bar_is_stale(cache, session, now=1100.0) is False

    next_session = session_state(_meta(
        regularMarketTime=SESSION_OPEN_EST + 86400,
        currentTradingPeriod={"regular": {
            "start": SESSION_OPEN_EST + 86400,
            "end": SESSION_CLOSE_EST + 86400,
        }},
    ))
    assert session_bar_is_stale(cache, next_session, now=1100.0) is True


def test_session_bar_is_stale_when_publisher_lags():
    session = session_state(_meta())
    # Yahoo still serves the previous session's bar.
    cache = {
        "date": "20261120",
        "open": 23592.67,
        "session_start": SESSION_OPEN_EST,
        "local_date": "20261123",
        "fetched_at": 1000.0,
    }
    assert session_bar_is_stale(cache, session, now=1100.0) is False
    # Retried, but only after the cool-down.
    assert session_bar_is_stale(cache, session, now=2000.0) is True
