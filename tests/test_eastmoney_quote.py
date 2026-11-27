"""East Money quote parsing, anchored on recorded real responses.

The field mapping here is the sort of thing that is easy to get backwards and
impossible to notice afterwards: ``f15``/``f18`` were read as previous close /
high in ``run_watchlist_intraday_eastmoney`` for seven months, and its unit
test agreed with the bug because the fixture was written from the same wrong
assumption.  Every assertion below is therefore pinned to a response recorded
from the live endpoint and cross-checked against our own stored daily bars,
not to a fixture invented from the implementation.
"""

from quant_data_engine.utils.eastmoney_quote import (
    EASTMONEY_SECIDS,
    parse_eastmoney_rows,
)


# Recorded 2026-11-27 from push2 for 100.HSI.  f18 (22226.88) is the close our
# own daily collection holds for 2026-11-26, which is what pins the semantics:
# previous close == yesterday's close.
HSI_ROW = {
    "f12": "HSI",
    "f13": 100,
    "f14": "恒生指数",
    "f2": 22694.77,    # latest
    "f3": 2.11,
    "f4": 467.89,
    "f5": 16611586816,
    "f15": 22781.03,   # high
    "f16": 22263.7,    # low
    "f17": 22263.7,    # open
    "f18": 22226.88,   # previous close
    "f124": 1795744304,
}

# Recorded the same day for 124.HSTECH.
HSTECH_ROW = {
    "f12": "HSTECH",
    "f13": 124,
    "f14": "恒生科技指数",
    "f2": 3637.88,
    "f15": 3648.74,
    "f16": 3433.38,
    "f17": 3433.38,
    "f18": 3417.38,
    "f124": 1795744304,
}

# The close our daily collection held for 2026-11-26, used to anchor f18.
HSI_PREV_CLOSE_DB = 22226.88
HSTECH_PREV_CLOSE_DB = 3417.38


def test_parse_maps_ohlc_fields_in_the_right_order():
    q = parse_eastmoney_rows([HSI_ROW])["^HSI"]
    assert q["price"] == 22694.77
    assert q["high"] == 22781.03
    assert q["low"] == 22263.7
    assert q["open"] == 22263.7
    assert q["prev_close"] == 22226.88


def test_prev_close_matches_our_own_daily_close():
    """The anchor that makes this a cross-check rather than a self-test."""
    q = parse_eastmoney_rows([HSI_ROW])["^HSI"]
    assert q["prev_close"] == HSI_PREV_CLOSE_DB

    q = parse_eastmoney_rows([HSTECH_ROW])["HSTECH.HK"]
    assert q["prev_close"] == HSTECH_PREV_CLOSE_DB


def test_ohlc_invariants_hold():
    for row in (HSI_ROW, HSTECH_ROW):
        parsed = parse_eastmoney_rows([row])
        q = next(iter(parsed.values()))
        assert q["high"] >= max(q["open"], q["price"], q["low"])
        assert q["low"] <= min(q["open"], q["price"], q["high"])


def test_both_market_ids_are_used():
    """Hang Seng TECH is only served on market 124; a 100.* guess returns nothing."""
    assert EASTMONEY_SECIDS["HSTECH.HK"] == "124.HSTECH"
    assert EASTMONEY_SECIDS["^HSI"] == "100.HSI"
    # Both keys must resolve through f13/f12, not be hardcoded per symbol.
    parsed = parse_eastmoney_rows([HSI_ROW, HSTECH_ROW])
    assert set(parsed) == {"^HSI", "HSTECH.HK"}


def test_rows_without_a_price_are_skipped():
    row = dict(HSI_ROW)
    row["f2"] = None
    assert parse_eastmoney_rows([row]) == {}


def test_unmapped_symbols_are_ignored():
    stranger = {"f12": "600519", "f13": 1, "f2": 1472.36, "f15": 1481.41,
                "f16": 1452.99, "f17": 1469.99, "f18": 1469.0}
    assert parse_eastmoney_rows([stranger]) == {}
