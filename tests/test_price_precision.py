"""Tests for per-symbol price precision: fund/bond tick 0.001 vs stock 0.01."""
from quant_data_engine.bridges.miniqmt.price_precision import (
    DEFAULT_PRICE_DIGITS,
    FINE_PRICE_DIGITS,
    price_digits,
    round_price,
)


def test_funds_and_bonds_get_three_digits():
    """ETF / LOF / 封基 / 可转债 —— QMT 原生就是 3 位小数（tick 0.001）。"""
    for symbol in (
        "159766.SZ",  # 深市 ETF（本次缺陷的当事票）
        "159847.SZ",
        "513260.SH",  # 沪市 ETF
        "510300.SH",
        "588000.SH",  # 科创板 ETF
        "161725.SZ",  # 深市 LOF
        "184699.SZ",  # 深市封基
        "113050.SH",  # 沪市可转债
        "123456.SZ",  # 深市可转债
        "132000.SH",  # 沪市可交换债
    ):
        assert price_digits(symbol) == FINE_PRICE_DIGITS, symbol


def test_stocks_and_indexes_keep_two_digits():
    for symbol in (
        "000001.SZ",
        "300750.SZ",
        "002594.SZ",
        "600428.SH",
        "688432.SH",  # 科创板股票仍是 0.01
        "900901.SH",  # B 股
        "000001.SH",  # 上证指数
        "399001.SZ",  # 深证成指
        "830799.BJ",
        "920002.BJ",
    ):
        assert price_digits(symbol) == DEFAULT_PRICE_DIGITS, symbol


def test_prefix_decides_without_exchange_suffix():
    """两位前缀跨市场无歧义（股票用 60/68/00/30/8/4/92），裸码不必看后缀。"""
    assert price_digits("159766") == FINE_PRICE_DIGITS
    assert price_digits("sh513260") == FINE_PRICE_DIGITS
    assert price_digits("SZ159766") == FINE_PRICE_DIGITS
    assert price_digits("600428") == DEFAULT_PRICE_DIGITS


def test_unrecognised_symbols_fall_back_to_default():
    """默认 2 位 = 既有行为：认不出来只会「维持现状」，不会静默变精度。"""
    for symbol in (None, "", "   ", "SZ", "not-a-symbol", 600428, "0.5"):
        assert price_digits(symbol) == DEFAULT_PRICE_DIGITS, symbol


def test_round_price_keeps_etf_precision_and_still_kills_float_noise():
    assert round_price(0.561, "159766.SZ") == 0.561
    assert round_price(0.5679999999, "159766.SZ") == 0.568
    assert round_price(0.5549, "159766.SZ") == 0.555
    # 股票照旧落在 0.01 网格
    assert round_price(11.169999999, "600428.SH") == 11.17
    assert round_price(0.561, "600428.SH") == 0.56
