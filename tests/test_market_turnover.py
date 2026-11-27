"""Tests for the market turnover aggregation (board split + ETF split)."""
from __future__ import annotations

import pytest

from quant_data_engine.tasks.run_market_turnover_daily import build_daily_doc
from quant_data_engine.tools.market_turnover import (
    aggregate_boards,
    aggregate_etf,
    classify_board,
    classify_fund_type,
)


class _FakeCursor:
    def __init__(self, docs):
        self._docs = list(docs)

    def __iter__(self):
        return iter(self._docs)


class _FakeCollection:
    def __init__(self, docs):
        self._docs = list(docs)

    def find(self, query=None, projection=None):
        docs = self._docs
        query = query or {}
        if "trade_date" in query:
            docs = [d for d in docs if d.get("trade_date") == query["trade_date"]]
        excluded = (query.get("source") or {}).get("$nin") or []
        if excluded:
            docs = [d for d in docs if d.get("source") not in excluded]
        return _FakeCursor(docs)


class _FakeDB:
    def __init__(self, mapping):
        self._mapping = mapping

    def __getitem__(self, name):
        return self._mapping[name]


@pytest.mark.parametrize(
    "symbol, expected",
    [
        ("600519.SH", "sh"),
        ("601318.SH", "sh"),
        ("605499.SH", "sh"),
        ("688981.SH", "star"),
        ("689009.SH", "star"),
        ("000001.SZ", "sz"),
        ("002594.SZ", "sz"),
        ("003816.SZ", "sz"),
        ("300750.SZ", "cx"),
        ("301269.SZ", "cx"),
        ("920001.BJ", "bse"),
        ("830799.BJ", "bse"),
        ("900901.SH", "other"),
        ("200002.SZ", "other"),
        ("", "other"),
        (None, "other"),
    ],
)
def test_classify_board(symbol, expected):
    assert classify_board(symbol) == expected


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("股票型", "equity"),
        ("债券型", "bond"),
        ("货币型", "money"),
        ("混合型", "other"),
        ("REITs", "other"),
        (None, "other"),
    ],
)
def test_classify_fund_type(raw, expected):
    assert classify_fund_type(raw) == expected


def test_aggregate_boards_is_mutually_exclusive():
    rows = [
        {"symbol": "600000.SH", "amount": 1e5},   # 1 亿 -> 沪市主板
        {"symbol": "688001.SH", "amount": 2e5},   # 2 亿 -> 科创板
        {"symbol": "000001.SZ", "amount": 3e5},   # 3 亿 -> 深市主板
        {"symbol": "300001.SZ", "amount": 4e5},   # 4 亿 -> 创业板
        {"symbol": "920001.BJ", "amount": 5e5},   # 5 亿 -> 北交所
    ]
    agg = aggregate_boards(rows)
    assert agg["amounts"] == {"sh": 1.0, "sz": 3.0, "cx": 4.0, "star": 2.0, "bse": 5.0, "other": 0.0}
    assert agg["total"] == pytest.approx(15.0)
    assert agg["n_stocks"] == 5
    # 沪市 = 沪市主板 + 科创板；深市 = 深市主板 + 创业板
    assert agg["amounts"]["sh"] + agg["amounts"]["star"] == pytest.approx(3.0)
    assert agg["amounts"]["sz"] + agg["amounts"]["cx"] == pytest.approx(7.0)


def test_aggregate_boards_ignores_bad_amounts():
    rows = [{"symbol": "600000.SH", "amount": None}, {"symbol": "600001.SH", "amount": "x"}]
    agg = aggregate_boards(rows)
    assert agg["amounts"]["sh"] == 0.0
    assert agg["n_stocks"] == 2


def test_aggregate_etf_keeps_unknown_codes():
    rows = [
        {"ts_code": "510300.SH", "amount": 1e5},
        {"ts_code": "511090.SH", "amount": 2e5},
        {"ts_code": "511880.SH", "amount": 3e5},
        {"ts_code": "508000.SH", "amount": 4e5},
        {"ts_code": "999999.SH", "amount": 5e5},  # not in fund_basic
    ]
    types = {"510300.SH": "股票型", "511090.SH": "债券型", "511880.SH": "货币型", "508000.SH": "REITs"}
    agg = aggregate_etf(rows, types)
    assert agg["amounts"] == {"equity": 1.0, "bond": 2.0, "money": 3.0, "other": 9.0}
    assert agg["total"] == pytest.approx(15.0)
    assert agg["n_funds"] == 5


def test_build_daily_doc_excludes_intraday_snapshots():
    db = _FakeDB(
        {
            "volume_price": _FakeCollection(
                [
                    {"trade_date": "20261126", "symbol": "600000.SH", "amount": 1e5, "source": "tushare"},
                    {"trade_date": "20261126", "symbol": "300001.SZ", "amount": 2e5, "source": "tushare"},
                    # snapshot rows must not be counted as end-of-day turnover
                    {"trade_date": "20261126", "symbol": "000001.SZ", "amount": 9e5, "source": "miniqmt"},
                ]
            ),
            "etf_daily": _FakeCollection([{"trade_date": "20261126", "ts_code": "510300.SH", "amount": 4e5}]),
        }
    )
    doc = build_daily_doc(db, "20261126", {"510300.SH": "股票型"})
    assert doc["sh"] == 1.0
    assert doc["cx"] == 2.0
    assert doc["sz"] == 0.0
    assert doc["stock_total"] == pytest.approx(3.0)
    assert doc["stock_count"] == 2
    assert doc["etf_equity"] == 4.0
    assert doc["etf_available"] is True
    assert doc["schema_version"] == 1


def test_build_daily_doc_returns_none_without_eod_rows():
    db = _FakeDB(
        {
            "volume_price": _FakeCollection(
                [{"trade_date": "20261126", "symbol": "600000.SH", "amount": 1e5, "source": "miniqmt"}]
            ),
            "etf_daily": _FakeCollection([]),
        }
    )
    assert build_daily_doc(db, "20261126", {}) is None


def test_build_daily_doc_flags_missing_etf():
    db = _FakeDB(
        {
            "volume_price": _FakeCollection(
                [{"trade_date": "20261126", "symbol": "600000.SH", "amount": 1e5, "source": "tushare"}]
            ),
            "etf_daily": _FakeCollection([]),
        }
    )
    doc = build_daily_doc(db, "20261126", {})
    assert doc["etf_available"] is False
    assert doc["etf_total"] == 0.0
