"""Unit tests for backfilling delisted names into stock_info.

bak_basic is a per-day snapshot of what traded, so a symbol that delisted
before the first sync never entered stock_info. Historical point-in-time
universes still score those names, and the missing row costs the industry
lookup used by the growth market-competitiveness submodule.
"""
from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd
import pytest

import pymongo

from quant_data_engine.tasks.run_stock_info_sync import (
    IDENTITY_FIELDS,
    _build_doc,
    _build_identity_doc,
    _ensure_indexes,
    sync_delisted_names,
)


class FakeAdapter:
    def __init__(self, by_status: Dict[str, pd.DataFrame]):
        self.by_status = by_status
        self.calls: List[Dict[str, Any]] = []

    def fetch_stock_basic(self, list_status: str = "L", exchange=None, fields=None) -> pd.DataFrame:
        self.calls.append({"list_status": list_status, "fields": fields})
        return self.by_status.get(list_status, pd.DataFrame())


class FakeCollection:
    def __init__(self):
        self.ops: List[Any] = []

    def bulk_write(self, ops, ordered=True):
        self.ops.extend(ops)

        class Result:
            upserted_count = len(ops)
            modified_count = 0

        return Result()


def _basic_df(rows):
    return pd.DataFrame(rows)


@pytest.fixture
def delisted_adapter():
    return FakeAdapter(
        {
            "D": _basic_df(
                [
                    {
                        "ts_code": "000018.SZ",
                        "name": "神州长城",
                        "industry": "建筑工程",
                        "area": "深圳",
                        "market": "主板",
                        "exchange": "SZSE",
                        "list_status": "D",
                        "list_date": "19920518",
                        "delist_date": "20200731",
                    }
                ]
            ),
            "P": _basic_df(
                [
                    {
                        "ts_code": "600565.SH",
                        "name": "迪马股份",
                        "industry": "汽车配件",
                        "area": "重庆",
                        "market": "主板",
                        "exchange": "SSE",
                        "list_status": "P",
                        "list_date": "20020801",
                        "delist_date": None,
                    }
                ]
            ),
        }
    )


def test_delisted_and_suspended_are_both_fetched(delisted_adapter):
    coll = FakeCollection()
    stats = sync_delisted_names(delisted_adapter, coll)

    assert [call["list_status"] for call in delisted_adapter.calls] == ["D", "P"]
    assert stats["rows"] == 2
    assert stats["upserted"] == 2


def test_fields_are_requested_because_tushare_omits_delist_date(delisted_adapter):
    """The default stock_basic column set has no list_status/delist_date."""
    sync_delisted_names(delisted_adapter, FakeCollection())

    for call in delisted_adapter.calls:
        assert "delist_date" in call["fields"]
        assert "list_status" in call["fields"]


def test_identity_doc_carries_no_valuation_columns():
    """A delisted row must not reset pe/pb/total_mv on a symbol that still trades."""
    row = pd.Series(
        {
            "ts_code": "000018.SZ",
            "name": "神州长城",
            "industry": "建筑工程",
            "area": "深圳",
            "market": "主板",
            "exchange": "SZSE",
            "list_status": "D",
            "list_date": "19920518",
            "delist_date": "20200731",
        }
    )
    doc = _build_identity_doc(row)

    for valuation_field in ("pe", "pb", "total_mv", "circ_mv", "market_cap", "valuation_date"):
        assert valuation_field not in doc
    assert set(IDENTITY_FIELDS) <= set(doc)
    assert doc["delist_date"] == "20200731"
    assert doc["symbol"] == doc["ts_code"] == "000018.SZ"


def test_missing_delist_date_becomes_empty_string():
    row = pd.Series({"ts_code": "600565.SH", "name": "迪马股份", "list_status": "P", "delist_date": None})
    assert _build_identity_doc(row)["delist_date"] == ""


def test_bak_basic_rows_are_stamped_as_listed():
    """bak_basic can only return names that traded, so the status is known."""
    row = pd.Series({"ts_code": "600000.SH", "name": "浦发银行", "trade_date": "20260910", "list_date": "19991110"})
    assert _build_doc(row)["list_status"] == "L"


class IndexCollection:
    """Collection whose ``symbol`` index already exists with other options."""

    def __init__(self, conflicting=("symbol",), code=86):
        self.conflicting = set(conflicting)
        self.code = code
        self.created: List[str] = []

    def create_index(self, key, background=False):
        if key in self.conflicting:
            raise pymongo.errors.OperationFailure("IndexKeySpecsConflict", code=self.code)
        self.created.append(key)


def test_existing_index_with_other_options_does_not_abort_sync():
    """A sparse symbol_1 seeded elsewhere used to kill the whole run."""
    coll = IndexCollection()

    _ensure_indexes(coll)

    assert coll.created == ["ts_code", "industry"]


def test_unrelated_index_failure_still_raises():
    coll = IndexCollection(code=13)

    with pytest.raises(pymongo.errors.OperationFailure):
        _ensure_indexes(coll)


def test_no_rows_means_no_write():
    coll = FakeCollection()
    stats = sync_delisted_names(FakeAdapter({}), coll)

    assert coll.ops == []
    assert stats == {"rows": 0, "upserted": 0, "modified": 0}
