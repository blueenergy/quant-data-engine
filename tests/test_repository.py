import pytest
from quant_data_engine.repositories.index_repository import IndexRepository
from unittest.mock import MagicMock
import pandas as pd

def test_upsert_daily_calls_bulk_write():
    repo = IndexRepository.__new__(IndexRepository)
    repo.coll = MagicMock()
    repo.coll.bulk_write.return_value.upserted_count = 2
    repo.coll.bulk_write.return_value.modified_count = 1
    records = [
        {"ts_code": "000300.SH", "trade_date": "20240101", "open": 10, "high": 12, "low": 9, "close": 11, "source": "tushare"},
        {"ts_code": "000300.SH", "trade_date": "20240102", "open": 11, "high": 12, "low": 10, "close": 11, "source": "tushare"},
    ]
    count = repo.upsert_daily(records)
    assert count == 3
    assert repo.coll.bulk_write.called

def test_get_series_returns_series():
    repo = IndexRepository.__new__(IndexRepository)
    repo.coll = MagicMock()
    repo.coll.find.return_value.sort.return_value = [
        {"ts_code": "000300.SH", "trade_date": "20240101", "close": 11},
        {"ts_code": "000300.SH", "trade_date": "20240102", "close": 12},
    ]
    ser = repo.get_series("000300.SH", "20240101", "20240102")
    assert isinstance(ser, pd.Series)
    assert ser.iloc[0] == 11
    assert ser.iloc[1] == 12

def test_get_normalized_returns_normalized_series():
    repo = IndexRepository.__new__(IndexRepository)
    repo.get_series = MagicMock(return_value=pd.Series([10, 20], index=["20240101", "20240102"]))
    ser = repo.get_normalized("000300.SH", "20240101", "20240102")
    assert ser.iloc[0] == 1.0
    assert ser.iloc[1] == 2.0
