"""Tests for FutRepository upsert."""
from __future__ import annotations

from unittest.mock import MagicMock

from quant_data_engine.repositories.fut_repository import FutRepository


def test_fut_repository_upsert_daily_builds_bulk_ops():
    repo = FutRepository("mongodb://localhost:27017", "quant_data")
    mock_coll = MagicMock()
    mock_result = MagicMock()
    mock_result.upserted_count = 1
    mock_result.modified_count = 0
    mock_coll.bulk_write.return_value = mock_result
    repo.daily_coll = mock_coll

    records = [
        {
            "ts_code": "IF.CFX",
            "trade_date": "20240102",
            "open": 1.0,
            "high": 2.0,
            "low": 0.5,
            "close": 1.5,
            "vol": 10.0,
            "source": "tushare",
        }
    ]
    n = repo.upsert_daily(records)
    assert n == 1
    mock_coll.bulk_write.assert_called_once()
    ops = mock_coll.bulk_write.call_args[0][0]
    assert len(ops) == 1
