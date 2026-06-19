import math
from unittest.mock import MagicMock
import pandas as pd
from quant_data_engine.repositories.stock_repository import StockRepository


def make_repo():
    repo = StockRepository.__new__(StockRepository)
    repo.volume_price_coll = MagicMock()
    repo.sync_volume_price_coll = None
    repo.sync_realtime_latest_coll = None
    repo.daily_basic_coll = MagicMock()
    return repo


def test_upsert_daily_cleans_nan_and_sets_symbol():
    repo = make_repo()
    repo.volume_price_coll.bulk_write.return_value.upserted_count = 1
    repo.volume_price_coll.bulk_write.return_value.modified_count = 0
    records = [{"ts_code": "000001.SZ", "trade_date": "20240101", "open": math.nan, "high": 2, "low": 1, "close": 1}]
    count = repo.upsert_daily(records)
    assert count == 1
    key, payload = repo.volume_price_coll.bulk_write.call_args[0][0][0]._filter, repo.volume_price_coll.bulk_write.call_args[0][0][0]._doc["$set"]
    assert key == {"symbol": "000001.SZ", "trade_date": "20240101"}
    assert payload["open"] is None


def test_upsert_daily_mirrors_to_sync_collection():
    repo = make_repo()
    repo.sync_volume_price_coll = MagicMock()
    repo.volume_price_coll.bulk_write.return_value.upserted_count = 1
    repo.volume_price_coll.bulk_write.return_value.modified_count = 0
    records = [{"ts_code": "000001.SZ", "trade_date": "20240101", "open": 1, "high": 2, "low": 1, "close": 1}]

    count = repo.upsert_daily(records)

    assert count == 1
    repo.volume_price_coll.bulk_write.assert_called_once()
    repo.sync_volume_price_coll.bulk_write.assert_called_once()


def test_upsert_realtime_latest_omits_universe_type_when_absent():
    repo = StockRepository.__new__(StockRepository)
    repo.realtime_latest_coll = MagicMock()
    repo.sync_realtime_latest_coll = None
    repo.realtime_latest_coll.bulk_write.return_value.upserted_count = 1
    repo.realtime_latest_coll.bulk_write.return_value.modified_count = 0
    records = [{"symbol": "000001.SZ", "trade_date": "20240101", "open": 1, "high": 2, "low": 1, "close": 1}]

    repo.upsert_realtime_latest(records)

    payload = repo.realtime_latest_coll.bulk_write.call_args[0][0][0]._doc["$set"]
    assert "universe_type" not in payload


def test_upsert_realtime_latest_keeps_universe_type_when_present():
    repo = StockRepository.__new__(StockRepository)
    repo.realtime_latest_coll = MagicMock()
    repo.sync_realtime_latest_coll = None
    repo.realtime_latest_coll.bulk_write.return_value.upserted_count = 1
    repo.realtime_latest_coll.bulk_write.return_value.modified_count = 0
    records = [
        {
            "symbol": "000001.SZ",
            "trade_date": "20240101",
            "open": 1,
            "high": 2,
            "low": 1,
            "close": 1,
            "universe_type": "watchlist_plan_strategy",
        }
    ]

    repo.upsert_realtime_latest(records)

    payload = repo.realtime_latest_coll.bulk_write.call_args[0][0][0]._doc["$set"]
    assert payload["universe_type"] == "watchlist_plan_strategy"


def test_upsert_daily_basic_sets_end_date_and_counts():
    repo = make_repo()
    repo.daily_basic_coll.bulk_write.return_value.upserted_count = 0
    repo.daily_basic_coll.bulk_write.return_value.modified_count = 2
    df = pd.DataFrame({"ts_code": ["000001.SZ", "000002.SZ"], "trade_date": ["20240101", "20240101"], "pe": [1.0, math.nan]})
    records = df.to_dict(orient="records")
    count = repo.upsert_daily_basic(records)
    assert count == 2
    ops = repo.daily_basic_coll.bulk_write.call_args[0][0]
    assert ops[0]._filter == {"ts_code": "000001.SZ", "end_date": "20240101"}
    payload = ops[1]._doc["$set"]
    assert payload["pe"] is None


def test_get_latest_trade_date_returns_sorted():
    repo = make_repo()
    repo.volume_price_coll.find_one.return_value = {"trade_date": "20240102"}
    assert repo.get_latest_trade_date() == "20240102"
