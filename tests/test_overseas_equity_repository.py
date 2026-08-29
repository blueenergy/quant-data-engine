from datetime import datetime
from unittest.mock import MagicMock

import pandas as pd

from quant_data_engine.repositories.overseas_equity_repository import OverseasEquityRepository


def _repo(market="hk"):
    repo = OverseasEquityRepository.__new__(OverseasEquityRepository)
    repo.market = market
    from quant_data_engine.repositories.overseas_equity_repository import MARKETS
    repo.spec = MARKETS[market]
    repo.info_col = MagicMock()
    repo.calendar_col = MagicMock()
    repo.daily_col = MagicMock()
    repo.adj_col = MagicMock()
    return repo


def _set_counts(col, upserted=1, modified=0):
    col.bulk_write.return_value.upserted_count = upserted
    col.bulk_write.return_value.modified_count = modified


def test_upsert_basic_keys_by_ts_code_and_sets_symbol():
    repo = _repo("hk")
    _set_counts(repo.info_col)
    df = pd.DataFrame(
        {
            "ts_code": ["00700.HK"],
            "name": ["腾讯控股"],
            "cn_spell": ["txkg"],
            "list_status": ["L"],
        }
    )
    n = repo.upsert_basic(df)
    assert n == 1
    op = repo.info_col.bulk_write.call_args[0][0][0]
    assert op._filter == {"ts_code": "00700.HK"}
    doc = op._doc["$set"]
    assert doc["symbol"] == "00700.HK"
    assert doc["pinyin"] == "TXKG"
    assert doc["market"] == "HK"
    assert doc["source"] == "tushare_hk_basic"
    assert isinstance(doc["updated_at"], datetime)


def test_upsert_calendar_aliases_trade_date():
    repo = _repo("us")
    _set_counts(repo.calendar_col)
    df = pd.DataFrame({"cal_date": ["20260828"], "is_open": [1], "pretrade_date": ["20260827"]})
    repo.upsert_calendar(df)
    op = repo.calendar_col.bulk_write.call_args[0][0][0]
    assert op._filter == {"cal_date": "20260828"}
    doc = op._doc["$set"]
    assert doc["trade_date"] == "20260828"
    assert doc["exchange"] == "US"
    assert doc["is_open"] == 1


def test_upsert_daily_maps_vol_and_pct_change():
    repo = _repo("us")
    _set_counts(repo.daily_col)
    df = pd.DataFrame(
        {
            "ts_code": ["AAPL"],
            "trade_date": ["20260828"],
            "open": [220.0],
            "high": [221.0],
            "low": [219.0],
            "close": [220.5],
            "vol": [1000],
            "pct_change": [1.2],
            "amount": [1.5e6],
        }
    )
    repo.upsert_daily(df)
    op = repo.daily_col.bulk_write.call_args[0][0][0]
    assert op._filter == {"symbol": "AAPL", "trade_date": "20260828"}
    doc = op._doc["$set"]
    assert doc["volume"] == 1000
    assert doc["pct_chg"] == 1.2
    assert doc["source"] == "tushare_us_daily"


def test_upsert_adj_accepts_cum_adjfactor():
    repo = _repo("hk")
    _set_counts(repo.adj_col)
    df = pd.DataFrame(
        {"ts_code": ["00700.HK"], "trade_date": ["20260828"], "cum_adjfactor": [1.05]}
    )
    repo.upsert_adj(df)
    op = repo.adj_col.bulk_write.call_args[0][0][0]
    assert op._filter == {"symbol": "00700.HK", "trade_date": "20260828"}
    assert op._doc["$set"]["adj_factor"] == 1.05


def test_daily_counts_by_date_groups():
    repo = _repo("hk")
    repo.daily_col.aggregate.return_value = [{"_id": "20260828", "n": 2700}]
    counts = repo.daily_counts_by_date(["20260828", "20260829"])
    assert counts == {"20260828": 2700, "20260829": 0}
    repo.daily_col.aggregate.assert_called_once()


def test_upsert_basic_skips_empty():
    repo = _repo()
    assert repo.upsert_basic(None) == 0
    assert repo.upsert_basic(pd.DataFrame()) == 0
    repo.info_col.bulk_write.assert_not_called()
