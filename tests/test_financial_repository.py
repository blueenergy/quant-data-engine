from datetime import datetime
from unittest.mock import MagicMock
import pandas as pd
from quant_data_engine.repositories.financial_repository import FinancialRepository


def make_repo():
    repo = FinancialRepository.__new__(FinancialRepository)
    repo.income_coll = MagicMock()
    repo.balance_coll = MagicMock()
    repo.cashflow_coll = MagicMock()
    repo.indicator_coll = MagicMock()
    repo.dividend_coll = MagicMock()
    repo.mainbz_coll = MagicMock()
    return repo


def test_upsert_income_dataframe_sets_synced_at_and_none():
    repo = make_repo()
    df = pd.DataFrame({"ts_code": ["000001.SZ"], "end_date": ["20231231"], "revenue": [None]})
    repo.income_coll.bulk_write.return_value.upserted_count = 1
    repo.income_coll.bulk_write.return_value.modified_count = 0
    count = repo.upsert_income(df=df)
    assert count == 1
    op = repo.income_coll.bulk_write.call_args[0][0][0]
    payload = op._doc["$set"]
    assert payload["revenue"] is None
    assert payload["data_source"] == "tushare"
    assert payload.get("report_type") == 1
    assert isinstance(payload["synced_at"], datetime)
    filt = op._filter
    assert filt.get("report_type") == 1


def test_upsert_indicator_records_counts():
    repo = make_repo()
    repo.indicator_coll.bulk_write.return_value.upserted_count = 0
    repo.indicator_coll.bulk_write.return_value.modified_count = 2
    records = [
        {"ts_code": "000001.SZ", "end_date": "20231231", "roe": 1.0},
        {"ts_code": "000002.SZ", "end_date": "20231231", "roe": 2.0},
    ]
    count = repo.upsert_indicators(records=records)
    assert count == 2


def test_upsert_dividend_dataframe_uses_stable_key():
    repo = make_repo()
    repo.dividend_coll.bulk_write.return_value.upserted_count = 1
    repo.dividend_coll.bulk_write.return_value.modified_count = 0
    df = pd.DataFrame({
        "ts_code": ["600132.SH"],
        "end_date": ["20251231"],
        "ann_date": ["20260320"],
        "div_proc": ["实施"],
        "ex_date": ["20260601"],
        "cash_div_tax": [2.8],
    })

    count = repo.upsert_dividends(df=df)

    assert count == 1
    op = repo.dividend_coll.bulk_write.call_args[0][0][0]
    assert op._filter == {
        "ts_code": "600132.SH",
        "end_date": "20251231",
        "ann_date": "20260320",
        "div_proc": "实施",
        "ex_date": "20260601",
    }
    assert op._doc["$set"]["cash_div_tax"] == 2.8


def test_upsert_mainbz_dataframe_uses_segment_key():
    repo = make_repo()
    repo.mainbz_coll.bulk_write.return_value.upserted_count = 1
    repo.mainbz_coll.bulk_write.return_value.modified_count = 0
    df = pd.DataFrame({
        "ts_code": ["000001.SZ"],
        "end_date": ["20231231"],
        "bz_type": ["P"],
        "bz_item": ["智能手机"],
        "bz_sales": [None],
        "bz_profit": [1234.5],
    })

    count = repo.upsert_mainbz(df=df)

    assert count == 1
    op = repo.mainbz_coll.bulk_write.call_args[0][0][0]
    assert op._filter == {
        "ts_code": "000001.SZ",
        "end_date": "20231231",
        "bz_type": "P",
        "bz_item": "智能手机",
    }
    payload = op._doc["$set"]
    assert payload["bz_sales"] is None
    assert payload["bz_profit"] == 1234.5
    assert payload["data_source"] == "tushare"
    assert isinstance(payload["synced_at"], datetime)


def test_upsert_mainbz_skips_rows_missing_key_fields():
    repo = make_repo()
    repo.mainbz_coll.bulk_write.return_value.upserted_count = 1
    repo.mainbz_coll.bulk_write.return_value.modified_count = 0
    df = pd.DataFrame({
        "ts_code": ["000001.SZ", "000001.SZ", "000001.SZ"],
        "end_date": ["20231231", "20231231", None],
        "bz_type": ["P", "P", "P"],
        "bz_item": ["智能手机", None, "服务"],
        "bz_sales": [100.0, 200.0, 300.0],
    })

    count = repo.upsert_mainbz(df=df)

    assert count == 1
    ops = repo.mainbz_coll.bulk_write.call_args[0][0]
    # 仅第一行合法，缺 bz_item / end_date 的两行被跳过
    assert len(ops) == 1
    assert ops[0]._filter == {
        "ts_code": "000001.SZ",
        "end_date": "20231231",
        "bz_type": "P",
        "bz_item": "智能手机",
    }


def test_upsert_mainbz_returns_zero_when_all_rows_invalid():
    repo = make_repo()
    df = pd.DataFrame({
        "ts_code": ["000001.SZ"],
        "end_date": ["20231231"],
        "bz_type": ["P"],
        "bz_item": [None],
        "bz_sales": [100.0],
    })

    count = repo.upsert_mainbz(df=df)

    assert count == 0
    repo.mainbz_coll.bulk_write.assert_not_called()
