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
    assert isinstance(payload["synced_at"], datetime)


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
