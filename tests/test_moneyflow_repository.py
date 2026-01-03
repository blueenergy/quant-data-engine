from unittest.mock import MagicMock
import pandas as pd
from quant_data_engine.repositories.moneyflow_repository import MoneyFlowRepository


def make_repo():
    repo = MoneyFlowRepository.__new__(MoneyFlowRepository)
    repo.coll = MagicMock()
    repo.state_coll = MagicMock()
    return repo


def test_upsert_moneyflow_symbol_fallback_and_counts():
    repo = make_repo()
    repo.coll.bulk_write.return_value.upserted_count = 1
    repo.coll.bulk_write.return_value.modified_count = 1
    records = [{"ts_code": "000001.SZ", "trade_date": "20240101", "buy_sm_vol": 1.0}]
    count = repo.upsert_moneyflow(records)
    assert count == 2
    op = repo.coll.bulk_write.call_args[0][0][0]
    assert op._filter == {"symbol": "000001", "trade_date": "20240101"}


def test_state_updates():
    repo = make_repo()
    repo.state_coll.find_one.return_value = {"last_date": "20240102"}
    repo.update_last_sync_date("20240103")
    repo.state_coll.update_one.assert_called_once()
    assert repo.get_last_sync_date() == "20240102"
