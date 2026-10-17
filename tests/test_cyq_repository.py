from unittest.mock import MagicMock

from quant_data_engine.repositories.cyq_repository import CyqRepository


def make_repo():
    repo = CyqRepository.__new__(CyqRepository)
    repo.coll = MagicMock()
    repo.state_coll = MagicMock()
    return repo


def test_upsert_cyq_perf_key_and_counts():
    repo = make_repo()
    repo.coll.bulk_write.return_value.upserted_count = 1
    repo.coll.bulk_write.return_value.modified_count = 1
    records = [{"ts_code": "000001.SZ", "trade_date": "20240101", "winner_rate": 83.11}]
    count = repo.upsert_cyq_perf(records)
    assert count == 2
    op = repo.coll.bulk_write.call_args[0][0][0]
    assert op._filter == {"symbol": "000001.SZ", "trade_date": "20240101"}


def test_upsert_skips_records_without_key():
    repo = make_repo()
    repo.coll.bulk_write.return_value.upserted_count = 0
    repo.coll.bulk_write.return_value.modified_count = 0
    count = repo.upsert_cyq_perf([{"trade_date": "20240101"}])
    assert count == 0
    repo.coll.bulk_write.assert_not_called()


def test_state_updates():
    repo = make_repo()
    repo.state_coll.find_one.return_value = {"last_date": "20240102"}
    repo.update_last_sync_date("20240103")
    repo.state_coll.update_one.assert_called_once()
    assert repo.get_last_sync_date() == "20240102"
