from datetime import datetime
from unittest.mock import MagicMock

import pandas as pd

from quant_data_engine.repositories.shareholder_repository import ShareholderRepository


def make_repo():
    repo = ShareholderRepository.__new__(ShareholderRepository)
    repo.holdernumber_col = MagicMock()
    repo.holdertrade_col = MagicMock()
    repo.share_float_col = MagicMock()
    repo.repurchase_col = MagicMock()
    repo.hk_hold_col = MagicMock()
    return repo


def _set_counts(col, upserted=1, modified=0):
    col.bulk_write.return_value.upserted_count = upserted
    col.bulk_write.return_value.modified_count = modified


def test_holdernumber_keyed_by_ts_code_and_end_date():
    repo = make_repo()
    _set_counts(repo.holdernumber_col)
    df = pd.DataFrame({"ts_code": ["300196.SZ"], "end_date": ["20260331"], "holder_num": [26237]})
    n = repo.upsert_holdernumber(df)
    assert n == 1
    op = repo.holdernumber_col.bulk_write.call_args[0][0][0]
    assert op._filter == {"ts_code": "300196.SZ", "end_date": "20260331"}
    assert op._doc["$set"]["data_source"] == "tushare"
    assert isinstance(op._doc["$set"]["synced_at"], datetime)


def test_holdertrade_exact_dupes_collapse_to_single_hash_op():
    repo = make_repo()
    _set_counts(repo.holdertrade_col, upserted=1, modified=0)
    df = pd.DataFrame(
        {
            "ts_code": ["300196.SZ", "300196.SZ"],
            "ann_date": ["20260430", "20260430"],
            "holder_name": ["周元龙", "周元龙"],
            "in_de": ["DE", "DE"],
            "change_vol": [30700.0, 30700.0],
        }
    )
    repo.upsert_holdertrade(df)
    ops = repo.holdertrade_col.bulk_write.call_args[0][0]
    # Exact-duplicate rows hash to the same _uid -> de-duped to one op.
    assert len(ops) == 1
    assert set(ops[0]._filter.keys()) == {"_uid"}
    assert ops[0]._doc["$set"]["_uid"] == ops[0]._filter["_uid"]


def test_holdertrade_distinct_rows_get_distinct_hashes():
    repo = make_repo()
    _set_counts(repo.holdertrade_col, upserted=2, modified=0)
    # Same (ts_code, ann_date, holder, in_de, change_vol) but different avg_price
    # -> previously collided; content hash keeps them distinct.
    df = pd.DataFrame(
        {
            "ts_code": ["300196.SZ", "300196.SZ"],
            "ann_date": ["20260430", "20260430"],
            "holder_name": ["周元龙", "周元龙"],
            "in_de": ["DE", "DE"],
            "change_vol": [30700.0, 30700.0],
            "avg_price": [19.5, 20.1],
        }
    )
    repo.upsert_holdertrade(df)
    ops = repo.holdertrade_col.bulk_write.call_args[0][0]
    assert len(ops) == 2
    assert ops[0]._filter["_uid"] != ops[1]._filter["_uid"]


def test_share_float_nan_field_stored_as_none_and_hash_keyed():
    repo = make_repo()
    _set_counts(repo.share_float_col)
    df = pd.DataFrame(
        {
            "ts_code": ["300196.SZ"],
            "float_date": ["20270517"],
            "holder_name": [float("nan")],
            "share_type": ["首发战略配售股份"],
            "float_share": [30000.0],
        }
    )
    repo.upsert_share_float(df)
    op = repo.share_float_col.bulk_write.call_args[0][0][0]
    assert set(op._filter.keys()) == {"_uid"}
    assert op._doc["$set"]["holder_name"] is None


def test_repurchase_hash_keyed_and_idempotent():
    repo = make_repo()
    _set_counts(repo.repurchase_col)
    df = pd.DataFrame({"ts_code": ["300196.SZ"], "ann_date": ["20250117"], "proc": ["完成"], "amount": [52994701.54]})
    repo.upsert_repurchase(df)
    op = repo.repurchase_col.bulk_write.call_args[0][0][0]
    assert set(op._filter.keys()) == {"_uid"}


def test_hk_hold_keyed_by_ts_code_and_trade_date():
    repo = make_repo()
    _set_counts(repo.hk_hold_col)
    df = pd.DataFrame({"ts_code": ["300196.SZ"], "trade_date": ["20260331"], "vol": [5559791], "ratio": [1.36]})
    repo.upsert_hk_hold(df)
    op = repo.hk_hold_col.bulk_write.call_args[0][0][0]
    assert op._filter == {"ts_code": "300196.SZ", "trade_date": "20260331"}


def test_empty_dataframe_is_noop():
    repo = make_repo()
    assert repo.upsert_holdernumber(pd.DataFrame()) == 0
    assert repo.upsert_hk_hold(None) == 0
    repo.holdernumber_col.bulk_write.assert_not_called()
