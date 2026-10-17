"""SW membership is a stint table, not one row per stock.

Tushare's ``index_member_all`` returns the current assignment by default and the
departed ones under ``is_new='N'``. The original ``(con_code, src)`` key could
only ever hold the current row, which is why ``out_date`` was empty for every
document and no historical membership existed to join against.
"""

from unittest.mock import MagicMock

from quant_data_engine.repositories.sw_industry_repository import SwIndustryRepository


def make_repo():
    """Skip ``__init__`` so no index is built against a live collection."""
    repo = SwIndustryRepository.__new__(SwIndustryRepository)
    for name in (
        "classify_coll",
        "member_coll",
        "daily_coll",
        "weekly_coll",
        "monthly_coll",
        "meta_coll",
    ):
        setattr(repo, name, MagicMock())
    return repo


def _ops(coll):
    return coll.bulk_write.call_args[0][0]


def _stint(con_code, in_date, out_date, is_new, l1_code):
    return {
        "con_code": con_code,
        "in_date": in_date,
        "out_date": out_date,
        "is_new": is_new,
        "l1_code": l1_code,
        "l1_name": "x",
    }


def test_two_stints_of_the_same_stock_land_on_distinct_keys():
    repo = make_repo()
    repo.upsert_members(
        [
            _stint("000004.SZ", "20081231", "20210610", "N", "801150.SI"),
            _stint("000004.SZ", "20210611", None, "Y", "801750.SI"),
        ],
        src="SW2021",
    )
    filters = [op._filter for op in _ops(repo.member_coll)]
    assert filters == [
        {"con_code": "000004.SZ", "src": "SW2021", "in_date": "20081231"},
        {"con_code": "000004.SZ", "src": "SW2021", "in_date": "20210611"},
    ]


def test_repeated_stint_is_idempotent():
    repo = make_repo()
    stint = _stint("000004.SZ", "20210611", None, "Y", "801750.SI")
    repo.upsert_members([stint, dict(stint)], src="SW2021")
    filters = [op._filter for op in _ops(repo.member_coll)]
    assert filters[0] == filters[1]


def test_rows_without_in_date_are_skipped_rather_than_merged():
    """An undated row must not overwrite a real stint."""
    repo = make_repo()
    repo.upsert_members(
        [
            _stint("000004.SZ", "20210611", None, "Y", "801750.SI"),
            {"con_code": "000004.SZ", "l1_code": "801150.SI"},
        ],
        src="SW2021",
    )
    assert len(_ops(repo.member_coll)) == 1


def test_ensure_indexes_replaces_the_legacy_con_src_unique_index():
    repo = make_repo()
    repo.member_coll.index_information.return_value = {
        "_id_": {"key": [("_id", 1)]},
        "sw_m_con_src": {"key": [("con_code", 1), ("src", 1)], "unique": True},
    }
    repo._ensure_indexes()

    repo.member_coll.drop_index.assert_called_once_with("sw_m_con_src")
    created = {call.kwargs.get("name") for call in repo.member_coll.create_index.call_args_list}
    assert "sw_m_con_src_in" in created
    stint_call = next(
        call for call in repo.member_coll.create_index.call_args_list
        if call.kwargs.get("name") == "sw_m_con_src_in"
    )
    assert stint_call.kwargs.get("unique") is True
    assert stint_call.args[0] == [("con_code", 1), ("src", 1), ("in_date", 1)]
    # The new key must be in place before the old guarantee is dropped.
    calls = repo.member_coll.method_calls
    create_idx = next(i for i, c in enumerate(calls) if c[0] == "create_index" and c[2].get("name") == "sw_m_con_src_in")
    drop_idx = next(i for i, c in enumerate(calls) if c[0] == "drop_index")
    assert create_idx < drop_idx


def test_ensure_indexes_is_idempotent_when_legacy_index_is_gone():
    repo = make_repo()
    repo.member_coll.index_information.return_value = {
        "_id_": {"key": [("_id", 1)]},
        "sw_m_con_src_in": {"key": [("con_code", 1), ("src", 1), ("in_date", 1)], "unique": True},
    }
    repo._ensure_indexes()
    repo.member_coll.drop_index.assert_not_called()
