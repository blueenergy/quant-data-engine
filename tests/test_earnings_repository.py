"""Regression tests for earnings announcement versioning.

A company can announce the same report period more than once: the first
预告/快报 plus later revisions, distinguishable only by ``ann_date``. The
original ``(ts_code, end_date)`` key made revisions unrepresentable — the
second announcement overwrote the first and the original value was gone.
"""

from unittest.mock import MagicMock

from quant_data_engine.repositories.earnings_repository import EarningsRepository


def make_repo():
    """Skip ``__init__`` so no index is built against a live collection."""
    repo = EarningsRepository.__new__(EarningsRepository)
    repo._forecast_col = MagicMock()
    repo._forecast_col.name = "earnings_forecast"
    repo._express_col = MagicMock()
    repo._express_col.name = "earnings_express"
    repo._disclosure_col = MagicMock()
    repo._disclosure_col.name = "earnings_disclosure"
    repo._report_rc_col = MagicMock()
    repo._report_rc_col.name = "earnings_report_rc"
    return repo


def _ops(col):
    return col.bulk_write.call_args[0][0]


def test_revision_upserts_under_its_own_ann_date_key():
    """同一报告期的两条公告必须落在两个不同的 upsert filter 上。

    只按 (ts_code, end_date) 过滤时，第二条会把第一条 $set 覆盖掉，修正公告
    永远取不到 —— 而「首亏 → 预减」这类翻转正是最强的 surprise 来源。
    """
    repo = make_repo()
    repo.upsert_forecasts(
        [
            {
                "ts_code": "000007.SZ",
                "end_date": "20251231",
                "ann_date": "20260128",
                "type": "首亏",
            },
            {
                "ts_code": "000007.SZ",
                "end_date": "20251231",
                "ann_date": "20260422",
                "type": "预减",
            },
        ]
    )

    assert [op._filter for op in _ops(repo._forecast_col)] == [
        {"ts_code": "000007.SZ", "end_date": "20251231", "ann_date": "20260128"},
        {"ts_code": "000007.SZ", "end_date": "20251231", "ann_date": "20260422"},
    ]


def test_duplicate_rows_in_one_batch_collapse_to_one_op():
    """批内同键必须先合并。

    ``ordered=False`` 下单个重复键会让整批 bulk_write 抛异常，而异常被
    except 吞成「写入 0 条」—— 表现为整批静默丢失，不是只丢那一条。
    """
    repo = make_repo()
    repo.upsert_express(
        [
            {"ts_code": "600000.SH", "end_date": "20251231", "ann_date": "20260128", "n_income": 1},
            {"ts_code": "600000.SH", "end_date": "20251231", "ann_date": "20260128", "n_income": 2},
        ]
    )

    ops = _ops(repo._express_col)
    assert len(ops) == 1
    # 同键重复时后者胜出，与 Mongo 对同一 filter 的「后写覆盖」一致
    assert ops[0]._doc["$set"]["n_income"] == 2


def test_nan_key_fields_are_normalised_in_filter_and_document():
    """filter 洗成空串而文档仍留 nan，下次同样的 upsert 会再插一份。"""
    repo = make_repo()
    repo.upsert_forecasts(
        [{"ts_code": "600000.SH", "end_date": "20251231", "ann_date": float("nan")}]
    )

    op = _ops(repo._forecast_col)[0]
    assert op._filter["ann_date"] == ""
    assert op._doc["$set"]["ann_date"] == ""


def test_legacy_unique_index_is_dropped():
    """旧唯一索引留着的话，Mongo 照样拒收第二条版本，改 upsert filter 也没用。"""
    col = MagicMock()
    col.name = "earnings_forecast"
    col.index_information.return_value = {
        "_id_": {"key": [("_id", 1)]},
        "ts_code_1_end_date_-1": {"key": [("ts_code", 1), ("end_date", -1)], "unique": True},
    }

    EarningsRepository._drop_legacy_unique_index(col, "ts_code_1_end_date_-1")

    col.drop_index.assert_called_once_with("ts_code_1_end_date_-1")


def test_drop_legacy_index_is_a_noop_when_absent():
    col = MagicMock()
    col.name = "earnings_forecast"
    col.index_information.return_value = {"_id_": {"key": [("_id", 1)]}}

    EarningsRepository._drop_legacy_unique_index(col, "ts_code_1_end_date_-1")

    col.drop_index.assert_not_called()


def test_one_broker_report_keeps_every_forecast_year():
    """一份研报预测三年 → 三行只差 ``quarter``，必须落在三个不同的 filter 上。

    旧键丢掉 ``quarter``，同组三行互相 $set 覆盖，批内最后一行胜出 —— 而
    Tushare 按预测年份由近到远返回，所以留下的是**最远那一年**。实测整个
    ``earnings_report_rc`` 有 70% 是 2028Q4，按 quarter 聚合的一致预期因此取不到。
    """
    repo = make_repo()
    repo.upsert_report_rc(
        [
            {"ts_code": "000036.SZ", "report_date": "20260827", "org_name": "华泰证券", "quarter": "2026Q4", "np": 9250},
            {"ts_code": "000036.SZ", "report_date": "20260827", "org_name": "华泰证券", "quarter": "2027Q4", "np": 18760},
            {"ts_code": "000036.SZ", "report_date": "20260827", "org_name": "华泰证券", "quarter": "2028Q4", "np": 19421},
        ]
    )

    ops = _ops(repo._report_rc_col)
    assert [op._filter["quarter"] for op in ops] == ["2026Q4", "2027Q4", "2028Q4"]
    # 三年各自的预测值都留下来，而不是只剩最远那年
    assert sorted(op._doc["$set"]["np"] for op in ops) == [9250, 18760, 19421]


def test_legacy_report_rc_index_is_dropped():
    """旧三键唯一索引留着的话，第 2/3 个预测年份照样被 Mongo 拒 —— 且是静默的。"""
    repo = make_repo()
    repo._report_rc_col.index_information.return_value = {
        "_id_": {"key": [("_id", 1)]},
        "ts_code_1_report_date_-1_org_name_1": {
            "key": [("ts_code", 1), ("report_date", -1), ("org_name", 1)],
            "unique": True,
        },
    }

    repo._ensure_indexes()

    repo._report_rc_col.drop_index.assert_called_once_with("ts_code_1_report_date_-1_org_name_1")
    repo._report_rc_col.create_index.assert_any_call(
        [("ts_code", 1), ("report_date", -1), ("org_name", 1), ("quarter", 1)], unique=True
    )


def test_nan_org_name_and_quarter_are_normalised():
    """``to_dict("records")`` 会把缺失的 org_name/quarter 带成 nan。"""
    repo = make_repo()
    repo.upsert_report_rc(
        [{"ts_code": "000036.SZ", "report_date": "20260827", "org_name": float("nan"), "quarter": None}]
    )

    op = _ops(repo._report_rc_col)[0]
    assert op._filter == {
        "ts_code": "000036.SZ",
        "report_date": "20260827",
        "org_name": "",
        "quarter": "",
    }


def test_ensure_indexes_versions_forecast_and_express_only():
    repo = make_repo()
    repo._forecast_col.index_information.return_value = {
        "ts_code_1_end_date_-1": {"key": [("ts_code", 1), ("end_date", -1)]}
    }
    repo._express_col.index_information.return_value = {}

    repo._ensure_indexes()

    for col in (repo._forecast_col, repo._express_col):
        col.create_index.assert_any_call(
            [("ts_code", 1), ("end_date", -1), ("ann_date", -1)], unique=True
        )
    repo._forecast_col.drop_index.assert_called_once_with("ts_code_1_end_date_-1")

    # 披露计划每期只有一条当前状态，没有多版本，键保持不变
    repo._disclosure_col.create_index.assert_any_call(
        [("ts_code", 1), ("end_date", -1)], unique=True
    )
    repo._disclosure_col.drop_index.assert_not_called()
