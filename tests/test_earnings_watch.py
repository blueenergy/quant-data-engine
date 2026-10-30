"""Unit tests for the watchlist/holdings earnings watcher."""

from unittest.mock import MagicMock

from quant_data_engine.tasks.run_earnings_watch import (
    build_digest,
    enqueue_task,
    idempotency_key,
    is_equity,
    normalize_symbol,
    report_kind,
)


class _FakeDb(dict):
    def __getitem__(self, item):
        return super().__getitem__(item)


def _tasks_col(existing=None):
    col = MagicMock()
    col.find_one.return_value = existing
    return col


def test_normalize_symbol_accepts_bare_and_lowercase():
    assert normalize_symbol("600048") == "600048.SH"
    assert normalize_symbol("000001.sz") == "000001.SZ"
    assert normalize_symbol(" 300750.SZ ") == "300750.SZ"
    assert normalize_symbol("") == ""


def test_is_equity_filters_etf_and_funds():
    assert is_equity("600048.SH")
    assert is_equity("002050.SZ")
    assert is_equity("830799.BJ")
    # 159766/513260 are ETFs that live in the watchlist and have no statements
    assert not is_equity("159766.SZ")
    assert not is_equity("513260.SH")
    assert not is_equity("510300.SH")


def test_report_kind_maps_period_month():
    assert report_kind("20260930") == "三季报"
    assert report_kind("20260630") == "半年报"
    assert report_kind("20261231") == "年报"
    assert report_kind("20260331") == "一季报"
    assert report_kind("20260115") == "定期报告"


def test_enqueue_skips_when_active_task_exists():
    finance_db = _FakeDb(analysis_tasks=_tasks_col({"status": "completed"}))
    assert enqueue_task(
        finance_db,
        ts_code="600048.SH",
        end_date="20260930",
        ann_date="20261029",
        stock={"name": "保利发展"},
        sources={"watchlist"},
    ) is None
    finance_db["analysis_tasks"].insert_one.assert_not_called()


def test_enqueue_retries_after_failed_task():
    col = _tasks_col(None)
    col.insert_one.return_value = MagicMock(inserted_id="task-1")
    finance_db = _FakeDb(analysis_tasks=col)

    task_id = enqueue_task(
        finance_db,
        ts_code="600048.SH",
        end_date="20260930",
        ann_date="20261029",
        stock={"name": "保利发展"},
        sources={"watchlist", "position"},
    )

    assert task_id == "task-1"
    doc = col.insert_one.call_args[0][0]
    assert doc["type"] == "earnings_report_analysis"
    assert doc["idempotency_key"] == idempotency_key("600048.SH", "20260930")
    assert doc["params"]["report_kind"] == "三季报"
    assert doc["params"]["sources"] == ["position", "watchlist"]


def test_build_digest_contains_core_numbers():
    text = build_digest(
        [
            {
                "ts_code": "600048.SH",
                "name": "保利发展",
                "report_kind": "三季报",
                "score": 6,
                "beat_miss": "符合预期",
                "verdict": "利润企稳",
                "key_numbers": {
                    "revenue": 102_860_000_000,
                    "revenue_yoy": -11.98,
                    "net_profit": 1_958_000_000,
                    "net_profit_yoy": -40.84,
                    "deduct_net_profit_yoy": -41.26,
                },
                "risk_flags": ["合同负债下滑"],
            }
        ],
        "20261030",
    )
    assert "财报雷达 20261030" in text
    assert "保利发展" in text
    assert "1028.60亿" in text
    assert "-12.0%" in text
    assert "合同负债下滑" in text
