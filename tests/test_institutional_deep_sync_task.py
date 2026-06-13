from datetime import date
from types import SimpleNamespace

from quant_data_engine.tasks.run_institutional_deep_sync import (
    _batch_hit_limit,
    _final_run_status,
    _normalize_ts_code,
    _stock_info_codes,
)


class FakeCursor(list):
    def sort(self, *args, **kwargs):
        return self


class FakeCollection:
    def __init__(self, docs):
        self.docs = list(docs)

    def find(self, query=None, projection=None, sort=None):
        rows = list(self.docs)
        if sort:
            for field, direction in reversed(sort):
                rows.sort(key=lambda row: row.get(field) or "", reverse=direction < 0)
        return FakeCursor(rows)

    def find_one(self, query=None, projection=None, sort=None):
        rows = [doc for doc in self.docs if all(doc.get(k) == v for k, v in (query or {}).items())]
        if sort:
            for field, direction in reversed(sort):
                rows.sort(key=lambda row: row.get(field) or "", reverse=direction < 0)
        return dict(rows[0]) if rows else None


class FakeDB(dict):
    def __getitem__(self, key):
        return dict.__getitem__(self, key)


class FakeClient(dict):
    def __getitem__(self, key):
        return dict.__getitem__(self, key)


def test_normalize_ts_code_adds_exchange_suffix():
    assert _normalize_ts_code("300750") == "300750.SZ"
    assert _normalize_ts_code("600519") == "600519.SH"
    assert _normalize_ts_code("300750.sz") == "300750.SZ"


def test_stock_info_codes_filters_stale_and_batches(monkeypatch):
    client = FakeClient(
        quant_data=FakeDB(
            stock_info=FakeCollection(
                [
                    {"ts_code": "000001.SZ", "list_status": "L"},
                    {"ts_code": "300750.SZ", "list_status": "L"},
                    {"ts_code": "600519.SH", "list_status": "L"},
                    {"ts_code": "000002.SZ", "list_status": "D"},
                ]
            ),
            institutional_deep_snapshot=FakeCollection(
                [
                    {"ts_code": "000001.SZ", "as_of": "20260610"},
                    {"ts_code": "300750.SZ", "as_of": "20250501"},
                ]
            ),
        )
    )

    monkeypatch.setattr("quant_data_engine.tasks.run_institutional_deep_sync.MongoClient", lambda uri: client)
    settings = SimpleNamespace(mongo_uri="mongodb://fake", data_db="quant_data")
    args = SimpleNamespace(start_after="", stale_days=30, offset=0, limit=2)

    codes = _stock_info_codes(settings, args, date(2026, 6, 13))

    # 000001 has a fresh snapshot and is skipped; delisted 000002 is skipped.
    assert codes == ["300750.SZ", "600519.SH"]


def test_batch_limit_marks_run_partial():
    args = SimpleNamespace(from_stock_info=True, limit=2)

    assert _batch_hit_limit(args, ["300750.SZ", "600519.SH"]) is True
    status, error = _final_run_status(args, ["300750.SZ", "600519.SH"], 0)

    assert status == "partial"
    assert "stale stocks may remain" in error


def test_batch_below_limit_marks_run_complete():
    args = SimpleNamespace(from_stock_info=True, limit=3)

    status, error = _final_run_status(args, ["300750.SZ", "600519.SH"], 0)

    assert status == "complete"
    assert error is None
