from quant_data_engine.tasks.run_holder_snapshot_sync import (
    _listed_codes,
    run_sync,
)


class _FakeStockInfo:
    def __init__(self, docs):
        self.docs = docs

    def find(self, query=None, projection=None, sort=None):
        return iter(self.docs)


class _FakeDB(dict):
    pass


class _FakeColl:
    def __init__(self, codes=None):
        self.codes = set(codes or [])

    def distinct(self, field, query=None):
        return list(self.codes)


class _FakeRepo:
    def __init__(self, existing_float=None, existing_holders=None):
        self.floatholders = _FakeColl(existing_float)
        self.holders = _FakeColl(existing_holders)
        self.float_calls = []
        self.holder_calls = []

    def upsert_top10_floatholders(self, code, period, df):
        self.float_calls.append((code, period, len(df)))
        return len(df)

    def upsert_top10_holders(self, code, period, df):
        self.holder_calls.append((code, period, len(df)))
        return len(df)


class _FakeAdapter:
    def __init__(self, float_responses=None, holder_responses=None):
        self.float_responses = float_responses or {}
        self.holder_responses = holder_responses or {}
        self.float_calls = []
        self.holder_calls = []

    def fetch_top10_floatholders(self, ts_code, period):
        self.float_calls.append((ts_code, period))
        result = self.float_responses.get(ts_code, [])
        if isinstance(result, Exception):
            raise result
        return result

    def fetch_top10_holders(self, ts_code, period):
        self.holder_calls.append((ts_code, period))
        result = self.holder_responses.get(ts_code, [])
        if isinstance(result, Exception):
            raise result
        return result


def test_listed_codes_filters_and_windows():
    data_db = _FakeDB(
        stock_info=_FakeStockInfo(
            [
                {"ts_code": "000001.SZ", "list_status": "L"},
                {"ts_code": "000002.SZ", "list_status": "D"},
                {"ts_code": "600000.SH", "list_status": "L"},
                {"ts_code": "00700.HK", "list_status": "L"},
                {"ts_code": "830001.BJ", "list_status": "L"},
                {"ts_code": "000001.SZ", "list_status": "L"},
            ]
        )
    )
    assert _listed_codes(data_db) == ["000001.SZ", "600000.SH", "830001.BJ"]
    assert _listed_codes(data_db, start_after="000001.SZ") == ["600000.SH", "830001.BJ"]
    assert _listed_codes(data_db, offset=1, limit=1) == ["600000.SH"]


def test_run_sync_upserts_and_counts_empty():
    adapter = _FakeAdapter(
        float_responses={
            "000001.SZ": [{"holder_name": "高盛"}],
            "000002.SZ": [],
        }
    )
    repo = _FakeRepo()
    stats = run_sync(
        adapter=adapter,
        repo=repo,
        codes=["000001.SZ", "000002.SZ"],
        period="20260930",
        progress_every=0,
    )
    assert stats["stocks_ok"] == 1
    assert stats["stocks_empty"] == 1
    assert stats["rows"] == 1
    assert stats["upserts"] == 1
    assert repo.float_calls == [("000001.SZ", "20260930", 1)]


def test_run_sync_skips_existing_codes():
    adapter = _FakeAdapter(
        float_responses={
            "000002.SZ": RuntimeError("must not be called"),
            "000003.SZ": [{"holder_name": "瑞银"}],
        }
    )
    repo = _FakeRepo(existing_float={"000001.SZ"})
    stats = run_sync(
        adapter=adapter,
        repo=repo,
        codes=["000001.SZ", "000002.SZ", "000003.SZ"],
        period="20260930",
        skip_existing=True,
        progress_every=0,
    )
    assert stats["stocks_skipped"] == 1
    assert stats["stocks_ok"] == 1
    assert stats["errors"] == ["000002.SZ: must not be called"]
    assert ("000001.SZ", "20260930") not in adapter.float_calls


def test_run_sync_continues_after_single_stock_error():
    adapter = _FakeAdapter(
        float_responses={
            "000001.SZ": RuntimeError("tushare timeout"),
            "000002.SZ": [{"holder_name": "社保基金"}],
        }
    )
    repo = _FakeRepo()
    stats = run_sync(
        adapter=adapter,
        repo=repo,
        codes=["000001.SZ", "000002.SZ"],
        period="20260930",
        progress_every=0,
    )
    assert len(stats["errors"]) == 1
    assert stats["stocks_ok"] == 1


def test_run_sync_kind_both_fetches_both_interfaces():
    adapter = _FakeAdapter(
        float_responses={"000001.SZ": [{"holder_name": "高盛"}]},
        holder_responses={"000001.SZ": [{"holder_name": "平安"}, {"holder_name": "社保"}]},
    )
    repo = _FakeRepo()
    stats = run_sync(
        adapter=adapter,
        repo=repo,
        codes=["000001.SZ"],
        period="20260930",
        kind="both",
        progress_every=0,
    )
    assert stats["rows"] == 3
    assert stats["upserts"] == 3
    assert repo.float_calls == [("000001.SZ", "20260930", 1)]
    assert repo.holder_calls == [("000001.SZ", "20260930", 2)]
