from quant_data_engine.tasks.run_market_breadth_daily import (
    _date_range,
    _has_volume_price_rows,
    run,
)


class _FakeCollection:
    def __init__(self, dates_with_rows):
        self._dates = set(dates_with_rows)

    def find_one(self, query, projection=None):
        trade_date = query["trade_date"]
        return {"_id": 1} if trade_date in self._dates else None


class _FakeDB:
    def __init__(self, dates_with_rows):
        self._coll = _FakeCollection(dates_with_rows)

    def __getitem__(self, name):
        assert name == "volume_price"
        return self._coll


class _FakeRepo:
    def __init__(self, dates_with_rows):
        self.db = _FakeDB(dates_with_rows)
        self.upserted = []

    def upsert_breadth(self, doc):
        self.upserted.append(doc)
        return True


class _FakeBuilder:
    def __init__(self):
        self.built = []

    def build(self, trade_date, *, warnings=None):
        self.built.append(trade_date)
        return {
            "trade_date": trade_date,
            "total_normal": 4000,
            "advances": 2500,
            "declines": 1400,
            "unchanged": 100,
            "new_high_count": 30,
            "new_low_count": 12,
        }


def test_date_range_skips_weekends():
    # 20250103 is Friday, 20250104/05 weekend, 20250106 Monday
    dates = _date_range("20250103", "20250106")
    assert dates == ["20250103", "20250106"]


def test_has_volume_price_rows():
    repo = _FakeRepo({"20250103"})
    assert _has_volume_price_rows(repo, "20250103") is True
    assert _has_volume_price_rows(repo, "20250104") is False


def test_run_skips_dates_without_volume_price():
    repo = _FakeRepo({"20250103"})
    builder = _FakeBuilder()
    summary = run(["20250103", "20250104"], builder, repo, dry_run=False, verbose=False)

    assert summary == {"ok": 1, "skipped": 1, "errors": 0, "records_changed": 1}
    assert builder.built == ["20250103"]
    assert len(repo.upserted) == 1
    assert repo.upserted[0]["trade_date"] == "20250103"


def test_run_dry_run_does_not_upsert():
    repo = _FakeRepo({"20250103"})
    builder = _FakeBuilder()
    summary = run(["20250103"], builder, repo, dry_run=True, verbose=True)

    assert summary == {"ok": 1, "skipped": 0, "errors": 0, "records_changed": 0}
    assert builder.built == ["20250103"]
    assert repo.upserted == []
