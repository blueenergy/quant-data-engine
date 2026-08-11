import sys
import types

import pytest

from quant_data_engine.tasks import run_market_breadth_daily as breadth_task
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


# ── recorded status ───────────────────────────────────────────────────────────
# The upstream volume_price rows used to land hours after this job ran, so every
# run skipped, recorded "complete", and market_breadth_daily quietly stopped
# advancing for ten trading days while the task log stayed green.
def _run_main(monkeypatch, dates_with_rows, argv, *, build_raises=False):
    repo = _FakeRepo(dates_with_rows)
    builder = _FakeBuilder()
    if build_raises:
        def _boom(trade_date, *, warnings=None):
            raise RuntimeError("builder exploded")
        builder.build = _boom

    monkeypatch.setattr(
        breadth_task,
        "get_settings",
        lambda: types.SimpleNamespace(mongo_uri="mongodb://stub", data_db="quant_data"),
    )
    monkeypatch.setattr(breadth_task, "MarketRegimeRepository", lambda *a, **k: repo)
    monkeypatch.setattr(breadth_task, "MarketBreadthBuilder", lambda *a, **k: builder)

    recorded = {}
    monkeypatch.setattr(
        breadth_task,
        "record_ingestion_run",
        lambda settings, **kwargs: recorded.update(kwargs),
    )
    monkeypatch.setattr(sys, "argv", argv)

    breadth_task.main()
    return recorded, repo


def test_main_records_skipped_when_it_computed_nothing(monkeypatch):
    recorded, repo = _run_main(
        monkeypatch, set(), ["run_market_breadth_daily", "--date", "20250106"]
    )

    assert recorded["status"] == "skipped"
    assert recorded["records_changed"] == 0
    assert "no volume_price rows" in recorded["error"]
    assert repo.upserted == []


def test_main_records_complete_when_it_computed(monkeypatch):
    recorded, repo = _run_main(
        monkeypatch, {"20250106"}, ["run_market_breadth_daily", "--date", "20250106"]
    )

    assert recorded["status"] == "complete"
    assert recorded["records_changed"] == 1
    assert recorded["error"] is None
    assert len(repo.upserted) == 1


def test_main_records_complete_when_only_some_dates_are_ready(monkeypatch):
    """A partially-ready backfill still did real work, so it is not "skipped"."""
    recorded, _ = _run_main(
        monkeypatch,
        {"20250106"},
        ["run_market_breadth_daily", "--start", "20250106", "--end", "20250107"],
    )

    assert recorded["status"] == "complete"
    assert recorded["records_changed"] == 1


def test_main_records_partial_and_exits_nonzero_on_error(monkeypatch):
    with pytest.raises(SystemExit) as excinfo:
        _run_main(
            monkeypatch,
            {"20250106"},
            ["run_market_breadth_daily", "--date", "20250106"],
            build_raises=True,
        )

    assert excinfo.value.code == 1
