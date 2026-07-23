"""Tests for market events repository merge and sync job status."""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

from quant_data_engine.market_events.repository import upsert_events
from quant_data_engine.tasks import run_market_events_sync as sync_task


class _FakeRepoCol:
    def __init__(self):
        self.store = {}

    def find_one(self, query):
        return self.store.get(query.get("identity_key"))

    def insert_one(self, doc):
        self.store[doc["identity_key"]] = dict(doc)

    def update_one(self, filt, update):
        key = filt["identity_key"]
        self.store[key].update(update["$set"])


def test_upsert_events_merges_metrics_fieldwise():
    col = _FakeRepoCol()
    now = datetime.now(timezone.utc)
    col.insert_one(
        {
            "identity_key": "k",
            "metrics": {"forecast": 3.1, "previous": 3.0},
            "sources": [{"name": "wallstreetcn"}],
            "llm_brief": {"summary": "keep"},
        }
    )
    n = upsert_events(
        col,
        [
            {
                "identity_key": "k",
                "title": "US CPI",
                "event_time": now,
                "region": "US",
                "category": "macro_data",
                "event_subtype": None,
                "symbol": None,
                "symbol_name": None,
                "metric_code": "CPI_YOY",
                "ref_period": "2026-06",
                "importance": 4,
                "status": "scheduled",
                "metrics": {"forecast": None, "actual": 3.2},
                "sources": [{"name": "baidu"}],
                "updated_at": now,
                "fetched_at": now,
            }
        ],
    )
    assert n == 1
    doc = col.store["k"]
    assert doc["metrics"]["forecast"] == 3.1
    assert doc["metrics"]["previous"] == 3.0
    assert doc["metrics"]["actual"] == 3.2
    assert {s["name"] for s in doc["sources"]} == {"wallstreetcn", "baidu"}
    assert doc["llm_brief"]["summary"] == "keep"


def test_run_once_records_skipped_when_no_events(monkeypatch):
    recorded = {}

    class _Client:
        def close(self):
            return None

    monkeypatch.setattr(sync_task, "get_settings", lambda: SimpleNamespace(mongo_uri="mongodb://x", data_db="db"))
    monkeypatch.setattr(sync_task, "MongoClient", lambda *_a, **_k: _Client())
    monkeypatch.setattr(sync_task, "run_sync_cycle", lambda *_a, **_k: {"merged_count": 0, "upserted": 0})

    def _record(settings, job_type, date_str, **kwargs):
        recorded.update({"job_type": job_type, **kwargs})
        return True

    monkeypatch.setattr(sync_task, "record_ingestion_run", _record)
    ok = sync_task.run_once()
    assert ok is False
    assert recorded["status"] == "skipped"
    assert recorded["records_changed"] == 0
