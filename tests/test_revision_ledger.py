from __future__ import annotations

import argparse
import importlib.util
import threading
import time
from contextlib import contextmanager
from contextvars import copy_context
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock
from zoneinfo import ZoneInfo

import mongomock
import pytest

from quant_data_engine import revision_ledger
from quant_data_engine.bridges.miniqmt import sinks as miniqmt_sinks
from quant_data_engine.bridges.miniqmt.sinks import RealtimeDailyKSink
from quant_data_engine.repositories import stock_repository
from quant_data_engine.repositories.stock_repository import StockRepository
from quant_data_engine.revision_ledger import (
    INDEX_CONSTITUENTS_SOURCE,
    STOCK_ADJ_FACTOR_SOURCE,
    SW_INDEX_MEMBER_SOURCE,
    VOLUME_PRICE_SOURCE,
    revision_ledger_job,
)
from quant_data_engine.tasks import (
    run_index_constituents_sync,
    run_realtime_daily_k_sync,
    run_volume_price_backfill,
    run_watchlist_intraday_eastmoney,
)
from quant_data_engine.tasks.run_sw_industry_backfill import should_ledger_members


def _collection():
    return mongomock.MongoClient().finance.portfolio_cache_source_revisions_v2


def test_default_ledger_timing_is_900_and_60(monkeypatch):
    monkeypatch.delenv(revision_ledger.LEDGER_TTL_SECONDS_ENV, raising=False)
    monkeypatch.delenv(revision_ledger.LEDGER_HEARTBEAT_SECONDS_ENV, raising=False)
    assert revision_ledger.ledger_timing() == (900.0, 60.0)


def test_success_commits_once_without_marking_coverage_complete(monkeypatch):
    monkeypatch.setenv(revision_ledger.SOURCE_LEDGER_MODE_ENV, "on")
    collection = _collection()
    collection.insert_one(
        {
            "source": VOLUME_PRICE_SOURCE,
            "revision": 4,
            "state": "committed",
            "coverage_complete": True,
            "coverage_valid_until": "20260822",
        }
    )

    with revision_ledger_job(
        VOLUME_PRICE_SOURCE,
        "test_writer",
        "mongodb://unused",
        collection=collection,
    ) as revision:
        assert revision == 5
        active = collection.find_one({"source": VOLUME_PRICE_SOURCE})
        assert active["state"] == "mutating"
        assert active["coverage_complete"] is False
        assert active["coverage_valid_until"] is None
        assert set(active["active_mutations"][0]) == {
            "mutation_id",
            "owner_id",
            "started_at",
            "expires_at",
        }

    committed = collection.find_one({"source": VOLUME_PRICE_SOURCE})
    assert committed["revision"] == 5
    assert committed["state"] == "committed"
    assert committed["coverage_complete"] is False


def test_exception_and_keyboard_interrupt_abort_with_second_increment(monkeypatch):
    monkeypatch.setenv(revision_ledger.SOURCE_LEDGER_MODE_ENV, "on")
    collection = _collection()

    with pytest.raises(ValueError, match="failed"):
        with revision_ledger_job(
            STOCK_ADJ_FACTOR_SOURCE,
            "failing_writer",
            "mongodb://unused",
            collection=collection,
        ):
            raise ValueError("failed")

    aborted = collection.find_one({"source": STOCK_ADJ_FACTOR_SOURCE})
    assert aborted["revision"] == 2
    assert aborted["state"] == "committed"
    assert aborted["last_aborted_mutation_id"].startswith("failing_writer:")

    with pytest.raises(KeyboardInterrupt):
        with revision_ledger_job(
            INDEX_CONSTITUENTS_SOURCE,
            "interrupted_writer",
            "mongodb://unused",
            collection=collection,
        ):
            raise KeyboardInterrupt

    interrupted = collection.find_one({"source": INDEX_CONSTITUENTS_SOURCE})
    assert interrupted["revision"] == 2
    assert interrupted["state"] == "committed"


def test_abort_failure_preserves_primary_and_marks_it_fatal(monkeypatch):
    monkeypatch.setenv(revision_ledger.SOURCE_LEDGER_MODE_ENV, "on")

    class AbortFails(revision_ledger.SourceRevisionLedger):
        def abort(self, source, mutation_id):
            raise revision_ledger.LedgerFatalError("abort unavailable")

    ledger = AbortFails(_collection())
    with pytest.raises(revision_ledger.LedgerFatalError, match="abort unavailable") as caught:
        with ledger.mutation(
            VOLUME_PRICE_SOURCE,
            "writer",
            owner_id="owner",
            heartbeat_seconds=1000,
        ):
            raise ValueError("business failed")

    assert isinstance(caught.value.__cause__, ValueError)
    assert str(caught.value.__cause__) == "business failed"
    assert (
        str(caught.value.__cause__._revision_ledger_secondary_error)
        == "abort unavailable"
    )


def test_each_source_has_an_independent_revision(monkeypatch):
    monkeypatch.setenv(revision_ledger.SOURCE_LEDGER_MODE_ENV, "on")
    collection = _collection()

    for source in (
        VOLUME_PRICE_SOURCE,
        STOCK_ADJ_FACTOR_SOURCE,
        INDEX_CONSTITUENTS_SOURCE,
        SW_INDEX_MEMBER_SOURCE,
    ):
        with revision_ledger_job(
            source,
            f"writer_{source}",
            "mongodb://unused",
            collection=collection,
        ):
            pass

    assert {
        document["source"]: document["revision"] for document in collection.find({})
    } == {
        VOLUME_PRICE_SOURCE: 1,
        STOCK_ADJ_FACTOR_SOURCE: 1,
        INDEX_CONSTITUENTS_SOURCE: 1,
        SW_INDEX_MEMBER_SOURCE: 1,
    }


def test_overlapping_mutations_commit_independently():
    collection = _collection()
    ledger = revision_ledger.SourceRevisionLedger(collection, ttl_seconds=300)

    assert ledger.begin(VOLUME_PRICE_SOURCE, "first", owner_id="backfill") == 1
    assert ledger.begin(VOLUME_PRICE_SOURCE, "second", owner_id="realtime") == 2
    active = collection.find_one({"source": VOLUME_PRICE_SOURCE})
    assert [item["mutation_id"] for item in active["active_mutations"]] == [
        "first",
        "second",
    ]

    assert ledger.commit(VOLUME_PRICE_SOURCE, "first") == 2
    still_active = collection.find_one({"source": VOLUME_PRICE_SOURCE})
    assert [item["mutation_id"] for item in still_active["active_mutations"]] == [
        "second"
    ]
    assert still_active["state"] == "mutating"

    assert ledger.commit(VOLUME_PRICE_SOURCE, "second") == 2
    committed = collection.find_one({"source": VOLUME_PRICE_SOURCE})
    assert committed["active_mutations"] == []
    assert committed["state"] == "committed"


def test_next_begin_recovers_expired_mutation_and_invalidates_revision():
    current = [datetime(2026, 8, 22, 8, 0, tzinfo=timezone.utc)]
    collection = _collection()
    ledger = revision_ledger.SourceRevisionLedger(
        collection,
        ttl_seconds=60,
        now=lambda: current[0],
    )

    assert ledger.begin(VOLUME_PRICE_SOURCE, "expired", owner_id="old") == 1
    current[0] += timedelta(seconds=61)
    assert ledger.begin(VOLUME_PRICE_SOURCE, "new", owner_id="new") == 3

    recovered = collection.find_one({"source": VOLUME_PRICE_SOURCE})
    assert recovered["revision"] == 3
    assert [item["mutation_id"] for item in recovered["active_mutations"]] == ["new"]
    assert recovered["coverage_complete"] is False
    assert recovered["coverage_valid_until"] is None


def test_heartbeat_extends_only_its_mutation():
    current = [datetime(2026, 8, 22, 8, 0, tzinfo=timezone.utc)]
    collection = _collection()
    ledger = revision_ledger.SourceRevisionLedger(
        collection,
        ttl_seconds=60,
        now=lambda: current[0],
    )
    ledger.begin(VOLUME_PRICE_SOURCE, "first", owner_id="one")
    ledger.begin(VOLUME_PRICE_SOURCE, "second", owner_id="two")
    before = collection.find_one({"source": VOLUME_PRICE_SOURCE})
    second_expiry = before["active_mutations"][1]["expires_at"]

    current[0] += timedelta(seconds=30)
    expected = ledger.heartbeat(VOLUME_PRICE_SOURCE, "first")

    active = collection.find_one({"source": VOLUME_PRICE_SOURCE})["active_mutations"]
    assert active[0]["expires_at"] == expected.replace(tzinfo=None)
    assert active[1]["expires_at"] == second_expiry


def test_repository_guards_reject_direct_writes_when_mode_on(monkeypatch):
    monkeypatch.setenv(revision_ledger.SOURCE_LEDGER_MODE_ENV, "on")

    stock = StockRepository.__new__(StockRepository)
    with pytest.raises(revision_ledger.LedgerFatalError, match="no active mutation"):
        stock.upsert_daily([{"symbol": "000001.SZ", "trade_date": "20260822"}])
    with pytest.raises(revision_ledger.LedgerFatalError, match="no active mutation"):
        stock.upsert_adj_factors(
            [{"symbol": "000001.SZ", "trade_date": "20260822"}]
        )

    from quant_data_engine.repositories.index_constituents_repository import (
        IndexConstituentsRepository,
    )
    from quant_data_engine.repositories.sw_industry_repository import (
        SwIndustryRepository,
    )

    index = IndexConstituentsRepository.__new__(IndexConstituentsRepository)
    with pytest.raises(revision_ledger.LedgerFatalError, match="no active mutation"):
        index.replace_for_sync_day("csi1000", "20260822", [])

    sw = SwIndustryRepository.__new__(SwIndustryRepository)
    with pytest.raises(revision_ledger.LedgerFatalError, match="no active mutation"):
        sw.upsert_members([{"con_code": "000001.SZ"}], src="tushare")


def test_unrelated_thread_cannot_borrow_active_token(monkeypatch):
    monkeypatch.setenv(revision_ledger.SOURCE_LEDGER_MODE_ENV, "on")
    errors = []

    with revision_ledger_job(
        VOLUME_PRICE_SOURCE,
        "threaded_writer",
        "mongodb://unused",
        collection=_collection(),
    ):
        thread = threading.Thread(
            target=lambda: _capture_error(
                errors,
                lambda: revision_ledger.require_active_ledger_token(
                    VOLUME_PRICE_SOURCE
                ),
            )
        )
        thread.start()
        thread.join()

    assert len(errors) == 1
    assert isinstance(errors[0], revision_ledger.LedgerFatalError)
    assert "no active mutation" in str(errors[0])


def test_capability_can_be_explicitly_propagated_to_worker(monkeypatch):
    monkeypatch.setenv(revision_ledger.SOURCE_LEDGER_MODE_ENV, "on")
    errors = []

    with revision_ledger_job(
        VOLUME_PRICE_SOURCE,
        "threaded_writer",
        "mongodb://unused",
        collection=_collection(),
    ):
        context = copy_context()
        thread = threading.Thread(
            target=lambda: context.run(
                _capture_error,
                errors,
                lambda: revision_ledger.require_active_ledger_token(
                    VOLUME_PRICE_SOURCE
                ),
            )
        )
        thread.start()
        thread.join()

    assert errors == []


def test_propagated_capability_rejects_writes_after_commit(monkeypatch):
    monkeypatch.setenv(revision_ledger.SOURCE_LEDGER_MODE_ENV, "on")
    errors = []

    with revision_ledger_job(
        VOLUME_PRICE_SOURCE,
        "writer",
        "mongodb://unused",
        collection=_collection(),
    ):
        context = copy_context()
        revision_ledger.require_active_ledger_token(VOLUME_PRICE_SOURCE)

    context.run(
        _capture_error,
        errors,
        lambda: revision_ledger.require_active_ledger_token(VOLUME_PRICE_SOURCE),
    )
    assert len(errors) == 1
    assert "no longer active" in str(errors[0])


def test_failed_concurrent_capability_does_not_poison_healthy_one(monkeypatch):
    monkeypatch.setenv(revision_ledger.SOURCE_LEDGER_MODE_ENV, "on")
    collection = _collection()
    barrier = threading.Barrier(2)
    results = {}

    def worker(name, fail):
        try:
            with revision_ledger_job(
                VOLUME_PRICE_SOURCE,
                name,
                "mongodb://unused",
                collection=collection,
            ):
                capability = revision_ledger.current_ledger_capability()
                assert capability is not None
                if fail:
                    capability.fail(
                        revision_ledger.LedgerFatalError("heartbeat failed")
                    )
                barrier.wait(timeout=2)
                try:
                    revision_ledger.require_active_ledger_token(VOLUME_PRICE_SOURCE)
                    results[f"{name}_guard"] = "ok"
                except BaseException as exc:
                    results[f"{name}_guard"] = exc
        except BaseException as exc:
            results[f"{name}_exit"] = exc

    failed = threading.Thread(target=worker, args=("failed", True))
    healthy = threading.Thread(target=worker, args=("healthy", False))
    failed.start()
    healthy.start()
    failed.join()
    healthy.join()

    assert results["healthy_guard"] == "ok"
    assert "healthy_exit" not in results
    assert isinstance(results["failed_guard"], revision_ledger.LedgerFatalError)
    assert isinstance(results["failed_exit"], revision_ledger.LedgerFatalError)
    document = collection.find_one({"source": VOLUME_PRICE_SOURCE})
    assert document["active_mutations"] == []
    assert document["revision"] == 3


def test_heartbeat_fatal_wins_over_concurrent_business_error(monkeypatch):
    monkeypatch.setenv(revision_ledger.SOURCE_LEDGER_MODE_ENV, "on")
    heartbeat_called = threading.Event()

    class HeartbeatFails(revision_ledger.SourceRevisionLedger):
        def heartbeat(self, source, mutation_id):
            heartbeat_called.set()
            raise revision_ledger.LedgerFatalError("heartbeat unavailable")

    collection = _collection()
    ledger = HeartbeatFails(collection)
    with pytest.raises(
        revision_ledger.LedgerFatalError,
        match="heartbeat unavailable",
    ) as caught:
        with ledger.mutation(
            VOLUME_PRICE_SOURCE,
            "writer",
            owner_id="owner",
            heartbeat_seconds=0.001,
        ):
            assert heartbeat_called.wait(timeout=1)
            capability = revision_ledger.current_ledger_capability()
            assert capability is not None
            deadline = time.monotonic() + 1
            while capability.failure() is None and time.monotonic() < deadline:
                time.sleep(0.001)
            raise ValueError("business failed too")

    assert isinstance(caught.value.__cause__, ValueError)
    assert str(caught.value.__cause__) == "business failed too"
    document = collection.find_one({"source": VOLUME_PRICE_SOURCE})
    assert document["revision"] == 2
    assert document["active_mutations"] == []


def test_heartbeat_fatal_immediately_blocks_next_repository_write(monkeypatch):
    monkeypatch.setenv(revision_ledger.SOURCE_LEDGER_MODE_ENV, "on")
    heartbeat_called = threading.Event()

    class HeartbeatFails(revision_ledger.SourceRevisionLedger):
        def heartbeat(self, source, mutation_id):
            heartbeat_called.set()
            raise revision_ledger.LedgerFatalError("heartbeat unavailable")

    with pytest.raises(revision_ledger.LedgerFatalError, match="heartbeat unavailable"):
        with HeartbeatFails(_collection()).mutation(
            VOLUME_PRICE_SOURCE,
            "writer",
            owner_id="owner",
            heartbeat_seconds=0.001,
        ):
            assert heartbeat_called.wait(timeout=1)
            capability = revision_ledger.current_ledger_capability()
            assert capability is not None
            deadline = time.monotonic() + 1
            while capability.failure() is None and time.monotonic() < deadline:
                time.sleep(0.001)
            revision_ledger.require_active_ledger_token(VOLUME_PRICE_SOURCE)


def test_heartbeat_join_timeout_aborts_and_never_commits(monkeypatch):
    monkeypatch.setenv(revision_ledger.SOURCE_LEDGER_MODE_ENV, "on")

    class StuckThread:
        def __init__(self, *args, **kwargs):
            self.started = False

        def start(self):
            self.started = True

        def join(self, timeout=None):
            pass

        def is_alive(self):
            return self.started

    monkeypatch.setattr(revision_ledger.threading, "Thread", StuckThread)
    collection = _collection()
    ledger = revision_ledger.SourceRevisionLedger(collection)

    with pytest.raises(
        revision_ledger.LedgerFatalError,
        match="heartbeat thread did not stop",
    ):
        with ledger.mutation(
            VOLUME_PRICE_SOURCE,
            "writer",
            owner_id="owner",
            heartbeat_seconds=60,
        ):
            revision_ledger.require_active_ledger_token(VOLUME_PRICE_SOURCE)

    document = collection.find_one({"source": VOLUME_PRICE_SOURCE})
    assert document["revision"] == 2
    assert document["active_mutations"] == []
    assert document["last_aborted_mutation_id"].startswith("writer:")


def _capture_error(errors, call):
    try:
        call()
    except BaseException as exc:
        errors.append(exc)


def test_mode_off_performs_no_mongo_io(monkeypatch):
    monkeypatch.delenv(revision_ledger.SOURCE_LEDGER_MODE_ENV, raising=False)

    class BombCollection:
        def __getattr__(self, name):
            raise AssertionError(f"unexpected Mongo access: {name}")

    with revision_ledger_job(
        VOLUME_PRICE_SOURCE,
        "disabled_writer",
        "mongodb://unused",
        collection=BombCollection(),
    ) as revision:
        assert revision is None


def test_mode_on_mongo_failure_prevents_job_body(monkeypatch):
    monkeypatch.setenv(revision_ledger.SOURCE_LEDGER_MODE_ENV, "on")
    body_ran = False

    class FailingClient:
        admin = None

        def __init__(self, *args, **kwargs):
            self.admin = self

        def command(self, name):
            raise OSError("mongo unavailable")

        def close(self):
            pass

    monkeypatch.setattr(revision_ledger, "MongoClient", FailingClient)

    with pytest.raises(revision_ledger.LedgerFatalError) as caught:
        with revision_ledger_job(
            VOLUME_PRICE_SOURCE,
            "closed_writer",
            "mongodb://unavailable",
        ):
            body_ran = True

    assert body_ran is False
    assert isinstance(caught.value.__cause__, OSError)


def test_mode_on_uses_configured_v2_database_and_collection(monkeypatch):
    monkeypatch.setenv(revision_ledger.SOURCE_LEDGER_MODE_ENV, "on")
    monkeypatch.setenv(revision_ledger.LEDGER_DB_ENV, "configured_finance")
    monkeypatch.setenv(revision_ledger.LEDGER_COLLECTION_ENV, "configured_ledger")
    mongo = mongomock.MongoClient()

    class Client:
        admin = None

        def __init__(self, *args, **kwargs):
            self.admin = self

        def command(self, name):
            return {"ok": 1}

        def __getitem__(self, name):
            return mongo[name]

        def close(self):
            pass

    monkeypatch.setattr(revision_ledger, "MongoClient", Client)

    with revision_ledger_job(
        VOLUME_PRICE_SOURCE,
        "configured_writer",
        "mongodb://configured",
    ):
        pass

    document = mongo.configured_finance.configured_ledger.find_one(
        {"source": VOLUME_PRICE_SOURCE}
    )
    assert document["revision"] == 1
    assert document["state"] == "committed"


def test_miniqmt_settings_none_uses_mongo_uri_at_write_time(monkeypatch):
    monkeypatch.setenv(revision_ledger.SOURCE_LEDGER_MODE_ENV, "on")
    monkeypatch.setenv("MONGO_URI", "mongodb://from-env")
    mongo = mongomock.MongoClient()
    seen_uris = []

    class Client:
        admin = None

        def __init__(self, uri, **kwargs):
            seen_uris.append(uri)
            self.admin = self

        def command(self, name):
            return {"ok": 1}

        def __getitem__(self, name):
            return mongo[name]

        def close(self):
            pass

    monkeypatch.setattr(revision_ledger, "MongoClient", Client)
    repo = SimpleNamespace(
        upsert_daily=MagicMock(return_value=1),
        upsert_realtime_latest=MagicMock(return_value=1),
    )
    sink = RealtimeDailyKSink(
        repo=repo,
        settings=None,
        write_interval=5,
        universe_type="watchlist_plan_strategy",
        record_runs=False,
    )

    result = sink.process(
        ticks={"000001.SZ": {"lastPrice": 10.5, "lastClose": 10.0}},
        symbols=["000001.SZ"],
        now=datetime(2026, 8, 22, 10, 30, tzinfo=ZoneInfo("Asia/Shanghai")),
        force=True,
    )

    assert result is not None
    assert seen_uris == ["mongodb://from-env"]


def test_miniqmt_missing_ledger_uri_fails_on_first_write(monkeypatch):
    monkeypatch.setenv(revision_ledger.SOURCE_LEDGER_MODE_ENV, "on")
    monkeypatch.delenv("MONGO_URI", raising=False)
    repo = SimpleNamespace(
        upsert_daily=MagicMock(),
        upsert_realtime_latest=MagicMock(),
    )
    sink = RealtimeDailyKSink(
        repo=repo,
        settings=None,
        write_interval=5,
        universe_type="watchlist_plan_strategy",
        record_runs=False,
    )

    with pytest.raises(revision_ledger.LedgerFatalError, match="no Mongo URI"):
        sink.process(
            ticks={"000001.SZ": {"lastPrice": 10.5, "lastClose": 10.0}},
            symbols=["000001.SZ"],
            now=datetime(2026, 8, 22, 10, 30, tzinfo=ZoneInfo("Asia/Shanghai")),
            force=True,
        )

    repo.upsert_daily.assert_not_called()


def test_realtime_snapshot_batch_commits_and_failure_aborts(monkeypatch):
    monkeypatch.setenv(revision_ledger.SOURCE_LEDGER_MODE_ENV, "on")
    settings = SimpleNamespace(mongo_uri="mongodb://unused")
    success_collection = _collection()
    monkeypatch.setattr(
        run_realtime_daily_k_sync,
        "revision_ledger_job",
        lambda source, writer_id, mongo_uri: revision_ledger_job(
            source,
            writer_id,
            mongo_uri,
            collection=success_collection,
        ),
    )
    monkeypatch.setattr(
        run_realtime_daily_k_sync,
        "post_realtime_daily_k_snapshot",
        lambda **kwargs: "published",
    )

    assert (
        run_realtime_daily_k_sync._post_snapshot_with_revision(
            settings,
            records=[{"symbol": "000001.SZ"}],
        )
        == "published"
    )
    committed = success_collection.find_one({"source": VOLUME_PRICE_SOURCE})
    assert committed["revision"] == 1
    assert committed["state"] == "committed"

    failure_collection = _collection()
    monkeypatch.setattr(
        run_realtime_daily_k_sync,
        "revision_ledger_job",
        lambda source, writer_id, mongo_uri: revision_ledger_job(
            source,
            writer_id,
            mongo_uri,
            collection=failure_collection,
        ),
    )

    def fail_snapshot(**kwargs):
        raise RuntimeError("postprocess failed")

    monkeypatch.setattr(
        run_realtime_daily_k_sync,
        "post_realtime_daily_k_snapshot",
        fail_snapshot,
    )
    with pytest.raises(RuntimeError, match="postprocess failed"):
        run_realtime_daily_k_sync._post_snapshot_with_revision(
            settings,
            records=[{"symbol": "000001.SZ"}],
        )

    aborted = failure_collection.find_one({"source": VOLUME_PRICE_SOURCE})
    assert aborted["revision"] == 2
    assert aborted["state"] == "committed"


def test_eastmoney_each_loop_batch_has_one_revision(monkeypatch):
    monkeypatch.setenv(revision_ledger.SOURCE_LEDGER_MODE_ENV, "on")
    collection = _collection()
    monkeypatch.setattr(
        run_watchlist_intraday_eastmoney,
        "revision_ledger_job",
        lambda source, writer_id, mongo_uri: revision_ledger_job(
            source,
            writer_id,
            mongo_uri,
            collection=collection,
        ),
    )
    repo = SimpleNamespace(upsert_daily=MagicMock(return_value=3))
    records = [{"symbol": "000001.SZ", "trade_date": "20260822"}]

    assert (
        run_watchlist_intraday_eastmoney._write_volume_price_batch(
            repo,
            records,
            "mongodb://unused",
        )
        == 3
    )
    assert (
        run_watchlist_intraday_eastmoney._write_volume_price_batch(
            repo,
            records,
            "mongodb://unused",
        )
        == 3
    )

    document = collection.find_one({"source": VOLUME_PRICE_SOURCE})
    assert document["revision"] == 2
    assert document["state"] == "committed"
    assert repo.upsert_daily.call_count == 2


def test_eastmoney_failed_batch_aborts(monkeypatch):
    monkeypatch.setenv(revision_ledger.SOURCE_LEDGER_MODE_ENV, "on")
    collection = _collection()
    monkeypatch.setattr(
        run_watchlist_intraday_eastmoney,
        "revision_ledger_job",
        lambda source, writer_id, mongo_uri: revision_ledger_job(
            source,
            writer_id,
            mongo_uri,
            collection=collection,
        ),
    )
    repo = SimpleNamespace(
        upsert_daily=MagicMock(side_effect=RuntimeError("primary write failed"))
    )

    with pytest.raises(RuntimeError, match="primary write failed"):
        run_watchlist_intraday_eastmoney._write_volume_price_batch(
            repo,
            [{"symbol": "000001.SZ", "trade_date": "20260822"}],
            "mongodb://unused",
        )

    document = collection.find_one({"source": VOLUME_PRICE_SOURCE})
    assert document["revision"] == 2
    assert document["state"] == "committed"


def test_miniqmt_flush_ledgers_primary_and_sync_mirror_once(monkeypatch):
    monkeypatch.setenv(revision_ledger.SOURCE_LEDGER_MODE_ENV, "on")
    collection = _collection()
    monkeypatch.setattr(
        miniqmt_sinks,
        "revision_ledger_job",
        lambda source, writer_id, mongo_uri: revision_ledger_job(
            source,
            writer_id,
            mongo_uri,
            collection=collection,
        ),
    )
    repo = StockRepository.__new__(StockRepository)
    repo.volume_price_coll = MagicMock()
    repo.sync_volume_price_coll = MagicMock()
    repo.realtime_latest_coll = MagicMock()
    repo.sync_realtime_latest_coll = MagicMock()
    for target in (
        repo.volume_price_coll,
        repo.sync_volume_price_coll,
        repo.realtime_latest_coll,
        repo.sync_realtime_latest_coll,
    ):
        target.bulk_write.return_value.upserted_count = 1
        target.bulk_write.return_value.modified_count = 0

    sink = RealtimeDailyKSink(
        repo=repo,
        settings=SimpleNamespace(mongo_uri="mongodb://unused"),
        write_interval=5,
        universe_type="watchlist_plan_strategy",
        record_runs=False,
    )
    result = sink.process(
        ticks={
            "000001.SZ": {
                "lastPrice": 10.5,
                "lastClose": 10.0,
                "volume": 1,
            }
        },
        symbols=["000001.SZ"],
        now=datetime(2026, 8, 22, 10, 30, tzinfo=ZoneInfo("Asia/Shanghai")),
        force=True,
    )

    assert result is not None
    repo.volume_price_coll.bulk_write.assert_called_once()
    repo.sync_volume_price_coll.bulk_write.assert_called_once()
    repo.realtime_latest_coll.bulk_write.assert_called_once()
    repo.sync_realtime_latest_coll.bulk_write.assert_called_once()
    document = collection.find_one({"source": VOLUME_PRICE_SOURCE})
    assert document["revision"] == 1
    assert document["state"] == "committed"


def test_secondary_mirror_failure_aborts_central_mutation(monkeypatch):
    monkeypatch.setenv(revision_ledger.SOURCE_LEDGER_MODE_ENV, "on")
    collection = _collection()
    repo = StockRepository.__new__(StockRepository)
    repo.volume_price_coll = MagicMock()
    repo.volume_price_coll.bulk_write.return_value.upserted_count = 1
    repo.volume_price_coll.bulk_write.return_value.modified_count = 0
    repo.sync_volume_price_coll = MagicMock()
    repo.sync_volume_price_coll.bulk_write.side_effect = RuntimeError(
        "secondary unavailable"
    )

    with pytest.raises(
        revision_ledger.LedgerSyncFatalError,
        match="secondary mirror volume_price write failed",
    ):
        with revision_ledger_job(
            VOLUME_PRICE_SOURCE,
            "mirrored_writer",
            "mongodb://unused",
            collection=collection,
        ):
            repo.upsert_daily(
                [
                    {
                        "symbol": "000001.SZ",
                        "trade_date": "20260822",
                        "open": 10,
                        "high": 11,
                        "low": 9,
                        "close": 10,
                    }
                ]
            )

    document = collection.find_one({"source": VOLUME_PRICE_SOURCE})
    assert document["revision"] == 2
    assert document["active_mutations"] == []


@pytest.mark.parametrize("mode", ["off", "on"])
def test_configured_secondary_connection_failure_is_fatal(monkeypatch, mode):
    monkeypatch.setenv(revision_ledger.SOURCE_LEDGER_MODE_ENV, mode)
    primary = MagicMock()
    primary.__getitem__.return_value = MagicMock()
    monkeypatch.setattr(
        stock_repository,
        "MongoClient",
        MagicMock(side_effect=[primary, OSError("secondary offline")]),
    )

    repo = StockRepository(
        "mongodb://primary",
        "quant_data",
        sync_uri="mongodb://secondary",
    )
    collection = _collection()

    def write():
        repo.upsert_daily(
            [
                {
                    "symbol": "000001.SZ",
                    "trade_date": "20260822",
                    "open": 10,
                    "high": 11,
                    "low": 9,
                    "close": 10,
                }
            ]
        )

    with pytest.raises(
        revision_ledger.LedgerSyncFatalError,
        match="secondary mirror connection failed",
    ):
        if mode == "on":
            with revision_ledger_job(
                VOLUME_PRICE_SOURCE,
                "miniqmt_realtime_daily_k",
                "mongodb://unused",
                collection=collection,
            ):
                write()
        else:
            write()

    if mode == "on":
        document = collection.find_one({"source": VOLUME_PRICE_SOURCE})
        assert document["revision"] == 2
        assert document["active_mutations"] == []


def test_configured_secondary_index_failure_is_fatal(monkeypatch):
    monkeypatch.setenv(revision_ledger.SOURCE_LEDGER_MODE_ENV, "on")
    primary = MagicMock()
    primary.__getitem__.return_value = MagicMock()
    secondary = MagicMock()
    secondary_db = MagicMock()
    secondary_collection = MagicMock()
    secondary_collection.create_index.side_effect = OSError("index denied")
    secondary.__getitem__.return_value = secondary_db
    secondary_db.__getitem__.return_value = secondary_collection
    monkeypatch.setattr(
        stock_repository,
        "MongoClient",
        MagicMock(side_effect=[primary, secondary]),
    )

    repo = StockRepository(
        "mongodb://primary",
        "quant_data",
        sync_uri="mongodb://secondary",
    )
    collection = _collection()
    with pytest.raises(
        revision_ledger.LedgerSyncFatalError,
        match="secondary mirror index initialization failed",
    ):
        with revision_ledger_job(
            VOLUME_PRICE_SOURCE,
            "miniqmt_realtime_daily_k",
            "mongodb://unused",
            collection=collection,
        ):
            repo.upsert_daily(
                [
                    {
                        "symbol": "000001.SZ",
                        "trade_date": "20260822",
                        "open": 10,
                        "high": 11,
                        "low": 9,
                        "close": 10,
                    }
                ]
            )

    document = collection.find_one({"source": VOLUME_PRICE_SOURCE})
    assert document["revision"] == 2
    assert document["active_mutations"] == []


def test_miniqmt_failed_flush_aborts_without_advancing_interval(monkeypatch):
    monkeypatch.setenv(revision_ledger.SOURCE_LEDGER_MODE_ENV, "on")
    collection = _collection()
    monkeypatch.setattr(
        miniqmt_sinks,
        "revision_ledger_job",
        lambda source, writer_id, mongo_uri: revision_ledger_job(
            source,
            writer_id,
            mongo_uri,
            collection=collection,
        ),
    )
    repo = SimpleNamespace(
        upsert_daily=MagicMock(return_value=1),
        upsert_realtime_latest=MagicMock(
            side_effect=RuntimeError("latest write failed")
        ),
    )
    sink = RealtimeDailyKSink(
        repo=repo,
        settings=SimpleNamespace(mongo_uri="mongodb://unused"),
        write_interval=5,
        universe_type="watchlist_plan_strategy",
        record_runs=False,
    )

    with pytest.raises(RuntimeError, match="latest write failed"):
        sink.process(
            ticks={"000001.SZ": {"lastPrice": 10.5, "lastClose": 10.0}},
            symbols=["000001.SZ"],
            now=datetime(2026, 8, 22, 10, 30, tzinfo=ZoneInfo("Asia/Shanghai")),
            force=True,
        )

    document = collection.find_one({"source": VOLUME_PRICE_SOURCE})
    assert document["revision"] == 2
    assert document["state"] == "committed"
    assert sink._last_write_ts is None


def test_volume_backfill_multiple_bulk_writes_use_one_revision(monkeypatch):
    monkeypatch.setenv(revision_ledger.SOURCE_LEDGER_MODE_ENV, "on")
    collection = _collection()
    pipeline_calls = []

    class FakeRepository:
        def __init__(self, *args, **kwargs):
            self.meta = {}

        def get_meta(self, key):
            return self.meta.get(key)

        def set_meta(self, key, value):
            self.meta[key] = value

    class FakePipeline:
        def __init__(self, *args, **kwargs):
            pass

        def run_by_date(self, days, **kwargs):
            pipeline_calls.append(list(days))
            return {
                "total_daily_records": 1,
                "total_basic_records": 0,
                "total_issues": 0,
            }

    args = argparse.Namespace(
        start="20240101",
        end="20240103",
        resume=False,
        include_basic=False,
        dry_run=False,
        verbose=True,
    )
    settings = SimpleNamespace(
        tushare_token="token",
        rate_limit_per_sec=4,
        mongo_uri="mongodb://unused",
        data_db="quant_data",
    )
    monkeypatch.setattr(run_volume_price_backfill, "parse_args", lambda: args)
    monkeypatch.setattr(run_volume_price_backfill, "get_settings", lambda: settings)
    monkeypatch.setattr(
        run_volume_price_backfill,
        "get_trading_days",
        lambda adapter, start, end: ["20240101", "20240102", "20240103"],
    )
    monkeypatch.setattr(
        run_volume_price_backfill, "StockAdapter", lambda *a, **k: object()
    )
    monkeypatch.setattr(run_volume_price_backfill, "StockRepository", FakeRepository)
    monkeypatch.setattr(run_volume_price_backfill, "StockDailyPipeline", FakePipeline)
    monkeypatch.setattr(
        run_volume_price_backfill, "record_ingestion_run", lambda *a, **k: None
    )
    monkeypatch.setattr(
        run_volume_price_backfill,
        "revision_ledger_job",
        lambda source, writer_id, mongo_uri: revision_ledger_job(
            source,
            writer_id,
            mongo_uri,
            collection=collection,
        ),
    )

    assert run_volume_price_backfill.main() == 0
    assert pipeline_calls == [["20240101"], ["20240102"], ["20240103"]]
    document = collection.find_one({"source": VOLUME_PRICE_SOURCE})
    assert document["revision"] == 1
    assert document["state"] == "committed"


@pytest.mark.parametrize(
    ("sync", "dry_run", "expected"),
    [
        ("members", False, True),
        ("all", False, True),
        ("members", True, False),
        ("daily", False, False),
        ("classify", False, False),
    ],
)
def test_sw_member_ledger_condition(sync, dry_run, expected):
    assert should_ledger_members(sync, dry_run) is expected


def test_index_failure_gate_aborts_empty_run(monkeypatch):
    monkeypatch.setenv(revision_ledger.SOURCE_LEDGER_MODE_ENV, "on")
    collection = _collection()
    ledger_job = revision_ledger_job(
        INDEX_CONSTITUENTS_SOURCE,
        "index_constituents_sync",
        "mongodb://unused",
        collection=collection,
    )

    with run_index_constituents_sync._abort_ledger_without_success(
        ledger_job,
        lambda: False,
    ):
        pass

    document = collection.find_one({"source": INDEX_CONSTITUENTS_SOURCE})
    assert document["revision"] == 2
    assert document["active_mutations"] == []


def test_index_zero_success_does_not_swallow_abort_fatal(monkeypatch):
    monkeypatch.setenv(revision_ledger.SOURCE_LEDGER_MODE_ENV, "on")

    class AbortFails(revision_ledger.SourceRevisionLedger):
        def abort(self, source, mutation_id):
            raise revision_ledger.LedgerFatalError("index abort failed")

    ledger_job = AbortFails(_collection()).mutation(
        INDEX_CONSTITUENTS_SOURCE,
        "index_constituents_sync",
        owner_id="owner",
        heartbeat_seconds=60,
    )
    with pytest.raises(
        revision_ledger.LedgerFatalError,
        match="index abort failed",
    ):
        with run_index_constituents_sync._abort_ledger_without_success(
            ledger_job,
            lambda: False,
        ):
            pass


def test_index_zero_success_does_not_swallow_heartbeat_fatal(monkeypatch):
    monkeypatch.setenv(revision_ledger.SOURCE_LEDGER_MODE_ENV, "on")
    heartbeat_called = threading.Event()

    class HeartbeatFails(revision_ledger.SourceRevisionLedger):
        def heartbeat(self, source, mutation_id):
            heartbeat_called.set()
            raise revision_ledger.LedgerFatalError("index heartbeat failed")

    ledger_job = HeartbeatFails(_collection()).mutation(
        INDEX_CONSTITUENTS_SOURCE,
        "index_constituents_sync",
        owner_id="owner",
        heartbeat_seconds=0.001,
    )
    with pytest.raises(
        revision_ledger.LedgerFatalError,
        match="index heartbeat failed",
    ):
        with run_index_constituents_sync._abort_ledger_without_success(
            ledger_job,
            lambda: False,
        ):
            assert heartbeat_called.wait(timeout=1)
            capability = revision_ledger.current_ledger_capability()
            assert capability is not None
            deadline = time.monotonic() + 1
            while capability.failure() is None and time.monotonic() < deadline:
                time.sleep(0.001)


def test_writer_manifest_covers_all_known_writers_with_no_exclusions():
    root = Path(__file__).resolve().parents[1]
    entries = {
        (entry["source"], entry["writer_id"]): entry
        for entry in revision_ledger.WRITER_MANIFEST
    }
    expected_included = {
        (VOLUME_PRICE_SOURCE, "stock_daily_eod"),
        (VOLUME_PRICE_SOURCE, "volume_price_raw_backfill"),
        (VOLUME_PRICE_SOURCE, "realtime_daily_k"),
        (VOLUME_PRICE_SOURCE, "watchlist_intraday_eastmoney"),
        (VOLUME_PRICE_SOURCE, "miniqmt_realtime_daily_k"),
        (STOCK_ADJ_FACTOR_SOURCE, "stock_adj_factor_sync"),
        (INDEX_CONSTITUENTS_SOURCE, "index_constituents_sync"),
        (INDEX_CONSTITUENTS_SOURCE, "index_constituents_backfill"),
        (SW_INDEX_MEMBER_SOURCE, "sw_industry_members_backfill"),
        (VOLUME_PRICE_SOURCE, "migrate_volume_price"),
        (INDEX_CONSTITUENTS_SOURCE, "migrate_index_constituents"),
    }
    assert expected_included <= entries.keys()
    assert all(entries[key]["coverage_included"] is True for key in expected_included)

    assert not [
        entry
        for entry in revision_ledger.WRITER_MANIFEST
        if entry["coverage_included"] is not True
    ]

    for entry in revision_ledger.WRITER_MANIFEST:
        entrypoint = str(entry["entrypoint"])
        if entrypoint.endswith(".py"):
            path = root / entrypoint
        else:
            path = root / "src" / Path(*entrypoint.split(".")).with_suffix(".py")
        assert path.is_file(), entrypoint

    hooked_task_files = {
        "stock_daily_eod": "src/quant_data_engine/tasks/run_stock_daily.py",
        "volume_price_raw_backfill": (
            "src/quant_data_engine/tasks/run_volume_price_backfill.py"
        ),
        "stock_adj_factor_sync": (
            "src/quant_data_engine/tasks/run_stock_adj_factor_sync.py"
        ),
        "index_constituents_sync": (
            "src/quant_data_engine/tasks/run_index_constituents_sync.py"
        ),
        "index_constituents_backfill": (
            "src/quant_data_engine/tasks/run_index_constituents_backfill.py"
        ),
        "sw_industry_members_backfill": (
            "src/quant_data_engine/tasks/run_sw_industry_backfill.py"
        ),
        "realtime_daily_k": (
            "src/quant_data_engine/tasks/run_realtime_daily_k_sync.py"
        ),
        "watchlist_intraday_eastmoney": (
            "src/quant_data_engine/tasks/run_watchlist_intraday_eastmoney.py"
        ),
        "miniqmt_realtime_daily_k": ("src/quant_data_engine/bridges/miniqmt/sinks.py"),
    }
    for writer_id, relative_path in hooked_task_files.items():
        task_source = (root / relative_path).read_text(encoding="utf-8")
        assert "revision_ledger_job(" in task_source
        assert f'"{writer_id}"' in task_source

    direct_upsert_callers = {
        path.relative_to(root).as_posix()
        for path in (root / "src" / "quant_data_engine").rglob("*.py")
        if ".upsert_daily(" in path.read_text(encoding="utf-8")
    }
    assert direct_upsert_callers == {
        "src/quant_data_engine/bridges/miniqmt/sinks.py",
        "src/quant_data_engine/pipelines/index_daily_pipeline.py",
        "src/quant_data_engine/pipelines/stock_daily_pipeline.py",
        "src/quant_data_engine/tasks/realtime_daily_k_postprocess.py",
        "src/quant_data_engine/tasks/run_watchlist_intraday_eastmoney.py",
    }

    guarded_repositories = (
        "src/quant_data_engine/repositories/stock_repository.py",
        "src/quant_data_engine/repositories/index_constituents_repository.py",
        "src/quant_data_engine/repositories/sw_industry_repository.py",
    )
    for relative_path in guarded_repositories:
        repository_source = (root / relative_path).read_text(encoding="utf-8")
        assert "require_active_ledger_token(" in repository_source

    readme = (root / "README.md").read_text(encoding="utf-8")
    assert "历史范围 cache" in readme
    assert "按日期/分区 revision" in readme


def test_migration_declares_every_tracked_collection():
    root = Path(__file__).resolve().parents[1]
    path = root / "tools" / "migrate_to_quant_data.py"
    spec = importlib.util.spec_from_file_location("migrate_to_quant_data", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    assert module.LEDGERED_MIGRATIONS == {
        "volume_price": (VOLUME_PRICE_SOURCE, "migrate_volume_price"),
        "index_constituents": (
            INDEX_CONSTITUENTS_SOURCE,
            "migrate_index_constituents",
        ),
    }


def test_migration_returns_nonzero_on_ledger_failure(monkeypatch):
    root = Path(__file__).resolve().parents[1]
    path = root / "tools" / "migrate_to_quant_data.py"
    spec = importlib.util.spec_from_file_location("migrate_to_quant_data_failure", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    mongo = mongomock.MongoClient()
    mongo.finance.volume_price.insert_one({"_id": "row"})
    import pymongo

    monkeypatch.setattr(pymongo, "MongoClient", lambda *args, **kwargs: mongo)
    monkeypatch.setattr(
        module,
        "parse_args",
        lambda: argparse.Namespace(dry_run=False, collections="volume_price"),
    )

    @contextmanager
    def failed_ledger(*args, **kwargs):
        raise revision_ledger.LedgerFatalError("ledger conflict")
        yield

    monkeypatch.setattr(module, "revision_ledger_job", failed_ledger)

    assert module.main() == 2
    assert mongo.quant_data.volume_price.count_documents({}) == 0
