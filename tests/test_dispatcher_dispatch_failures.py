"""Guard the dispatcher's failure visibility.

For six trading days every dispatch died with exit 127 (the sync script path
resolved into site-packages' parent) while the cron wrapper still logged
"job succeeded": the failure was counted as `skipped` and the process exited 0.
These tests pin the three fixes: script resolution, fail-fast on a missing
script, and a non-zero exit when a dispatch fails.
"""

import subprocess
import sys
import types
from pathlib import Path

import pytest

from quant_data_engine.tasks import run_dispatcher


# ── script resolution ─────────────────────────────────────────────────────────
def test_sync_script_prefers_env_override(monkeypatch, tmp_path):
    script = tmp_path / "daily_market_sync.sh"
    script.write_text("#!/bin/bash\n")
    monkeypatch.setenv(run_dispatcher.SYNC_SCRIPT_ENV_VAR, str(script))

    assert run_dispatcher._resolve_sync_script() == script


def test_sync_script_falls_back_to_next_existing_candidate(monkeypatch, tmp_path):
    missing = tmp_path / "dev-tree" / "daily_market_sync.sh"
    installed = tmp_path / "app" / "daily_market_sync.sh"
    installed.parent.mkdir(parents=True)
    installed.write_text("#!/bin/bash\n")
    monkeypatch.setattr(run_dispatcher, "_sync_script_candidates", lambda: [missing, installed])

    assert run_dispatcher._resolve_sync_script() == installed


def test_installed_layout_still_offers_the_container_path(monkeypatch):
    """The original bug: pip drops the `src` level, so walking four parents up
    from the module landed in /usr/local/lib/python3.x and nothing else was
    tried. The container's real location must remain a candidate."""
    monkeypatch.delenv(run_dispatcher.SYNC_SCRIPT_ENV_VAR, raising=False)
    monkeypatch.setattr(run_dispatcher, "SCRIPT_DIR", Path("/usr/local/lib/python3.11"))

    candidates = run_dispatcher._sync_script_candidates()

    assert Path("/app/daily_market_sync.sh") in candidates


# ── failure accounting ────────────────────────────────────────────────────────
class _FakeColl:
    def __init__(self, statuses):
        self._statuses = statuses
        self.marked_failed = []

    def find(self, *_args, **_kwargs):
        return [{"task_type": k, "status": v} for k, v in self._statuses.items()]

    def find_one_and_update(self, *_args, **_kwargs):
        return None  # nothing existed → we won the claim

    def update_one(self, flt, update, **_kwargs):
        self.marked_failed.append((flt.get("task_type"), update))


def _one_ready_pipeline():
    return [{"id": "stock_daily", "depends_on": ["tushare_daily_ready"], "trigger_on_complete": True}]


def test_dispatch_failure_is_counted_as_failed_not_skipped(monkeypatch):
    coll = _FakeColl({"tushare_daily_ready": "complete"})
    monkeypatch.setattr(
        run_dispatcher.subprocess,
        "run",
        lambda *a, **k: subprocess.CompletedProcess(args=a, returncode=127),
    )

    counts = run_dispatcher.dispatch_once(coll, _one_ready_pipeline(), "20260810", dry_run=False)

    assert counts["failed"] == 1
    assert counts["skipped"] == 0
    assert counts["fired"] == 0
    assert coll.marked_failed, "a failed dispatch must be recorded in pipeline_tasks"


def test_dispatch_success_is_counted_as_fired(monkeypatch):
    coll = _FakeColl({"tushare_daily_ready": "complete"})
    monkeypatch.setattr(
        run_dispatcher.subprocess,
        "run",
        lambda *a, **k: subprocess.CompletedProcess(args=a, returncode=0),
    )

    counts = run_dispatcher.dispatch_once(coll, _one_ready_pipeline(), "20260810", dry_run=False)

    assert counts["fired"] == 1
    assert counts["failed"] == 0


def test_upstream_failure_still_counts_as_skipped(monkeypatch):
    """A deliberately not-run pipeline stays `skipped` and must not exit non-zero."""
    coll = _FakeColl({"tushare_daily_ready": "failed"})

    counts = run_dispatcher.dispatch_once(coll, _one_ready_pipeline(), "20260810", dry_run=False)

    assert counts["skipped"] == 1
    assert counts["failed"] == 0


# ── exit codes ────────────────────────────────────────────────────────────────
def _stub_mongo(monkeypatch):
    settings = types.SimpleNamespace(mongo_uri="mongodb://stub", mongo_db="finance")
    monkeypatch.setattr(run_dispatcher, "get_settings", lambda: settings)

    class _FakeClient:
        def __init__(self, *_a, **_k):
            pass

        def __getitem__(self, _name):
            return {"pipeline_tasks": _FakeColl({})}

        def close(self):
            pass

    monkeypatch.setattr(run_dispatcher, "MongoClient", _FakeClient)


def test_main_exits_nonzero_when_sync_script_missing(monkeypatch, tmp_path):
    monkeypatch.setattr(run_dispatcher, "SYNC_SCRIPT", tmp_path / "absent.sh")
    monkeypatch.setattr(sys, "argv", ["run_dispatcher", "--live", "--once"])

    with pytest.raises(SystemExit) as excinfo:
        run_dispatcher.main()

    assert excinfo.value.code == 1


def test_main_exits_nonzero_when_a_dispatch_failed(monkeypatch, tmp_path):
    script = tmp_path / "daily_market_sync.sh"
    script.write_text("#!/bin/bash\n")
    monkeypatch.setattr(run_dispatcher, "SYNC_SCRIPT", script)
    _stub_mongo(monkeypatch)
    monkeypatch.setattr(run_dispatcher, "load_catalog", lambda _p: _one_ready_pipeline())
    monkeypatch.setattr(
        run_dispatcher,
        "dispatch_once",
        lambda *a, **k: {"fired": 0, "failed": 1, "skipped": 0, "waiting": 0, "already_done": 0},
    )
    monkeypatch.setattr(sys, "argv", ["run_dispatcher", "--live", "--once"])

    with pytest.raises(SystemExit) as excinfo:
        run_dispatcher.main()

    assert excinfo.value.code == 1


def test_main_exits_zero_when_everything_dispatched(monkeypatch, tmp_path):
    script = tmp_path / "daily_market_sync.sh"
    script.write_text("#!/bin/bash\n")
    monkeypatch.setattr(run_dispatcher, "SYNC_SCRIPT", script)
    _stub_mongo(monkeypatch)
    monkeypatch.setattr(run_dispatcher, "load_catalog", lambda _p: _one_ready_pipeline())
    monkeypatch.setattr(
        run_dispatcher,
        "dispatch_once",
        lambda *a, **k: {"fired": 1, "failed": 0, "skipped": 0, "waiting": 0, "already_done": 0},
    )
    monkeypatch.setattr(sys, "argv", ["run_dispatcher", "--live", "--once"])

    run_dispatcher.main()
