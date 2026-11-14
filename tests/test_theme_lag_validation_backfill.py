"""主题补涨验证回补：锚定日选择、窗口成熟判定、逐锚点调用。"""
from __future__ import annotations

from typing import Any, Dict, List

import mongomock

from quant_data_engine.config.settings import Settings
from quant_data_engine.tasks import run_theme_lag_candidate_validation as mod


def _settings() -> Settings:
    return Settings(
        mongo_uri="mongodb://x",
        mongo_db="finance",
        trading_db="quant_trading",
        data_db="quant_data",
        realtime_db="quant_realtime",
        tushare_token=None,
        rate_limit_per_sec=1.0,
        job_status_sync_uri=None,
    )


def test_list_recent_anchor_dates_dedupes_filters_and_limits():
    db = mongomock.MongoClient().quant_data
    db["theme_lag_candidates"].insert_many(
        [
            {"trade_date": "20260105", "index_store_key": "csi1000"},
            {"trade_date": "20260105", "index_store_key": "csi1000"},
            {"trade_date": "20260106", "index_store_key": "csi1000"},
            {"trade_date": "20260107", "index_store_key": "csi1000"},
            {"trade_date": "20260107", "index_store_key": "csi2000"},
        ]
    )
    assert mod.list_recent_anchor_dates(db, "csi1000", 2) == ["20260106", "20260107"]
    assert mod.list_recent_anchor_dates(db, "csi1000", 0) == []
    assert mod.list_recent_anchor_dates(db, "csi2000", 5) == ["20260107"]


def test_window_matured_requires_two_slots_with_real_closes():
    slots = ["20260105", "20260106", "20260107"]
    assert mod.window_matured(slots, {}) is False
    assert mod.window_matured(slots[:1], {("X", "20260105"): 10.0}) is False
    # 只有 T+1 收盘：第 2 根有效收盘还没到位
    assert mod.window_matured(slots, {("X", "20260105"): 10.0}) is False
    # T+2 已有数据：窗口成熟
    assert mod.window_matured(slots, {("X", "20260106"): 10.0}) is True
    # 更晚的数据也算成熟（个别标的停牌不影响日历级判定）
    assert mod.window_matured(slots, {("X", "20260107"): 10.0}) is True


def test_run_backfill_calls_validation_per_anchor(monkeypatch):
    calls: List[Dict[str, Any]] = []
    monkeypatch.setattr(
        mod, "list_recent_anchor_dates", lambda db, sk, n: ["20260105", "20260106"]
    )

    def fake_run_validation(settings, td, sk, **kwargs):
        calls.append({"td": td, "sk": sk, **kwargs})
        return {"ok": True, "symbols_count": 1, "aggregate": {"denominator": 1}}

    monkeypatch.setattr(mod, "run_validation", fake_run_validation)
    out = mod.run_backfill(_settings(), "csi1000", 2, skip_empty=True)

    assert out["succeeded"] == 2
    assert out["failed"] == 0
    assert out["ok"] is True
    assert [c["td"] for c in calls] == ["20260105", "20260106"]
    assert all(c["skip_empty"] is True for c in calls)


def test_run_backfill_continues_after_anchor_failure(monkeypatch):
    monkeypatch.setattr(
        mod, "list_recent_anchor_dates", lambda db, sk, n: ["20260105", "20260106"]
    )

    def fake_run_validation(settings, td, sk, **kwargs):
        if td == "20260106":
            raise RuntimeError("boom")
        return {"ok": True}

    monkeypatch.setattr(mod, "run_validation", fake_run_validation)
    out = mod.run_backfill(_settings(), "csi1000", 2)

    assert out["succeeded"] == 1
    assert out["failed"] == 1
    assert out["ok"] is False
    assert out["results"][1]["reason"] == "exception"


def test_run_backfill_dry_run_carries_run_status(monkeypatch):
    monkeypatch.setattr(
        mod, "list_recent_anchor_dates", lambda db, sk, n: ["20260105"]
    )

    def fake_run_validation(settings, td, sk, **kwargs):
        return {
            "ok": True,
            "dry_run": True,
            "document": {"run_status": "pending_window"},
        }

    monkeypatch.setattr(mod, "run_validation", fake_run_validation)
    out = mod.run_backfill(_settings(), "csi1000", 1, dry_run=True)

    assert out["results"][0]["run_status"] == "pending_window"
    assert "document" not in out["results"][0]
