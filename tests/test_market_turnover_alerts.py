"""Tests for the intraday turnover alert gating and message."""
from __future__ import annotations

from datetime import datetime

import pytest

from quant_data_engine.tools.market_turnover import alerts


class _FakeRepo:
    def __init__(self, *, exists=False):
        self.exists = exists
        self.records = []

    def alert_exists(self, trade_date, stage):
        return self.exists

    def record_alert(self, trade_date, stage, ratio, *, sent, error=None):
        self.records.append({"trade_date": trade_date, "stage": stage, "ratio": ratio, "sent": sent, "error": error})


def _doc(total=20000.0):
    return {
        "trade_date": "20261127",
        "snapshot_time": "202611271405",
        "total": total,
        "coverage": "boards",
        "sh": 6000.0,
        "sz": 5100.0,
        "cx": 5400.0,
        "star": 3200.0,
        "bse": 170.0,
    }


@pytest.fixture(autouse=True)
def _stub_inputs(monkeypatch):
    monkeypatch.setattr(alerts, "trailing_mean", lambda repo, trade_date, window=20: 16204.0)
    monkeypatch.setattr(
        alerts,
        "load_profile",
        lambda repo, trade_date, days=60: [{"t": "1400", "p10": 0.76, "p50": 0.80, "p90": 0.83, "n": 50}],
    )
    monkeypatch.setattr(alerts, "history_line", lambda repo, threshold, index_code="000852.SH": "中证1000 同档历史 688 次")


def test_no_alert_before_the_window():
    result = alerts.maybe_send(_FakeRepo(), _doc(), now=datetime(2026, 11, 27, 13, 59))
    assert result["status"] == "outside_alert_window"


def test_first_alert_carries_the_projected_ratio():
    repo = _FakeRepo()
    document = _doc(total=20000.0)
    result = alerts.maybe_send(repo, document, now=datetime(2026, 11, 27, 14, 2), dry_run=True)
    assert result["status"] == "dry_run"
    assert result["stage"] == "first"
    # 20000 / (16204 * 0.80) ≈ 1.543
    assert result["ratio"] == pytest.approx(1.543, abs=0.01)
    assert "【量能放量】2026-11-27 14:02" in result["message"]
    assert "688 次" in result["message"]
    assert "分板块" in result["message"]
    assert repo.records == []  # 干跑不落状态


def test_confirm_is_a_separate_stage():
    repo = _FakeRepo()
    result = alerts.maybe_send(repo, _doc(), now=datetime(2026, 11, 27, 14, 51), dry_run=True)
    assert result["stage"] == "confirm"
    assert "收盘前确认" in result["message"]


def test_same_stage_is_not_sent_twice():
    result = alerts.maybe_send(_FakeRepo(exists=True), _doc(), now=datetime(2026, 11, 27, 14, 5))
    assert result["status"] == "already_sent"


def test_below_threshold_records_the_confirm_without_sending():
    repo = _FakeRepo()
    # 12000 / (16204 * 0.80) ≈ 0.93
    result = alerts.maybe_send(repo, _doc(total=12000.0), now=datetime(2026, 11, 27, 14, 52))
    assert result["status"] == "below_threshold"
    assert repo.records and repo.records[0]["sent"] is False


def test_below_threshold_before_confirm_keeps_waiting():
    repo = _FakeRepo()
    result = alerts.maybe_send(repo, _doc(total=12000.0), now=datetime(2026, 11, 27, 14, 10))
    assert result["status"] == "below_threshold"
    assert repo.records == []  # 14:10 没到确认时点，后面还可能放量


def test_profile_fraction_uses_the_last_bucket_not_after():
    profile = [{"t": "0930", "p50": 0.03}, {"t": "1400", "p50": 0.80}]
    assert alerts.fraction_at(profile, "1401")["p50"] == 0.80
    assert alerts.fraction_at(profile, "0959")["p50"] == 0.03
    assert alerts.fraction_at(profile, "0900") is None


def test_build_profile_matches_cumulative_fraction():
    series = {
        "20261125": [{"t": "0930", "total": 250.0}, {"t": "1400", "total": 800.0}, {"t": "1500", "total": 1000.0}],
        "20261126": [{"t": "0930", "total": 300.0}, {"t": "1400", "total": 850.0}, {"t": "1500", "total": 1000.0}],
        "20261124": [{"t": "0930", "total": 200.0}, {"t": "1400", "total": 750.0}, {"t": "1500", "total": 1000.0}],
    }
    profile = alerts.build_profile(series)
    by_time = {entry["t"]: entry for entry in profile}
    assert by_time["1400"]["p50"] == pytest.approx(0.80)
    assert by_time["1500"]["p50"] == pytest.approx(1.0)
    assert by_time["1400"]["n"] == 3
