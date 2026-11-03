"""Unit tests for the supercronic wall-clock gate (cron_gate.py)."""
from __future__ import annotations

import datetime as dt
import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
_spec = importlib.util.spec_from_file_location("cron_gate", ROOT / "cron_gate.py")
cron_gate = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(cron_gate)


def test_parse_step_schedule():
    sched = cron_gate.parse_schedule("*/5_*_*_*_*")
    assert sched[0] == set(range(0, 60, 5))


def test_matches_daily_weekday():
    sched = cron_gate.parse_schedule("20_17_*_*_1-5")
    assert cron_gate.matches(dt.datetime(2026, 11, 3, 17, 20), sched)  # Tuesday
    assert not cron_gate.matches(dt.datetime(2026, 11, 3, 17, 21), sched)
    assert not cron_gate.matches(dt.datetime(2026, 11, 7, 17, 20), sched)  # Saturday


def test_dom_dow_or_semantics():
    # cron: if both day-of-month and day-of-week are restricted, either matches.
    sched = cron_gate.parse_schedule("0_0_1_*_1")
    assert cron_gate.matches(dt.datetime(2026, 12, 1, 0, 0), sched)   # 1st (Tuesday)
    assert cron_gate.matches(dt.datetime(2026, 11, 9, 0, 0), sched)   # Monday
    assert not cron_gate.matches(dt.datetime(2026, 11, 10, 0, 0), sched)  # neither


def test_find_next_daily_occurrence():
    sched = cron_gate.parse_schedule("20_17_*_*_1-5")
    nxt = cron_gate.find_occurrence(dt.datetime(2026, 11, 3, 16, 45), sched, +1)
    assert nxt == dt.datetime(2026, 11, 3, 17, 20)


def test_rewrite_wraps_jobs_and_is_idempotent(tmp_path):
    path = tmp_path / "crontab.txt"
    path.write_text(
        "# comment\n"
        "20 17 * * 1-5 /app/daily_market_sync.sh stockinfo\n"
        "CRON_TZ=Asia/Shanghai\n",
        encoding="utf-8",
    )
    cron_gate.rewrite(str(path))
    text = path.read_text(encoding="utf-8")
    assert "# comment" in text
    assert "CRON_TZ=Asia/Shanghai" in text
    assert (
        "20 17 * * 1-5 python /app/cron_gate.py 20_17_\\*_\\*_1-5 -- "
        "/app/daily_market_sync.sh stockinfo"
    ) in text
    assert text.count("/app/cron_gate.py") == 1

    cron_gate.rewrite(str(path))  # idempotent
    assert path.read_text(encoding="utf-8").count("/app/cron_gate.py") == 1
