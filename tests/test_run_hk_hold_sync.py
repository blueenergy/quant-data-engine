"""hk_hold is a quarter-end holdings snapshot, not a financial report.

Gating it on the A-share report disclosure calendar held the 20260630 snapshot
back until Aug 31 while every sync re-fetched the same two older quarters and
reported success, so the collection sat on 20260331 for 133 days.
"""
import argparse

from quant_data_engine.tasks.run_hk_hold_sync import (
    available_quarter_ends,
    resolve_dates,
)


def _args(**kw):
    defaults = {"start": None, "end": None, "recent_quarters": 2, "daily": False}
    defaults.update(kw)
    return argparse.Namespace(**defaults)


def test_quarter_end_is_available_once_the_grace_period_passes():
    # 20260630 closed well over the settling period before 20260811.
    assert "20260630" in available_quarter_ends("20240801", "20260811")


def test_quarter_end_is_withheld_immediately_after_it_closes():
    assert "20260630" not in available_quarter_ends("20240801", "20260702")
    assert "20260331" in available_quarter_ends("20240801", "20260702")


def test_report_disclosure_calendar_no_longer_delays_the_snapshot():
    """H1's report deadline is Aug 31; the snapshot must not wait for it."""
    quarters = available_quarter_ends("20240801", "20260811")
    assert quarters[-1] == "20260630"


def test_recent_quarters_picks_the_two_newest_available(monkeypatch):
    import quant_data_engine.tasks.run_hk_hold_sync as mod

    class _FixedDatetime(mod.datetime):
        @classmethod
        def now(cls):
            return cls(2026, 8, 11)

    monkeypatch.setattr(mod, "datetime", _FixedDatetime)

    assert resolve_dates(_args(recent_quarters=2)) == ["20260331", "20260630"]


def test_explicit_range_is_untouched():
    dates = resolve_dates(_args(start="20250101", end="20250930"))
    assert dates == ["20250331", "20250630", "20250930"]


def test_daily_range_expands_every_calendar_day():
    dates = resolve_dates(_args(start="20250101", end="20250104", daily=True))
    assert dates == ["20250101", "20250102", "20250103", "20250104"]
