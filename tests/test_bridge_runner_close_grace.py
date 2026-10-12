"""BridgeRunner session-boundary behavior around the 15:00 closing auction.

2026-10-12 live evidence: the closing-auction final tick (tick time 15:00:00)
is not visible through the quote channel until ~15:01:24. The runner used to
enter its out-of-session sleep at 15:00:0x, so realtime_latest close froze at
the last continuous-auction price (1-3 fen off the official close). The
15:00-15:05 grace window keeps polling until the final tick lands.
"""
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock
from zoneinfo import ZoneInfo

import pytest

from quant_data_engine.bridges.miniqmt.config import BridgeConfig, UniverseConfig
from quant_data_engine.bridges.miniqmt.runner import (
    BridgeRunner,
    in_close_grace_window,
)

CN_TZ = ZoneInfo("Asia/Shanghai")


def _cn(hour: int, minute: int, second: int = 0) -> datetime:
    return datetime(2026, 10, 13, hour, minute, second, tzinfo=CN_TZ)  # Tuesday


@pytest.mark.parametrize(
    ("hour", "minute", "second", "expected"),
    [
        (14, 59, 59, False),
        (15, 0, 0, False),  # in_cn_stock_session still true at exactly 15:00
        (15, 0, 1, True),
        (15, 3, 0, True),
        (15, 5, 0, True),
        (15, 5, 1, False),
    ],
)
def test_in_close_grace_window_boundaries(hour, minute, second, expected):
    assert in_close_grace_window(_cn(hour, minute, second)) is expected


class _SeqClock:
    def __init__(self, runner: BridgeRunner, times: list[datetime]):
        self._runner = runner
        self._times = times
        self._i = 0

    def __call__(self) -> datetime:
        when = self._times[min(self._i, len(self._times) - 1)]
        self._i += 1
        if self._i >= len(self._times):
            self._runner.stop_requested = True
        return when


class _CountingSink:
    def __init__(self):
        self.polls = 0
        self.skipped: list[str] = []

    def process(self, **kwargs):
        self.polls += 1
        return SimpleNamespace(
            expected_symbols=1, synced_symbols=1, daily_changed=1, latest_changed=1
        )

    def record_skipped(self, **kwargs):
        self.skipped.append(kwargs.get("reason", ""))


def _make_runner(times: list[datetime]):
    config = BridgeConfig(
        mode="loop",
        ignore_session=False,
        refresh=0,
        universe=UniverseConfig(mode="targeted", auto=False, symbols=("000001.SZ",)),
    )
    source = MagicMock()
    source.payload_kind = "ticks"
    source.xtdata = None
    universe = MagicMock()
    universe.resolve.return_value = ["000001.SZ"]
    sink = _CountingSink()
    sleeps = {"n": 0}

    def _sleep(_sec: float) -> None:
        sleeps["n"] += 1

    runner = BridgeRunner(
        config=config,
        universe=universe,
        source=source,
        sinks=[sink],
        sleep_func=_sleep,
        install_signal_handlers=False,
    )
    runner.clock = _SeqClock(runner, times)
    return runner, source, sink, sleeps


def test_grace_window_keeps_polling_after_session():
    times = [_cn(15, 0, 30), _cn(15, 3, 0), _cn(15, 6, 0)]
    runner, source, sink, sleeps = _make_runner(times)
    runner.run()
    # 15:00:30 and 15:03:00 fall through to the poll path; 15:06 exits grace.
    assert source.poll.call_count == 2
    assert sink.polls == 2
    # After 15:05 the loop goes back to sleeping (each skip iteration sleeps).
    assert sleeps["n"] >= 1


def test_weekend_stays_outside_grace():
    saturday = datetime(2026, 10, 24, 15, 2, 0, tzinfo=CN_TZ)
    runner, source, sink, sleeps = _make_runner([saturday])
    runner.run()
    assert source.poll.call_count == 0
    assert sleeps["n"] >= 1