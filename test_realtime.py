"""Fast unit tests for realtime components.

These tests avoid long-running sleeps by directly invoking the
pipeline callbacks with mock adapters/repos.
"""

import time
import threading
from typing import List
from quant_data_engine.models.bar import Bar
from quant_data_engine.adapters.realtime_adapter_base import RealtimeAdapter


class MockRealtimeAdapter(RealtimeAdapter):
    """Minimal adapter that exposes _emit_bar for tests without threads."""

    def __init__(self, symbols: List[str]):
        super().__init__()
        self.symbols = symbols

    def start(self):
        self.running = True

    def stop(self):
        self.running = False


class MockRepo:
    def __init__(self):
        self.calls = []

    def upsert_minute_bars(self, symbol, df):
        self.calls.append((symbol, df))


class TimedMockAdapter(RealtimeAdapter):
    """Threaded adapter emitting a fixed number of bars with tiny sleeps."""

    def __init__(self, symbols: List[str], bars_to_emit: int = 5, interval: float = 0.01):
        super().__init__()
        self.symbols = symbols
        self.bars_to_emit = bars_to_emit
        self.interval = interval
        self._stop_event = threading.Event()
        self._thread = None

    def start(self):
        if self.running:
            return
        self.running = True
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def stop(self):
        self.running = False
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=1)

    def _loop(self):
        count = 0
        while not self._stop_event.is_set() and count < self.bars_to_emit:
            for symbol in self.symbols:
                now = time.time()
                bar = Bar(
                    symbol=symbol,
                    datetime=now,
                    open=1.0,
                    high=1.0,
                    low=1.0,
                    close=1.0,
                    volume=1,
                )
                self._emit_bar(symbol, bar)
                count += 1
                if count >= self.bars_to_emit:
                    break
            self._stop_event.wait(self.interval)
        self.running = False


def test_realtime_pipeline_on_bar():
    from quant_data_engine.pipelines.realtime_pipeline import RealtimePipeline

    adapter = MockRealtimeAdapter(symbols=["000001.SZ"])
    repo = MockRepo()
    pipeline = RealtimePipeline(adapter=adapter, repository=repo)

    bar = Bar(
        symbol="000001.SZ",
        datetime=time.time(),
        open=10.0,
        high=10.1,
        low=9.9,
        close=10.0,
        volume=1000,
    )

    adapter._emit_bar("000001.SZ", bar)

    assert pipeline.bars_processed == 1
    assert len(repo.calls) == 1
    symbol, df = repo.calls[0]
    assert symbol == "000001"
    assert df.iloc[0]["close"] == 10.0


def test_realtime_pipeline_timed_adapter():
    from quant_data_engine.pipelines.realtime_pipeline import RealtimePipeline

    adapter = TimedMockAdapter(symbols=["000002.SZ"], bars_to_emit=5, interval=0.005)
    repo = MockRepo()
    pipeline = RealtimePipeline(adapter=adapter, repository=repo)

    pipeline.start()
    if adapter._thread:
        adapter._thread.join(timeout=1)
    pipeline.stop()

    assert not adapter.is_running()
    assert pipeline.bars_processed == 5
    assert len(repo.calls) == 5


def test_trading_scheduler():
    """Test the trading scheduler."""
    from quant_data_engine.utils.trading_scheduler import TradingScheduler
    from datetime import datetime
    
    print("=" * 80)
    print("Testing Trading Scheduler")
    print("=" * 80)
    
    scheduler = TradingScheduler()
    
    # Test cases
    test_times = [
        datetime(2025, 12, 13, 9, 0),   # Before open
        datetime(2025, 12, 13, 9, 30),  # Morning open
        datetime(2025, 12, 13, 11, 30), # Morning close
        datetime(2025, 12, 13, 12, 0),  # Lunch break
        datetime(2025, 12, 13, 13, 0),  # Afternoon open
        datetime(2025, 12, 13, 15, 0),  # Afternoon close
        datetime(2025, 12, 13, 16, 0),  # After close
        datetime(2025, 12, 14, 10, 0),  # Saturday
    ]
    
    for dt in test_times:
        is_trading = scheduler.is_trading_time(dt)
        status = "✅ TRADING" if is_trading else "❌ CLOSED"
        print(f"{dt.strftime('%Y-%m-%d %H:%M')} ({dt.strftime('%A')}) -> {status}")
    
    print("=" * 80)


def test_bar_model():
    """Test the Bar data model."""
    from quant_data_engine.models.bar import Bar
    
    print("=" * 80)
    print("Testing Bar Model")
    print("=" * 80)
    
    # Valid bar
    bar = Bar(
        symbol="000001",
        datetime=time.time(),
        open=10.0,
        high=10.5,
        low=9.8,
        close=10.2,
        volume=10000
    )
    print(f"✅ Valid bar: {bar}")
    
    # Invalid bar (high < low)
    try:
        invalid_bar = Bar(
            symbol="000001",
            datetime=time.time(),
            open=10.0,
            high=9.0,  # Invalid: high < low
            low=10.0,
            close=10.0,
            volume=1000
        )
        print("❌ Should have raised ValueError")
    except ValueError as e:
        print(f"✅ Caught expected error: {e}")
    
    print("=" * 80)


if __name__ == "__main__":
    print("\n🧪 Running quant_data_engine realtime tests (fast)\n")
    test_bar_model()
    test_trading_scheduler()
    test_realtime_pipeline_on_bar()
    test_realtime_pipeline_timed_adapter()
    print("\n✅ All tests complete!")
