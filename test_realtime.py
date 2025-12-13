"""Test script for realtime pipeline.

This script tests the realtime data collection components
without actually running miniQMT. It uses mock data to verify
the pipeline architecture.
"""

import time
import threading
from typing import List
from quant_data_engine.models.bar import Bar
from quant_data_engine.adapters.realtime_adapter_base import RealtimeAdapter


class MockRealtimeAdapter(RealtimeAdapter):
    """Mock realtime adapter for testing.
    
    Generates fake bars every 5 seconds.
    """
    
    def __init__(self, symbols: List[str]):
        super().__init__()
        self.symbols = symbols
        self._thread = None
        self._stop_event = threading.Event()
        self._bar_count = 0
    
    def start(self):
        """Start generating fake bars."""
        if self.running:
            return
        
        print(f"[MockAdapter] Starting for {len(self.symbols)} symbols")
        self.running = True
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
    
    def stop(self):
        """Stop generating fake bars."""
        if not self.running:
            return
        
        print("[MockAdapter] Stopping...")
        self.running = False
        self._stop_event.set()
        
        if self._thread:
            self._thread.join(timeout=5)
    
    def _loop(self):
        """Generate fake bars periodically."""
        while not self._stop_event.is_set():
            for symbol in self.symbols:
                bar = Bar(
                    symbol=symbol,
                    datetime=time.time(),
                    open=10.0 + self._bar_count * 0.1,
                    high=10.1 + self._bar_count * 0.1,
                    low=9.9 + self._bar_count * 0.1,
                    close=10.0 + self._bar_count * 0.1,
                    volume=1000 + self._bar_count * 100
                )
                self._emit_bar(symbol, bar)
                print(f"[MockAdapter] Generated bar for {symbol}")
            
            self._bar_count += 1
            time.sleep(5)  # Generate bar every 5 seconds


def test_realtime_pipeline():
    """Test the realtime pipeline with mock adapter."""
    from quant_data_engine import get_settings, create_realtime_pipeline
    from quant_data_engine.repositories.minute_repository import MinuteRepository
    from quant_data_engine.pipelines.realtime_pipeline import RealtimePipeline
    
    print("=" * 80)
    print("Testing Realtime Pipeline")
    print("=" * 80)
    
    # Create mock adapter
    mock_adapter = MockRealtimeAdapter(symbols=["000001", "600618"])
    
    # Create repository
    settings = get_settings()
    repository = MinuteRepository(
        mongo_uri=settings.mongo_uri,
        mongo_db=settings.mongo_db
    )
    
    # Create pipeline
    pipeline = RealtimePipeline(
        adapter=mock_adapter,
        repository=repository
    )
    
    # Start pipeline
    print("\n🚀 Starting pipeline...")
    pipeline.start()
    
    # Run for 20 seconds
    print("⏱️ Running for 20 seconds...")
    time.sleep(20)
    
    # Stop pipeline
    print("\n⏹️ Stopping pipeline...")
    pipeline.stop()
    
    print(f"\n✅ Test complete. Processed {pipeline.bars_processed} bars")
    print("=" * 80)


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
    print("\n🧪 Running quant_data_engine realtime tests\n")
    
    # Test 1: Bar model
    test_bar_model()
    print()
    
    # Test 2: Trading scheduler
    test_trading_scheduler()
    print()
    
    # Test 3: Realtime pipeline (requires MongoDB)
    test_realtime_pipeline()
    
    print("\n✅ All tests complete!")
