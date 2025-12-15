"""MiniQMT realtime data adapter.

Subscribes to miniQMT tick data and aggregates into minute bars in realtime.
Compatible with quant_data_engine architecture.
"""

import time
import threading
from typing import List, Dict, Tuple, Optional
from datetime import datetime

try:
    from xtquant import xtdata
    _XT_AVAILABLE = True
except ImportError:
    _XT_AVAILABLE = False
    xtdata = None

from .realtime_adapter_base import RealtimeAdapter
from ..models.bar import Bar
from ..utils.trading_scheduler import TradingScheduler


class MiniQMTRealtimeAdapter(RealtimeAdapter):
    """MiniQMT realtime minute bar adapter.
    
    Subscribes to tick data from miniQMT, aggregates into 1-minute bars,
    and emits them via callback. Supports trading time scheduling.
    
    Features:
    - Automatic tick to minute bar aggregation
    - Trading hours scheduling (optional)
    - Volume delta calculation
    - Baseline volume handling
    
    Args:
        symbols: List of symbols with exchange suffix (e.g., ['000001.SZ'])
        interval_sec: Tick polling interval in seconds (default: 1.0)
        enable_schedule: Enable trading time scheduling (default: True)
        debug: Enable debug logging (default: False)
    """
    
    def __init__(
        self,
        symbols: List[str],
        interval_sec: float = 1.0,
        enable_schedule: bool = True,
        debug: bool = False
    ):
        super().__init__()
        
        if not _XT_AVAILABLE:
            raise RuntimeError(
                "xtquant not available. "
                "Install miniQMT/xtquant on Windows."
            )
        
        self.symbols = symbols
        self.interval_sec = interval_sec
        self.enable_schedule = enable_schedule
        self.debug = debug
        
        self.scheduler = TradingScheduler()
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        
        # Minute buckets: key=(minute_str, symbol), value=OHLCV dict
        self._buckets: Dict[Tuple[str, str], Dict] = {}
        
        # Volume tracking
        self._prev_cumulative_volume: Dict[str, int] = {s: 0 for s in symbols}
        self._baseline_volume: Dict[str, int] = {s: 0 for s in symbols}
        
        # Tick sampling statistics
        self._tick_count: Dict[str, int] = {s: 0 for s in symbols}
        self._poll_count = 0
        
        # Last emitted minute to avoid duplicates
        self._last_emitted_minute: str = ""
    
    def start(self):
        """Start the realtime data stream."""
        if self.running:
            print("[MiniQMTRealtime] Already running")
            return
        
        print(f"[MiniQMTRealtime] Starting for {len(self.symbols)} symbols")
        
        # Set baseline volume BEFORE subscribing (match old version order)
        self._set_baseline_volume()
        
        # Subscribe to data
        try:
            xtdata.subscribe_whole_quote(self.symbols)
            print(f"[MiniQMTRealtime] Subscribed to: {', '.join(self.symbols)}")
        except Exception as e:
            print(f"[MiniQMTRealtime] Subscription error: {e}")
            raise
        
        # Start background thread
        self.running = True
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._loop,
            name="MiniQMTRealtimeLoop",
            daemon=True
        )
        self._thread.start()
        
        print("[MiniQMTRealtime] Started")
    
    def stop(self):
        """Stop the realtime data stream."""
        if not self.running:
            return
        
        print("[MiniQMTRealtime] Stopping...")
        self.running = False
        self._stop_event.set()
        
        if self._thread:
            self._thread.join(timeout=5)
        
        print("[MiniQMTRealtime] Stopped")
    
    def _set_baseline_volume(self):
        """Set volume baseline at startup.
        
        This ensures the first bar's volume represents only trades
        during that minute, not cumulative from market open.
        """
        print(f"\n[Volume Baseline] Setting volume baseline for {len(self.symbols)} symbols...")
        
        for sym in self.symbols:
            try:
                # Get current tick data to establish baseline
                ticks = xtdata.get_full_tick([sym])
                t = ticks.get(sym)
                if t and "volume" in t:
                    baseline_vol = int(t["volume"])
                    self._baseline_volume[sym] = baseline_vol
                    print(f"  ✅ {sym}: Baseline {baseline_vol:,}")
                else:
                    print(f"  ⚠️  {sym}: No volume data")
            except Exception as e:
                print(f"  ❌ {sym}: Baseline failed - {e}")
        
        print("[Volume Baseline] Complete\n")
    
    def _loop(self):
        """Main loop: poll ticks and aggregate into bars."""
        while not self._stop_event.is_set():
            try:
                # Check trading time
                if self.enable_schedule and not self.scheduler.is_trading_time():
                    if self.debug:
                        print("[MiniQMTRealtime] Non-trading time, sleeping...")
                    time.sleep(60)
                    continue
                
                # Get tick data
                ticks = xtdata.get_full_tick(self.symbols)
                now_ts = time.time()
                current_sys_minute = time.strftime(
                    "%Y-%m-%d %H:%M",
                    time.localtime(now_ts)
                )
                
                # Count poll attempts
                self._poll_count += 1
                
                # Process each symbol's tick
                for sym in self.symbols:
                    t = ticks.get(sym)
                    if not t:
                        continue
                    
                    # Count successful ticks
                    self._tick_count[sym] = self._tick_count.get(sym, 0) + 1
                    
                    self._process_tick(sym, t, now_ts)
                
                # Emit completed minute bars
                if self._last_emitted_minute and current_sys_minute > self._last_emitted_minute:
                    self._emit_finished_minute(current_sys_minute)
                
                self._last_emitted_minute = current_sys_minute
                
            except Exception as e:
                print(f"[MiniQMTRealtime] Error in loop: {e}")
                if self.debug:
                    import traceback
                    traceback.print_exc()
            
            time.sleep(self.interval_sec)
    
    def _process_tick(self, sym: str, tick: dict, now_ts: float):
        """Process a single tick and update minute bucket.
        
        Args:
            sym: Symbol
            tick: Tick data dictionary
            now_ts: Current timestamp
        """
        # Use exchange timestamp (milliseconds) if available
        # Match the exact logic from the old working version
        time_ms = tick.get("time")
        if time_ms and time_ms != 0:  # Simple check: not None and not 0
            ts = float(time_ms) / 1000.0  # convert ms to seconds
        else:
            ts = now_ts
        
        # Calculate minute bucket key (next minute boundary)
        tick_time = time.localtime(ts)
        bar_minute = time.mktime((
            tick_time.tm_year, tick_time.tm_mon, tick_time.tm_mday,
            tick_time.tm_hour, tick_time.tm_min, 0, 0, 0, -1
        )) + 60
        minute_key = time.strftime("%Y-%m-%d %H:%M", time.localtime(bar_minute))
        key = (minute_key, sym)
        
        price = float(tick.get("lastPrice", 0.0))
        
        # Calculate volume delta
        cumulative_vol = int(tick.get("volume", 0))
        adjusted_cumulative = max(
            0,
            cumulative_vol - self._baseline_volume.get(sym, 0)
        )
        prev_vol = self._prev_cumulative_volume.get(sym, 0)
        vol_delta = max(0, adjusted_cumulative - prev_vol)
        self._prev_cumulative_volume[sym] = adjusted_cumulative
        
        # Volume debug is disabled by default to reduce noise when monitoring multiple stocks
        # Uncomment the lines below if you need to debug volume calculation issues
        # if vol_delta > 0:
        #     print(
        #         f"[Volume Debug] {sym}: raw={cumulative_vol:,} "
        #         f"baseline={self._baseline_volume.get(sym, 0):,} "
        #         f"adjusted={adjusted_cumulative:,} prev={prev_vol:,} delta={vol_delta:,}"
        #     )
        
        # Update or create bucket
        bucket = self._buckets.get(key)
        if bucket is None:
            bucket = {
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "volume": vol_delta,
                "ts": bar_minute,
            }
            self._buckets[key] = bucket
        else:
            bucket["high"] = max(bucket["high"], price)
            bucket["low"] = min(bucket["low"], price)
            bucket["close"] = price
            bucket["volume"] += vol_delta
    
    def _emit_finished_minute(self, minute_str: str):
        """Emit all completed bars for the given minute.
        
        Args:
            minute_str: Minute string in format "YYYY-MM-DD HH:MM"
        """
        finished_keys = [
            k for k in list(self._buckets.keys())
            if k[0] == minute_str
        ]
        
        for key in finished_keys:
            try:
                _, sym = key
                b = self._buckets.pop(key)
                
                # Use the timestamp from the bucket, or current time if not available
                bar_timestamp = b.get("ts")
                if bar_timestamp is None:
                    bar_timestamp = time.time()
                
                bar = Bar(
                    symbol=sym,
                    datetime=bar_timestamp,
                    open=b["open"],
                    high=b["high"],
                    low=b["low"],
                    close=b["close"],
                    volume=b["volume"],
                )
                
                # Emit via callback
                self._emit_bar(sym, bar)
                
                # Formatted bar output (match old version format)
                bar_time_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(bar_timestamp))
                
                # Show tick sampling statistics
                tick_count = self._tick_count.get(sym, 0)
                poll_count = self._poll_count
                coverage = (tick_count / poll_count * 100) if poll_count > 0 else 0
                
                print(f"\n[1-Min Bar] {minute_str} | {sym}")
                print(f"  Open:   {b['open']:.2f}")
                print(f"  High:   {b['high']:.2f}")
                print(f"  Low:    {b['low']:.2f}")
                print(f"  Close:  {b['close']:.2f}")
                print(f"  Volume: {int(b['volume'])} 手")
                print(f"  Time:   {bar_time_str}")
                print(f"  📊 Tick Coverage: {tick_count}/{poll_count} polls ({coverage:.1f}%)")
                    
            except Exception as e:
                print(f"[MiniQMTRealtime] Error emitting bar for {key}: {e}")
                print(f"  Close:  {b['close']:.2f}")
                print(f"  Volume: {int(b['volume'])} 手")
                print(f"  Time:   {bar_time_str}")
                print(f"  📊 Tick Coverage: {tick_count}/{poll_count} polls ({coverage:.1f}%)")
                    
            except Exception as e:
                print(f"[MiniQMTRealtime] Error emitting bar for {key}: {e}")
