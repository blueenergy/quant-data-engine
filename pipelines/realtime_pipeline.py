"""Realtime data processing pipeline.

Connects a realtime adapter to a repository, enabling continuous
data flow from source → aggregation → storage.
"""

import time
import pandas as pd
from ..adapters.realtime_adapter_base import RealtimeAdapter
from ..repositories.minute_repository import MinuteRepository
from ..models.bar import Bar


class RealtimePipeline:
    """Realtime data processing pipeline.
    
    Receives bars from a realtime adapter and stores them in the repository.
    This decouples data collection from storage, following the repository pattern.
    
    Args:
        adapter: Realtime data adapter
        repository: Minute data repository
    """
    
    def __init__(
        self,
        adapter: RealtimeAdapter,
        repository: MinuteRepository
    ):
        self.adapter = adapter
        self.repository = repository
        self.bars_processed = 0
        
        # Register callback
        self.adapter.set_on_bar_callback(self._on_bar)
    
    def _on_bar(self, symbol: str, bar: Bar):
        """Handle new bar from adapter.
        
        Args:
            symbol: Stock symbol (may have exchange suffix)
            bar: Bar data
        """
        try:
            # Remove exchange suffix (normalize)
            normalized_symbol = symbol.split('.')[0] if '.' in symbol else symbol
            
            # Ensure bar.datetime is valid (safety check for 0 or negative values)
            bar_ts = float(bar.datetime)
            if bar_ts <= 0:  # Only reject if 0 or negative
                bar_ts = time.time()
            
            # Convert to DataFrame for repository
            lt = time.localtime(bar_ts)
            trade_date = (
                f"{lt.tm_year:04d}{lt.tm_mon:02d}{lt.tm_mday:02d}"
                f"{lt.tm_hour:02d}{lt.tm_min:02d}"
            )
            
            df = pd.DataFrame([{
                'symbol': normalized_symbol,
                'trade_date': trade_date,
                'datetime': bar_ts,
                'open': round(float(bar.open), 2),      # Round to 2 decimals
                'high': round(float(bar.high), 2),
                'low': round(float(bar.low), 2),
                'close': round(float(bar.close), 2),
                'volume': float(bar.volume),
            }])
            
            # Store in repository
            self.repository.upsert_minute_bars(normalized_symbol, df)
            self.bars_processed += 1
            
            print(
                f"✅ Saved {normalized_symbol} {trade_date} | "
                f"C={bar.close:.2f} V={int(bar.volume)}"
            )
            
        except Exception as e:
            print(f"❌ Error saving bar for {symbol}: {e}")
            import traceback
            traceback.print_exc()
    
    def start(self):
        """Start the pipeline."""
        print("[RealtimePipeline] Starting...")
        self.adapter.start()
        print("[RealtimePipeline] Running")
    
    def stop(self):
        """Stop the pipeline."""
        print("[RealtimePipeline] Stopping...")
        self.adapter.stop()
        print(f"[RealtimePipeline] Stopped (processed {self.bars_processed} bars)")
    
    def is_running(self) -> bool:
        """Check if pipeline is running.
        
        Returns:
            bool: True if adapter is active
        """
        return self.adapter.is_running()
