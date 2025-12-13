"""Base class for realtime data adapters."""

from abc import ABC, abstractmethod
from typing import Callable, Optional
from ..models.bar import Bar


class RealtimeAdapter(ABC):
    """Abstract base class for realtime data adapters.
    
    Realtime adapters continuously stream data and emit bars via callbacks.
    This is different from historical adapters which fetch batches of data.
    
    Subclasses should implement:
    - start(): Begin streaming data
    - stop(): Stop streaming data
    - is_running(): Check if adapter is currently running
    """
    
    def __init__(self):
        self.running = False
        self.on_bar_callback: Optional[Callable[[str, Bar], None]] = None
    
    @abstractmethod
    def start(self):
        """Start the realtime data stream.
        
        This method should be non-blocking and start a background thread
        or async task to continuously fetch and process data.
        """
        pass
    
    @abstractmethod
    def stop(self):
        """Stop the realtime data stream.
        
        This method should gracefully shutdown any background tasks
        and clean up resources.
        """
        pass
    
    def is_running(self) -> bool:
        """Check if the adapter is currently running.
        
        Returns:
            bool: True if adapter is active, False otherwise
        """
        return self.running
    
    def set_on_bar_callback(self, callback: Callable[[str, Bar], None]):
        """Set the callback function to be called when a new bar is available.
        
        Args:
            callback: Function that takes (symbol, bar) as arguments
        """
        self.on_bar_callback = callback
    
    def _emit_bar(self, symbol: str, bar: Bar):
        """Emit a bar to the registered callback.
        
        Args:
            symbol: Stock symbol
            bar: Bar data to emit
        """
        if self.on_bar_callback:
            try:
                self.on_bar_callback(symbol, bar)
            except Exception as e:
                print(f"[RealtimeAdapter] Error in bar callback: {e}")
