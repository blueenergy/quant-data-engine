"""Bar data model for OHLCV bars."""

from dataclasses import dataclass
from typing import Optional


@dataclass
class Bar:
    """Represents a single OHLCV bar.
    
    Attributes:
        symbol: Stock symbol (e.g., '000001' or '000001.SZ')
        datetime: Unix timestamp (seconds since epoch)
        open: Opening price
        high: Highest price
        low: Lowest price
        close: Closing price
        volume: Trading volume
    """
    symbol: str
    datetime: float
    open: float
    high: float
    low: float
    close: float
    volume: float
    
    def __post_init__(self):
        """Validate bar data."""
        if self.high < self.low:
            raise ValueError(f"High ({self.high}) cannot be less than Low ({self.low})")
        if self.open < 0 or self.high < 0 or self.low < 0 or self.close < 0:
            raise ValueError("Prices cannot be negative")
        if self.volume < 0:
            raise ValueError("Volume cannot be negative")
