"""Tushare adapter with simple rate limiting & retry logic (synchronous minimal version)."""
from __future__ import annotations
import time
import threading
import logging
from typing import List, Dict, Any, Optional
import pandas as pd

logger = logging.getLogger(__name__)

try:
    import tushare as ts  # type: ignore
except Exception:  # pragma: no cover - optional dependency for tests
    ts = None
    logger.warning("Tushare not installed or import failed")

class RateLimiter:
    def __init__(self, rate_per_sec: float):
        self.interval = 1.0 / max(rate_per_sec, 0.1)
        self._lock = threading.Lock()
        self._last = 0.0

    def wait(self):
        with self._lock:
            now = time.time()
            delta = now - self._last
            if delta < self.interval:
                time.sleep(self.interval - delta)
            self._last = time.time()

class TushareAdapter:
    def __init__(self, token: Optional[str], rate_limit_per_sec: float = 4.0, max_retries: int = 2):
        self.enabled = bool(token and ts)
        self._max_retries = max_retries
        self._rl = RateLimiter(rate_limit_per_sec)
        if self.enabled:
            ts.set_token(token)
            self._pro = ts.pro_api()
        else:
            self._pro = None

    def fetch_index_daily(self, ts_code: str, start_date: str, end_date: str):
        if not self.enabled:
            return None
        last_err = None
        for _ in range(self._max_retries + 1):
            try:
                self._rl.wait()
                df = self._pro.index_daily(ts_code=ts_code, start_date=start_date, end_date=end_date)  # type: ignore[attr-defined]
                return df
            except Exception as e:  # pragma: no cover - network dependent
                last_err = e
                time.sleep(0.5)
        raise RuntimeError(f"Tushare fetch_index_daily failed after retries: {last_err}")

    def fetch_etf_daily(self, ts_code: str, start_date: str, end_date: str):
        """Fetch ETF daily OHLC using fund_daily endpoint."""
        if not self.enabled:
            return None
        last_err = None
        for _ in range(self._max_retries + 1):
            try:
                self._rl.wait()
                df = self._pro.fund_daily(ts_code=ts_code, start_date=start_date, end_date=end_date)  # type: ignore[attr-defined]
                return df
            except Exception as e:  # pragma: no cover
                last_err = e
                time.sleep(0.5)
        raise RuntimeError(f"Tushare fetch_etf_daily failed after retries: {last_err}")

class TushareLadderAdapter:
    """
    Adapter for fetching limit-up ladder data from Tushare.
    Handles rate limiting and data formatting.
    """
    
    def __init__(self, token: str):
        if not token:
            raise ValueError("Tushare token is required")
        
        if ts is None:
            raise ImportError("Tushare package is not installed")
            
        try:
            ts.set_token(token)
            self.pro = ts.pro_api()
            self._last_request_time = 0
            logger.info("Tushare adapter initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Tushare adapter: {e}")
            raise

    def _rate_limit(self):
        """
        Enforce rate limiting (approx 1 request per 1.1 seconds).
        Tushare standard limit is often around 100-200 requests/minute depending on score.
        1.1s is a safe conservative default.
        """
        current_time = time.time()
        elapsed = current_time - self._last_request_time
        if elapsed < 1.1:
            sleep_time = 1.1 - elapsed
            logger.debug(f"Rate limiting: sleeping for {sleep_time:.2f}s")
            time.sleep(sleep_time)
        self._last_request_time = time.time()

    def get_limit_up_stocks(self, date: str) -> List[Dict]:
        """
        Fetch daily limit-up stocks list using `limit_list_d`.
        
        Args:
            date: YYYYMMDD format
            
        Returns:
            List of dictionaries with stock data
        """
        self._rate_limit()
        try:
            # limit_type='U' for limit up (D for limit down)
            df = self.pro.limit_list_d(trade_date=date, limit_type='U')
            if df.empty:
                logger.warning(f"No limit-up data found for date {date} from Tushare")
                return []
            
            # Convert to list of dicts
            return df.to_dict('records')
        except Exception as e:
            logger.error(f"Error fetching limit_list_d for {date}: {e}")
            return []

    def get_limit_up_stocks_range(self, start_date: str, end_date: str) -> List[Dict]:
        """
        Fetch limit-up stocks for a date range.
        
        Args:
            start_date: YYYYMMDD
            end_date: YYYYMMDD
            
        Returns:
            List of dicts (flat list of all stocks across all dates)
        """
        self._rate_limit()
        try:
            df = self.pro.limit_list_d(start_date=start_date, end_date=end_date, limit_type='U')
            if df.empty:
                logger.warning(f"No limit-up data found for period {start_date}-{end_date}")
                return []
            return df.to_dict('records')
        except Exception as e:
            logger.error(f"Error fetching limit_list_d range {start_date}-{end_date}: {e}")
            return []

    def get_limit_step(self, date: str) -> List[Dict]:
        """
        Fetch limit-up ladder streak data using `limit_step`.
        Contains consecutive limit-up counts.
        
        Args:
            date: YYYYMMDD format
            
        Returns:
            List of dictionaries with streak info
        """
        self._rate_limit()
        try:
            df = self.pro.limit_step(trade_date=date)
            if df.empty:
                logger.warning(f"No limit_step data found for date {date} from Tushare")
                return []
            
            return df.to_dict('records')
        except Exception as e:
            logger.error(f"Error fetching limit_step for {date}: {e}")
            return []

    def get_sector_ranking(self, date: str) -> List[Dict]:
        """
        Fetch strongest concept sectors using `limit_cpt_list`.
        
        Args:
            date: YYYYMMDD format
        
        Returns:
            List of dictionaries with sector ranking
        """
        self._rate_limit()
        try:
            df = self.pro.limit_cpt_list(trade_date=date)
            if df.empty:
                return []
            
            # Rename columns to match our internal schema if needed, or keep as is
            # Tushare returns: ts_code, name, trade_date, days, up_stat, cons_nums, up_nums, pct_chg, rank
            return df.to_dict('records')
        except Exception as e:
            logger.error(f"Error fetching limit_cpt_list for {date}: {e}")
            return []

__all__ = ["TushareAdapter", "TushareLadderAdapter"]
