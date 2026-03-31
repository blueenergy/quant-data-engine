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

    def fetch_etf_daily(self, ts_code: str = '', trade_date: str = '', start_date: str = '', end_date: str = ''):
        """Fetch ETF daily OHLC using fund_daily endpoint."""
        if not self.enabled:
            return None
        last_err = None
        for _ in range(self._max_retries + 1):
            try:
                self._rl.wait()
                df = self._pro.fund_daily(ts_code=ts_code, trade_date=trade_date, start_date=start_date, end_date=end_date)  # type: ignore[attr-defined]
                return df
            except Exception as e:  # pragma: no cover
                last_err = e
                time.sleep(0.5)
        raise RuntimeError(f"Tushare fetch_etf_daily failed after retries: {last_err}")

    def fetch_fund_basic(self, market: str = 'E', status: str = 'L'):
        """Fetch ETF basic info (market='E' for exchange traded funds, status='L' for listed)."""
        if not self.enabled: return None
        last_err = None
        for _ in range(self._max_retries + 1):
            try:
                self._rl.wait()
                df = self._pro.fund_basic(market=market, status=status)
                return df
            except Exception as e:
                last_err = e
                time.sleep(0.5)
        raise RuntimeError(f"Tushare fetch_fund_basic failed: {last_err}")

    def fetch_fund_adj(self, ts_code: str = '', trade_date: str = '', start_date: str = '', end_date: str = ''):
        """Fetch ETF adjustment factors."""
        if not self.enabled: return None
        last_err = None
        for _ in range(self._max_retries + 1):
            try:
                self._rl.wait()
                df = self._pro.fund_adj(ts_code=ts_code, trade_date=trade_date, start_date=start_date, end_date=end_date)
                return df
            except Exception as e:
                last_err = e
                time.sleep(0.5)
        raise RuntimeError(f"Tushare fetch_fund_adj failed: {last_err}")
        
    def fetch_fund_share(self, ts_code: str = '', trade_date: str = '', start_date: str = '', end_date: str = ''):
        """Fetch ETF share scale."""
        if not self.enabled: return None
        last_err = None
        for _ in range(self._max_retries + 1):
            try:
                self._rl.wait()
                df = self._pro.fund_share(ts_code=ts_code, trade_date=trade_date, start_date=start_date, end_date=end_date)
                return df
            except Exception as e:
                last_err = e
                time.sleep(0.5)
        raise RuntimeError(f"Tushare fetch_fund_share failed: {last_err}")

    def fetch_forecast(self, ts_code: str = '', ann_date: str = '', start_date: str = '', end_date: str = '', period: str = ''):
        """Fetch earnings forecast (业绩预告)."""
        if not self.enabled: return None
        last_err = None
        for _ in range(self._max_retries + 1):
            try:
                self._rl.wait()
                df = self._pro.forecast(ts_code=ts_code, ann_date=ann_date, start_date=start_date, end_date=end_date, period=period)
                return df
            except Exception as e:
                last_err = e
                time.sleep(0.5)
        raise RuntimeError(f"Tushare fetch_forecast failed: {last_err}")

    def fetch_express(self, ts_code: str = '', ann_date: str = '', start_date: str = '', end_date: str = '', period: str = ''):
        """Fetch earnings express (业绩快报)."""
        if not self.enabled: return None
        last_err = None
        for _ in range(self._max_retries + 1):
            try:
                self._rl.wait()
                df = self._pro.express(ts_code=ts_code, ann_date=ann_date, start_date=start_date, end_date=end_date, period=period)
                return df
            except Exception as e:
                last_err = e
                time.sleep(0.5)
        raise RuntimeError(f"Tushare fetch_express failed: {last_err}")

    def fetch_disclosure_date(self, ts_code: str = '', end_date: str = '', pre_date: str = '', actual_date: str = ''):
        """Fetch earnings disclosure date schedule (财报披露计划)."""
        if not self.enabled: return None
        last_err = None
        for _ in range(self._max_retries + 1):
            try:
                self._rl.wait()
                df = self._pro.disclosure_date(ts_code=ts_code, end_date=end_date, pre_date=pre_date, actual_date=actual_date)
                return df
            except Exception as e:
                last_err = e
                time.sleep(0.5)
        raise RuntimeError(f"Tushare fetch_disclosure_date failed: {last_err}")

    def fetch_report_rc(self, ts_code: str = '', report_date: str = '', start_date: str = '', end_date: str = ''):
        """Fetch broker earnings forecasts/ratings (券商盈利预测与评级)."""
        if not self.enabled: return None
        last_err = None
        for _ in range(self._max_retries + 1):
            try:
                self._rl.wait()
                df = self._pro.report_rc(ts_code=ts_code, report_date=report_date, start_date=start_date, end_date=end_date)
                return df
            except Exception as e:
                last_err = e
                time.sleep(0.5)
        raise RuntimeError(f"Tushare fetch_report_rc failed: {last_err}")

    def fetch_ths_hot(self, trade_date: str = '') -> Optional[pd.DataFrame]:
        """Fetch THS (同花顺) hot stock ranking.
        
        Args:
            trade_date: Trade date in YYYYMMDD format. Empty string for latest.
        """
        if not self.enabled:
            return None
        last_err = None
        for _ in range(self._max_retries + 1):
            try:
                self._rl.wait()
                df = self._pro.ths_hot(trade_date=trade_date)
                return df
            except Exception as e:
                last_err = e
                time.sleep(0.5)
        raise RuntimeError(f"Tushare fetch_ths_hot failed after retries: {last_err}")

    def fetch_dc_hot(self, trade_date: str = '') -> Optional[pd.DataFrame]:
        """Fetch DC (东方财富) hot stock ranking.
        
        Args:
            trade_date: Trade date in YYYYMMDD format. Empty string for latest.
        """
        if not self.enabled:
            return None
        last_err = None
        for _ in range(self._max_retries + 1):
            try:
                self._rl.wait()
                df = self._pro.dc_hot(trade_date=trade_date)
                return df
            except Exception as e:
                last_err = e
                time.sleep(0.5)
        raise RuntimeError(f"Tushare fetch_dc_hot failed after retries: {last_err}")

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
    def get_limit_down_stocks(self, date: str) -> List[Dict]:
        """
        Fetch daily limit-down stocks list using `limit_list_d`.
        
        Args:
            date: YYYYMMDD format
            
        Returns:
            List of dictionaries with stock data
        """
        self._rate_limit()
        try:
            # limit_type='D' for limit down
            df = self.pro.limit_list_d(trade_date=date, limit_type='D')
            if df.empty:
                logger.warning(f"No limit-down data found for date {date} from Tushare")
                return []
            
            return df.to_dict('records')
        except Exception as e:
            logger.error(f"Error fetching limit_list_d (down) for {date}: {e}")
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

    # ========== East Money Concept APIs ==========
    
    _concept_cache: Dict[str, str] = {}  # BK代码 -> 概念名称
    _concept_cache_time: float = 0
    
    def get_concept_mapping(self, force_refresh: bool = False) -> Dict[str, str]:
        """
        Get mapping of concept code (BKxxxx.DC) to concept name.
        Uses in-memory cache with 24-hour TTL (concept list rarely changes).
        
        Returns:
            Dict mapping ts_code -> name (e.g., {'BK0428.DC': '电力'})
        """
        # Check cache (24 hour TTL - concept list rarely changes)
        current_time = time.time()
        if not force_refresh and self._concept_cache and (current_time - self._concept_cache_time) < 86400:
            return self._concept_cache
        
        self._rate_limit()
        try:
            df = self.pro.dc_index()
            if df.empty:
                logger.warning("No concept data from dc_index")
                return self._concept_cache or {}
            
            # Build mapping: BKxxxx.DC -> 概念名称
            self._concept_cache = dict(zip(df['ts_code'], df['name']))
            self._concept_cache_time = current_time
            logger.info(f"Cached {len(self._concept_cache)} concept mappings")
            return self._concept_cache
        except Exception as e:
            logger.error(f"Error fetching dc_index: {e}")
            return self._concept_cache or {}
    
    def get_stock_concepts(self, ts_code: str, trade_date: str) -> List[str]:
        """
        Get list of concept names for a stock using dc_member API.
        
        Args:
            ts_code: Stock code (e.g., '600900.SH')
            trade_date: Trade date in YYYYMMDD format
            
        Returns:
            List of concept names (e.g., ['电力', '新能源', '央企改革'])
        """
        self._rate_limit()
        try:
            df = self.pro.dc_member(con_code=ts_code, trade_date=trade_date)
            if df.empty:
                logger.debug(f"No concept membership found for {ts_code}")
                return []
            
            # Get concept codes from result
            concept_codes = df['ts_code'].tolist()
            
            # Get mapping and resolve names
            concept_map = self.get_concept_mapping()
            concept_names = []
            for code in concept_codes:
                name = concept_map.get(code)
                if name:
                    concept_names.append(name)
                else:
                    # Fallback: use code if name not found
                    logger.debug(f"Concept name not found for {code}")
                    concept_names.append(code)
            
            return concept_names
        except Exception as e:
            logger.error(f"Error fetching dc_member for {ts_code}: {e}")
            return []

__all__ = ["TushareAdapter", "TushareLadderAdapter"]
