"""Industry and ETF data adapter using Baostock."""
from __future__ import annotations
import logging
import pandas as pd
from typing import Optional, List

logger = logging.getLogger(__name__)

try:
    import baostock as bs
except ImportError:
    bs = None
    logger.warning("Baostock not installed")

class IndustryAdapter:
    def __init__(self):
        self.bs_logged_in = False

    def _login_bs(self):
        if bs and not self.bs_logged_in:
            lg = bs.login()
            if lg.error_code == '0':
                self.bs_logged_in = True
            else:
                logger.error(f"Baostock login failed: {lg.error_msg}")

    def _logout_bs(self):
        if bs and self.bs_logged_in:
            bs.logout()
            self.bs_logged_in = False

    def fetch_etf_daily(self, etf_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """Fetch historical daily data for an ETF using Baostock."""
        if not bs:
            return pd.DataFrame()
        
        self._login_bs()
        try:
            # Baostock code format: sh.516160 or sz.159995
            symbol = f"sh.{etf_code}" if etf_code.startswith(('58', '51', '60')) else f"sz.{etf_code}"
            
            # Format dates: YYYYMMDD -> YYYY-MM-DD
            s_date = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:]}"
            e_date = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:]}"
            
            rs = bs.query_history_k_data_plus(
                symbol,
                "date,open,high,low,close,pctChg,volume,amount",
                start_date=s_date,
                end_date=e_date,
                frequency="d",
                adjustflag="3" # No adjustment needed for basic comparison or 2 for qfq
            )
            
            if rs.error_code != '0':
                logger.error(f"Baostock query error for {etf_code}: {rs.error_msg}")
                return pd.DataFrame()
                
            return rs.get_data()
        except Exception as e:
            logger.error(f"Error fetching ETF daily for {etf_code}: {e}")
            return pd.DataFrame()
        finally:
            self._logout_bs()

    def fetch_industry_list(self) -> List[str]:
        """Placeholder for industry list discovery (now handled via DB)."""
        return []

    def fetch_industry_daily(self, industry_name: str, start_date: str, end_date: str) -> pd.DataFrame:
        """Fetch historical daily data for industry board (Disabled due to network)."""
        return pd.DataFrame()

__all__ = ["IndustryAdapter"]
