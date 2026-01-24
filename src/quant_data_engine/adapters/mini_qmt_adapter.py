"""Adapter for fetching minute-level data using miniQMT (国金QMT)."""

from typing import Optional
import pandas as pd
import time

try:
    from xtquant import xtdata
    xtdata.enable_hello = False  # Hide hello message
    _XT_DATA_AVAILABLE = True
except ImportError:
    _XT_DATA_AVAILABLE = False
    xtdata = None


class MiniQMTAdapter:
    def __init__(self):
        if not _XT_DATA_AVAILABLE:
            raise RuntimeError("xtquant not available. Run on Windows with miniQMT installed.")
    
    def fetch_minute_bars(self, code: str, start_date: str, end_date: str, period: str = '1m', max_wait: int = 60):
        """Fetch minute bars for a stock code. Returns pandas.DataFrame.
        
        Args:
            code: Stock symbol (e.g., '300722.SZ')
            start_date: Start date in format 'YYYYMMDD'
            end_date: End date in format 'YYYYMMDD'
            period: Data frequency ('1m', '5m', '15m', '30m', '1h', '1d')
            max_wait: Max seconds to wait for data. Set to -1 for infinite wait (default 60)
            
        Returns:
            pandas.DataFrame: DataFrame containing the requested data
        """
        try:
            # Request data download
            print(f"  Requesting data for {code}...")
            xtdata.download_history_data(
                stock_code=code,
                period=period,
                start_time=start_date,
                end_time=end_date
            )
            
            # Poll for data with timeout
            wait_msg = "infinite" if max_wait == -1 else f"max {max_wait}s"
            print(f"  Waiting for data to be ready ({wait_msg})...")
            elapsed = 0
            while True:
                data = xtdata.get_local_data(
                    field_list=[],
                    stock_list=[code],
                    period=period,
                    start_time=start_date,
                    end_time=end_date
                )
                
                if data and code in data:
                    df = data[code]
                    if not df.empty:
                        print(f"  Data ready: {len(df)} records")
                        print(f"  [Debug] Columns: {df.columns.tolist()}")
                        print(f"  [Debug] Index: {df.index.name}, dtype: {df.index.dtype}")
                        print(f"  [Debug] First row: {df.iloc[0].to_dict()}")
                        # Reset index to convert time index to column if needed
                        if not isinstance(df, pd.DataFrame):
                            return pd.DataFrame()
                        if df.index.name == 'time':
                            df = df.reset_index()
                            df.rename(columns={'time': 'datetime'}, inplace=True)
                        elif df.index.name is None and isinstance(df.index, pd.DatetimeIndex):
                            df = df.reset_index()
                            df.rename(columns={df.columns[0]: 'datetime'}, inplace=True)
                        return df
                
                # Check timeout (skip if max_wait is -1)
                if max_wait != -1 and elapsed >= max_wait:
                    print(f"  Timeout waiting for data")
                    return pd.DataFrame()
                
                # Wait before retrying
                time.sleep(2)
                elapsed += 2
                
        except Exception as e:
            print(f"Error fetching data for {code}: {e}")
            return pd.DataFrame()
    
    def download_batch_data(self, symbols: list, start_date: str, end_date: str, period: str = '1m'):
        """Download data for multiple symbols.
        
        Args:
            symbols: List of stock symbols
            start_date: Start date in format 'YYYYMMDD'
            end_date: End date in format 'YYYYMMDD'
            period: Data frequency
            
        Returns:
            dict: Dictionary with symbol as key and success status as value
        """
        results = {}
        for symbol in symbols:
            try:
                xtdata.download_history_data(
                    stock_code=symbol,
                    period=period,
                    start_time=start_date,
                    end_time=end_date
                )
                results[symbol] = True
                print(f"✅ Download initiated for {symbol}")
            except Exception as e:
                print(f"❌ Error initiating download for {symbol}: {e}")
                results[symbol] = False
        return results