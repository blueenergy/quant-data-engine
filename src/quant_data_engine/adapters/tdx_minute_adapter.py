"""Adapter for fetching minute-level data using pytdx."""
from pytdx.reader import TdxExHqDailyBarReader
from pytdx.hq import TdxHq_API
import pandas as pd

class TdxMinuteAdapter:
    def __init__(self, ip_list=None, port: int = 7709):
        self.api = TdxHq_API()
        self.port = port
        self.ip_list = ip_list or [
            '60.191.117.167',
            '218.75.126.9',
            '183.60.224.178',
            '119.147.212.81',
            '119.147.212.82',
            '119.147.212.83',
        ]

    def fetch_minute_bars(self, code: str, market: int, start: int = 0, count: int = 240):
        """Fetch minute bars for a stock code. Returns pandas.DataFrame."""
        bars = None
        for ip in self.ip_list:
            try:
                self.api.connect(ip, self.port)
                bars = self.api.get_security_bars(8, market, code, start, count)
                print(f"尝试IP {ip},bars结果:", bars)
                self.api.disconnect()
                if bars:
                    break
            except Exception as e:
                print(f"连接IP {ip}失败: {e}")
        df = pd.DataFrame(bars) if bars else pd.DataFrame()
        return df
