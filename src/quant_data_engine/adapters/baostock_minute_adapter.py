"""Adapter for fetching minute-level data using baostock."""
import baostock as bs
import pandas as pd

class BaostockMinuteAdapter:
    def __init__(self):
        pass

    def fetch_minute_bars(self, code: str, start_date: str, end_date: str, freq: str = "1"):
        """Fetch minute bars for a stock code. Returns pandas.DataFrame.
        freq: "1" for 1min, "5" for 5min, etc.
        code: e.g. 'sz.300722', 'sh.600000'
        """
        bs.login()
        rs = bs.query_history_k_data_plus(
            code,
            "date,time,open,high,low,close,volume",
            start_date=start_date,
            end_date=end_date,
            frequency=freq,
            adjustflag="2"
        )
        data_list = []
        while (rs.error_code == '0') & rs.next():
            data_list.append(rs.get_row_data())
        bs.logout()
        df = pd.DataFrame(data_list, columns=rs.fields)
        # 合并日期和时间为 trade_date
        df['trade_date'] = df['date'] + df['time'].str.replace(':', '')
        df.rename(columns={'volume': 'vol'}, inplace=True)
        return df
