import pandas as pd
from quant_data_engine.pipelines.stock_daily_pipeline import StockDailyPipeline


class FakeAdapter:
    def __init__(self, daily_df, basic_df):
        self.daily_df = daily_df
        self.basic_df = basic_df

    def fetch_daily(self, **kwargs):
        return self.daily_df

    def fetch_daily_basic(self, **kwargs):
        return self.basic_df


class FakeRepo:
    def __init__(self):
        self.daily = 0
        self.basic = 0

    def upsert_daily(self, records):
        self.daily += len(records)
        return len(records)

    def upsert_daily_basic(self, records):
        self.basic += len(records)
        return len(records)


def test_by_date_counts_daily_and_basic():
    daily_df = pd.DataFrame({"ts_code": ["000001.SZ"], "trade_date": ["20240101"], "open": [1], "high": [2], "low": [1], "close": [2], "vol": [1], "amount": [1]})
    basic_df = pd.DataFrame({"ts_code": ["000001.SZ"], "trade_date": ["20240101"], "pe": [10.0]})
    adapter = FakeAdapter(daily_df, basic_df)
    repo = FakeRepo()
    pipe = StockDailyPipeline(adapter, repo)
    result = pipe.run_by_date(["20240101"])
    assert result["total_daily_records"] == 1
    assert result["total_basic_records"] == 1
    assert repo.daily == 1 and repo.basic == 1


def test_by_stock_with_empty_daily():
    daily_df = pd.DataFrame()
    basic_df = pd.DataFrame({"ts_code": ["000001.SZ"], "trade_date": ["20240101"], "pe": [10.0]})
    adapter = FakeAdapter(daily_df, basic_df)
    repo = FakeRepo()
    pipe = StockDailyPipeline(adapter, repo)
    result = pipe.run_by_stock(["000001.SZ"], "20240101", "20240102")
    assert result["total_daily_records"] == 0
    assert result["total_basic_records"] == 1
