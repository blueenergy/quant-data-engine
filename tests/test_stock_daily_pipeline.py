import pandas as pd
from quant_data_engine.pipelines.stock_daily_pipeline import StockDailyPipeline


class FakeAdapter:
    def __init__(self, daily_df, basic_df, adj_df=None):
        self.daily_df = daily_df
        self.basic_df = basic_df
        self.adj_df = adj_df if adj_df is not None else pd.DataFrame()
        self.daily_calls = 0
        self.basic_calls = 0
        self.adj_calls = 0

    def fetch_daily(self, **kwargs):
        self.daily_calls += 1
        return self.daily_df

    def fetch_daily_basic(self, **kwargs):
        self.basic_calls += 1
        return self.basic_df

    def fetch_adj_factor(self, **kwargs):
        self.adj_calls += 1
        return self.adj_df


class FakeRepo:
    def __init__(self):
        self.daily = 0
        self.basic = 0
        self.adj = 0
        self.basic_records = []
        self.adj_records = []

    def upsert_daily(self, records):
        self.daily += len(records)
        return len(records)

    def upsert_daily_basic(self, records):
        self.basic_records.extend(records)
        self.basic += len(records)
        return len(records)

    def upsert_adj_factors(self, records):
        self.adj_records.extend(records)
        self.adj += len(records)
        return len(records)


def test_by_date_counts_daily_and_basic():
    daily_df = pd.DataFrame({"ts_code": ["000001.SZ"], "trade_date": ["20240101"], "open": [1], "high": [2], "low": [1], "close": [2], "vol": [1], "amount": [1]})
    basic_df = pd.DataFrame({
        "ts_code": ["000001.SZ"],
        "trade_date": ["20240101"],
        "pe": [10.0],
        "turnover_rate_f": [2.5],
        "dv_ratio": [1.2],
        "dv_ttm": [1.4],
    })
    adapter = FakeAdapter(daily_df, basic_df)
    repo = FakeRepo()
    pipe = StockDailyPipeline(adapter, repo)
    result = pipe.run_by_date(["20240101"])
    assert result["total_daily_records"] == 1
    assert result["total_basic_records"] == 1
    assert result["total_adj_factor_records"] == 0
    assert repo.daily == 1 and repo.basic == 1
    assert repo.basic_records[0]["turnover_rate_f"] == 2.5
    assert repo.basic_records[0]["dv_ratio"] == 1.2
    assert repo.basic_records[0]["dv_ttm"] == 1.4


def test_by_stock_with_empty_daily():
    daily_df = pd.DataFrame()
    basic_df = pd.DataFrame({"ts_code": ["000001.SZ"], "trade_date": ["20240101"], "pe": [10.0]})
    adapter = FakeAdapter(daily_df, basic_df)
    repo = FakeRepo()
    pipe = StockDailyPipeline(adapter, repo)
    result = pipe.run_by_stock(["000001.SZ"], "20240101", "20240102")
    assert result["total_daily_records"] == 0
    assert result["total_basic_records"] == 1


def test_by_date_basic_only_skips_daily_fetch_and_write():
    daily_df = pd.DataFrame({"ts_code": ["000001.SZ"], "trade_date": ["20240101"], "open": [1], "high": [2], "low": [1], "close": [2], "vol": [1], "amount": [1]})
    basic_df = pd.DataFrame({"ts_code": ["000001.SZ"], "trade_date": ["20240101"], "pe": [10.0]})
    adapter = FakeAdapter(daily_df, basic_df)
    repo = FakeRepo()
    pipe = StockDailyPipeline(adapter, repo)

    result = pipe.run_by_date(["20240101"], include_daily=False)

    assert result["total_daily_records"] == 0
    assert result["total_basic_records"] == 1
    assert adapter.daily_calls == 0
    assert adapter.basic_calls == 1
    assert adapter.adj_calls == 0
    assert repo.daily == 0
    assert repo.basic == 1


def test_by_date_adj_factor_only_fetches_and_writes_adj():
    daily_df = pd.DataFrame({"ts_code": ["000001.SZ"], "trade_date": ["20240101"], "open": [1], "high": [2], "low": [1], "close": [2]})
    basic_df = pd.DataFrame({"ts_code": ["000001.SZ"], "trade_date": ["20240101"], "pe": [10.0]})
    adj_df = pd.DataFrame({"ts_code": ["000001.SZ"], "trade_date": ["20240101"], "adj_factor": [1.234]})
    adapter = FakeAdapter(daily_df, basic_df, adj_df)
    repo = FakeRepo()
    pipe = StockDailyPipeline(adapter, repo)

    result = pipe.run_by_date(
        ["20240101"],
        include_daily=False,
        include_basic=False,
        include_adj_factor=True,
    )

    assert result["total_daily_records"] == 0
    assert result["total_basic_records"] == 0
    assert result["total_adj_factor_records"] == 1
    assert adapter.daily_calls == 0
    assert adapter.basic_calls == 0
    assert adapter.adj_calls == 1
    assert repo.adj_records[0]["adj_factor"] == 1.234
