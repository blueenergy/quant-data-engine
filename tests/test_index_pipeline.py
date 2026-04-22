import types
import pandas as pd
from quant_data_engine.pipelines.index_daily_pipeline import IndexDailyPipeline
from quant_data_engine.quality.validators import IndexPriceValidator

class FakeAdapter:
    def __init__(self, df: pd.DataFrame):
        self.df = df
    def fetch_index_daily(self, ts_code: str, start_date: str, end_date: str):
        return self.df.assign(trade_date=["20240101","20240102"]) if not self.df.empty else self.df

class FakeRepo:
    def __init__(self):
        self.upserts = 0
    def upsert_daily(self, records):
        self.upserts += len(records)
        return len(records)


def make_df():
    return pd.DataFrame({"open":[10,11],"high":[12,12],"low":[9,10],"close":[11,11]})


def test_pipeline_success():
    adapter = FakeAdapter(make_df())
    repo = FakeRepo()
    validator = IndexPriceValidator()
    pipe = IndexDailyPipeline(adapter, repo, validator)
    out = pipe.run(["000300.SH"], "20240101", "20240102")
    assert out["results"][0]["inserted"] == 2
    assert repo.upserts == 2


def test_pipeline_empty():
    adapter = FakeAdapter(pd.DataFrame())
    repo = FakeRepo()
    validator = IndexPriceValidator()
    pipe = IndexDailyPipeline(adapter, repo, validator)
    out = pipe.run(["000300.SH"], "20240101", "20240102")
    assert out["results"][0]["inserted"] == 0
    assert repo.upserts == 0
