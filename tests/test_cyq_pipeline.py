import pandas as pd
from quant_data_engine.pipelines.cyq_pipeline import CyqPipeline


class FakeAdapter:
    def __init__(self, df):
        self.df = df
        self.calls = []

    def fetch_cyq_perf(self, **kwargs):
        self.calls.append(kwargs)
        return self.df


class FakeRepo:
    def __init__(self):
        self.count = 0
        self.records = []
        self.state_updates = 0

    def upsert_cyq_perf(self, records):
        self.records.extend(records)
        self.count += len(records)
        return len(records)

    def update_last_sync_date(self, date_str):
        self.state_updates += 1

    def get_last_sync_date(self):
        return None


def test_run_by_date_no_data_marks_issue():
    adapter = FakeAdapter(pd.DataFrame())
    repo = FakeRepo()
    pipe = CyqPipeline(adapter, repo)
    result = pipe.run_by_date(["20240101"])
    assert result["total_records"] == 0
    assert result["results"][0].issues == ["no_data"]


def test_run_by_date_success_updates_state():
    adapter = FakeAdapter(pd.DataFrame({
        "ts_code": ["000001.SZ"],
        "trade_date": ["20240101"],
        "cost_15pct": [10.0],
        "cost_85pct": [11.6],
        "weight_avg": [11.0],
        "winner_rate": [83.11],
    }))
    repo = FakeRepo()
    pipe = CyqPipeline(adapter, repo)
    result = pipe.run_by_date(["20240101"])
    assert result["total_records"] == 1
    assert repo.state_updates == 1
    record = repo.records[0]
    assert record["symbol"] == "000001.SZ"
    assert record["trade_date"] == "20240101"
    assert record["data_source"] == "tushare"


def test_transform_nan_becomes_none_and_symbol_from_ts_code():
    adapter = FakeAdapter(pd.DataFrame({
        "ts_code": ["000001.SZ"],
        "trade_date": ["20240101"],
        "winner_rate": [float("nan")],
    }))
    repo = FakeRepo()
    pipe = CyqPipeline(adapter, repo)
    pipe.run_by_date(["20240101"])
    record = repo.records[0]
    assert record["winner_rate"] is None
    assert record["symbol"] == "000001.SZ"
