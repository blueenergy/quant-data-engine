import pandas as pd
from quant_data_engine.pipelines.moneyflow_pipeline import MoneyFlowPipeline


class FakeAdapter:
    def __init__(self, df):
        self.df = df
    def fetch_moneyflow(self, **kwargs):
        return self.df


class FakeRepo:
    def __init__(self):
        self.count = 0
        self.state_updates = 0
    def upsert_moneyflow(self, records):
        self.count += len(records)
        return len(records)
    def update_last_sync_date(self, date_str):
        self.state_updates += 1
    def get_last_sync_date(self):
        return None


def test_run_by_date_no_data_marks_issue():
    adapter = FakeAdapter(pd.DataFrame())
    repo = FakeRepo()
    pipe = MoneyFlowPipeline(adapter, repo)
    result = pipe.run_by_date(["20240101"])
    assert result["total_records"] == 0
    assert result["results"][0].issues == ["no_data"]


def test_run_by_date_success_updates_state():
    adapter = FakeAdapter(pd.DataFrame({"ts_code": ["000001.SZ"], "trade_date": ["20240101"], "buy_sm_vol": [1.0]}))
    repo = FakeRepo()
    pipe = MoneyFlowPipeline(adapter, repo, ts_code_map={"000001.SZ": "000001"})
    result = pipe.run_by_date(["20240101"])
    assert result["total_records"] == 1
    assert repo.state_updates == 1
