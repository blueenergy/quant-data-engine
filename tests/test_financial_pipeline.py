import pandas as pd
from quant_data_engine.pipelines.financial_pipeline import FinancialPipeline


class FakeAdapter:
    def __init__(self, income=None, balance=None, cashflow=None, indicator=None):
        self.income = income
        self.balance = balance
        self.cashflow = cashflow
        self.indicator = indicator

    def fetch_income(self, **kwargs):
        return self.income

    def fetch_balancesheet(self, **kwargs):
        return self.balance

    def fetch_cashflow(self, **kwargs):
        return self.cashflow

    def fetch_fina_indicator(self, **kwargs):
        return self.indicator


class FakeRepo:
    def __init__(self):
        self.counts = {"income": 0, "balance": 0, "cashflow": 0, "indicator": 0}

    def upsert_income(self, df=None, records=None):
        self.counts["income"] += len(df) if df is not None else 0
        return self.counts["income"]

    def upsert_balance(self, df=None, records=None):
        self.counts["balance"] += len(df) if df is not None else 0
        return self.counts["balance"]

    def upsert_cashflow(self, df=None, records=None):
        self.counts["cashflow"] += len(df) if df is not None else 0
        return self.counts["cashflow"]

    def upsert_indicators(self, df=None, records=None):
        self.counts["indicator"] += len(df) if df is not None else 0
        return self.counts["indicator"]


def test_data_types_filtering():
    adapter = FakeAdapter(
        income=pd.DataFrame({"ts_code": ["000001.SZ"], "end_date": ["20231231"]}),
        balance=pd.DataFrame({"ts_code": ["000001.SZ"], "end_date": ["20231231"]}),
        cashflow=pd.DataFrame({"ts_code": ["000001.SZ"], "end_date": ["20231231"]}),
        indicator=pd.DataFrame({"ts_code": ["000001.SZ"], "end_date": ["20231231"]}),
    )
    repo = FakeRepo()
    pipe = FinancialPipeline(adapter, repo)
    result = pipe.run(["000001.SZ"], data_types=["income", "financial_indicator"])
    assert result["income_records"] == 1
    assert result["indicator_records"] == 1
    assert result["balance_records"] == 0
    assert result["cashflow_records"] == 0


def test_error_capture():
    class ErrorAdapter(FakeAdapter):
        def fetch_income(self, **kwargs):
            raise Exception("fail")
    adapter = ErrorAdapter()
    repo = FakeRepo()
    pipe = FinancialPipeline(adapter, repo)
    result = pipe.run(["000001.SZ"], data_types=["income"])
    assert result["failed"] == 1
    assert result["results"][0].issues
