import pandas as pd
import pytest
from quant_data_engine.adapters import stock_adapter
from quant_data_engine.adapters.stock_adapter import StockAdapter


class DummyPro:
    def __init__(self):
        self.calls = 0
        self.daily_basic_kwargs = None

    def daily(self, **kwargs):
        self.calls += 1
        if self.calls == 1:
            raise Exception("transient")
        return pd.DataFrame({"ts_code": ["000001.SZ"], "trade_date": ["20240101"], "open": [1], "high": [2], "low": [1], "close": [2], "pre_close": [1], "change": [1], "pct_chg": [100.0], "vol": [10], "amount": [20]})

    def daily_basic(self, **kwargs):
        self.daily_basic_kwargs = kwargs
        return pd.DataFrame({"ts_code": ["000001.SZ"], "trade_date": ["20240101"], "pe": [10.0]})

    def moneyflow(self, **kwargs):
        return pd.DataFrame({"ts_code": ["000001.SZ"], "trade_date": ["20240101"], "buy_sm_vol": [1.0]})


class DummyTs:
    pro = None

    @staticmethod
    def set_token(token):
        pass

    @staticmethod
    def pro_api():
        DummyTs.pro = DummyPro()
        return DummyTs.pro


def test_adapter_retry(monkeypatch):
    monkeypatch.setattr(stock_adapter, "ts", DummyTs)
    adapter = StockAdapter(token="x", max_retries=1, rate_limit_per_sec=1000)
    df = adapter.fetch_daily(trade_date="20240101")
    assert not df.empty


def test_adapter_basic_and_moneyflow(monkeypatch):
    monkeypatch.setattr(stock_adapter, "ts", DummyTs)
    adapter = StockAdapter(token="x", max_retries=0, rate_limit_per_sec=1000)
    basic = adapter.fetch_daily_basic(trade_date="20240101")
    mf = adapter.fetch_moneyflow(trade_date="20240101")
    assert list(basic.columns) == ["ts_code", "trade_date", "pe"]
    assert DummyTs.pro is not None
    fields = DummyTs.pro.daily_basic_kwargs["fields"].split(",")
    assert "dv_ratio" in fields
    assert "dv_ttm" in fields
    assert "turnover_rate_f" in fields
    assert list(mf.columns) == ["ts_code", "trade_date", "buy_sm_vol"]


def test_windows_only_factories(monkeypatch):
    from quant_data_engine import IS_WINDOWS, create_mini_qmt_pipeline, create_realtime_pipeline
    if IS_WINDOWS:
        pytest.skip("windows specific")
    with pytest.raises(RuntimeError):
        create_mini_qmt_pipeline()
    with pytest.raises(RuntimeError):
        create_realtime_pipeline(symbols=["000001.SZ"])
