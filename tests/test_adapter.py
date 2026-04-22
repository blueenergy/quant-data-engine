import pytest
from quant_data_engine.adapters.tushare_adapter import TushareAdapter

class DummyProApi:
    def index_daily(self, ts_code, start_date, end_date):
        raise Exception("API error")

def test_adapter_error_handling(monkeypatch):
    # Patch ts and pro_api
    class DummyTs:
        @staticmethod
        def set_token(token):
            pass
        @staticmethod
        def pro_api():
            return DummyProApi()
    monkeypatch.setattr("quant_data_engine.adapters.tushare_adapter.ts", DummyTs)
    adapter = TushareAdapter(token="dummy", max_retries=1)
    adapter._pro = DummyProApi()
    adapter.enabled = True
    with pytest.raises(RuntimeError) as excinfo:
        adapter.fetch_index_daily("000300.SH", "20240101", "20240102")
    assert "failed after retries" in str(excinfo.value)
