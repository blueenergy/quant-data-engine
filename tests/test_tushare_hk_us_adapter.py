from quant_data_engine.adapters.tushare_adapter import (
    TUSHARE_PER_HOUR_WAIT_SEC,
    TUSHARE_PER_MINUTE_WAIT_SEC,
    TushareAdapter,
    tushare_rate_limit_sleep_sec,
)


class RecordingPro:
    def __init__(self):
        self.calls = []

    def query(self, name, **kw):
        self.calls.append((name, kw))
        import pandas as pd
        return pd.DataFrame({"ts_code": ["x"]})


def _adapter():
    ad = TushareAdapter(token=None)
    ad.enabled = True
    ad._pro = RecordingPro()
    ad._max_retries = 0
    ad._rl = type("RL", (), {"wait": lambda self: None})()
    return ad


def test_tushare_rate_limit_sleep_sec():
    assert tushare_rate_limit_sleep_sec(RuntimeError("频率超限(1次/分钟)")) == TUSHARE_PER_MINUTE_WAIT_SEC
    assert tushare_rate_limit_sleep_sec(RuntimeError("频率超限(1次/小时)")) == TUSHARE_PER_HOUR_WAIT_SEC
    assert tushare_rate_limit_sleep_sec(RuntimeError("timeout")) is None


def test_query_retries_once_after_per_minute_limit(monkeypatch):
    slept = []
    monkeypatch.setattr("quant_data_engine.adapters.tushare_adapter.time.sleep", slept.append)

    class Pro:
        def __init__(self):
            self.n = 0

        def query(self, name, **kw):
            self.n += 1
            if self.n == 1:
                raise RuntimeError(
                    "抱歉，您访问接口(hk_daily)频率超限(1次/分钟)，"
                    "具体频次详情：https://tushare.pro/document/1?doc_id=108。"
                )
            import pandas as pd
            return pd.DataFrame({"ts_code": ["x"]})

    ad = _adapter()
    ad._pro = Pro()
    df = ad._query("hk_daily", trade_date="20260828")
    assert slept == [TUSHARE_PER_MINUTE_WAIT_SEC]
    assert ad._pro.n == 2
    assert list(df["ts_code"]) == ["x"]


def test_query_does_not_retry_hourly_rate_limit(monkeypatch):
    slept = []
    monkeypatch.setattr("quant_data_engine.adapters.tushare_adapter.time.sleep", slept.append)

    class Pro:
        n = 0

        def query(self, name, **kw):
            type(self).n += 1
            raise RuntimeError("抱歉，您访问接口(hk_daily)频率超限(1次/小时)")

    ad = _adapter()
    ad._max_retries = 2
    ad._pro = Pro()
    try:
        ad._query("hk_daily", trade_date="20260828")
    except RuntimeError as e:
        assert "频率超限" in str(e)
    else:
        raise AssertionError("expected RuntimeError")
    assert Pro.n == 1
    assert slept == []


class RecordingPro:
    def __init__(self):
        self.calls = []

    def query(self, name, **kw):
        self.calls.append((name, kw))
        import pandas as pd
        return pd.DataFrame({"ts_code": ["x"]})


def _adapter():
    ad = TushareAdapter(token=None)
    ad.enabled = True
    ad._pro = RecordingPro()
    ad._max_retries = 0
    ad._rl = type("RL", (), {"wait": lambda self: None})()
    return ad


def test_fetch_hk_and_us_equity_use_query_names():
    ad = _adapter()
    ad.fetch_hk_basic(list_status="L")
    ad.fetch_hk_tradecal(start_date="20200101", end_date="20200131")
    ad.fetch_hk_daily(trade_date="20260828")
    ad.fetch_hk_daily_adj(ts_code="00700.HK")
    ad.fetch_us_basic(offset=0, limit=6000)
    ad.fetch_us_tradecal(start_date="20200101")
    ad.fetch_us_daily(ts_code="AAPL")
    ad.fetch_us_daily_adj(trade_date="20260828", exchange="NAS")
    names = [c[0] for c in ad._pro.calls]
    assert names == [
        "hk_basic",
        "hk_tradecal",
        "hk_daily",
        "hk_daily_adj",
        "us_basic",
        "us_tradecal",
        "us_daily",
        "us_daily_adj",
    ]
    us_basic_kw = ad._pro.calls[4][1]
    assert us_basic_kw["offset"] == 0
    assert us_basic_kw["limit"] == 6000
