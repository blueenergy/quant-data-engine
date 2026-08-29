import pandas as pd
import pytest

from quant_data_engine.pipelines.overseas_equity_pipeline import (
    OverseasEquityPipeline,
    OverseasPermissionError,
    classify_tushare_error,
)


class FakeAdapter:
    enabled = True

    def __init__(self):
        self.calls = []

    def fetch_hk_basic(self, list_status="L", offset=None, limit=None):
        self.calls.append(("hk_basic", list_status, offset))
        if list_status == "L":
            return pd.DataFrame({"ts_code": ["00700.HK"], "name": ["腾讯控股"], "cn_spell": ["txkg"]})
        if list_status == "D":
            return pd.DataFrame({"ts_code": ["00018.HK"], "name": ["退市样例"]})
        return pd.DataFrame()

    def fetch_us_basic(self, offset=None, limit=None, **_):
        self.calls.append(("us_basic", offset, limit))
        off = offset or 0
        if off == 0:
            return pd.DataFrame({"ts_code": [f"T{i}" for i in range(limit or 2)]})
        if off == (limit or 2):
            return pd.DataFrame({"ts_code": ["AAPL"]})
        return pd.DataFrame()

    def fetch_hk_tradecal(self, start_date="", end_date="", is_open="", offset=None, limit=None):
        self.calls.append(("hk_tradecal", start_date, end_date, is_open))
        return pd.DataFrame(
            {"cal_date": ["20260828", "20260829"], "is_open": [1, 0], "pretrade_date": ["20260827", "20260828"]}
        )

    def fetch_us_tradecal(self, start_date="", end_date="", is_open="", offset=None, limit=None):
        self.calls.append(("us_tradecal", start_date, end_date, is_open))
        return pd.DataFrame({"cal_date": ["20260828"], "is_open": [1]})

    def fetch_hk_daily(self, trade_date="", offset=None, limit=None, **_):
        self.calls.append(("hk_daily", trade_date, offset))
        if offset:
            return pd.DataFrame()
        return pd.DataFrame(
            {
                "ts_code": ["00700.HK"],
                "trade_date": [trade_date],
                "open": [440.0],
                "high": [450.0],
                "low": [438.0],
                "close": [445.0],
                "vol": [100],
                "pct_chg": [1.1],
            }
        )

    def fetch_hk_daily_adj(self, trade_date="", offset=None, limit=None, **_):
        self.calls.append(("hk_daily_adj", trade_date, offset))
        raise RuntimeError("抱歉，您没有接口(hk_daily_adj)访问权限")

    def fetch_us_daily(self, trade_date="", offset=None, limit=None, **_):
        self.calls.append(("us_daily", trade_date, offset))
        raise RuntimeError("抱歉，您没有接口(us_daily)访问权限")

    def fetch_us_daily_adj(self, trade_date="", offset=None, limit=None, **_):
        self.calls.append(("us_daily_adj", trade_date, offset))
        raise RuntimeError("抱歉，您没有接口(us_daily_adj)访问权限")


class CountingRepo:
    def __init__(self, market="hk"):
        self.market = market
        self.basic = 0
        self.cal = 0
        self.daily = 0
        self.adj = 0
        self.daily_sources = []

    def upsert_basic(self, df):
        n = 0 if df is None or df.empty else len(df)
        self.basic += n
        return n

    def upsert_calendar(self, df):
        n = 0 if df is None or df.empty else len(df)
        self.cal += n
        return n

    def upsert_daily(self, df, source=None):
        n = 0 if df is None or df.empty else len(df)
        self.daily += n
        self.daily_sources.append(source)
        return n

    def upsert_adj(self, df, source=None):
        n = 0 if df is None or df.empty else len(df)
        self.adj += n
        return n

    def daily_counts_by_date(self, dates):
        return getattr(self, "_counts", {})


def test_classify_permission_and_rate():
    assert classify_tushare_error(RuntimeError("抱歉，您没有接口(us_daily)访问权限")) == "permission"
    assert classify_tushare_error(RuntimeError("频率超限(1次/小时)")) == "rate_limit"
    assert classify_tushare_error(RuntimeError("timeout")) == "error"


def test_run_basic_hk_fetches_listed_and_delisted():
    adapter = FakeAdapter()
    repo = CountingRepo("hk")
    result = OverseasEquityPipeline(adapter, repo).run_basic()
    assert result["status"] == "success"
    assert result["records"] == 2
    statuses = [c[1] for c in adapter.calls if c[0] == "hk_basic"]
    assert statuses == ["L", "D"]


def test_run_basic_us_pages_until_short():
    adapter = FakeAdapter()
    repo = CountingRepo("us")
    pipe = OverseasEquityPipeline(adapter, repo, page_limit=2)
    result = pipe.run_basic()
    assert result["status"] == "success"
    assert result["records"] == 3  # 2 + 1
    offsets = [c[1] for c in adapter.calls if c[0] == "us_basic"]
    assert offsets == [0, 2]


def test_run_tradecal_year_chunks():
    adapter = FakeAdapter()
    repo = CountingRepo("hk")
    result = OverseasEquityPipeline(adapter, repo).run_tradecal("20200101", "20211231")
    assert result["status"] == "success"
    assert result["chunks"] == 2
    assert result["records"] == 4  # 2 years * 2 rows


def test_run_daily_hk_ignores_missing_adj_permission():
    adapter = FakeAdapter()
    repo = CountingRepo("hk")
    result = OverseasEquityPipeline(adapter, repo).run_daily(["20260828"])
    assert result["status"] == "success"
    assert result["daily_records"] == 1
    assert result["adj_records"] == 0
    assert repo.daily_sources == ["tushare_hk_daily"]


def test_run_daily_hk_skips_adj_when_disabled():
    adapter = FakeAdapter()
    repo = CountingRepo("hk")
    result = OverseasEquityPipeline(adapter, repo).run_daily(["20260828"], want_adj=False)
    assert result["status"] == "success"
    assert result["daily_records"] == 1
    assert not any(c[0] == "hk_daily_adj" for c in adapter.calls)
    assert any(c[0] == "hk_daily" for c in adapter.calls)


def test_run_daily_skips_complete_hk_dates():
    adapter = FakeAdapter()
    repo = CountingRepo("hk")
    repo._counts = {"20260828": 2700}
    result = OverseasEquityPipeline(adapter, repo).run_daily(
        ["20260828"], skip_complete_min_symbols=1500
    )
    assert result["status"] == "skipped"
    assert result["reason"] == "already_present"
    assert result["skipped_dates"] == ["20260828"]
    assert result["daily_records"] == 0
    assert not any(c[0] == "hk_daily" for c in adapter.calls)


def test_run_daily_us_permission_denied_stops():
    adapter = FakeAdapter()
    repo = CountingRepo("us")
    result = OverseasEquityPipeline(adapter, repo).run_daily(["20260828"])
    assert result["status"] == "failed"
    assert result["reason"] == "permission_denied"
    assert result["daily_records"] == 0


def test_open_days_filters_closed():
    adapter = FakeAdapter()
    repo = CountingRepo("hk")
    days = OverseasEquityPipeline(adapter, repo).open_days("20260801", "20260831")
    assert days == ["20260828"]


def test_fetch_all_raises_permission_error():
    adapter = FakeAdapter()
    repo = CountingRepo("us")
    pipe = OverseasEquityPipeline(adapter, repo)
    with pytest.raises(OverseasPermissionError):
        pipe._fetch_all(adapter.fetch_us_daily, trade_date="20260828")
