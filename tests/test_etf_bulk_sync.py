"""ETF 日线 / etf_share_size：by_trade_date（codes=None）与 by_ts_code 行为验证。"""
from __future__ import annotations

import pandas as pd

from quant_data_engine.pipelines.etf_daily_pipeline import EtfDailyPipeline
from quant_data_engine.pipelines.etf_share_size_pipeline import (
    EtfShareSizePipeline,
    records_from_share_size_df,
)
from quant_data_engine.quality.validators import IndexPriceValidator


class FakeEtfAdapter:
    def __init__(self):
        self.daily_calls = []
        self.adj_calls = []
        self.share_calls = []
        self.share_size_calls = []

    def fetch_fund_basic(self, market: str = "E", status: str = "L"):
        return pd.DataFrame(
            {"ts_code": ["510330.SH", "159919.SZ"], "name": ["A", "B"], "market": ["E", "E"]}
        )

    def fetch_etf_daily(
        self,
        ts_code: str = "",
        trade_date: str = "",
        start_date: str = "",
        end_date: str = "",
    ):
        self.daily_calls.append((ts_code, trade_date, start_date, end_date))
        if trade_date == "20240101":
            return pd.DataFrame(
                {
                    "ts_code": ["510330.SH", "159919.SZ"],
                    "trade_date": ["20240101", "20240101"],
                    "open": [1.0, 2.0],
                    "high": [1.1, 2.1],
                    "low": [0.9, 1.9],
                    "close": [1.05, 2.05],
                    "vol": [100.0, 200.0],
                    "amount": [1000.0, 2000.0],
                }
            )
        if trade_date == "20240102":
            return pd.DataFrame(
                {
                    "ts_code": ["510330.SH"],
                    "trade_date": ["20240102"],
                    "open": [1.0],
                    "high": [1.1],
                    "low": [0.9],
                    "close": [1.0],
                    "vol": [50.0],
                    "amount": [500.0],
                }
            )
        return pd.DataFrame()

    def fetch_fund_adj(
        self,
        ts_code: str = "",
        trade_date: str = "",
        start_date: str = "",
        end_date: str = "",
        offset=None,
        limit=None,
    ):
        self.adj_calls.append((ts_code, trade_date, offset, limit))
        if ts_code and start_date and not trade_date:
            return pd.DataFrame(
                {
                    "ts_code": [ts_code],
                    "trade_date": ["20240101"],
                    "adj_factor": [1.0],
                }
            )
        if not trade_date:
            return pd.DataFrame()
        if offset is None or offset == 0:
            return pd.DataFrame(
                {
                    "ts_code": ["510330.SH", "159919.SZ"],
                    "trade_date": [trade_date, trade_date],
                    "adj_factor": [1.0, 1.0],
                }
            )
        return pd.DataFrame()

    def fetch_fund_share(
        self,
        ts_code: str = "",
        trade_date: str = "",
        start_date: str = "",
        end_date: str = "",
        offset=None,
        limit=None,
    ):
        self.share_calls.append((ts_code, trade_date, offset, limit))
        if ts_code and start_date and not trade_date:
            return pd.DataFrame(
                {
                    "ts_code": [ts_code],
                    "trade_date": ["20240101"],
                    "fd_share": [100.0],
                }
            )
        if not trade_date:
            return pd.DataFrame()
        if offset is None or offset == 0:
            return pd.DataFrame(
                {
                    "ts_code": ["510330.SH"],
                    "trade_date": [trade_date],
                    "fd_share": [100.0],
                }
            )
        return pd.DataFrame()

    def fetch_etf_share_size(
        self,
        ts_code: str = "",
        trade_date: str = "",
        start_date: str = "",
        end_date: str = "",
        exchange: str = "",
    ):
        self.share_size_calls.append((ts_code, trade_date, start_date, end_date, exchange))
        if trade_date == "20240101" and not ts_code:
            return pd.DataFrame(
                {
                    "trade_date": ["20240101", "20240101"],
                    "ts_code": ["510330.SH", "159919.SZ"],
                    "etf_name": ["A", "B"],
                    "total_share": [100.0, 200.0],
                    "total_size": [1000.0, 2000.0],
                    "exchange": ["SSE", "SZSE"],
                }
            )
        return pd.DataFrame()


class FakeEtfRepo:
    def __init__(self):
        self.basic_batches: list[int] = []
        self.daily_batches: list[int] = []
        self.adj_batches: list[int] = []
        self.share_batches: list[int] = []
        self.share_size_batches: list[int] = []

    def upsert_basic(self, records):
        n = len(records)
        self.basic_batches.append(n)
        return n

    def upsert_daily_prices(self, records):
        n = len(records)
        self.daily_batches.append(n)
        return n

    def upsert_adj_factors(self, records):
        n = len(records)
        self.adj_batches.append(n)
        return n

    def upsert_shares(self, records):
        n = len(records)
        self.share_batches.append(n)
        return n

    def upsert_etf_share_size(self, records):
        n = len(records)
        self.share_size_batches.append(n)
        return n


def test_etf_daily_run_none_uses_trade_date_per_day():
    ad = FakeEtfAdapter()
    repo = FakeEtfRepo()
    pipe = EtfDailyPipeline(ad, repo, IndexPriceValidator())
    out = pipe.run(None, start_date="20240101", end_date="20240102")
    assert out["mode"] == "by_trade_date"
    assert out["days"] == 2
    assert len(out["results"]) == 2
    assert out["results"][0]["inserted"] == 2
    assert out["results"][1]["inserted"] == 1
    assert sum(repo.basic_batches) == 2
    assert repo.daily_batches == [2, 1]
    assert len(ad.daily_calls) == 2
    assert all(call[0] == "" for call in ad.daily_calls)
    assert ad.daily_calls[0][1] == "20240101" and ad.daily_calls[1][1] == "20240102"
    assert len(ad.adj_calls) == 2
    assert all(c[1] == "20240101" or c[1] == "20240102" for c in ad.adj_calls)


def test_etf_daily_run_codes_filters_basic():
    class AdapterWithCodePath(FakeEtfAdapter):
        def fetch_etf_daily(
            self,
            ts_code: str = "",
            trade_date: str = "",
            start_date: str = "",
            end_date: str = "",
        ):
            if ts_code:
                return pd.DataFrame(
                    {
                        "trade_date": ["20240101"],
                        "open": [1.0],
                        "high": [1.1],
                        "low": [0.9],
                        "close": [1.0],
                        "vol": [10.0],
                        "amount": [100.0],
                    }
                )
            return super().fetch_etf_daily(ts_code, trade_date, start_date, end_date)

    ad = AdapterWithCodePath()
    repo = FakeEtfRepo()
    pipe = EtfDailyPipeline(ad, repo, IndexPriceValidator())
    out = pipe.run(["510330.SH"], start_date="20240101", end_date="20240101")
    assert out["mode"] == "by_ts_code"
    assert len(out["results"]) == 1
    assert out["results"][0]["inserted"] == 1
    assert repo.basic_batches == [1]


def test_etf_share_size_run_none_by_trade_date():
    ad = FakeEtfAdapter()
    repo = FakeEtfRepo()
    pipe = EtfShareSizePipeline(ad, repo)
    out = pipe.run(None, start_date="20240101", end_date="20240101")
    assert out["mode"] == "by_trade_date"
    assert len(out["results"]) == 1
    assert out["results"][0]["inserted"] == 2
    assert ad.share_size_calls[0][0] == ""
    assert ad.share_size_calls[0][1] == "20240101"


def test_records_from_share_size_df_decimal128_or_float():
    df = pd.DataFrame(
        {
            "trade_date": ["20240101"],
            "ts_code": ["510330.SH"],
            "total_share": [4741854.98],
            "total_size": [2.287898e07],
            "nav": [1.23],
            "close": [1.24],
            "etf_name": ["测试"],
            "exchange": ["SSE"],
        }
    )
    recs = records_from_share_size_df(df, "510330.SH")
    assert len(recs) == 1
    r = recs[0]
    assert r["ts_code"] == "510330.SH"
    assert r["trade_date"] == "20240101"
    assert "total_share" in r and "total_size" in r


def test_fetch_etf_share_size_full_market_refetch_by_exchange_when_capped():
    from quant_data_engine.pipelines import etf_share_size_pipeline as ssp

    class Ad:
        def __init__(self) -> None:
            self.calls: list[tuple[str, str]] = []

        def fetch_etf_share_size(
            self,
            ts_code: str = "",
            trade_date: str = "",
            start_date: str = "",
            end_date: str = "",
            exchange: str = "",
        ):
            self.calls.append((trade_date, exchange))
            if trade_date == "20240101" and not exchange:
                return pd.DataFrame(
                    {
                        "trade_date": ["20240101"] * ssp._MAX_ROWS,
                        "ts_code": [f"{i:06d}.SH" for i in range(ssp._MAX_ROWS)],
                        "total_share": [1.0] * ssp._MAX_ROWS,
                    }
                )
            if trade_date == "20240101" and exchange == "SSE":
                return pd.DataFrame(
                    {
                        "trade_date": ["20240101"] * 3000,
                        "ts_code": [f"s{i:04d}.SH" for i in range(3000)],
                        "total_share": [1.0] * 3000,
                    }
                )
            if trade_date == "20240101" and exchange == "SZSE":
                return pd.DataFrame(
                    {
                        "trade_date": ["20240101"] * 2500,
                        "ts_code": [f"z{i:04d}.SZ" for i in range(2500)],
                        "total_share": [2.0] * 2500,
                    }
                )
            if exchange == "BSE":
                return pd.DataFrame(
                    {
                        "trade_date": ["20240101"],
                        "ts_code": ["899001.BJ"],
                        "total_share": [3.0],
                    }
                )
            return pd.DataFrame()

    ad = Ad()
    df = ssp._fetch_etf_share_size_full_market_one_day(ad, "20240101")
    assert len(df) == 5501
    assert {c[1] for c in ad.calls if c[0] == "20240101"} == {"", "SSE", "SZSE", "BSE"}


def test_paginate_fund_adj_for_ts_range_merges_two_pages():
    from quant_data_engine.pipelines import etf_daily_pipeline as mod

    class PagedAdapter:
        def fetch_fund_adj(
            self,
            ts_code="",
            trade_date="",
            start_date="",
            end_date="",
            offset=None,
            limit=None,
        ):
            if ts_code != "510330.SH" or not start_date:
                return pd.DataFrame()
            if offset == 0 or offset is None:
                rows = [
                    {"ts_code": f"{i:06d}.SH", "trade_date": "20240101", "adj_factor": 1.0}
                    for i in range(2000)
                ]
                return pd.DataFrame(rows)
            if offset == 2000:
                return pd.DataFrame(
                    [{"ts_code": "999999.SH", "trade_date": "20240101", "adj_factor": 1.1}]
                )
            return pd.DataFrame()

    ad = PagedAdapter()
    df = mod._paginate_fund_adj_for_ts_range(ad, "510330.SH", "20240101", "20241231")
    assert len(df) == 2001


def test_fund_adj_pagination_merges_two_pages(monkeypatch):
    from quant_data_engine.pipelines import etf_daily_pipeline as mod

    class PagedAdapter:
        def fetch_fund_adj(
            self,
            ts_code="",
            trade_date="",
            start_date="",
            end_date="",
            offset=None,
            limit=None,
        ):
            if offset == 0 or offset is None:
                rows = [{"ts_code": f"{i:06d}.SH", "trade_date": trade_date, "adj_factor": 1.0} for i in range(2000)]
                return pd.DataFrame(rows)
            if offset == 2000:
                return pd.DataFrame(
                    [
                        {"ts_code": "999999.SH", "trade_date": trade_date, "adj_factor": 1.1},
                    ]
                )
            return pd.DataFrame()

    ad = PagedAdapter()
    df = mod._paginate_fund_adj_for_trade_date(ad, "20240101")
    assert len(df) == 2001
    assert df["ts_code"].nunique() == 2001
