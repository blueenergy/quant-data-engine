"""Tests for futures daily pipeline."""
from __future__ import annotations

import pandas as pd

from quant_data_engine.pipelines.fut_daily_pipeline import FutDailyPipeline


class _FakeAdapter:
    def __init__(self, daily_df, mapping_df):
        self.daily_df = daily_df
        self.mapping_df = mapping_df

    def fetch_fut_mapping(self, ts_code="", trade_date="", start_date="", end_date=""):
        return self.mapping_df

    def fetch_fut_daily(self, ts_code="", trade_date="", start_date="", end_date="", exchange="", fields=None):
        return self.daily_df


class _FakeRepo:
    def __init__(self):
        self.mapping = []
        self.daily = []

    def upsert_mapping(self, records):
        self.mapping.extend(records)
        return len(records)

    def upsert_daily(self, records):
        self.daily.extend(records)
        return len(records)


class _PassValidator:
    def validate(self, df):
        return {"is_valid": True, "issues": [], "records": len(df)}


def test_fut_pipeline_upserts_daily_and_mapping():
    mapping_df = pd.DataFrame(
        {
            "ts_code": ["IF.CFX", "IF.CFX"],
            "trade_date": ["20240102", "20240103"],
            "mapping_ts_code": ["IF2401.CFX", "IF2401.CFX"],
        }
    )
    daily_df = pd.DataFrame(
        {
            "ts_code": ["IF.CFX", "IF.CFX"],
            "trade_date": ["20240102", "20240103"],
            "open": [3500.0, 3510.0],
            "high": [3520.0, 3530.0],
            "low": [3490.0, 3500.0],
            "close": [3515.0, 3525.0],
            "vol": [1000.0, 1100.0],
            "amount": [2000.0, 2200.0],
            "oi": [50000.0, 51000.0],
            "settle": [3510.0, 3520.0],
        }
    )
    repo = _FakeRepo()
    pipeline = FutDailyPipeline(_FakeAdapter(daily_df, mapping_df), repo, _PassValidator())
    result = pipeline.run(["IF.CFX"], start_date="20240102", end_date="20240103")

    assert result["results"][0]["inserted"] == 2
    assert len(repo.mapping) == 2
    assert len(repo.daily) == 2
    assert repo.daily[0]["contract"] == "IF2401.CFX"
    assert repo.daily[0]["source"] == "tushare"


def test_fut_pipeline_handles_empty_daily():
    repo = _FakeRepo()
    pipeline = FutDailyPipeline(_FakeAdapter(pd.DataFrame(), pd.DataFrame()), repo, _PassValidator())
    result = pipeline.run(["IF.CFX"], start_date="20240102", end_date="20240103")
    assert result["results"][0]["inserted"] == 0
    assert "no_data" in result["results"][0]["issues"]
