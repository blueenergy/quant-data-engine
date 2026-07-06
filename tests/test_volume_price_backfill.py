import pytest

from quant_data_engine.tasks.run_volume_price_backfill import (
    DEFAULT_BACKFILL_START,
    filter_days_after_checkpoint,
    get_trading_days,
)


class _FakeTradeCalAdapter:
    def fetch_trade_cal(self, start_date: str, end_date: str):
        import pandas as pd

        return pd.DataFrame(
            {
                "cal_date": ["20240102", "20240103", "20240104"],
                "is_open": [1, 1, 1],
            }
        )


def test_filter_days_after_checkpoint_skips_completed_days():
    days = ["20240102", "20240103", "20240104"]
    assert filter_days_after_checkpoint(days, "20240103") == ["20240104"]
    assert filter_days_after_checkpoint(days, None) == days
    assert filter_days_after_checkpoint(days, "bad") == days


def test_get_trading_days_returns_open_days_sorted():
    adapter = _FakeTradeCalAdapter()
    out = get_trading_days(adapter, "20240101", "20240110")
    assert out == ["20240102", "20240103", "20240104"]


def test_default_backfill_start_is_2016():
    assert DEFAULT_BACKFILL_START == "20160101"
