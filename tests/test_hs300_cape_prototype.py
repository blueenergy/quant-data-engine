from __future__ import annotations

import pandas as pd

from quant_data_engine.tasks.run_hs300_cape_prototype import compute_cape_series


def test_compute_cape_series_constant_pe_and_cpi() -> None:
    dates = pd.date_range("2020-01-01", periods=30, freq="D")
    trade_dates = [d.strftime("%Y%m%d") for d in dates]
    closes = [1000.0] * len(dates)

    index_daily = pd.DataFrame(
        {
            "ts_code": "000300.SH",
            "trade_date": trade_dates,
            "close": closes,
        }
    )
    index_basic = pd.DataFrame(
        {
            "ts_code": "000300.SH",
            "trade_date": trade_dates,
            "pe_ttm": [10.0] * len(dates),
        }
    )
    cpi_index = pd.DataFrame(
        {
            "month": ["202001"],
            "cpi_date": [pd.Timestamp("2020-01-01")],
            "cpi_index": [100.0],
        }
    )

    result = compute_cape_series(
        index_daily,
        index_basic,
        cpi_index,
        window_years=0.02,
        min_coverage=1.0,
    )

    valid = result.dropna(subset=["cape_0.02y"])
    assert not valid.empty
    assert valid["cape_0.02y"].iloc[-1] == 10.0
