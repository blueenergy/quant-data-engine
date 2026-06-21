import pandas as pd

from quant_data_engine.tasks.run_dividend_sync import _filter_recent_report_years


def test_filter_recent_report_years_keeps_latest_years():
    df = pd.DataFrame(
        {
            "ts_code": ["600132.SH"] * 4,
            "end_date": ["20251231", "20241231", "20231231", "20221231"],
            "cash_div_tax": [3.0, 2.5, 2.0, 1.5],
        }
    )

    filtered = _filter_recent_report_years(df, 2)

    assert filtered["end_date"].tolist() == ["20251231", "20241231"]


def test_filter_recent_report_years_all_years_passthrough():
    df = pd.DataFrame({"end_date": ["20251231"]})

    filtered = _filter_recent_report_years(df, None)

    assert filtered is df
