from __future__ import annotations

import pandas as pd

from quant_data_engine.tools.risk_scanner.breadth_signals import (
    aggregate_regime_alert,
    compute_breadth_table,
    compute_cape_series,
    compute_hindenburg,
    compute_mcl,
    count_hindenburg_cluster,
)
from quant_data_engine.tools.risk_scanner.models import CAPESignal, MCLSignal


def _trade_cal(start: str, periods: int) -> pd.DataFrame:
    dates = pd.date_range(start, periods=periods, freq="B")
    return pd.DataFrame({"cal_date": [d.strftime("%Y%m%d") for d in dates], "is_open": 1})


def test_compute_mcl_handles_zero_denominator() -> None:
    dates = pd.date_range("2024-01-01", periods=50, freq="B")
    breadth = pd.DataFrame(
        {
            "trade_date": [d.strftime("%Y%m%d") for d in dates],
            "advances": [0] + [60] * 49,
            "declines": [0] + [40] * 49,
            "unchanged": [100] + [0] * 49,
        }
    )

    warnings: list[str] = []
    signal = compute_mcl(breadth, dates[-1].strftime("%Y%m%d"), warnings=warnings)

    assert signal is not None
    assert signal.advances == 60
    assert signal.declines == 40
    assert not any("ZeroDivision" in warning for warning in warnings)


def test_count_hindenburg_cluster_includes_today_only_when_triggered() -> None:
    prior = ["20260530", "20260531", "20260401"]

    assert count_hindenburg_cluster(prior, "20260629", True) == 3
    assert count_hindenburg_cluster(prior, "20260629", False) == 2


def test_compute_hindenburg_four_conditions_and_cluster() -> None:
    dates = pd.date_range("2026-03-01", periods=70, freq="B")
    close = pd.Series(range(100, 170), index=dates, dtype=float)
    scan_date = dates[-1].strftime("%Y%m%d")
    mcl = MCLSignal(
        trade_date=scan_date,
        advances=100,
        declines=200,
        unchanged=0,
        net_advance_ratio=-333.3,
        ema19=-10.0,
        ema39=-5.0,
        mcl=-5.0,
        mcl_ma9=-3.0,
        pct_rank_to_date=30.0,
        signal="weak",
    )

    signal = compute_hindenburg(
        {
            "total_normal": 5000,
            "new_high_count": 100,
            "new_low_count": 100,
            "new_high_pct": 0.02,
            "new_low_pct": 0.02,
        },
        mcl,
        close,
        [(dates[-5]).strftime("%Y%m%d")],
        scan_date,
    )

    assert signal.triggered is True
    assert signal.alert_level == "cluster"
    assert signal.cluster_count_30d == 2


def test_compute_breadth_table_uses_close_for_new_high_low() -> None:
    trade_dates = [d.strftime("%Y%m%d") for d in pd.date_range("2024-01-01", periods=5, freq="B")]
    records = []
    for ts_code, closes in {"000001.SZ": [1, 2, 3, 4, 5], "000002.SZ": [5, 4, 3, 2, 1]}.items():
        for trade_date, close in zip(trade_dates, closes):
            records.append({"ts_code": ts_code, "trade_date": trade_date, "pct_chg": 1 if close == closes[-1] else 0, "close": close})
    stock_daily = pd.DataFrame(records)
    stock_info = pd.DataFrame({"ts_code": ["000001.SZ", "000002.SZ"], "name": ["A", "B"], "list_date": ["20200101", "20200101"]})

    result = compute_breadth_table(
        stock_daily,
        stock_info,
        trade_dates[-1],
        _trade_cal("2020-01-01", 1200),
        lookback_trading_days=5,
        exclude_ipo_trading_days=1,
    )

    assert result["new_high_count"] == 1
    assert result["new_low_count"] == 1


def test_compute_breadth_table_derives_missing_pct_chg_from_close() -> None:
    trade_dates = [d.strftime("%Y%m%d") for d in pd.date_range("2024-01-01", periods=3, freq="B")]
    records = []
    for ts_code, closes in {
        "000001.SZ": [10.0, 11.0, 12.0],
        "000002.SZ": [10.0, 9.0, 8.0],
        "000003.SZ": [10.0, 10.0, 10.0],
    }.items():
        for trade_date, close in zip(trade_dates, closes):
            records.append({"ts_code": ts_code, "trade_date": trade_date, "close": close})
    stock_daily = pd.DataFrame(records)
    stock_info = pd.DataFrame(
        {"ts_code": ["000001.SZ", "000002.SZ", "000003.SZ"], "name": ["A", "B", "C"], "list_date": ["20200101"] * 3}
    )

    result = compute_breadth_table(
        stock_daily,
        stock_info,
        trade_dates[-1],
        _trade_cal("2020-01-01", 1200),
        lookback_trading_days=3,
        exclude_ipo_trading_days=1,
    )

    assert result["advances"] == 1
    assert result["declines"] == 1
    assert result["unchanged"] == 1


def test_compute_breadth_table_uses_pct_chg_when_available() -> None:
    trade_dates = [d.strftime("%Y%m%d") for d in pd.date_range("2024-01-01", periods=2, freq="B")]
    stock_daily = pd.DataFrame(
        [
            {"ts_code": "000001.SZ", "trade_date": trade_dates[0], "close": 10.0},
            {"ts_code": "000001.SZ", "trade_date": trade_dates[1], "close": 9.0, "pct_chg": 1.0},
            {"ts_code": "000002.SZ", "trade_date": trade_dates[0], "close": 10.0},
            {"ts_code": "000002.SZ", "trade_date": trade_dates[1], "close": 11.0},
        ]
    )
    stock_info = pd.DataFrame({"ts_code": ["000001.SZ", "000002.SZ"], "name": ["A", "B"], "list_date": ["20200101", "20200101"]})

    result = compute_breadth_table(
        stock_daily,
        stock_info,
        trade_dates[-1],
        _trade_cal("2020-01-01", 1200),
        lookback_trading_days=2,
        exclude_ipo_trading_days=1,
    )

    assert result["advances"] == 2
    assert result["declines"] == 0


def test_compute_cape_series_constant_pe_and_chained_cpi() -> None:
    dates = pd.date_range("2020-01-01", periods=30, freq="D")
    trade_dates = [d.strftime("%Y%m%d") for d in dates]
    index_daily = pd.DataFrame({"ts_code": "000300.SH", "trade_date": trade_dates, "close": [1000.0] * len(dates)})
    index_basic = pd.DataFrame({"ts_code": "000300.SH", "trade_date": trade_dates, "pe_ttm": [10.0] * len(dates)})
    cpi = pd.DataFrame({"month": ["202001"], "nt_mom": [0.0]})

    result = compute_cape_series(index_daily, index_basic, cpi, window_years=0.02, min_coverage=1.0)

    valid = result.dropna(subset=["cape_0.02y"])
    assert not valid.empty
    assert valid["cape_0.02y"].iloc[-1] == 10.0
    assert valid["cape_0.02y_pct_rank_to_date"].iloc[-1] == 100.0


def test_aggregate_regime_alert_extreme_on_cluster_and_rich_cape() -> None:
    cape = CAPESignal(
        trade_date="20260629",
        index_code="000300.SH",
        index_name="沪深300",
        close=100.0,
        pe_ttm=10.0,
        pe_source_date="20260629",
        cape=12.0,
        window_years=5.0,
        pct_rank_to_date=80.0,
        valuation_band="rich",
    )
    level, warnings = aggregate_regime_alert(None, type("H", (), {"alert_level": "cluster", "cluster_count_30d": 2})(), [cape])

    assert level == "extreme"
    assert any("兴登堡集群" in warning for warning in warnings)