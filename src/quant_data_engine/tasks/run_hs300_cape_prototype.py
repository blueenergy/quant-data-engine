"""Prototype: compute HS300 5-year CPI-adjusted CAPE from Tushare.

This task is intentionally read-only: it fetches index prices, index valuation
metrics, and China CPI from Tushare, computes the CAPE series in memory, then
prints a compact summary and optionally writes the curve to CSV.

Formula:
    earnings_proxy_t = index_close_t / index_pe_ttm_t
    real_avg_earnings_T = CPI_T * rolling_mean(earnings_proxy_t / CPI_t, 5y)
    cape_T = index_close_T / real_avg_earnings_T

Usage:
    python -m quant_data_engine.tasks.run_hs300_cape_prototype
    python -m quant_data_engine.tasks.run_hs300_cape_prototype --csv /tmp/hs300_cape.csv
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from typing import Dict, Optional

import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from quant_data_engine.adapters.tushare_adapter import TushareAdapter
from quant_data_engine.config.settings import get_settings


DEFAULT_INDEX_CODE = "000300.SH"
DEFAULT_START_DATE = "20100101"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compute a prototype HS300 5-year CPI-adjusted CAPE series",
    )
    parser.add_argument("--index", default=DEFAULT_INDEX_CODE, help="Index ts_code, default: 000300.SH")
    parser.add_argument("--start", default=DEFAULT_START_DATE, help="Start date YYYYMMDD")
    parser.add_argument("--end", default=datetime.today().strftime("%Y%m%d"), help="End date YYYYMMDD")
    parser.add_argument("--window-years", type=float, default=5.0, help="Rolling earnings window in years")
    parser.add_argument(
        "--min-coverage",
        type=float,
        default=0.80,
        help="Minimum rolling-window coverage before CAPE is reported",
    )
    parser.add_argument(
        "--pe-field",
        default="pe_ttm",
        choices=("pe_ttm", "pe"),
        help="Index valuation field used to infer earnings proxy",
    )
    parser.add_argument("--csv", default="", help="Optional output CSV path for the full CAPE curve")
    parser.add_argument("--json-summary", action="store_true", help="Print summary as JSON")
    return parser.parse_args()


def _months_before_yyyymm(date_str: str, months: int) -> str:
    year = int(date_str[:4])
    month = int(date_str[4:6]) - months
    while month <= 0:
        month += 12
        year -= 1
    return f"{year}{month:02d}"


def _prepare_cpi_index(cpi_df: pd.DataFrame) -> pd.DataFrame:
    if cpi_df is None or cpi_df.empty:
        raise ValueError("cn_cpi returned no rows")
    if "month" not in cpi_df.columns:
        raise ValueError("cn_cpi missing required 'month' column")

    df = cpi_df.copy()
    df["month"] = df["month"].astype(str)
    df = df.sort_values("month").reset_index(drop=True)

    level_col = next((col for col in ("cpi", "cpi_index") if col in df.columns), None)
    if level_col is not None:
        df["cpi_index"] = pd.to_numeric(df[level_col], errors="coerce")
    elif "nt_mom" in df.columns:
        mom = pd.to_numeric(df["nt_mom"], errors="coerce").fillna(0.0)
        df["cpi_index"] = (1.0 + mom / 100.0).cumprod() * 100.0
    else:
        raise ValueError("cn_cpi needs cpi/cpi_index or nt_mom to build a CPI level series")

    df["cpi_date"] = pd.to_datetime(df["month"] + "01", format="%Y%m%d", errors="coerce")
    df = df.dropna(subset=["cpi_date", "cpi_index"])
    if df.empty:
        raise ValueError("cn_cpi has no usable CPI rows after cleaning")
    return df[["month", "cpi_date", "cpi_index"]].sort_values("cpi_date").reset_index(drop=True)


def _expanding_percentile_rank(values: pd.Series) -> pd.Series:
    ranks = []
    seen = []
    for value in values:
        if pd.isna(value):
            ranks.append(float("nan"))
            continue
        seen.append(float(value))
        ranks.append(sum(v <= value for v in seen) / len(seen) * 100.0)
    return pd.Series(ranks, index=values.index)


def compute_cape_series(
    index_daily: pd.DataFrame,
    index_basic: pd.DataFrame,
    cpi_index: pd.DataFrame,
    *,
    pe_field: str = "pe_ttm",
    window_years: float = 5.0,
    min_coverage: float = 0.80,
) -> pd.DataFrame:
    if index_daily is None or index_daily.empty:
        raise ValueError("index_daily returned no rows")
    if index_basic is None or index_basic.empty:
        raise ValueError("index_dailybasic returned no rows")
    if pe_field not in index_basic.columns:
        raise ValueError(f"index_dailybasic missing required '{pe_field}' column")

    prices = index_daily.copy()
    basics = index_basic.copy()
    for df in (prices, basics):
        df["trade_date"] = df["trade_date"].astype(str)

    prices = prices[["ts_code", "trade_date", "close"]]
    basics = basics[["ts_code", "trade_date", pe_field]]
    df = pd.merge(prices, basics, on=["ts_code", "trade_date"], how="inner")
    if df.empty:
        raise ValueError("index_daily and index_dailybasic have no overlapping rows")

    df = df.sort_values("trade_date").reset_index(drop=True)
    df["date"] = pd.to_datetime(df["trade_date"], format="%Y%m%d", errors="coerce")
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df[pe_field] = pd.to_numeric(df[pe_field], errors="coerce")
    df = df.dropna(subset=["date", "close", pe_field])
    df = df[df[pe_field] > 0].copy()
    if df.empty:
        raise ValueError("no rows with positive PE values after cleaning")

    df = pd.merge_asof(
        df.sort_values("date"),
        cpi_index.sort_values("cpi_date"),
        left_on="date",
        right_on="cpi_date",
        direction="backward",
    )
    df = df.dropna(subset=["cpi_index"]).reset_index(drop=True)
    if df.empty:
        raise ValueError("no rows could be matched with CPI history")

    window_days = int(round(window_years * 252))
    min_periods = max(1, int(round(window_days * min_coverage)))
    cape_col = f"cape_{window_years:g}y"

    df["earnings_proxy"] = df["close"] / df[pe_field]
    df["real_earnings_base"] = df["earnings_proxy"] / df["cpi_index"]
    df["rolling_real_earnings_base"] = (
        df["real_earnings_base"].rolling(window=window_days, min_periods=min_periods).mean()
    )
    df["real_avg_earnings"] = df["rolling_real_earnings_base"] * df["cpi_index"]
    df[cape_col] = df["close"] / df["real_avg_earnings"]
    df[f"{cape_col}_pct_rank_to_date"] = _expanding_percentile_rank(df[cape_col])

    return df[
        [
            "ts_code",
            "trade_date",
            "close",
            pe_field,
            "earnings_proxy",
            "cpi_index",
            "real_avg_earnings",
            cape_col,
            f"{cape_col}_pct_rank_to_date",
        ]
    ]


def summarize(result: pd.DataFrame, cape_col: str, pe_field: str) -> Dict[str, object]:
    valid = result.dropna(subset=[cape_col]).copy()
    if valid.empty:
        raise ValueError(f"{cape_col} has no valid rows; lower --min-coverage or extend --start")

    latest = valid.iloc[-1]
    cape = valid[cape_col]
    quantiles = cape.quantile([0.10, 0.20, 0.50, 0.80, 0.90]).to_dict()

    return {
        "index": latest["ts_code"],
        "latest_trade_date": latest["trade_date"],
        "latest_close": round(float(latest["close"]), 4),
        pe_field: round(float(latest[pe_field]), 4),
        cape_col: round(float(latest[cape_col]), 4),
        "sample_start": valid["trade_date"].iloc[0],
        "sample_rows": int(len(valid)),
        "latest_full_sample_percentile": round(float((cape <= latest[cape_col]).mean() * 100.0), 2),
        "latest_to_date_percentile": round(float(latest[f"{cape_col}_pct_rank_to_date"]), 2),
        "quantiles": {f"p{int(k * 100):02d}": round(float(v), 4) for k, v in quantiles.items()},
    }


def main() -> None:
    args = parse_args()
    settings = get_settings()
    if not settings.tushare_token:
        raise SystemExit("ERROR: TUSHARE_TOKEN is not configured")

    adapter = TushareAdapter(
        token=settings.tushare_token,
        rate_limit_per_sec=settings.rate_limit_per_sec,
    )
    cpi_start = _months_before_yyyymm(args.start, 2)
    cpi_end = args.end[:6]

    index_daily = adapter.fetch_index_daily(args.index, start_date=args.start, end_date=args.end)
    index_basic = adapter.fetch_index_dailybasic(
        ts_code=args.index,
        start_date=args.start,
        end_date=args.end,
        fields=f"ts_code,trade_date,pe,pe_ttm,pb",
    )
    cpi_raw = adapter.fetch_cn_cpi(start_m=cpi_start, end_m=cpi_end)
    cpi_index = _prepare_cpi_index(cpi_raw)

    result = compute_cape_series(
        index_daily,
        index_basic,
        cpi_index,
        pe_field=args.pe_field,
        window_years=args.window_years,
        min_coverage=args.min_coverage,
    )
    cape_col = f"cape_{args.window_years:g}y"
    summary = summarize(result, cape_col, args.pe_field)

    if args.csv:
        result.to_csv(args.csv, index=False)

    if args.json_summary:
        print(json.dumps(summary, ensure_ascii=False, indent=2))
    else:
        print(f"Index: {summary['index']}")
        print(f"Latest date: {summary['latest_trade_date']}")
        print(f"Close: {summary['latest_close']}")
        print(f"{args.pe_field}: {summary[args.pe_field]}")
        print(f"{cape_col}: {summary[cape_col]}")
        print(f"Sample: {summary['sample_start']} -> {summary['latest_trade_date']} ({summary['sample_rows']} rows)")
        print(f"Latest percentile (full sample): {summary['latest_full_sample_percentile']}%")
        print(f"Latest percentile (to date): {summary['latest_to_date_percentile']}%")
        print(f"Quantiles: {summary['quantiles']}")
        if args.csv:
            print(f"CSV written: {args.csv}")


if __name__ == "__main__":  # pragma: no cover
    main()
