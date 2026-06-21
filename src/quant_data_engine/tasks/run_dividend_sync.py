"""CLI task to sync Tushare dividend records for DDM validation.

Environment:
  MONGO_URI, DATA_DB_NAME, TUSHARE_TOKEN via ``get_settings()``.

Usage:
  # Sync selected stocks, keeping the latest 5 report years by default
  python -m quant_data_engine.tasks.run_dividend_sync --codes 600132.SH 300750.SZ

  # Sync all listed stocks in test mode
  python -m quant_data_engine.tasks.run_dividend_sync --all --test

  # Sync all historical dividend records for selected stocks
  python -m quant_data_engine.tasks.run_dividend_sync --codes 600132.SH --all-years
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime
from typing import Any, Iterable

import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from quant_data_engine.adapters.stock_adapter import StockAdapter
from quant_data_engine.config.settings import get_settings
from quant_data_engine.repositories.financial_repository import FinancialRepository
from quant_data_engine.utils.ingestion_job import record_ingestion_run


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run dividend record synchronization")
    stock_group = parser.add_mutually_exclusive_group(required=True)
    stock_group.add_argument("--codes", nargs="+", help="Stock ts_codes (e.g., 600132.SH 300750.SZ)")
    stock_group.add_argument("--all", action="store_true", help="Sync all listed stocks")
    parser.add_argument("--years", type=int, default=5, help="Keep latest N report years (default: 5)")
    parser.add_argument("--all-years", action="store_true", help="Keep all returned dividend records")
    parser.add_argument("--test", action="store_true", help="Test mode: only sync first 10 stocks")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be synced without writing")
    return parser.parse_args()


def _filter_recent_report_years(df: pd.DataFrame, years: int | None) -> pd.DataFrame:
    if df is None or df.empty or years is None:
        return df
    if "end_date" not in df.columns:
        return df
    years_seen = sorted(
        {
            str(value)[:4]
            for value in df["end_date"].dropna().tolist()
            if len(str(value)) >= 4 and str(value)[:4].isdigit()
        },
        reverse=True,
    )
    keep = set(years_seen[: max(years, 0)])
    if not keep:
        return df.iloc[0:0]
    return df[df["end_date"].astype(str).str[:4].isin(keep)]


def _load_stock_codes(adapter: StockAdapter, *, codes: Iterable[str] | None, all_stocks: bool, test: bool) -> list[str]:
    if codes:
        ts_codes = [str(code).strip() for code in codes if str(code).strip()]
    elif all_stocks:
        df = adapter.fetch_stock_basic(list_status="L")
        if df is None or df.empty or "ts_code" not in df.columns:
            return []
        ts_codes = df["ts_code"].dropna().astype(str).tolist()
    else:
        ts_codes = []
    return ts_codes[:10] if test else ts_codes


def main() -> None:
    args = parse_args()
    settings = get_settings()
    job_date = datetime.today().strftime("%Y%m%d")
    adapter = StockAdapter(settings.tushare_token, rate_limit_per_sec=settings.rate_limit_per_sec)
    repository = FinancialRepository(settings.mongo_uri, settings.data_db)
    ts_codes = _load_stock_codes(adapter, codes=args.codes, all_stocks=args.all, test=args.test)
    if not ts_codes:
        print("Failed to load stock codes")
        record_ingestion_run(settings, "dividend_sync", job_date, status="skipped", error="No stock codes")
        return

    years = None if args.all_years else args.years
    print("Dividend Record Synchronization")
    print(f"   Stocks: {len(ts_codes)}")
    print(f"   Years: {'all' if years is None else years}")
    if args.dry_run:
        print(f"Dry-run mode: would sync {len(ts_codes)} stocks")
        print(f"   Codes: {', '.join(ts_codes[:5])}{'...' if len(ts_codes) > 5 else ''}")
        return

    total_records = 0
    failed: list[dict[str, Any]] = []
    try:
        for idx, ts_code in enumerate(ts_codes, start=1):
            print(f"[{idx}/{len(ts_codes)}] {ts_code} dividend")
            try:
                df = adapter.fetch_dividend(ts_code=ts_code)
                df = _filter_recent_report_years(df, years)
                count = repository.upsert_dividends(df=df) if df is not None and not df.empty else 0
                total_records += count
                print(f"  records: {count}")
            except Exception as exc:
                failed.append({"ts_code": ts_code, "error": str(exc)})
                print(f"  failed: {exc}")
    except Exception as exc:
        record_ingestion_run(settings, "dividend_sync", job_date, status="failed", error=str(exc))
        raise

    status = "completed" if not failed else "partial"
    print("\nSynchronization completed:")
    print(f"   Stocks processed: {len(ts_codes)}")
    print(f"   Dividend records: {total_records}")
    print(f"   Failed: {len(failed)}")
    if failed:
        for item in failed[:5]:
            print(f"   {item['ts_code']}: {item['error']}")
    record_ingestion_run(
        settings,
        "dividend_sync",
        job_date,
        records_changed=total_records,
        status=status,
        mode="by_stock",
    )


if __name__ == "__main__":
    main()
