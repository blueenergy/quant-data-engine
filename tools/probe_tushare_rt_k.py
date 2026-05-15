#!/usr/bin/env python
"""Temporary probe for Tushare rt_k availability and response shape.

Usage:
    PYTHONPATH=src python tools/probe_tushare_rt_k.py
    PYTHONPATH=src python tools/probe_tushare_rt_k.py --ts-code "3*.SZ,6*.SH,0*.SZ,9*.BJ"
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from quant_data_engine.adapters.stock_adapter import StockAdapter
from quant_data_engine.config.settings import get_settings


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Probe Tushare rt_k wildcard support")
    parser.add_argument(
        "--ts-code",
        default=os.getenv("REALTIME_DAILY_K_TS_CODE_PATTERNS", "3*.SZ,6*.SH,0*.SZ,9*.BJ"),
        help="rt_k ts_code expression, e.g. 3*.SZ,6*.SH,0*.SZ,9*.BJ",
    )
    parser.add_argument("--trade-date", default=None, help="Optional trade_date YYYYMMDD")
    parser.add_argument(
        "--fields",
        default=os.getenv(
            "REALTIME_DAILY_K_FIELDS",
            "ts_code,name,pre_close,high,open,low,close,vol,amount,num,"
            "ask_price1,ask_volume1,bid_price1,bid_volume1,trade_time",
        ),
        help="Optional rt_k fields list",
    )
    parser.add_argument("--head", type=int, default=5, help="Rows to print")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    settings = get_settings()
    adapter = StockAdapter(settings.tushare_token, rate_limit_per_sec=settings.rate_limit_per_sec)
    if not adapter.enabled:
        raise SystemExit("Tushare adapter disabled: check TUSHARE_TOKEN and tushare package")

    print(f"rt_k probe ts_code={args.ts_code!r} trade_date={args.trade_date!r} fields={args.fields!r}")
    df = adapter.fetch_rt_k(ts_code=args.ts_code, trade_date=args.trade_date, fields=args.fields)
    if df is None or df.empty:
        print("NO_ROWS")
        return

    print(f"rows={len(df)}")
    print(f"columns={list(df.columns)}")
    print(df.head(args.head).to_string(index=False))


if __name__ == "__main__":
    main()
