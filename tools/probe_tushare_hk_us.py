#!/usr/bin/env python
"""Read-only probe for Tushare HK / US equity interfaces.

Confirms permission, row shape, and per-call caps before we wire pipelines.

This script ONLY reads from Tushare. It never writes to Mongo.

``hk_daily`` and ``hk_daily_adj`` are skipped unless named explicitly, and are
probed at most once per run: the hourly cap they share is the one the daily
sync depends on.

Usage:
    PYTHONPATH=src python tools/probe_tushare_hk_us.py
    PYTHONPATH=src python tools/probe_tushare_hk_us.py --interfaces hk_basic,us_daily
"""
from __future__ import annotations

import argparse
import sys
import time
import traceback
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import pandas as pd

try:
    import tushare as ts  # type: ignore
except Exception as exc:  # pragma: no cover
    raise SystemExit(f"tushare package not importable: {exc}")

from quant_data_engine.config.settings import get_settings

# (api_name, kwargs, note)
PROBES = [
    ("hk_basic", {"list_status": "L"}, "港股列表（上市）"),
    ("hk_tradecal", {"start_date": "20260801", "end_date": "20260831"}, "港股交易日历"),
    ("hk_daily", {"ts_code": "00700.HK", "start_date": "20260801", "end_date": "20260828"}, "港股未复权日线"),
    ("hk_daily", {"trade_date": "20260828"}, "港股未复权日线（单日全市场）"),
    ("hk_daily_adj", {"ts_code": "00700.HK", "start_date": "20260801", "end_date": "20260828"}, "港股复权日线"),
    ("hk_daily_adj", {"trade_date": "20260828"}, "港股复权日线（单日全市场）"),
    ("us_basic", {}, "美股列表（默认页）"),
    ("us_basic", {"offset": 0, "limit": 6000}, "美股列表（分页首页）"),
    ("us_tradecal", {"start_date": "20260801", "end_date": "20260831"}, "美股交易日历"),
    ("us_daily", {"ts_code": "AAPL", "start_date": "20260801", "end_date": "20260828"}, "美股未复权日线"),
    ("us_daily", {"trade_date": "20260828"}, "美股未复权日线（单日全市场）"),
    ("us_daily_adj", {"ts_code": "AAPL", "start_date": "20260801", "end_date": "20260828"}, "美股复权日线"),
    ("us_daily_adj", {"trade_date": "20260828", "exchange": "NAS"}, "美股复权日线（NAS 单日）"),
    ("us_adjfactor", {"ts_code": "AAPL", "start_date": "20260801", "end_date": "20260828"}, "美股复权因子（对照）"),
]

# Probing these casually costs the hourly hk_daily quota that
# run_overseas_equity_sync needs; a short retry burst turns the per-minute
# limit into a per-hour lock. Require an explicit --interfaces.
RATE_LIMITED = {"hk_daily", "hk_daily_adj"}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Probe Tushare HK/US equity interfaces")
    p.add_argument(
        "--interfaces",
        default="",
        help="Comma list of api names to limit probe, e.g. hk_basic,us_daily",
    )
    p.add_argument("--head", type=int, default=2, help="Sample rows to print")
    return p.parse_args()


def _summarize_error(exc: BaseException) -> str:
    text = str(exc).replace("\n", " ").strip()
    if len(text) > 240:
        text = text[:237] + "..."
    return text or exc.__class__.__name__


def main() -> None:
    args = parse_args()
    settings = get_settings()
    token = settings.tushare_token
    if not token:
        raise SystemExit("TUSHARE_TOKEN not set; cannot probe")

    ts.set_token(token)
    pro = ts.pro_api()
    wanted = {s.strip() for s in args.interfaces.split(",") if s.strip()}

    print("Tushare HK/US equity probe (read-only)")
    print("=" * 72)
    seen_limited = set()
    for name, kwargs, note in PROBES:
        if wanted and name not in wanted:
            continue
        if name in RATE_LIMITED:
            if name in seen_limited:
                continue
            seen_limited.add(name)
            if not wanted:
                print(f"\n>> {name}  SKIP (rate-limited; use --interfaces {name})")
                continue
            print(f"\n>> {name}  probing once only (shared hourly cap)")
        time.sleep(0.35)
        label = f"{name}({', '.join(f'{k}={v}' for k, v in kwargs.items())})"
        print(f"\n>> {label}")
        print(f"   {note}")
        try:
            df = pro.query(name, **kwargs)
        except Exception as exc:
            print(f"   FAIL  {_summarize_error(exc)}")
            if args.head >= 5:
                traceback.print_exc()
            continue
        if df is None:
            print("   EMPTY (None)")
            continue
        if not isinstance(df, pd.DataFrame):
            print(f"   UNEXPECTED type={type(df)}")
            continue
        if df.empty:
            print("   EMPTY (0 rows) — likely no permission, non-trading day, or trial cap")
            continue
        cols = list(df.columns)
        print(f"   OK    rows={len(df)} cols={len(cols)}")
        print(f"   columns: {', '.join(cols)}")
        with pd.option_context("display.max_columns", None, "display.width", 220, "display.max_colwidth", 40):
            print(df.head(args.head).to_string(index=False))


if __name__ == "__main__":
    main()
