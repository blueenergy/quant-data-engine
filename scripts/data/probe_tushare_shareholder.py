#!/usr/bin/env python
"""Read-only probe for Tushare shareholder / chips interfaces.

Pulls a single sample stock (default 长海股份 300196.SZ) from each interface to
confirm field names, data start year, and return volume before we write data
contracts + Mongo schemas into the pipeline catalog.

This script ONLY reads from Tushare. It never writes to Mongo.

Usage:
    PYTHONPATH=src python scripts/data/probe_tushare_shareholder.py
    PYTHONPATH=src python scripts/data/probe_tushare_shareholder.py --ts-code 600519.SH
    PYTHONPATH=src python scripts/data/probe_tushare_shareholder.py --interfaces hk_hold,repurchase
"""
from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import pandas as pd

try:
    import tushare as ts  # type: ignore
except Exception as exc:  # pragma: no cover
    raise SystemExit(f"tushare package not importable: {exc}")

from quant_data_engine.config.settings import get_settings


# Each interface: the Tushare api name, the primary date field used for
# coverage / freshness, and a short note on cadence so the output is
# self-documenting when deciding the contract model.
INTERFACES = [
    {
        "name": "hk_hold",
        "label": "沪深港股通持股明细(北向)",
        "date_field": "trade_date",
        "cadence": "daily",
    },
    {
        "name": "stk_holdertrade",
        "label": "股东增减持",
        "date_field": "ann_date",
        "cadence": "announcement",
    },
    {
        "name": "stk_holdernumber",
        "label": "股东户数",
        "date_field": "end_date",
        "cadence": "quarterly",
    },
    {
        "name": "share_float",
        "label": "限售股解禁",
        "date_field": "float_date",
        "cadence": "announcement",
    },
    {
        "name": "repurchase",
        "label": "股票回购",
        "date_field": "ann_date",
        "cadence": "announcement",
    },
    {
        "name": "ccass_hold",
        "label": "中央结算系统持股汇总",
        "date_field": "trade_date",
        "cadence": "daily",
    },
    {
        "name": "ccass_hold_detail",
        "label": "中央结算系统持股明细",
        "date_field": "trade_date",
        "cadence": "daily",
    },
]

# Single-call row counts at/above these round numbers strongly suggest the
# response was truncated by Tushare's per-call cap, so we re-scan year by year.
SUSPECT_CAPS = {1000, 2000, 3000, 5000, 6000}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Probe Tushare shareholder interfaces (read-only)")
    parser.add_argument("--ts-code", default="300196.SZ", help="Sample ts_code (default 长海股份 300196.SZ)")
    parser.add_argument("--start", default="20100101", help="History start YYYYMMDD for coverage scan")
    parser.add_argument(
        "--interfaces",
        default="",
        help="Comma list to limit probe, e.g. hk_hold,repurchase (default: all)",
    )
    parser.add_argument("--head", type=int, default=3, help="Sample rows to print")
    return parser.parse_args()


def _call(pro, name: str, **params) -> pd.DataFrame | None:
    """Single rate-limited Tushare call; returns None on error."""
    time.sleep(0.35)  # soft rate limit
    try:
        return pro.query(name, **params)
    except Exception as exc:  # pragma: no cover - network/permission dependent
        print(f"    ! call {name}({params}) failed: {exc}")
        return None


def _per_year_counts(df: pd.DataFrame, date_field: str) -> dict[str, int]:
    if df is None or df.empty or date_field not in df.columns:
        return {}
    years = df[date_field].astype(str).str.slice(0, 4)
    return {str(k): int(v) for k, v in years.value_counts().sort_index().items()}


def _yearly_scan(pro, name: str, ts_code: str, date_field: str, start: str) -> pd.DataFrame:
    """Re-fetch year by year to defeat per-call row caps and pin true coverage."""
    start_year = int(start[:4])
    end_year = datetime.today().year
    frames = []
    for year in range(start_year, end_year + 1):
        params = {
            "ts_code": ts_code,
            "start_date": f"{year}0101",
            "end_date": f"{year}1231",
        }
        df = _call(pro, name, **params)
        if df is not None and not df.empty:
            frames.append(df)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def probe_interface(pro, spec: dict, ts_code: str, start: str, head: int) -> None:
    name = spec["name"]
    date_field = spec["date_field"]
    print(f"\n=== {name}  ({spec['label']}, cadence={spec['cadence']}, date_field={date_field}) ===")

    df = _call(pro, name, ts_code=ts_code)
    if df is None:
        print("  RESULT: call returned None (error or no permission)")
        return
    rows = len(df)
    print(f"  single-call rows={rows}")

    if rows == 0:
        # Stock may simply have no events (e.g. never did a buyback). Do a tiny
        # market-wide recent call just to capture the schema / confirm access.
        recent = (datetime.today().strftime("%Y%m%d"))
        probe = _call(pro, name, start_date=f"{recent[:4]}0101", end_date=recent)
        if probe is not None and not probe.empty:
            print(f"  (sample stock empty) market-wide columns={list(probe.columns)}")
            print(f"  market-wide recent rows={len(probe)}")
        else:
            print("  no rows for sample stock and market-wide recent probe empty")
        return

    print(f"  columns={list(df.columns)}")

    capped = rows in SUSPECT_CAPS
    full = df
    if capped:
        print(f"  ! rows={rows} hits a likely cap; running yearly scan for true coverage")
        scan = _yearly_scan(pro, name, ts_code, date_field, start)
        if not scan.empty:
            full = scan
            print(f"  yearly-scan total rows={len(full)}")

    if date_field in full.columns:
        dates = full[date_field].astype(str)
        print(f"  {date_field}: min={dates.min()} max={dates.max()}")
        counts = _per_year_counts(full, date_field)
        if counts:
            pretty = "  ".join(f"{y}:{c}" for y, c in counts.items())
            print(f"  per-year counts: {pretty}")
    else:
        print(f"  (date_field {date_field!r} not present in columns)")

    print(f"  sample rows (head {head}):")
    with pd.option_context("display.max_columns", None, "display.width", 200):
        print(df.head(head).to_string(index=False))


def main() -> None:
    args = parse_args()
    settings = get_settings()
    token = settings.tushare_token
    if not token:
        raise SystemExit("TUSHARE_TOKEN not set; cannot probe")

    ts.set_token(token)
    pro = ts.pro_api()

    wanted = {s.strip() for s in args.interfaces.split(",") if s.strip()}
    specs = [s for s in INTERFACES if not wanted or s["name"] in wanted]

    print(f"Probing {len(specs)} interface(s) for ts_code={args.ts_code} since {args.start}")
    for spec in specs:
        try:
            probe_interface(pro, spec, args.ts_code, args.start, args.head)
        except Exception as exc:  # pragma: no cover
            print(f"  !! unexpected error probing {spec['name']}: {exc}")


if __name__ == "__main__":
    main()
