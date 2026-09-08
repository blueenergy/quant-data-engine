#!/usr/bin/env python
"""Read-only probe for Tushare fina_indicator_vip candidate fields.

Validates field availability, non-null rates, and Tushare behavior when
unknown field names are included in the ``fields`` parameter.

This script ONLY reads from Tushare. It never writes to Mongo.

Usage:
    PYTHONPATH=src python scripts/data/probe_fina_indicator_fields.py
    PYTHONPATH=src python scripts/data/probe_fina_indicator_fields.py --ts-codes 688111.SH,600519.SH
    PYTHONPATH=src python scripts/data/probe_fina_indicator_fields.py --period 20250930
"""
from __future__ import annotations

import argparse
import sys
import time
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

# Candidate fields from the expansion plan (excluding keys already in _CORE_FIELDS).
CANDIDATE_FIELDS = [
    "roe_avg",
    "opincome_of_ebt",
    "investincome_of_ebt",
    "n_op_profit_of_ebt",
    "tax_to_ebt",
    "dtprofit_to_profit",
    "salescash_to_or",
    "ocf_to_or",
    "ocf_to_opincome",
    "ocf_to_profit",
    "ocf_yoy",
    "cfps_yoy",
    "capitalized_to_da",
    "roa_dp",
    "fixed_assets",
    "profit_prefin_exp",
    "non_op_profit",
    "op_to_ebt",
    "nop_to_ebt",
    "op_to_debt",
    "cash_to_liqdebt",
    "cash_to_liqdebt_withinterest",
    "op_to_liqdebt",
    "roic_yearly",
    "total_fa_trun",
    "profit_to_op",
    "q_opincome",
    "q_investincome",
    "q_dtprofit",
    "q_eps",
    "q_netprofit_margin",
    "q_gsprofit_margin",
    "q_exp_to_sales",
    "q_profit_to_gr",
    "q_saleexp_to_gr",
    "q_adminexp_to_gr",
    "q_finaexp_to_gr",
    "q_gc_to_gr",
    "q_op_to_gr",
    "q_roe",
    "q_dt_roe",
    "q_npta",
    "q_salescash_to_or",
    "q_ocf_to_sales",
    "q_ocf_to_or",
]

# Suspected typos in current _CORE_FIELDS vs likely official names.
TYPO_CANDIDATES = {
    "assets_to_equity": "assets_to_eqt",
    "dp_assets_to_assets": "dp_assets_to_eqt",
    "tangasset_to_netdebt": "tangasset_to_netdebt",  # verify exists
    "roan_yearly": "roa_yearly",  # likely duplicate typo of roa_yearly
}

DEFAULT_TS_CODES = ["688111.SH", "600519.SH", "000001.SZ"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Probe fina_indicator_vip candidate fields (read-only)")
    parser.add_argument(
        "--ts-codes",
        default=",".join(DEFAULT_TS_CODES),
        help="Comma-separated ts_codes (default: 688111.SH,600519.SH,000001.SZ)",
    )
    parser.add_argument("--period", default="", help="Report period YYYYMMDD (default: latest per stock)")
    parser.add_argument("--head", type=int, default=2, help="Sample rows per stock")
    return parser.parse_args()


def _call(pro, **params) -> pd.DataFrame | None:
    time.sleep(0.35)
    try:
        return pro.fina_indicator_vip(**params)
    except Exception as exc:
        print(f"    ! fina_indicator_vip({params}) failed: {exc}")
        return None


def _non_null_summary(df: pd.DataFrame, fields: list[str]) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for field in fields:
        if field not in df.columns:
            out[field] = {"present": False, "non_null": 0, "sample": None}
            continue
        series = df[field]
        non_null = int(series.notna().sum())
        sample = series.dropna().iloc[0] if non_null else None
        out[field] = {"present": True, "non_null": non_null, "sample": sample}
    return out


def probe_unknown_field_behavior(pro, ts_code: str, period: str | None) -> None:
    print("\n=== Unknown field behavior test ===")
    bogus = "___bogus_field_name___"
    params: dict = {"ts_code": ts_code, "fields": f"ts_code,end_date,{bogus}"}
    if period:
        params["period"] = period
    df = _call(pro, **params)
    if df is None:
        print("  RESULT: request with bogus field FAILED (Tushare rejects unknown fields)")
        return
    print(f"  RESULT: request succeeded; columns={list(df.columns)}")
    if bogus in df.columns:
        print("  bogus field was returned (unexpected)")
    else:
        print("  bogus field ignored; safe to include extra names in fields list")


def probe_typo_fields(pro, ts_code: str, period: str | None) -> None:
    print("\n=== Typo / alias field comparison ===")
    for wrong, right in TYPO_CANDIDATES.items():
        if wrong == right:
            fields = f"ts_code,end_date,{wrong}"
        else:
            fields = f"ts_code,end_date,{wrong},{right}"
        params: dict = {"ts_code": ts_code, "fields": fields}
        if period:
            params["period"] = period
        df = _call(pro, **params)
        if df is None or df.empty:
            print(f"  {wrong} vs {right}: call failed or empty")
            continue
        wrong_ok = wrong in df.columns and df[wrong].notna().any()
        right_ok = right in df.columns and df[right].notna().any()
        print(
            f"  {wrong}: present={wrong in df.columns} non_null={wrong_ok} | "
            f"{right}: present={right in df.columns} non_null={right_ok}"
        )


def probe_stock(pro, ts_code: str, period: str | None, candidates: list[str], head: int) -> dict:
    print(f"\n=== {ts_code} ===")
    base_params: dict = {"ts_code": ts_code}
    if period:
        base_params["period"] = period

    full_df = _call(pro, **base_params)
    if full_df is None or full_df.empty:
        print("  no data (full request)")
        return {"ts_code": ts_code, "rows": 0, "summary": {}}

    print(f"  full request rows={len(full_df)} columns={len(full_df.columns)}")
    if not period and "end_date" in full_df.columns:
        latest_period = str(full_df["end_date"].astype(str).max())
        print(f"  latest end_date={latest_period}")
        full_df = full_df[full_df["end_date"].astype(str) == latest_period]

    all_cols = set(full_df.columns)
    present_in_full = [f for f in candidates if f in all_cols]
    missing_in_full = [f for f in candidates if f not in all_cols]
    print(f"  candidates present in full response: {len(present_in_full)}/{len(candidates)}")
    if missing_in_full:
        print(f"  not in full response: {missing_in_full}")

    fields_str = "ts_code,end_date," + ",".join(candidates)
    params = dict(base_params)
    params["fields"] = fields_str
    cand_df = _call(pro, **params)
    if cand_df is None:
        print("  candidate fields request FAILED")
        return {"ts_code": ts_code, "rows": len(full_df), "summary": {}}

    returned = [f for f in candidates if f in cand_df.columns]
    not_returned = [f for f in candidates if f not in cand_df.columns]
    print(f"  explicit fields request: rows={len(cand_df)} returned={len(returned)} not_returned={len(not_returned)}")
    if not_returned:
        print(f"  not_returned: {not_returned}")

    summary = _non_null_summary(cand_df, candidates)
    print(f"  non-null counts (head {head} rows):")
    for field in candidates:
        info = summary[field]
        if info["present"] and info["non_null"]:
            print(f"    {field}: non_null={info['non_null']} sample={info['sample']}")

    if head:
        with pd.option_context("display.max_columns", None, "display.width", 240):
            cols = ["ts_code", "end_date"] + [f for f in candidates if f in cand_df.columns][:8]
            print(cand_df[cols].head(head).to_string(index=False))

    return {"ts_code": ts_code, "rows": len(cand_df), "summary": summary}


def main() -> None:
    args = parse_args()
    settings = get_settings()
    token = settings.tushare_token
    if not token:
        raise SystemExit("TUSHARE_TOKEN not set; cannot probe")

    ts.set_token(token)
    pro = ts.pro_api()

    ts_codes = [c.strip() for c in args.ts_codes.split(",") if c.strip()]
    period = args.period.strip() or None
    candidates = CANDIDATE_FIELDS

    print(f"Probing fina_indicator_vip for {len(ts_codes)} stock(s), period={period or 'latest'}")

    probe_unknown_field_behavior(pro, ts_codes[0], period)
    probe_typo_fields(pro, ts_codes[0], period)

    all_summaries: list[dict] = []
    for code in ts_codes:
        all_summaries.append(probe_stock(pro, code, period, candidates, args.head))

    # Aggregate non-null rate across stocks
    print("\n=== Aggregate candidate field availability ===")
    field_hits = {f: 0 for f in candidates}
    field_stocks = {f: 0 for f in candidates}
    for item in all_summaries:
        for field, info in item.get("summary", {}).items():
            if info.get("present"):
                field_stocks[field] += 1
            if info.get("non_null", 0) > 0:
                field_hits[field] += 1

    n_stocks = max(len(all_summaries), 1)
    core_fields = [
        "q_netprofit_margin",
        "dtprofit_to_profit",
        "opincome_of_ebt",
        "investincome_of_ebt",
        "salescash_to_or",
        "ocf_to_or",
        "ocf_yoy",
        "fixed_assets",
        "total_fa_trun",
        "capitalized_to_da",
        "roic_yearly",
    ]
    core_with_data = sum(1 for f in core_fields if field_hits.get(f, 0) > 0)
    print(f"  core fields with data on >=1 stock: {core_with_data}/{len(core_fields)}")
    print(f"  threshold 70% => need >= {int(len(core_fields) * 0.7 + 0.999)} fields")

    usable = [f for f in candidates if field_hits.get(f, 0) > 0]
    unusable = [f for f in candidates if field_hits.get(f, 0) == 0]
    print(f"  usable (non-null on any stock): {len(usable)}")
    print(f"  never non-null: {unusable}")


if __name__ == "__main__":
    main()
