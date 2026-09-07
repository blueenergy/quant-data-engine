#!/usr/bin/env python3
"""Compare MongoDB ``etf_share_size`` (Tushare etf_share_size) vs ``etf_shares`` (fund_share).

Join key: (ts_code, trade_date). Reports rows only in one side, and relative
mismatch between total_share (万份) and fd_share (万份) when both exist.

Env: MONGO_URI, DATA_DB_NAME (default quant_data). Loads quant-data-engine/.env if present.
"""
from __future__ import annotations

import argparse
import csv
import os
import pathlib
import sys
from typing import Any, Dict, Iterator, Optional, Tuple

from dotenv import load_dotenv

_env = pathlib.Path(__file__).resolve().parent.parent / ".env"
if _env.exists():
    load_dotenv(_env, override=True)
else:
    load_dotenv(override=True)

from pymongo import MongoClient


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--start", required=True, help="trade_date >= YYYYMMDD")
    p.add_argument("--end", required=True, help="trade_date <= YYYYMMDD")
    p.add_argument("--ts-code", dest="ts_code", default=None, help="Optional single ETF ts_code")
    p.add_argument("--rel-eps", type=float, default=0.001, help="Relative diff threshold (0.001=0.1%%)")
    p.add_argument("--abs-eps", type=float, default=1.0, help="Absolute diff threshold (万份)")
    p.add_argument("--sample-limit", type=int, default=30, help="Max sample rows printed per category")
    p.add_argument("--csv", dest="csv_path", default=None, help="Write mismatch samples to CSV path")
    return p.parse_args()


def _flt(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def main() -> None:
    args = parse_args()
    uri = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    db_name = os.getenv("DATA_DB_NAME", "quant_data")
    client = MongoClient(uri, serverSelectionTimeoutMS=8000)
    db = client[db_name]
    coll_sz = db["etf_share_size"]
    coll_sh = db["etf_shares"]

    q: Dict[str, Any] = {
        "trade_date": {"$gte": args.start, "$lte": args.end},
    }
    if args.ts_code:
        q["ts_code"] = args.ts_code

    by_key_sz: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for doc in coll_sz.find(q, {"_id": 0}):
        k = (doc.get("ts_code"), str(doc.get("trade_date")))
        if k[0] and k[1]:
            by_key_sz[k] = doc

    by_key_sh: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for doc in coll_sh.find(q, {"_id": 0}):
        k = (doc.get("ts_code"), str(doc.get("trade_date")))
        if k[0] and k[1]:
            by_key_sh[k] = doc

    keys_sz = set(by_key_sz)
    keys_sh = set(by_key_sh)
    only_sz = keys_sz - keys_sh
    only_sh = keys_sh - keys_sz
    both = keys_sz & keys_sh

    mismatch: list[Dict[str, Any]] = []
    for k in both:
        a = by_key_sz[k]
        b = by_key_sh[k]
        ts = _flt(a.get("total_share"))
        fd = _flt(b.get("fd_share"))
        if ts is None or fd is None:
            mismatch.append(
                {
                    "ts_code": k[0],
                    "trade_date": k[1],
                    "reason": "missing_numeric",
                    "total_share": ts,
                    "fd_share": fd,
                }
            )
            continue
        diff = abs(ts - fd)
        rel = diff / max(abs(ts), abs(fd), 1e-9)
        if diff > args.abs_eps and rel > args.rel_eps:
            mismatch.append(
                {
                    "ts_code": k[0],
                    "trade_date": k[1],
                    "reason": "value_mismatch",
                    "total_share": ts,
                    "fd_share": fd,
                    "abs_diff": diff,
                    "rel_diff": rel,
                }
            )

    print(f"DB={db_name} range={args.start}..{args.end} ts_code={args.ts_code or '*'}")
    print(f"etf_share_size rows: {len(by_key_sz)}  etf_shares rows: {len(by_key_sh)}")
    print(f"only_in_etf_share_size: {len(only_sz)}  only_in_etf_shares: {len(only_sh)}  both: {len(both)}")
    print(f"numeric_mismatch (threshold rel>{args.rel_eps} and abs>{args.abs_eps} 万份): {len(mismatch)}")

    def _samples(keys: Iterator[Tuple[str, str]], n: int) -> list:
        out = []
        for i, k in enumerate(keys):
            if i >= n:
                break
            out.append(k)
        return out

    if only_sz:
        print("\nSample only_in_etf_share_size:")
        for k in _samples(iter(only_sz), args.sample_limit):
            print(f"  {k} total_share={by_key_sz[k].get('total_share')}")
    if only_sh:
        print("\nSample only_in_etf_shares:")
        for k in _samples(iter(only_sh), args.sample_limit):
            print(f"  {k} fd_share={by_key_sh[k].get('fd_share')}")
    if mismatch:
        print("\nSample value mismatches:")
        for row in mismatch[: args.sample_limit]:
            print(f"  {row}")

    if args.csv_path and mismatch:
        with open(args.csv_path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(mismatch[0].keys()))
            w.writeheader()
            w.writerows(mismatch)
        print(f"\nWrote {len(mismatch)} mismatch rows to {args.csv_path}")

    client.close()


if __name__ == "__main__":
    main()
