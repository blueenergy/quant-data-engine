#!/usr/bin/env python3
"""Optional delete + full-market ``etf_share_size`` backfill for a date range.

Use when you want a clean rebuild after pipeline fixes (e.g. exchange merge
past 5000 rows, Decimal128). Upserts alone usually suffice; deleting first
removes stale (ts_code, trade_date) keys no longer returned by Tushare.

Env: MONGO_URI, DATA_DB_NAME, TUSHARE_TOKEN (e.g. via quantFinance/.env).

Examples::

    # Dry-run: show how many rows would be deleted for a trade_date range
    PYTHONPATH=src python tools/rebuild_etf_share_size_range.py \\
        --start 20250101 --end 20251231 --drop range --dry-run

    # Delete rows in range, then re-fetch full market per calendar day
    PYTHONPATH=src python tools/rebuild_etf_share_size_range.py \\
        --start 20250401 --end 20250430 --drop range

    # Drop entire collection, recreate indexes via pipeline repo, then backfill
    PYTHONPATH=src python tools/rebuild_etf_share_size_range.py \\
        --start 20200101 --end $(date +%Y%m%d) --drop full

    # Save compare reports before/after (same args as compare_etf_share_size_vs_fund_share)
    PYTHONPATH=src python tools/rebuild_etf_share_size_range.py \\
        --start 20250401 --end 20250418 --drop range \\
        --compare-before /tmp/before.txt --compare-after /tmp/after.txt
"""
from __future__ import annotations

import argparse
import pathlib
import subprocess
import sys
import types

from dotenv import load_dotenv

_env = pathlib.Path(__file__).resolve().parent.parent.parent / "quantFinance" / ".env"
if _env.exists():
    load_dotenv(_env, override=True)
else:
    load_dotenv(override=True)

_ROOT = pathlib.Path(__file__).resolve().parent.parent
_SRC = _ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

# Stubs so `import quant_data_engine` works without optional pytdx / baostock (same idea as tests/conftest.py).
def _ensure_module(name: str) -> types.ModuleType:
    mod = sys.modules.get(name)
    if mod is None:
        mod = types.ModuleType(name)
        sys.modules[name] = mod
    return mod


if "pytdx" not in sys.modules:
    _px = _ensure_module("pytdx")
    _reader = _ensure_module("pytdx.reader")
    _hq = _ensure_module("pytdx.hq")

    class _Dr:
        pass

    class _Hq:
        pass

    _reader.TdxExHqDailyBarReader = _Dr
    _hq.TdxHq_API = _Hq
    _px.reader = _reader
    _px.hq = _hq
if "baostock" not in sys.modules:
    _ensure_module("baostock")

from pymongo import MongoClient  # noqa: E402

from quant_data_engine import create_etf_share_size_pipeline, get_settings  # noqa: E402


def _run_compare(start: str, end: str, out_path: pathlib.Path) -> None:
    compare = _ROOT / "tools" / "compare_etf_share_size_vs_fund_share.py"
    cmd = [sys.executable, str(compare), "--start", start, "--end", end]
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as fp:
        subprocess.run(cmd, check=True, stdout=fp, stderr=subprocess.STDOUT, cwd=str(_ROOT))


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--start", required=True, help="Inclusive trade_date YYYYMMDD (string compare)")
    p.add_argument("--end", required=True, help="Inclusive trade_date YYYYMMDD")
    p.add_argument(
        "--drop",
        choices=("none", "range", "full"),
        default="none",
        help="none: only upsert backfill; range: deleteMany trade_date in [start,end]; "
        "full: drop etf_share_size collection (indexes recreated on next repo init)",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print delete counts only; do not delete or call Tushare",
    )
    p.add_argument(
        "--compare-before",
        metavar="PATH",
        type=pathlib.Path,
        default=None,
        help="Run compare script and write stdout+stderr to PATH before delete/backfill",
    )
    p.add_argument(
        "--compare-after",
        metavar="PATH",
        type=pathlib.Path,
        default=None,
        help="Run compare script after backfill and write output to PATH",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    settings = get_settings()
    client = MongoClient(settings.mongo_uri)
    coll = client[settings.data_db]["etf_share_size"]

    if args.compare_before:
        print(f"Writing compare baseline -> {args.compare_before}")
        _run_compare(args.start, args.end, args.compare_before)

    if args.drop == "range":
        filt = {"trade_date": {"$gte": args.start, "$lte": args.end}}
        n = coll.count_documents(filt)
        if args.dry_run:
            print(f"[dry-run] would deleteMany {n} documents matching trade_date {args.start}..{args.end}")
        else:
            r = coll.delete_many(filt)
            print(f"deleteMany trade_date range: deleted_count={r.deleted_count}")
    elif args.drop == "full":
        est = coll.estimated_document_count()
        if args.dry_run:
            print(f"[dry-run] would drop collection etf_share_size (~{est} docs)")
        else:
            coll.drop()
            print("dropped collection etf_share_size")

    if args.dry_run:
        print("[dry-run] skipping Tushare backfill")
        if args.compare_after:
            print("[dry-run] skipping --compare-after")
        return 0

    print(f"Running etf_share_size full-market sync {args.start}..{args.end} ...")
    pipeline = create_etf_share_size_pipeline()
    result = pipeline.run(None, start_date=args.start, end_date=args.end)
    total = sum(r.get("inserted", 0) for r in result.get("results", []))
    failed = [r for r in result.get("results", []) if r.get("issues")]
    print(f"done mode={result.get('mode')} total_upsert_touches={total} days={result.get('days')}")
    if failed:
        print(f"warning: {len(failed)} day rows have issues, sample={failed[:3]}")

    if args.compare_after:
        print(f"Writing compare post-run -> {args.compare_after}")
        _run_compare(args.start, args.end, args.compare_after)

    return 0 if not failed else 1


if __name__ == "__main__":
    raise SystemExit(main())
