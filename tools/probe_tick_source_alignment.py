#!/usr/bin/env python
"""Compare miniQMT xtdata vs Big QMT RPC ticks for the same symbols (phase 0).

Run on Windows while both miniQMT and the quote instance RPC are available::

    PYTHONPATH=src python tools/probe_tick_source_alignment.py \\
        --qmt-instances scripts/qmt_instances.local.json \\
        --symbols 000001.SZ 600000.SH 300750.SZ

Writes a JSON diff report suitable for wiki phase-0 writeback.
"""
from __future__ import annotations

import argparse
import json
import math
import sys
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from quant_data_engine.bridges.bigqmt import (  # noqa: E402
    BigQmtZmqRpcClient,
    default_zmq_address,
    normalize_bigqmt_tick,
)
from quant_data_engine.bridges.bigqmt.qmt_instances import quote_account_id_from_config  # noqa: E402


def _load_xtdata() -> Any:
    from xtquant import xtdata  # type: ignore

    return xtdata


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--quote-rpc-address")
    parser.add_argument("--quote-account-id")
    parser.add_argument(
        "--qmt-instances",
        type=Path,
        help="Path to qmt_instances.local.json (fills quote-account-id when omitted)",
    )
    parser.add_argument("--symbols", nargs="+", required=True)
    parser.add_argument("--timeout", type=float, default=15.0)
    parser.add_argument("--output", help="Write JSON report path")
    return parser.parse_args()


def _resolve_address(args: argparse.Namespace) -> str:
    if args.quote_rpc_address:
        return args.quote_rpc_address
    if args.quote_account_id:
        return default_zmq_address(args.quote_account_id)
    raise SystemExit("need --quote-rpc-address, --quote-account-id, or --qmt-instances")


def _values_equivalent(left: Any, right: Any) -> bool:
    if left == right:
        return True
    if left is None or right is None:
        return left is right
    try:
        left_num = float(left)
        right_num = float(right)
    except (TypeError, ValueError):
        return False
    if not math.isfinite(left_num) or not math.isfinite(right_num):
        return False
    # Ignore IEEE754 noise (e.g. 11.87 vs 11.870000000000001) and int/float formatting.
    return math.isclose(left_num, right_num, rel_tol=1e-9, abs_tol=1e-6)


def _pick_fields(tick: dict) -> dict:
    keys = ("lastPrice", "open", "high", "low", "lastClose", "volume", "amount", "time", "stime")
    return {key: tick.get(key) for key in keys}


def main() -> None:
    args = parse_args()
    if not args.quote_account_id and args.qmt_instances:
        args.quote_account_id = quote_account_id_from_config(args.qmt_instances)
    address = _resolve_address(args)
    xtdata = _load_xtdata()
    rpc = BigQmtZmqRpcClient(connect_address=address, account_id=args.quote_account_id or "")
    report = {"connect_address": address, "symbols": {}, "captured_at": time.strftime("%Y-%m-%d %H:%M:%S")}

    try:
        mini_started = time.perf_counter()
        mini_ticks = xtdata.get_full_tick(list(args.symbols)) or {}
        mini_ms = round((time.perf_counter() - mini_started) * 1000, 2)

        rpc_started = time.perf_counter()
        rpc_ticks = rpc.get_ticks(list(args.symbols), timeout_seconds=args.timeout)
        rpc_ms = round((time.perf_counter() - rpc_started) * 1000, 2)

        for symbol in args.symbols:
            mini = _pick_fields(mini_ticks.get(symbol) or {})
            big = _pick_fields(normalize_bigqmt_tick(rpc_ticks.get(symbol) or {}))
            diffs = {
                key: {"miniqmt": mini.get(key), "bigqmt": big.get(key)}
                for key in sorted(set(mini) | set(big))
                if not _values_equivalent(mini.get(key), big.get(key))
            }
            report["symbols"][symbol] = {"miniqmt": mini, "bigqmt": big, "field_diffs": diffs}

        report["timing_ms"] = {"miniqmt": mini_ms, "bigqmt_rpc": rpc_ms}
    finally:
        rpc.close()

    print(json.dumps(report, ensure_ascii=False, indent=2))
    if args.output:
        Path(args.output).write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
