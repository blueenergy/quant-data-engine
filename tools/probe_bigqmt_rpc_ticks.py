#!/usr/bin/env python
"""Probe Big QMT ZMQ RPC get_ticks latency and payload size (phase 0/1).

Run on Windows against the quote instance RPC::

    copy scripts\\qmt_instances.example.json scripts\\qmt_instances.local.json
    PYTHONPATH=src python tools/probe_bigqmt_rpc_ticks.py \\
        --qmt-instances scripts/qmt_instances.local.json \\
        --symbols 000001.SZ 600000.SH \\
        --batch-size 400

Full-market whole-exchange probe (use sparingly during phase 0)::

    PYTHONPATH=src python tools/probe_bigqmt_rpc_ticks.py \\
        --quote-account-id 62129066 \\
        --whole-market
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from quant_data_engine.bridges.bigqmt import (  # noqa: E402
    BigQmtZmqRpcClient,
    default_zmq_address,
    encode_rpc_request_payload,
)
from quant_data_engine.bridges.bigqmt.qmt_instances import quote_account_id_from_config  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--quote-rpc-address", help="tcp://127.0.0.1:PORT")
    parser.add_argument("--quote-account-id", help="Derive default ZMQ port from account id")
    parser.add_argument(
        "--qmt-instances",
        type=Path,
        help="Path to qmt_instances.local.json (fills quote-account-id when omitted)",
    )
    parser.add_argument("--symbols", nargs="*", default=["000001.SZ", "600000.SH"])
    parser.add_argument("--whole-market", action="store_true", help="Request SH+SZ in one RPC call")
    parser.add_argument("--batch-size", type=int, default=0, help="Split symbol list into batches")
    parser.add_argument("--timeout", type=float, default=15.0)
    parser.add_argument("--ping", action="store_true", help="Also run ping probe")
    parser.add_argument("--output", help="Write JSON metrics to this path")
    return parser.parse_args()


def _resolve_address(args: argparse.Namespace) -> str:
    if args.quote_rpc_address:
        return args.quote_rpc_address
    if args.quote_account_id:
        return default_zmq_address(args.quote_account_id)
    raise SystemExit("need --quote-rpc-address, --quote-account-id, or --qmt-instances")


def _payload_bytes(request: dict) -> int:
    return len(encode_rpc_request_payload(request).encode("utf-8"))


def main() -> None:
    args = parse_args()
    if not args.quote_account_id and args.qmt_instances:
        args.quote_account_id = quote_account_id_from_config(args.qmt_instances)
    address = _resolve_address(args)
    client = BigQmtZmqRpcClient(connect_address=address, account_id=args.quote_account_id or "")
    metrics: dict = {"connect_address": address, "probes": []}

    try:
        if args.ping:
            started = time.perf_counter()
            pong = client.ping(timeout_seconds=min(args.timeout, 6.0))
            metrics["ping_ms"] = round((time.perf_counter() - started) * 1000, 2)
            metrics["ping_response"] = pong.get("data")

        codes = ["SH", "SZ"] if args.whole_market else list(args.symbols)
        batch_size = args.batch_size or len(codes)
        for index in range(0, len(codes), batch_size):
            batch = codes[index : index + batch_size]
            started = time.perf_counter()
            ticks = client.get_ticks(batch, timeout_seconds=args.timeout)
            elapsed_ms = round((time.perf_counter() - started) * 1000, 2)
            sample = next(iter(ticks.values()), {}) if ticks else {}
            metrics["probes"].append(
                {
                    "codes": batch,
                    "elapsed_ms": elapsed_ms,
                    "tick_count": len(ticks),
                    "sample_keys": sorted(sample.keys())[:20],
                    "sample_lastPrice": sample.get("lastPrice"),
                    "sample_time": sample.get("time"),
                    "sample_stime": sample.get("stime"),
                }
            )
    finally:
        client.close()

    print(json.dumps(metrics, ensure_ascii=False, indent=2))
    if args.output:
        Path(args.output).write_text(json.dumps(metrics, ensure_ascii=False, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
