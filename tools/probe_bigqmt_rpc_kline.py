#!/usr/bin/env python
"""Probe Big QMT RPC get_market_data_ex 1m (path B / Issue #95).

Run on Windows against the quote instance::

    PYTHONPATH=src python tools/probe_bigqmt_rpc_kline.py ^
        --qmt-instances scripts/qmt_instances.local.json ^
        --symbols 000001.SZ 600000.SH 300750.SZ
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
)
from quant_data_engine.bridges.bigqmt.qmt_instances import quote_account_id_from_config  # noqa: E402
from quant_data_engine.bridges.miniqmt.kline_bars import (  # noqa: E402
    _frame_rows,
    market_data_to_minute_records,
)


def _records_kind(payload: object) -> str:
    if not isinstance(payload, dict):
        return type(payload).__name__
    rec = payload.get("records")
    if isinstance(rec, dict):
        arrays = [key for key, value in rec.items() if isinstance(value, (list, tuple))]
        n_bars = max((len(rec[key]) for key in arrays), default=0)
        return "columnar_dict cols=%d bars=%d" % (len(rec), n_bars)
    if isinstance(rec, list):
        return "row_list len=%d" % len(rec)
    if rec is None:
        return "records_missing"
    return type(rec).__name__


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--quote-rpc-address")
    parser.add_argument("--quote-account-id")
    parser.add_argument("--qmt-instances", type=Path)
    parser.add_argument("--symbols", nargs="+", default=["000001.SZ", "600000.SH"])
    parser.add_argument("--period", default="1m")
    parser.add_argument("--count", type=int, default=20)
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--output", help="Write JSON report path")
    return parser.parse_args()


def _resolve_address(args: argparse.Namespace) -> str:
    if args.quote_rpc_address:
        return args.quote_rpc_address
    if args.quote_account_id:
        return default_zmq_address(args.quote_account_id)
    raise SystemExit("need --quote-rpc-address, --quote-account-id, or --qmt-instances")


def main() -> None:
    args = parse_args()
    if not args.quote_account_id and args.qmt_instances:
        args.quote_account_id = quote_account_id_from_config(args.qmt_instances)
    address = _resolve_address(args)
    client = BigQmtZmqRpcClient(connect_address=address, account_id=args.quote_account_id or "")
    report: dict = {
        "connect_address": address,
        "period": args.period,
        "count": args.count,
        "symbols": {},
    }
    try:
        started = time.perf_counter()
        frames = client.get_market_data_ex(
            field_list=["open", "high", "low", "close", "volume"],
            stock_list=list(args.symbols),
            period=args.period,
            count=args.count,
            timeout_seconds=args.timeout,
        )
        elapsed_ms = round((time.perf_counter() - started) * 1000, 2)
        records = market_data_to_minute_records(frames, args.symbols)
        by_symbol: dict[str, list] = {}
        for row in records:
            by_symbol.setdefault(row["symbol"], []).append(row)
        empty = []
        for symbol in args.symbols:
            payload = frames.get(symbol) or {}
            rows = _frame_rows(payload)
            converted = by_symbol.get(symbol) or []
            sample = converted[-1] if converted else None
            report["symbols"][symbol] = {
                "raw_rows": len(rows),
                "converted_bars": len(converted),
                "records_kind": _records_kind(payload),
                "sample": sample,
            }
            if not converted:
                empty.append(symbol)
        report["elapsed_ms"] = elapsed_ms
        report["empty_symbols"] = empty
        parsed = any((report["symbols"][sym]["raw_rows"] or 0) > 0 for sym in args.symbols)
        if not empty:
            report["verdict"] = "ok"
        elif parsed:
            report["verdict"] = "unconverted_rows"
        else:
            report["verdict"] = "empty_1m_issue_95"
    finally:
        client.close()

    print(json.dumps(report, ensure_ascii=False, indent=2))
    if empty:
        if report.get("verdict") == "unconverted_rows":
            print(
                "\n# RPC 有行但未能转成 K 线（检查 records 形态 / 时间列），不是 Issue #95 空缓存。",
                file=sys.stderr,
            )
        else:
            print(
                "\n# 这些代码 1m 为空（Issue #95）。在证券端 QMT「数据管理」补 1m/5m 后再探，"
                "或策略内 down_history_data。空则路径 B 无法对齐同花顺。",
                file=sys.stderr,
            )
        raise SystemExit(2)
    if args.output:
        Path(args.output).write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
