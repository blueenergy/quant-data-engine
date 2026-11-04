#!/usr/bin/env python
"""Poll broad-index quotes from Big QMT and persist 2-minute snapshots.

Runs as its own container (``quant-index-quote``) next to the realtime
bridges. It owns a dedicated file mailbox (``<bqbox>/index-quote``) so it never
touches the ``dayk`` / ``minute`` mailboxes used by the full-market bridges.

Why a separate process instead of adding indices to the full-market bridge:
the bridge's universe is stock-only and its sink feeds sector strength; mixing
indices in would couple an experimental chart feed to the critical bridge.

Data written to ``quant_realtime.index_realtime_snapshots`` (one doc per index
per snapshot)::

    {"ts_code","name","trade_date","snapshot_time","last_price","pre_close",
     "open","high","low","pct_chg","intraday_move","range_pos",
     "volume","amount","quote_epoch_ms","quote_time","source","updated_at"}

``pct_chg`` is vs yesterday's close; ``intraday_move`` is vs today's open;
``range_pos`` is where the last price sits in the day's high-low range.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, time as dtime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence

from pymongo import MongoClient, UpdateOne

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from quant_data_engine.bridges.bigqmt import create_quote_rpc_client  # noqa: E402

LOG = logging.getLogger("index_quote_poller")

DEFAULT_INDICES: List[Dict[str, str]] = [
    {"ts_code": "000001.SH", "name": "上证指数"},
    {"ts_code": "000016.SH", "name": "上证50"},
    {"ts_code": "000300.SH", "name": "沪深300"},
    {"ts_code": "000905.SH", "name": "中证500"},
    {"ts_code": "000852.SH", "name": "中证1000"},
    {"ts_code": "399006.SZ", "name": "创业板指"},
    {"ts_code": "000688.SH", "name": "科创50"},
    {"ts_code": "399673.SZ", "name": "创业板50"},
    # 中证2000（932000.SH）在 QMT 只有昨收、vol=0；用国证2000 作为小盘代理
    {"ts_code": "399303.SZ", "name": "国证2000"},
]

COLLECTION_NAME = "index_realtime_snapshots"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Poll Big QMT index quotes into Mongo")
    parser.add_argument("--config", help="JSON config path")
    parser.add_argument("--once", action="store_true", help="Fetch once and exit")
    parser.add_argument("--loop", action="store_true", help="Run continuously")
    parser.add_argument("--dry-run", action="store_true", help="Print rows instead of writing")
    parser.add_argument("--mailbox", help="Override quote_rpc_file_mailbox")
    parser.add_argument("--account-id", help="Override quote_account_id")
    parser.add_argument("--poll-interval", type=float, help="Seconds between get_ticks calls")
    parser.add_argument("--snapshot-interval", type=float, help="Seconds between Mongo snapshots")
    parser.add_argument("--mongo-uri", help="Override MONGO_URI")
    parser.add_argument("--db", help="Override REALTIME_DB_NAME")
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def _load_config(path: Optional[str]) -> Dict[str, Any]:
    if not path:
        return {}
    with open(path, "r", encoding="utf-8") as handle:
        data = json.load(handle)
    return data if isinstance(data, dict) else {}


def _to_float(value: Any) -> Optional[float]:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if out == out else None


def _is_trading_time(now: datetime) -> bool:
    if now.weekday() >= 5:
        return False
    current = now.time()
    return (dtime(9, 25) <= current <= dtime(11, 32)) or (dtime(12, 58) <= current <= dtime(15, 2))


def _should_snapshot(now: datetime, interval_seconds: float) -> bool:
    if interval_seconds <= 60:
        return True
    minutes = max(1, int(interval_seconds // 60))
    return now.minute % minutes == 0


def _quote_time(tick: Dict[str, Any]) -> Optional[str]:
    tag = str(tick.get("timetag") or "").strip()
    if tag:
        return tag
    epoch = _to_float(tick.get("time"))
    if epoch and epoch > 1e12:
        return datetime.fromtimestamp(epoch / 1000.0).strftime("%Y-%m-%d %H:%M:%S")
    return None


def _normalize(item: Dict[str, str], tick: Dict[str, Any], snapshot_time: str) -> Dict[str, Any]:
    last = _to_float(tick.get("lastPrice"))
    pre_close = _to_float(tick.get("lastClose"))
    open_price = _to_float(tick.get("open"))
    high = _to_float(tick.get("high"))
    low = _to_float(tick.get("low"))
    volume = _to_float(tick.get("volume"))
    amount = _to_float(tick.get("amount"))

    pct_chg = (last / pre_close - 1.0) * 100.0 if last and pre_close else None
    intraday_move = (last / open_price - 1.0) * 100.0 if last and open_price else None
    range_pos = (last - low) / (high - low) if last is not None and high and low is not None and high > low else None
    epoch = _to_float(tick.get("time"))

    return {
        "ts_code": item["ts_code"],
        "name": item.get("name") or item["ts_code"],
        "trade_date": snapshot_time[:8],
        "snapshot_time": snapshot_time,
        "last_price": last,
        "pre_close": pre_close,
        "open": open_price,
        "high": high,
        "low": low,
        "pct_chg": pct_chg,
        "intraday_move": intraday_move,
        "range_pos": range_pos,
        "volume": volume,
        "amount": amount,
        "quote_epoch_ms": int(epoch) if epoch else None,
        "quote_time": _quote_time(tick),
        "source": "bigqmt_file_rpc",
        "updated_at": datetime.now(timezone.utc),
    }


def _fetch_rows(
    client: Any,
    indices: Sequence[Dict[str, str]],
    *,
    timeout_seconds: float,
    snapshot_time: str,
) -> List[Dict[str, Any]]:
    codes = [item["ts_code"] for item in indices]
    ticks = client.get_ticks(codes, timeout_seconds=timeout_seconds)
    rows: List[Dict[str, Any]] = []
    missing: List[str] = []
    for item in indices:
        tick = ticks.get(item["ts_code"]) if isinstance(ticks, dict) else None
        if not tick:
            missing.append(item["ts_code"])
            continue
        rows.append(_normalize(item, tick, snapshot_time))
    if missing:
        LOG.warning("missing ticks: %s", ",".join(missing))
    return rows


def _ensure_indexes(collection: Any) -> None:
    collection.create_index(
        [("ts_code", 1), ("snapshot_time", 1)],
        unique=True,
        name="index_quote_code_time_uidx",
        background=True,
    )
    collection.create_index([("snapshot_time", -1)], name="index_quote_time", background=True)
    collection.create_index([("trade_date", 1)], name="index_quote_date", background=True)


def _write_rows(collection: Any, rows: Iterable[Dict[str, Any]]) -> int:
    ops = [
        UpdateOne(
            {"ts_code": row["ts_code"], "snapshot_time": row["snapshot_time"]},
            {"$set": row},
            upsert=True,
        )
        for row in rows
    ]
    if not ops:
        return 0
    collection.bulk_write(ops, ordered=False)
    return len(ops)


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
    )
    config = _load_config(args.config)

    indices = config.get("indices") or DEFAULT_INDICES
    poll_interval = float(args.poll_interval or config.get("poll_interval") or 30.0)
    snapshot_interval = float(args.snapshot_interval or config.get("snapshot_interval") or 120.0)
    timeout_seconds = float(config.get("quote_rpc_timeout_sec") or 15.0)

    client = create_quote_rpc_client(
        transport=config.get("quote_rpc_transport", "file"),
        connect_address=config.get("quote_rpc_connect_address", ""),
        account_id=args.account_id or config.get("quote_account_id", ""),
        file_mailbox=args.mailbox or config.get("quote_rpc_file_mailbox", ""),
    )

    mongo_uri = args.mongo_uri or os.environ.get("MONGO_URI", "mongodb://localhost:27017")
    db_name = args.db or os.environ.get("REALTIME_DB_NAME", "quant_realtime")
    collection = MongoClient(mongo_uri)[db_name][COLLECTION_NAME]
    _ensure_indexes(collection)

    LOG.info(
        "index quote poller ready: %d indices, poll=%ss, snapshot=%ss, mailbox=%s",
        len(indices),
        poll_interval,
        snapshot_interval,
        getattr(client, "connect_address", ""),
    )

    if args.once:
        snapshot_time = datetime.now().strftime("%Y%m%d%H%M")
        rows = _fetch_rows(client, indices, timeout_seconds=timeout_seconds, snapshot_time=snapshot_time)
        if args.dry_run:
            print(json.dumps(rows, ensure_ascii=False, indent=2, default=str))
            return
        written = _write_rows(collection, rows)
        LOG.info("one-shot wrote %d/%d rows at %s", written, len(indices), snapshot_time)
        return

    while True:
        now = datetime.now()
        if not _is_trading_time(now):
            time.sleep(min(60.0, poll_interval))
            continue
        try:
            snapshot_time = now.strftime("%Y%m%d%H%M")
            rows = _fetch_rows(client, indices, timeout_seconds=timeout_seconds, snapshot_time=snapshot_time)
            if rows and _should_snapshot(now, snapshot_interval):
                written = _write_rows(collection, rows)
                LOG.info(
                    "snapshot %s: %d/%d indices, latest=%s",
                    snapshot_time,
                    written,
                    len(indices),
                    rows[0].get("pct_chg"),
                )
        except Exception as exc:  # noqa: BLE001 - a poll failure must not kill the loop
            LOG.warning("poll failed: %s", exc)
        time.sleep(poll_interval)


if __name__ == "__main__":
    main()
