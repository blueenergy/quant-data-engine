"""Backfill 09:15-09:26 auction snapshots/features from QMT tick history.

The live capture writes the production collections in real time.  This task is
the lunch-time / post-close reconciliation path: it downloads the same tick
window from QMT history, rebuilds the same ``auction_v1`` feature with the
shared ``quant_data_engine.auction.core`` logic, and upserts by
``(symbol, trade_date)``.

Why a separate task instead of ``jupiter_script/learning/auction_backfill.py``:
the learning script has a fixed 9-symbol list and predates the shared core.
This task uses the live-capture universe (watchlist/portfolios + yesterday's
first-board pool + today's limit-up rows) and the same code path as the live
capture, so live and backfill cannot drift.

Run inside ``quant-realtime-minute`` (it mounts ``/app/bqbox``)::

    python3 -m quant_data_engine.tasks.run_auction_backfill --date 20261106

Do not run during 09:30-11:30 / 13:00-15:00: tick downloads compete with the
quote bridge.  The host wrapper enforces the lunch/after-close window.
"""
from __future__ import annotations

import argparse
from datetime import datetime
import logging
import os
import sys
import time
from typing import Any, Iterable, Mapping, Sequence

from pymongo import MongoClient, UpdateOne

from quant_data_engine.auction.core import build_features, hhmmss, snapshot_from_tick
from quant_data_engine.bridges.bigqmt.file_rpc_client import BigQmtFileRpcClient
from quant_data_engine.config.settings import get_settings
from quant_data_engine.tasks.run_auction_live_capture import (
    CONTEXT_SYMBOLS,
    DB,
    FEAT,
    RUNS,
    SNAP,
    load_avg_1m_amount,
    load_bridge_config,
    load_prev_close,
    resolve_symbols,
)

logger = logging.getLogger(__name__)

SOURCE = "qmt_tick_backfill"
FEATURE_VERSION = "auction_v1"
DEFAULT_MAILBOX = "/app/bqbox/auction-backfill-live"
TICK_FIELDS = [
    "time", "stime", "lastPrice", "volume", "amount", "transactionNum",
    "askPrice", "bidPrice", "askVol", "bidVol", "stockStatus",
]
AUCTION_START = 91500
AUCTION_END = 92600


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill 09:15-09:26 auction from QMT tick history")
    parser.add_argument("--date", default="", help="trade date YYYYMMDD; default today")
    parser.add_argument("--config", default="/app/scripts/bigqmt_rpc_bridge.watchlist_minute_kline.json")
    parser.add_argument("--mailbox", default=DEFAULT_MAILBOX)
    parser.add_argument("--account-id", default="")
    parser.add_argument("--symbols", nargs="*", help="manual symbol override")
    parser.add_argument("--no-limit-up", dest="include_limit_up", action="store_false", default=True)
    parser.add_argument("--include-today-limit-up", action="store_true", default=True,
                        help="add today's daily_limit_up rows (leaders that may be outside the live universe)")
    parser.add_argument("--only-missing", action="store_true",
                        help="skip symbols whose feature is already complete")
    parser.add_argument("--chunk-size", type=int, default=10)
    parser.add_argument("--download-timeout", type=float, default=180.0)
    parser.add_argument("--rpc-timeout", type=float, default=60.0)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args(argv)


def _chunks(items: Sequence[str], size: int) -> Iterable[Sequence[str]]:
    step = max(1, int(size))
    for start in range(0, len(items), step):
        yield items[start:start + step]


def _tick_rows(records: Mapping[str, Any], symbol: str, trade_date: str) -> list[dict[str, Any]]:
    """Convert a QMT tick records dict into ``auction_snapshots`` rows.

    QMT can repeat the same ``stime`` with a zero-volume frame after the match
    frame.  Keep the richer row per timestamp; otherwise a later zero frame
    overwrites the match row and zeroes ``auction_volume/amount``.
    """
    if not isinstance(records, Mapping):
        return []
    stimes = records.get("stime") or []
    by_ts: dict[str, dict[str, Any]] = {}
    prev_volume: float | None = None
    prev_amount: float | None = None
    for index, stime in enumerate(stimes):
        hhmmss_value = hhmmss(stime)
        if hhmmss_value is None or not (AUCTION_START <= hhmmss_value < AUCTION_END):
            continue
        tick = {
            key: (records.get(key)[index] if index < len(records.get(key) or []) else None)
            for key in TICK_FIELDS
        }
        row = snapshot_from_tick(
            tick,
            symbol,
            trade_date,
            prev_volume=prev_volume,
            prev_amount=prev_amount,
            source=SOURCE,
        )
        if row is None:
            continue
        prev_volume = row["volume_cum"]
        prev_amount = row["amount_cum"]
        existing = by_ts.get(row["ts"])
        rank = (
            float(row.get("volume_cum") or 0.0),
            float(row.get("amount_cum") or 0.0),
            1 if row.get("ref_price") is not None else 0,
            1 if row.get("last_price") is not None else 0,
        )
        old_rank = (
            float((existing or {}).get("volume_cum") or 0.0),
            float((existing or {}).get("amount_cum") or 0.0),
            1 if (existing or {}).get("ref_price") is not None else 0,
            1 if (existing or {}).get("last_price") is not None else 0,
        )
        if existing is None or rank > old_rank:
            by_ts[row["ts"]] = row
    return [by_ts[key] for key in sorted(by_ts)]


def _complete_feature(db: Any, symbol: str, trade_date: str) -> bool:
    doc = db[FEAT].find_one(
        {"symbol": symbol, "trade_date": trade_date},
        {"_id": 0, "is_auction_complete": 1, "tick_count_match_0925": 1},
    )
    return bool(
        doc
        and doc.get("is_auction_complete")
        and int(doc.get("tick_count_match_0925") or 0) > 0
    )


def _resolve_backfill_symbols(
    *,
    client: MongoClient,
    settings: Any,
    config: Any,
    trade_date: str,
    include_limit_up: bool,
    include_today_limit_up: bool,
    manual_symbols: Sequence[str] | None,
) -> list[str]:
    symbols = set(resolve_symbols(
        client=client,
        settings=settings,
        config=config,
        trade_date=trade_date,
        include_limit_up=include_limit_up,
        manual_symbols=manual_symbols,
    ))
    if include_today_limit_up:
        # The live universe is watchlists/portfolios + yesterday's first-board
        # pool; high-streak leaders (e.g. 永臻股份) may be outside it.  Today's
        # daily_limit_up rows are the authoritative leader supplement.
        symbols.update(str(s) for s in client[DB].daily_limit_up.distinct(
            "symbol", {"date": trade_date}
        ))
    # Indices are captured separately by the live task; their tick history has
    # no auction match and no volume_price.pre_close here.
    symbols.difference_update(CONTEXT_SYMBOLS)
    return sorted(s for s in symbols if s)


def _write_rows(db: Any, symbol: str, trade_date: str, rows: Sequence[dict[str, Any]],
                feature: dict[str, Any] | None, dry_run: bool) -> None:
    if dry_run:
        return
    if rows:
        db[SNAP].bulk_write(
            [
                UpdateOne(
                    {"symbol": row["symbol"], "ts": row["ts"]},
                    {"$set": row},
                    upsert=True,
                )
                for row in rows
            ],
            ordered=False,
        )
    if feature:
        db[FEAT].update_one(
            {"symbol": symbol, "trade_date": trade_date},
            {"$set": feature},
            upsert=True,
        )


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s [auction_backfill] %(levelname)s %(message)s",
    )
    settings = get_settings()
    trade_date = str(args.date or datetime.now().strftime("%Y%m%d")).replace("-", "")
    client = MongoClient(settings.mongo_uri)
    try:
        db = client[DB]
        config = load_bridge_config(args.config)
        symbols = _resolve_backfill_symbols(
            client=client,
            settings=settings,
            config=config,
            trade_date=trade_date,
            include_limit_up=args.include_limit_up,
            include_today_limit_up=args.include_today_limit_up,
            manual_symbols=args.symbols,
        )
        if args.only_missing:
            symbols = [s for s in symbols if not _complete_feature(db, s, trade_date)]
        if not symbols:
            logger.info("no symbols to backfill date=%s", trade_date)
            return 0
        prev_close = load_prev_close(db, symbols, trade_date)
        avg_1m_amount = load_avg_1m_amount(db, symbols, trade_date)
        account_id = args.account_id or str(config.quote_account_id or "")
        rpc = BigQmtFileRpcClient(mailbox=args.mailbox, account_id=account_id)
        started = time.time()
        completed: list[str] = []
        missing: list[str] = []
        snapshot_count = 0
        try:
            for chunk_index, chunk in enumerate(_chunks(symbols, args.chunk_size), start=1):
                logger.info("chunk %d/%d symbols=%d", chunk_index,
                            (len(symbols) + args.chunk_size - 1) // args.chunk_size, len(chunk))
                try:
                    rpc.download_history_data2(
                        list(chunk),
                        period="tick",
                        start_time=trade_date,
                        end_time=trade_date,
                        timeout_seconds=args.download_timeout,
                    )
                    data = rpc.get_market_data_ex(
                        field_list=TICK_FIELDS,
                        stock_list=list(chunk),
                        period="tick",
                        count=100000,
                        start_time=trade_date,
                        end_time=trade_date,
                        dividend_type="none",
                        fill_data=False,
                        timeout_seconds=args.rpc_timeout,
                    )
                except Exception as exc:
                    logger.warning("chunk download/read failed: %s", exc)
                    missing.extend(chunk)
                    continue
                for symbol in chunk:
                    payload = (data or {}).get(symbol) or {}
                    records = payload.get("records") if isinstance(payload, Mapping) else None
                    rows = _tick_rows(records or {}, symbol, trade_date)
                    if not rows:
                        missing.append(symbol)
                        continue
                    feature = build_features(
                        rows,
                        symbol,
                        trade_date,
                        prev_close.get(symbol),
                        avg_1m_amount.get(symbol),
                        source=SOURCE,
                    )
                    if feature is None:
                        missing.append(symbol)
                        continue
                    _write_rows(db, symbol, trade_date, rows, feature, args.dry_run)
                    completed.append(symbol)
                    snapshot_count += len(rows)
                logger.info(
                    "chunk %d done completed=%d missing=%d elapsed=%.1fs",
                    chunk_index, len(completed), len(missing), time.time() - started,
                )
            if not args.dry_run:
                db[RUNS].update_one(
                    {"trade_date": trade_date, "source": SOURCE},
                    {
                        "$set": {
                            "trade_date": trade_date,
                            "source": SOURCE,
                            "symbols": symbols,
                            "completed_symbols": sorted(completed),
                            "missing_symbols": sorted(missing),
                            "status": "completed" if not missing else "partial",
                            "feature_version": FEATURE_VERSION,
                            "snapshot_count": snapshot_count,
                            "coverage_ratio": len(completed) / len(symbols) if symbols else 0.0,
                            "finished_at": datetime.now(),
                        },
                        "$setOnInsert": {"started_at": datetime.fromtimestamp(started)},
                    },
                    upsert=True,
                )
            logger.info(
                "backfill done date=%s symbols=%d completed=%d missing=%d snapshots=%d elapsed=%.1fs",
                trade_date, len(symbols), len(completed), len(missing), snapshot_count,
                time.time() - started,
            )
        finally:
            rpc.close()
        return 0
    finally:
        client.close()


if __name__ == "__main__":
    raise SystemExit(main())
