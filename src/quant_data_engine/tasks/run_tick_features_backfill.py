"""Backfill intraday minute tick features from QMT tick history.

The QMT tick history is a **3-second five-level snapshot** stream — there is
no Level-2 permission on the account, so this is a book-state and
*inferred*-flow feature set, not real order flow.  See
:mod:`quant_data_engine.tick.features` for the exact caveats.

Why a separate task instead of the auction backfill: the auction task keeps
the 09:15-09:30 window and writes ``auction_features``; the intraday feature
set needs the whole session and a different key space.  Both read the same
QMT tick history, and both resolve the universe through the same
``resolve_symbols`` helper, so the two feature sets cover the same stocks.

Run inside ``quant-realtime-minute`` (it mounts ``/app/bqbox``)::

    python3 -m quant_data_engine.tasks.run_tick_features_backfill --date 20261118
    python3 -m quant_data_engine.tasks.run_tick_features_backfill \\
        --start 20261020 --end 20261118 --collection tick_features_1m_probe

Do not run during 09:30-11:30 / 13:00-15:00: tick downloads compete with the
live quote bridge.  The host wrapper enforces that window.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta
import logging
import time
from typing import Any, Iterable, Mapping, Sequence

from pymongo import MongoClient, UpdateOne

from quant_data_engine.bridges.bigqmt.file_rpc_client import BigQmtFileRpcClient
from quant_data_engine.config.settings import get_settings
from quant_data_engine.tick.features import (
    FEATURE_VERSION,
    TICK_FIELDS,
    build_minute_features,
)
from quant_data_engine.tasks.run_auction_live_capture import (
    CONTEXT_SYMBOLS,
    DB,
    load_bridge_config,
    resolve_symbols,
)

logger = logging.getLogger(__name__)

SOURCE = "qmt_tick_backfill"
COLLECTION = "tick_features_1m"
PROBE_COLLECTION = "tick_features_1m_probe"
RUNS = "tick_feature_runs"
PROBE_RUNS = "tick_feature_runs_probe"
DEFAULT_MAILBOX = "/app/bqbox/tick-features-backfill"

# Indices have no tick history (only ETFs do), so they can never produce a
# feature row — do not spend RPC round trips on them.
INDEX_CONTEXT = tuple(s for s in CONTEXT_SYMBOLS if s.startswith(("000", "399")))


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill intraday minute tick features from QMT tick history"
    )
    parser.add_argument("--date", default="", help="trade date YYYYMMDD; default today")
    parser.add_argument("--start", default="", help="range start YYYYMMDD")
    parser.add_argument("--end", default="", help="range end YYYYMMDD")
    parser.add_argument(
        "--config",
        default="/app/scripts/bigqmt_rpc_bridge.watchlist_minute_kline.json",
    )
    parser.add_argument("--mailbox", default=DEFAULT_MAILBOX)
    parser.add_argument("--account-id", default="")
    parser.add_argument("--symbols", nargs="*", help="manual symbol override")
    parser.add_argument(
        "--only-symbols",
        action="store_true",
        help="use --symbols as the whole universe (no first-board pool, ladder or context)",
    )
    parser.add_argument(
        "--collection",
        default="",
        help=f"target collection; default {COLLECTION} (or {PROBE_COLLECTION} with --probe)",
    )
    parser.add_argument("--probe", action="store_true", help="write the scratch collection")
    parser.add_argument("--chunk-size", type=int, default=5)
    parser.add_argument("--download-timeout", type=float, default=180.0)
    parser.add_argument("--rpc-timeout", type=float, default=120.0)
    parser.add_argument("--no-limit-up", dest="include_limit_up", action="store_false", default=True)
    parser.add_argument("--only-missing", action="store_true",
                        help="skip symbols that already have rows for the date")
    parser.add_argument(
        "--verify-daily",
        action="store_true",
        help="log the tick day totals against the volume_price daily bar (unit check)",
    )
    parser.add_argument(
        "--verify-bars",
        action="store_true",
        help="log the per-minute tick volumes against minute_bars (alignment check)",
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args(argv)


def _chunks(items: Sequence[str], size: int) -> Iterable[Sequence[str]]:
    step = max(1, int(size))
    for start in range(0, len(items), step):
        yield items[start:start + step]


def trading_days(data_db: Any, start: str, end: str) -> list[str]:
    """Open days in ``[start, end]`` from the ``trade_calendar`` collection."""
    if not start or not end:
        return []
    rows = list(
        data_db["trade_calendar"].find(
            {"trade_date": {"$gte": start, "$lte": end}},
            {"_id": 0, "trade_date": 1, "is_open": 1},
        )
    )
    days = []
    for row in rows:
        flag = row.get("is_open", 1)
        if flag in (1, "1", True):
            days.append(str(row.get("trade_date")))
    if days:
        return sorted(days)
    # Fallback: weekdays only (calendar missing or empty).
    out = []
    current = datetime.strptime(start, "%Y%m%d")
    stop = datetime.strptime(end, "%Y%m%d")
    while current <= stop:
        if current.weekday() < 5:
            out.append(current.strftime("%Y%m%d"))
        current += timedelta(days=1)
    return out


def _has_rows(db: Any, collection: str, symbol: str, trade_date: str) -> bool:
    return bool(
        db[collection].find_one(
            {"symbol": symbol, "trade_date": trade_date}, {"_id": 1}
        )
    )


def _write_rows(db: Any, collection: str, rows: Sequence[dict[str, Any]], dry_run: bool) -> None:
    if dry_run or not rows:
        return
    db[collection].bulk_write(
        [
            UpdateOne(
                {"symbol": row["symbol"], "minute": row["minute"]},
                {"$set": row},
                upsert=True,
            )
            for row in rows
        ],
        ordered=False,
    )


def _verify_daily(db: Any, symbol: str, trade_date: str, rows: Sequence[dict[str, Any]]) -> None:
    """Compare the tick day totals with the ``volume_price`` daily bar.

    QMT reports tick volume in lots (手) and the daily bar keeps Tushare's
    ``vol`` in the same unit, so the volume ratio should land near 1.  The
    daily ``amount`` is in thousand yuan, so the amount ratio is compared
    after scaling by 1000.
    """
    bar = db["volume_price"].find_one(
        {"symbol": symbol, "trade_date": trade_date},
        {"_id": 0, "volume": 1, "amount": 1},
    )
    if not bar:
        logger.info("verify %s %s no daily bar", symbol, trade_date)
        return
    tick_volume = sum(float(row.get("volume_sum") or 0.0) for row in rows)
    tick_amount = sum(float(row.get("amount_sum") or 0.0) for row in rows)
    bar_volume = float(bar.get("volume") or 0.0)
    bar_amount = float(bar.get("amount") or 0.0) * 1000.0
    logger.info(
        "verify %s %s tick_vol=%.0f daily_vol=%.0f vol_ratio=%.4f | "
        "tick_amt=%.0f daily_amt=%.0f amt_ratio=%.4f",
        symbol, trade_date, tick_volume, bar_volume,
        tick_volume / bar_volume if bar_volume else float("nan"),
        tick_amount, bar_amount,
        tick_amount / bar_amount if bar_amount else float("nan"),
    )


def _verify_bars(db: Any, symbol: str, trade_date: str, rows: Sequence[dict[str, Any]]) -> None:
    """Compare the per-minute tick volumes with the 1m bars.

    The tick rows and ``minute_bars`` must line up minute by minute; a
    systematic shift means the labelling convention differs, a constant ratio
    means a unit mismatch.
    """
    bars = list(
        db["minute_bars"].find(
            {
                "symbol": symbol,
                "trade_date": {
                    "$gte": f"{trade_date}0000",
                    "$lte": f"{trade_date}2359",
                },
            },
            {"_id": 0, "trade_date": 1, "volume": 1},
        )
    )
    if not bars:
        logger.info("verify-bars %s %s no minute_bars", symbol, trade_date)
        return
    bar_by_minute = {str(row["trade_date"]): row for row in bars}
    best: tuple[float, int, int, float] | None = None
    for shift in range(-3, 4):
        pairs: list[tuple[float, float]] = []
        for row in rows:
            minute = str(row["minute"])
            other = f"{minute[:-4]}{int(minute[-4:]) + shift:04d}"
            bar = bar_by_minute.get(other)
            if bar is None:
                continue
            tick_volume = float(row.get("volume_sum") or 0.0)
            bar_volume = float(bar.get("volume") or 0.0)
            if tick_volume or bar_volume:
                pairs.append((tick_volume, bar_volume))
        if len(pairs) < 10:
            continue
        mae = sum(abs(a - b) for a, b in pairs) / len(pairs)
        scale = (sum(b for _, b in pairs) / len(pairs)) or 1.0
        score = mae / scale
        if best is None or score < best[0]:
            best = (score, shift, len(pairs), mae)
    if best is None:
        logger.info("verify-bars %s %s not enough overlap", symbol, trade_date)
        return
    score, shift, count, mae = best
    logger.info(
        "verify-bars %s %s best_shift=%+d pairs=%d mean_abs_diff=%.1f rel=%.4f",
        symbol, trade_date, shift, count, mae, score,
    )


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s [tick_features] %(levelname)s %(message)s",
    )
    settings = get_settings()
    client = MongoClient(settings.mongo_uri)
    try:
        db = client[DB]
        data_db = client[settings.data_db]
        collection = args.collection or (PROBE_COLLECTION if args.probe else COLLECTION)
        runs_collection = PROBE_RUNS if collection == PROBE_COLLECTION else RUNS
        if not args.dry_run:
            db[collection].create_index([("symbol", 1), ("minute", 1)], unique=True)
            db[collection].create_index([("trade_date", 1), ("symbol", 1)])
            db[runs_collection].create_index([("trade_date", 1), ("source", 1)], unique=True)

        if args.start or args.end:
            dates = trading_days(data_db, args.start, args.end)
        else:
            dates = [str(args.date or datetime.now().strftime("%Y%m%d")).replace("-", "")]
        if not dates:
            logger.info("no trading days in range")
            return 0

        config = load_bridge_config(args.config)
        account_id = args.account_id or str(config.quote_account_id or "")
        rpc = BigQmtFileRpcClient(mailbox=args.mailbox, account_id=account_id)
        started = time.time()
        total_rows = 0
        try:
            for trade_date in dates:
                symbols = resolve_symbols(
                    client=client,
                    settings=settings,
                    config=config,
                    trade_date=trade_date,
                    include_limit_up=args.include_limit_up,
                    manual_symbols=args.symbols,
                )
                if args.only_symbols:
                    symbols = sorted(set(args.symbols or ()))
                symbols = sorted(s for s in symbols if s and s not in INDEX_CONTEXT)
                if args.only_missing:
                    symbols = [s for s in symbols if not _has_rows(db, collection, s, trade_date)]
                if not symbols:
                    logger.info("no symbols for date=%s", trade_date)
                    continue
                completed: list[str] = []
                missing: list[str] = []
                row_count = 0
                date_started = time.time()
                for chunk_index, chunk in enumerate(
                    _chunks(symbols, args.chunk_size), start=1
                ):
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
                        logger.warning("chunk failed date=%s: %s", trade_date, exc)
                        missing.extend(chunk)
                        continue
                    for symbol in chunk:
                        payload = (data or {}).get(symbol) or {}
                        records = payload.get("records") if isinstance(payload, Mapping) else None
                        rows = build_minute_features(
                            records or {}, symbol, trade_date, source=SOURCE
                        )
                        if not rows:
                            missing.append(symbol)
                            continue
                        _write_rows(db, collection, rows, args.dry_run)
                        completed.append(symbol)
                        row_count += len(rows)
                        if args.verify_daily:
                            _verify_daily(db, symbol, trade_date, rows)
                        if args.verify_bars:
                            _verify_bars(db, symbol, trade_date, rows)
                    logger.info(
                        "date=%s chunk %d/%d completed=%d missing=%d rows=%d elapsed=%.1fs",
                        trade_date,
                        chunk_index,
                        (len(symbols) + args.chunk_size - 1) // args.chunk_size,
                        len(completed),
                        len(missing),
                        row_count,
                        time.time() - date_started,
                    )
                total_rows += row_count
                if not args.dry_run:
                    db[runs_collection].update_one(
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
                                "row_count": row_count,
                                "coverage_ratio": len(completed) / len(symbols) if symbols else 0.0,
                                "finished_at": datetime.now(),
                            },
                            "$setOnInsert": {"started_at": datetime.now()},
                        },
                        upsert=True,
                    )
                logger.info(
                    "date=%s done symbols=%d completed=%d missing=%d rows=%d elapsed=%.1fs",
                    trade_date, len(symbols), len(completed), len(missing), row_count,
                    time.time() - date_started,
                )
        finally:
            rpc.close()
        logger.info(
            "tick feature backfill done dates=%d rows=%d elapsed=%.1fs",
            len(dates), total_rows, time.time() - started,
        )
        return 0
    finally:
        client.close()


if __name__ == "__main__":
    raise SystemExit(main())
