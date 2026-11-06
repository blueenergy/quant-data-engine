"""Live call-auction capture (09:15-09:26) over Big QMT file RPC.

Run inside ``quant-realtime-minute`` (it mounts ``/app/bqbox``)::

    docker exec -i quant-realtime-minute python3 -m \
        quant_data_engine.tasks.run_auction_live_capture

Scheduling: host cron at 09:14:55 on weekdays.  The task waits for the wall
clock, polls ``get_ticks`` every 3 s, writes ``auction_snapshots``, then writes
``auction_features`` at 09:25:15 (partial, for the 09:25 detector) and at
09:26:15 (full, for the 09:27 first-board scorer).  The nightly backfill
(21:30) upserts the same ``(symbol, ts)`` keys and fills missing symbols.

Never calls ``download_history_data*``: historical downloads in the auction
window compete with the live quote bridge and can time out.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime, time as dtime, timedelta
import json
import logging
import signal
import time
from typing import Any, Callable, Iterable, Mapping, Sequence
from zoneinfo import ZoneInfo

from pymongo import MongoClient, UpdateOne

from quant_data_engine.auction.core import (
    FEATURE_VERSION,
    build_features,
    snapshot_from_tick,
)
from quant_data_engine.auction.universe import (
    load_first_board_pool,
    load_limit_up_ladder_with_date,
    normalize_symbol,
)
from quant_data_engine.bridges.bigqmt.calendar import make_trading_day_checker
from quant_data_engine.bridges.bigqmt.file_rpc_client import BigQmtFileRpcClient
from quant_data_engine.bridges.miniqmt.config import BridgeConfig
from quant_data_engine.bridges.miniqmt.runner import is_cn_trading_day
from quant_data_engine.bridges.miniqmt.universe import SymbolUniverse
from quant_data_engine.config.settings import get_settings

CN_TZ = ZoneInfo("Asia/Shanghai")
logger = logging.getLogger(__name__)

DB = "quant_data"
SNAP = "auction_snapshots"
FEAT = "auction_features"
RUNS = "auction_capture_runs"
SNAP_PROBE = "auction_snapshots_probe"
FEAT_PROBE = "auction_features_probe"
RUNS_PROBE = "auction_capture_runs_probe"

DEFAULT_CONFIG = "/app/scripts/bigqmt_rpc_bridge.watchlist_minute_kline.json"
DEFAULT_MAILBOX = "/app/bqbox/auction-live"
DEFAULT_PROBE_MAILBOX = "/app/bqbox/auction-live-probe"
SOURCE_LIVE = "qmt_tick_live"
SOURCE_PROBE = "qmt_tick_probe"

# 沪深300 / 中证1000 / 国证2000 / 沪深300ETF.  Live index ticks work even
# though historical get_market_data_ex returns 0 rows for indices.
CONTEXT_SYMBOLS = ("000300.SH", "000852.SH", "399303.SZ", "510300.SH")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Live call-auction capture via Big QMT file RPC")
    parser.add_argument("--config", default=DEFAULT_CONFIG, help="bridge JSON used for the watchlist universe")
    parser.add_argument("--mailbox", default="", help="file RPC mailbox; defaults to %s" % DEFAULT_MAILBOX)
    parser.add_argument("--account-id", default="", help="quote account id; defaults to the bridge config")
    parser.add_argument("--symbols", nargs="*", help="manual symbol override (still adds context symbols)")
    parser.add_argument("--no-limit-up", dest="include_limit_up", action="store_false", default=True,
                        help="do not add yesterday's first-board limit-up pool")
    parser.add_argument("--no-ladder", dest="include_ladder", action="store_false", default=True,
                        help="do not add yesterday's multi-board limit-up ladder (streak>=2)")
    parser.add_argument("--start", default="09:15:00", help="capture start (HH:MM:SS)")
    parser.add_argument("--end", default="09:26:10", help="capture end (HH:MM:SS)")
    parser.add_argument("--partial-at", default="09:25:15", help="write partial features at (HH:MM:SS)")
    parser.add_argument("--poll-interval", type=float, default=3.0, help="seconds between get_ticks polls")
    parser.add_argument("--rpc-timeout", type=float, default=15.0, help="per-poll RPC timeout seconds")
    parser.add_argument("--probe", action="store_true",
                        help="write to *_probe collections and qmt_tick_probe source")
    parser.add_argument("--ignore-clock", action="store_true",
                        help="skip waiting/session checks (tests and manual single polls)")
    parser.add_argument("--run-seconds", type=float, default=5.0,
                        help="with --ignore-clock: poll for this many seconds")
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args(argv)


def load_bridge_config(path: str) -> BridgeConfig:
    with open(path, "r", encoding="utf-8") as fh:
        raw = json.load(fh)
    return BridgeConfig.from_mapping(raw)


def resolve_symbols(
    *,
    client: MongoClient,
    settings: Any,
    config: BridgeConfig,
    trade_date: str,
    include_limit_up: bool,
    manual_symbols: Sequence[str] | None,
    include_ladder: bool = True,
) -> list[str]:
    symbols: set[str] = set()
    if manual_symbols:
        symbols.update(normalize_symbol(s) for s in manual_symbols if normalize_symbol(s))
    else:
        try:
            universe = SymbolUniverse(
                finance_db=client[settings.mongo_db],
                trading_db=client[settings.trading_db],
                data_db=client[settings.data_db],
                config=config.universe,
            )
            symbols.update(universe.resolve())
        except Exception as exc:  # pragma: no cover - defensive against missing collections
            logger.warning("SymbolUniverse resolve failed: %s; falling back to config symbols", exc)
            symbols.update(normalize_symbol(s) for s in config.universe.symbols)
    if include_limit_up:
        symbols.update(load_first_board_pool(client[DB], trade_date))
    if include_ladder:
        _, ladder = load_limit_up_ladder_with_date(client[DB], trade_date)
        symbols.update(ladder)
    symbols.update(CONTEXT_SYMBOLS)
    return sorted(symbols)


def load_prev_close(db: Any, symbols: Sequence[str], trade_date: str) -> dict[str, float]:
    """Prefer today's ``volume_price.pre_close`` (adjustment-aware), then prior close."""
    out: dict[str, float] = {}
    for doc in db.volume_price.find(
        {"symbol": {"$in": list(symbols)}, "trade_date": trade_date},
        {"_id": 0, "symbol": 1, "pre_close": 1},
    ):
        value = doc.get("pre_close")
        if value:
            out[str(doc["symbol"])] = float(value)
    missing = [s for s in symbols if s not in out]
    for symbol in missing:
        doc = db.volume_price.find_one(
            {"symbol": symbol, "trade_date": {"$lt": trade_date}, "close": {"$gt": 0}},
            {"_id": 0, "close": 1},
            sort=[("trade_date", -1)],
        )
        if doc and doc.get("close"):
            out[symbol] = float(doc["close"])
    return out


def load_index_prev_close(db: Any, symbols: Iterable[str], trade_date: str) -> dict[str, float]:
    """Prior close for index/ETF context symbols (index_prices / etf_daily)."""
    out: dict[str, float] = {}
    for symbol in symbols:
        doc = db.index_prices.find_one(
            {"ts_code": symbol, "trade_date": {"$lt": trade_date}, "close": {"$gt": 0}},
            {"_id": 0, "close": 1},
            sort=[("trade_date", -1)],
        )
        if doc and doc.get("close"):
            out[symbol] = float(doc["close"])
            continue
        doc = db.etf_daily.find_one(
            {"ts_code": symbol, "trade_date": {"$lt": trade_date}, "close": {"$gt": 0}},
            {"_id": 0, "close": 1},
            sort=[("trade_date", -1)],
        )
        if doc and doc.get("close"):
            out[symbol] = float(doc["close"])
    return out


def load_avg_1m_amount(
    db: Any, symbols: Sequence[str], trade_date: str, lookback_days: int = 20,
) -> dict[str, float]:
    """Per-symbol mean 1-minute amount over the prior ``lookback_days`` days."""
    try:
        anchor = datetime.strptime(trade_date, "%Y%m%d")
    except ValueError:
        return {}
    lower = (anchor - timedelta(days=60)).strftime("%Y%m%d") + "0000"
    pipeline = [
        {"$match": {
            "symbol": {"$in": list(symbols)},
            "trade_date": {"$gte": lower, "$lt": f"{trade_date}0000"},
        }},
        {"$group": {
            "_id": {
                "symbol": "$symbol",
                "date": {"$substr": ["$trade_date", 0, 8]},
            },
            "amount": {"$sum": "$amount"},
            "n": {"$sum": 1},
        }},
    ]
    per_symbol: dict[str, list[tuple[str, float]]] = defaultdict(list)
    try:
        for doc in db.minute_bars.aggregate(pipeline, allowDiskUse=True):
            key = doc.get("_id") or {}
            amount = doc.get("amount")
            count = doc.get("n") or 0
            if amount is None or count <= 0:
                continue
            per_symbol[str(key.get("symbol"))].append((str(key.get("date")), float(amount) / count))
    except Exception as exc:
        logger.warning("avg 1m amount preload failed: %s", exc)
        return {}
    out: dict[str, float] = {}
    for symbol, pairs in per_symbol.items():
        prior = [avg for day, avg in sorted(pairs) if day < trade_date][-lookback_days:]
        if prior:
            out[symbol] = sum(prior) / len(prior)
    return out


def load_sector_info(db: Any, symbols: Sequence[str]) -> dict[str, tuple[str | None, str | None]]:
    l1_names: dict[str, str] = {}
    for doc in db.sw_index_member.find(
        {"src": "SW2021", "is_new": "Y"}, {"_id": 0, "l1_code": 1, "l1_name": 1}
    ):
        code = doc.get("l1_code")
        name = doc.get("l1_name")
        if code and name:
            l1_names[str(name)] = str(code)
    out: dict[str, tuple[str | None, str | None]] = {}
    for doc in db.stock_info.find(
        {"symbol": {"$in": list(symbols)}}, {"_id": 0, "symbol": 1, "sw_l1": 1}
    ):
        symbol = str(doc.get("symbol"))
        name = doc.get("sw_l1")
        out[symbol] = (l1_names.get(str(name)) if name else None, name)
    return out


def _pct(a: float | None, b: float | None) -> float | None:
    if a is None or b is None or b == 0:
        return None
    return (a / b - 1.0) * 100.0


def _row_rank(row: Mapping[str, Any]) -> tuple[float, float, int, int]:
    """Prefer the richer row when QMT repeats the same tick timestamp.

    The 09:25:01 match frame arrives with the auction volume, but a later poll
    can repeat the same ``ts`` with ``volume=0``.  Overwriting the match row
    with that stale tick zeroes the auction feature.  Rank by cumulative
    volume first, then amount, then a usable reference price.
    """
    return (
        float(row.get("volume_cum") or 0.0),
        float(row.get("amount_cum") or 0.0),
        1 if row.get("ref_price") is not None else 0,
        1 if row.get("last_price") is not None else 0,
    )


def _bulk_upsert(collection: Any, documents: Iterable[dict[str, Any]], key_func: Callable[[dict[str, Any]], dict[str, Any]]) -> None:
    """Bulk upsert with a per-row fallback for drivers that lack UpdateOne.sort.

    Production pymongo takes the bulk path; mongomock (and some old drivers)
    reject ``UpdateOne`` objects that carry ``sort=None``, so fall back to
    ``update_one`` rather than losing the whole poll.
    """
    docs = list(documents)
    if not docs:
        return
    ops = [UpdateOne(key_func(doc), {"$set": doc}, upsert=True) for doc in docs]
    try:
        collection.bulk_write(ops, ordered=False)
    except (TypeError, AttributeError) as exc:
        logger.warning("bulk_write unsupported (%s); falling back to update_one", exc)
        for doc in docs:
            collection.update_one(key_func(doc), {"$set": doc}, upsert=True)


def _at(now: datetime, text: str) -> datetime:
    hour, minute, second = (int(part) for part in text.split(":"))
    return datetime.combine(now.date(), dtime(hour, minute, second), tzinfo=CN_TZ)


class AuctionLiveCapture:
    """Poll one RPC tick source, persist snapshots, then persist features."""

    def __init__(
        self,
        *,
        db: Any,
        rpc: Any,
        symbols: Sequence[str],
        trade_date: str,
        prev_close: Mapping[str, float],
        avg_1m_amount: Mapping[str, float],
        index_prev_close: Mapping[str, float],
        sector_info: Mapping[str, tuple[str | None, str | None]],
        start_dt: datetime,
        end_dt: datetime,
        partial_dt: datetime,
        poll_interval: float = 3.0,
        rpc_timeout: float = 15.0,
        source: str = SOURCE_LIVE,
        snap_collection: str = SNAP,
        feat_collection: str = FEAT,
        runs_collection: str = RUNS,
        clock: Callable[[], datetime] | None = None,
        sleep_func: Callable[[float], None] = time.sleep,
    ) -> None:
        self.db = db
        self.rpc = rpc
        self.symbols = sorted(set(symbols))
        self.symbol_set = set(self.symbols)
        self.trade_date = trade_date
        self.prev_close = dict(prev_close)
        self.avg_1m_amount = dict(avg_1m_amount)
        self.index_prev_close = dict(index_prev_close)
        self.sector_info = dict(sector_info)
        self.start_dt = start_dt
        self.end_dt = end_dt
        self.partial_dt = partial_dt
        self.poll_interval = float(poll_interval)
        self.rpc_timeout = float(rpc_timeout)
        self.source = source
        self.snap_col = db[snap_collection]
        self.feat_col = db[feat_collection]
        self.runs_col = db[runs_collection]
        self.clock = clock or (lambda: datetime.now(CN_TZ))
        self.sleep_func = sleep_func
        self.rows: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
        self.last_volume: dict[str, float] = {}
        self.last_amount: dict[str, float] = {}
        self.partial_written = False
        self.stop_requested = False
        self.poll_count = 0
        self.snapshot_count = 0
        self.started_at: datetime | None = None
        self.last_poll_at: datetime | None = None

    def _now(self) -> datetime:
        now = self.clock()
        if now.tzinfo is None:
            return now.replace(tzinfo=CN_TZ)
        return now.astimezone(CN_TZ)

    def request_stop(self, signum: int | None = None, frame: Any = None) -> None:
        logger.info("received signal %s, stopping auction capture", signum)
        self.stop_requested = True

    def run(self) -> None:
        self.started_at = self._now()
        self._ensure_indexes()
        if self._now() < self.start_dt:
            if not self._wait_until(self.start_dt):
                return
        if self._now() > self.end_dt:
            logger.warning("auction window already passed (end=%s)", self.end_dt.strftime("%H:%M:%S"))
            self._write_run()
            return
        while not self.stop_requested:
            now = self._now()
            if now > self.end_dt:
                break
            self._poll(now)
            if not self.partial_written and now >= self.partial_dt:
                self._write_features(partial=True)
            elapsed = (self._now() - now).total_seconds()
            remaining = self.poll_interval - elapsed
            if remaining > 0:
                self.sleep_func(remaining)
        self._write_features(partial=False)
        self._write_run()

    def _wait_until(self, target: datetime) -> bool:
        while not self.stop_requested:
            now = self._now()
            if now >= target:
                return True
            self.sleep_func(min(1.0, (target - now).total_seconds()))
        return False

    def _ensure_indexes(self) -> None:
        self.snap_col.create_index([("symbol", 1), ("ts", 1)], unique=True)
        self.snap_col.create_index([("trade_date", 1), ("symbol", 1)])
        self.feat_col.create_index([("symbol", 1), ("trade_date", 1)], unique=True)
        self.feat_col.create_index([("trade_date", 1)])
        self.runs_col.create_index([("trade_date", 1), ("source", 1)], unique=True)

    def _poll(self, now: datetime) -> None:
        started = time.time()
        try:
            ticks = self.rpc.get_ticks(self.symbols, timeout_seconds=self.rpc_timeout)
        except Exception as exc:
            logger.warning("get_ticks failed: %s", exc)
            return
        self.poll_count += 1
        self.last_poll_at = now
        new_rows: list[dict[str, Any]] = []
        seen_this_poll = 0
        for symbol, tick in (ticks or {}).items():
            if symbol not in self.symbol_set or not isinstance(tick, dict):
                continue
            seen_this_poll += 1
            row = snapshot_from_tick(
                tick,
                symbol,
                self.trade_date,
                source=self.source,
                prev_volume=self.last_volume.get(symbol),
                prev_amount=self.last_amount.get(symbol),
            )
            if row is None:
                continue
            ts = row["ts"]
            existing = self.rows[symbol].get(ts)
            if existing is not None and _row_rank(row) < _row_rank(existing):
                # Stale duplicate (often volume=0): keep the richer row.
                row = existing
            else:
                # New timestamp, or a richer/equal replacement: persist it.
                new_rows.append(row)
            self.rows[symbol][ts] = row
            self.last_volume[symbol] = max(
                self.last_volume.get(symbol, 0.0), float(row.get("volume") or 0.0)
            )
            self.last_amount[symbol] = max(
                self.last_amount.get(symbol, 0.0), float(row.get("amount_cum") or 0.0)
            )
        if new_rows:
            try:
                _bulk_upsert(
                    self.snap_col,
                    new_rows,
                    lambda row: {"symbol": row["symbol"], "ts": row["ts"]},
                )
                self.snapshot_count += len(new_rows)
            except Exception as exc:
                logger.warning("snapshot upsert failed: %s", exc)
        elapsed_ms = (time.time() - started) * 1000.0
        if self.poll_count % 10 == 0 or seen_this_poll < len(self.symbols):
            logger.info(
                "poll=%d ticks=%d/%d new_rows=%d elapsed=%.0fms",
                self.poll_count, seen_this_poll, len(self.symbols), len(new_rows), elapsed_ms,
            )

    def _latest_index_price(self, symbol: str) -> float | None:
        rows = self.rows.get(symbol)
        if not rows:
            return None
        latest = max(rows.values(), key=lambda row: row["ts"])
        return latest.get("ref_price")

    def _enrich_context(self, feature: dict[str, Any], symbol: str) -> None:
        feature["market_index"] = "000300.SH"
        feature["market_gap_pct"] = _pct(
            self._latest_index_price("000300.SH"), self.index_prev_close.get("000300.SH")
        )
        feature["market_gap_1000_pct"] = _pct(
            self._latest_index_price("000852.SH"), self.index_prev_close.get("000852.SH")
        )
        sector_code, sector_name = self.sector_info.get(symbol, (None, None))
        feature["sector_l1_code"] = sector_code
        feature["sector_l1_name"] = sector_name
        # Index/sector/futures context is completed by the 21:30 backfill.
        feature["sector_gap_pct"] = None
        feature["sector_excess_gap_pct"] = None
        for prefix in ("if", "ic", "im", "ih"):
            feature[f"{prefix}_gap_pct"] = None
            feature[f"{prefix}_basis_pct"] = None
            feature[f"{prefix}_basis_chg_pct"] = None
            feature[f"{prefix}_oi_chg_pct"] = None

    def _write_features(self, *, partial: bool) -> None:
        feature_docs: list[dict[str, Any]] = []
        count = 0
        for symbol in self.symbols:
            rows_by_ts = self.rows.get(symbol) or {}
            rows = [rows_by_ts[ts] for ts in sorted(rows_by_ts)]
            if not rows:
                continue
            feature = build_features(
                rows,
                symbol,
                self.trade_date,
                self.prev_close.get(symbol),
                self.avg_1m_amount.get(symbol),
                source=self.source,
                partial=partial,
            )
            if feature is None:
                continue
            self._enrich_context(feature, symbol)
            feature_docs.append(feature)
            count += 1
        if feature_docs:
            try:
                _bulk_upsert(
                    self.feat_col,
                    feature_docs,
                    lambda doc: {"symbol": doc["symbol"], "trade_date": doc["trade_date"]},
                )
            except Exception as exc:
                logger.warning("feature upsert failed: %s", exc)
        if partial:
            self.partial_written = True
        logger.info(
            "features written partial=%s symbols=%d/%d",
            partial, count, len(self.symbols),
        )

    def _completed_symbols(self) -> tuple[list[str], list[str]]:
        completed: list[str] = []
        missing: list[str] = []
        for symbol in self.symbols:
            rows = self.rows.get(symbol) or {}
            if any(row["phase"] == "match_0925" for row in rows.values()):
                completed.append(symbol)
            else:
                missing.append(symbol)
        return completed, missing

    def _write_run(self) -> None:
        completed, missing = self._completed_symbols()
        seen = sorted(s for s in self.symbols if self.rows.get(s))
        coverage = len(seen) / len(self.symbols) if self.symbols else 0.0
        match_coverage = len(completed) / len(self.symbols) if self.symbols else 0.0
        now = self._now()
        try:
            self.runs_col.update_one(
                {"trade_date": self.trade_date, "source": self.source},
                {
                    "$set": {
                        "trade_date": self.trade_date,
                        "source": self.source,
                        "symbols": self.symbols,
                        "seen_symbols": seen,
                        "completed_symbols": completed,
                        "missing_symbols": missing,
                        "status": "completed" if not missing else "partial",
                        "feature_version": FEATURE_VERSION,
                        "coverage_ratio": coverage,
                        "match_coverage_ratio": match_coverage,
                        "poll_count": self.poll_count,
                        "snapshot_count": self.snapshot_count,
                        "finished_at": now,
                    },
                    "$setOnInsert": {"started_at": self.started_at or now},
                },
                upsert=True,
            )
        except Exception as exc:
            logger.warning("run doc write failed: %s", exc)
        logger.info(
            "run done source=%s seen=%d match=%d missing=%d polls=%d snapshots=%d",
            self.source, len(seen), len(completed), len(missing), self.poll_count, self.snapshot_count,
        )


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s [auction_live] %(levelname)s %(message)s",
    )
    now = datetime.now(CN_TZ)
    trade_date = now.strftime("%Y%m%d")
    settings = get_settings()
    client = MongoClient(settings.mongo_uri)
    try:
        data_db = client[settings.data_db]
        checker = make_trading_day_checker(data_db)
        if not args.ignore_clock and not is_cn_trading_day(now, trading_day_checker=checker):
            logger.info("%s is not a trading day; skip", trade_date)
            return 0
        config = load_bridge_config(args.config)
        symbols = resolve_symbols(
            client=client,
            settings=settings,
            config=config,
            trade_date=trade_date,
            include_limit_up=args.include_limit_up,
            manual_symbols=args.symbols,
            include_ladder=args.include_ladder,
        )
        if not symbols:
            logger.error("empty universe; nothing to capture")
            return 2
        prev_close = load_prev_close(data_db, symbols, trade_date)
        index_prev_close = load_index_prev_close(data_db, CONTEXT_SYMBOLS, trade_date)
        avg_1m_amount = load_avg_1m_amount(data_db, symbols, trade_date)
        sector_info = load_sector_info(data_db, symbols)
        account_id = args.account_id or str(config.quote_account_id or "")
        mailbox = args.mailbox or (DEFAULT_PROBE_MAILBOX if args.probe else DEFAULT_MAILBOX)
        rpc = BigQmtFileRpcClient(mailbox=mailbox, account_id=account_id)
        if args.ignore_clock:
            start_dt = now
            end_dt = now + timedelta(seconds=max(0.0, args.run_seconds))
            partial_dt = start_dt
        else:
            start_dt = _at(now, args.start)
            end_dt = _at(now, args.end)
            partial_dt = _at(now, args.partial_at)
        capture = AuctionLiveCapture(
            db=data_db,
            rpc=rpc,
            symbols=symbols,
            trade_date=trade_date,
            prev_close=prev_close,
            avg_1m_amount=avg_1m_amount,
            index_prev_close=index_prev_close,
            sector_info=sector_info,
            start_dt=start_dt,
            end_dt=end_dt,
            partial_dt=partial_dt,
            poll_interval=args.poll_interval,
            rpc_timeout=args.rpc_timeout,
            source=SOURCE_PROBE if args.probe else SOURCE_LIVE,
            snap_collection=SNAP_PROBE if args.probe else SNAP,
            feat_collection=FEAT_PROBE if args.probe else FEAT,
            runs_collection=RUNS_PROBE if args.probe else RUNS,
        )
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                signal.signal(sig, capture.request_stop)
            except ValueError:
                pass
        logger.info(
            "start source=%s symbols=%d mailbox=%s window=%s-%s partial=%s",
            capture.source, len(symbols), mailbox,
            start_dt.strftime("%H:%M:%S"), end_dt.strftime("%H:%M:%S"),
            partial_dt.strftime("%H:%M:%S"),
        )
        try:
            capture.run()
        finally:
            rpc.close()
        return 0
    finally:
        client.close()


if __name__ == "__main__":
    raise SystemExit(main())
