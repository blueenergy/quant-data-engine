"""Watchlist minute backfill via quote Big QMT ZMQ RPC (path B timestamps).

Two-phase per symbol:

1. Read local 1m with ``get_market_data_ex(start_time, end_time)``.
2. Chunks holding no real bars — empty, or all-filler placeholders — get
   ``download_history_data`` into the quote terminal, then a re-read.

Without a local 1m cache QMT replies with placeholder bars rather than nothing
(``fill_data=False`` does not suppress them), so a cache miss must be judged by
bar *content*: a non-empty reply is not evidence of data. Filler days are never
written, only used to trigger the download.

Do not use process-local ``xtquant`` on Big QMT. Do not apply path A +1 minute
labels. Prefer ``--outside-session-only``: sync download runs on the quote
strategy thread.
"""
from __future__ import annotations

import argparse
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Sequence

from pymongo.collection import Collection

from quant_data_engine.bridges.bigqmt.kline_source import KLINE_FIELDS
from quant_data_engine.bridges.bigqmt.zmq_rpc_client import (
    BigQmtZmqRpcClient,
    RpcTransportError,
    RpcTransportTimeout,
    default_zmq_address,
)
from quant_data_engine.bridges.miniqmt.config import BridgeConfig
from quant_data_engine.bridges.miniqmt.kline_bars import market_data_to_minute_records
from quant_data_engine.config.settings import Settings
from quant_data_engine.repositories.minute_repository import MinuteRepository

SOURCE_BIGQMT_RPC = "bigqmt_rpc"
SOURCE_MINIQMT = "miniqmt"
DEFAULT_CHUNK_DAYS = 5
DEFAULT_RPC_TIMEOUT_SEC = 60.0
_WATCHLIST_CONFIG_NAME = "bigqmt_rpc_bridge.watchlist_minute.json"


def add_backfill_source_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--source",
        choices=(SOURCE_BIGQMT_RPC, SOURCE_MINIQMT),
        default=SOURCE_BIGQMT_RPC,
        help="bigqmt_rpc=quote ZMQ (default); miniqmt=local xtquant (pre-9/18)",
    )
    parser.add_argument(
        "--config",
        help="Bridge JSON with quote_account_id / quote_rpc_address "
        f"(default: scripts/{_WATCHLIST_CONFIG_NAME} if present)",
    )
    parser.add_argument(
        "--quote-rpc-address",
        help="ZMQ connect address, e.g. tcp://127.0.0.1:15626",
    )
    parser.add_argument(
        "--quote-account-id",
        help="Quote account id; derives default ZMQ port 15560 + account_id%100 when address omitted",
    )
    parser.add_argument(
        "--quote-rpc-timeout",
        type=float,
        default=None,
        help=(
            f"get_market_data_ex timeout seconds (default: {int(DEFAULT_RPC_TIMEOUT_SEC)}; "
            "do not inherit the live bridge JSON 15s get_ticks budget)"
        ),
    )
    parser.add_argument(
        "--chunk-days",
        type=int,
        default=DEFAULT_CHUNK_DAYS,
        help="Calendar days per get_market_data_ex window (default: 5; 0 = whole range)",
    )


def iter_date_chunks(start: str, end: str, chunk_days: int) -> list[tuple[str, str]]:
    start_dt = datetime.strptime(str(start), "%Y%m%d")
    end_dt = datetime.strptime(str(end), "%Y%m%d")
    if end_dt < start_dt:
        start_dt, end_dt = end_dt, start_dt
    if int(chunk_days) <= 0:
        return [(start_dt.strftime("%Y%m%d"), end_dt.strftime("%Y%m%d"))]
    chunks: list[tuple[str, str]] = []
    cursor = start_dt
    step = timedelta(days=int(chunk_days) - 1)
    one_day = timedelta(days=1)
    while cursor <= end_dt:
        chunk_end = min(cursor + step, end_dt)
        chunks.append((cursor.strftime("%Y%m%d"), chunk_end.strftime("%Y%m%d")))
        cursor = chunk_end + one_day
    return chunks


def _is_weekend(ymd: str) -> bool:
    return datetime.strptime(str(ymd), "%Y%m%d").weekday() >= 5


def chunk_has_session_day(
    start: str,
    end: str,
    *,
    trading_day_checker: Any | None = None,
) -> bool:
    """True if any calendar day in ``[start, end]`` can have A-share 1m bars."""
    cursor = datetime.strptime(str(start), "%Y%m%d")
    end_dt = datetime.strptime(str(end), "%Y%m%d")
    one_day = timedelta(days=1)
    while cursor <= end_dt:
        if trading_day_checker is not None:
            if bool(trading_day_checker(cursor)):
                return True
        elif cursor.weekday() < 5:
            return True
        cursor += one_day
    return False


def filter_session_chunks(
    chunks: Sequence[tuple[str, str]],
    *,
    trading_day_checker: Any | None = None,
) -> list[tuple[str, str]]:
    return [
        chunk
        for chunk in chunks
        if chunk_has_session_day(chunk[0], chunk[1], trading_day_checker=trading_day_checker)
    ]


def _engine_root() -> Path:
    return Path(__file__).resolve().parents[3]


def default_watchlist_config_path() -> Path | None:
    candidates = (
        Path("scripts") / _WATCHLIST_CONFIG_NAME,
        _engine_root() / "scripts" / _WATCHLIST_CONFIG_NAME,
    )
    for path in candidates:
        if path.is_file():
            return path
    return None


def resolve_quote_rpc(
    *,
    quote_rpc_address: str | None = None,
    quote_account_id: str | None = None,
    config_path: str | None = None,
    rpc_timeout_seconds: float | None = None,
) -> tuple[str, str, float]:
    """Return ``(connect_address, account_id, timeout_seconds)``."""
    address = (quote_rpc_address or os.getenv("QUOTE_RPC_ADDRESS") or "").strip()
    account_id = (quote_account_id or os.getenv("QUOTE_ACCOUNT_ID") or "").strip()
    timeout = float(rpc_timeout_seconds) if rpc_timeout_seconds else 0.0
    path = (config_path or "").strip() or None
    if not path:
        guessed = default_watchlist_config_path()
        path = str(guessed) if guessed else None
    if path:
        cfg = BridgeConfig.from_json(path)
        address = address or (cfg.quote_rpc_address or "")
        account_id = account_id or (cfg.quote_account_id or "")
        # Do not inherit quote_rpc_timeout_sec from the live bridge JSON.
        # Path A files use 15s for get_ticks; historical 1m windows need more.
    if timeout <= 0:
        timeout = DEFAULT_RPC_TIMEOUT_SEC
    if not address and account_id:
        address = default_zmq_address(account_id)
    if not address:
        raise RuntimeError(
            "bigqmt_rpc requires --quote-rpc-address, --quote-account-id, or "
            f"--config with quote_account_id (tried scripts/{_WATCHLIST_CONFIG_NAME})"
        )
    return address, account_id, timeout


def _chunk_bar_count(chunk_start: str, chunk_end: str) -> int:
    start_dt = datetime.strptime(chunk_start, "%Y%m%d")
    end_dt = datetime.strptime(chunk_end, "%Y%m%d")
    days = max(1, (end_dt - start_dt).days + 1)
    return days * 240 + 80


def _in_window(trade_date: str, start: str, end: str) -> bool:
    day = str(trade_date or "")[:8]
    return bool(day) and start <= day <= end


def is_filler_bar(row: dict[str, Any]) -> bool:
    """Whether one row is a QMT placeholder bar: four prices equal, no volume.

    QMT returns these for symbols with no local 1m cache, **even with
    ``fill_data=False``** — the flag does not suppress them.

    **Never filter individual bars with this.** A real day carries one stray
    flat bar at 14:59 (a bridge artifact), so per-bar filtering would shrink
    241-bar days to 240 and silently break the ``close`` horizon, which
    consumers read as the day's last bar. Classify whole days via
    :func:`split_real_days` instead.

    Limit-up/limit-down (一字板) rows are flat too but carry volume, so they
    are not misread as filler.

    Total function: anything unparseable is *not* filler, matching the Mongo
    ``is_flat`` predicate in ``watchlist_minute_backfill.verify_minute_bars``,
    which likewise evaluates to false rather than raising.
    """
    try:
        open_ = float(row["open"])
        high = float(row["high"])
        low = float(row["low"])
        close = float(row["close"])
        volume = float(row.get("volume") or 0)
    except (KeyError, TypeError, ValueError):
        return False
    if open_ != high or high != low or low != close:
        return False
    return volume == 0


def split_real_days(
    records: Sequence[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[str]]:
    """Split rows into ``(writable real rows, days whose every bar is filler)``.

    Day-level on purpose — see :func:`is_filler_bar`. A day is filler only when
    *all* of its bars are, so real days keep their stray 14:59 bar and their
    241-bar shape. Empty input yields ``([], [])``.
    """
    by_day: dict[str, list[dict[str, Any]]] = {}
    for row in records:
        day = str(row.get("trade_date") or "")[:8]
        if day:
            by_day.setdefault(day, []).append(row)
    real: list[dict[str, Any]] = []
    filler_days: list[str] = []
    for day, rows in by_day.items():
        if all(is_filler_bar(row) for row in rows):
            filler_days.append(day)
        else:
            real.extend(rows)
    return real, filler_days


def _download_symbol(
    client: Any,
    symbol: str,
    *,
    period: str,
    start: str,
    end: str,
    timeout_seconds: float,
) -> Any:
    try:
        return client.download_history_data(
            stock_code=symbol,
            period=period,
            start_time=start,
            end_time=end,
            timeout_seconds=timeout_seconds,
        )
    except RpcTransportError as exc:
        message = str(exc).lower()
        if "not implemented" not in message and "is not implemented" not in message:
            raise
        return client.download_history_data2(
            stock_list=[symbol],
            period=period,
            start_time=start,
            end_time=end,
            timeout_seconds=timeout_seconds,
        )


def _read_chunk(
    client: Any,
    symbol: str,
    *,
    period: str,
    start: str,
    end: str,
    timeout_seconds: float,
) -> list[dict[str, Any]]:
    last_exc: BaseException | None = None
    frames: Any = {}
    for attempt in (1, 2):
        try:
            frames = client.get_market_data_ex(
                field_list=list(KLINE_FIELDS),
                stock_list=[symbol],
                period=period,
                count=_chunk_bar_count(start, end),
                start_time=start,
                end_time=end,
                dividend_type="none",
                fill_data=False,
                timeout_seconds=timeout_seconds,
            )
            last_exc = None
            break
        except RpcTransportTimeout as exc:
            last_exc = exc
            print(
                f"  timeout get_market_data_ex {symbol} {start}..{end} "
                f"attempt={attempt}/2 ({timeout_seconds}s)"
            )
    if last_exc is not None:
        return []
    records = market_data_to_minute_records(frames or {}, [symbol])
    return [row for row in records if _in_window(str(row.get("trade_date") or ""), start, end)]


def backfill_symbol_via_bigqmt(
    client: Any,
    repository: MinuteRepository,
    symbol: str,
    *,
    start: str,
    end: str,
    period: str = "1m",
    rpc_timeout_seconds: float = DEFAULT_RPC_TIMEOUT_SEC,
    download_timeout_seconds: float = 180.0,
    chunk_days: int = DEFAULT_CHUNK_DAYS,
    trading_day_checker: Any | None = None,
) -> int:
    """Read+upsert one symbol. Returns number of records written (0 = still empty).

    **Only whole real days are ever upserted.** Without a local 1m cache QMT
    answers with placeholder bars rather than nothing, so treating any non-empty
    reply as "has data" both skips the download and writes filler over real
    rows. All-filler days count as a cache miss and are never written.
    """
    chunks = filter_session_chunks(
        iter_date_chunks(start, end, chunk_days),
        trading_day_checker=trading_day_checker,
    )
    written = 0
    need_download: list[tuple[str, str]] = []
    for chunk_start, chunk_end in chunks:
        records = _read_chunk(
            client,
            symbol,
            period=period,
            start=chunk_start,
            end=chunk_end,
            timeout_seconds=rpc_timeout_seconds,
        )
        real, filler_days = split_real_days(records)
        if real:
            written += int(repository.upsert_bar_records(real) or 0)
        if not records or filler_days:
            need_download.append((chunk_start, chunk_end))
    if not need_download:
        return written
    print(
        f"  cache miss {len(need_download)}/{len(chunks)} session chunks; "
        f"download_history_data {start}..{end}"
    )
    _download_symbol(
        client,
        symbol,
        period=period,
        start=start,
        end=end,
        timeout_seconds=download_timeout_seconds,
    )
    for chunk_start, chunk_end in need_download:
        records = _read_chunk(
            client,
            symbol,
            period=period,
            start=chunk_start,
            end=chunk_end,
            timeout_seconds=rpc_timeout_seconds,
        )
        real, filler_days = split_real_days(records)
        if real:
            written += int(repository.upsert_bar_records(real) or 0)
        if not records or filler_days:
            if _is_weekend(chunk_start) and _is_weekend(chunk_end):
                continue
            if filler_days:
                print(
                    f"  still filler after download: {symbol} {chunk_start}..{chunk_end} "
                    f"({len(filler_days)} days) (Issue #95 / no 1m cache)"
                )
            else:
                print(f"  empty after download: {symbol} {chunk_start}..{chunk_end} (Issue #95 / no 1m cache)")
    return written


def run_bigqmt_backfill_download(
    collection: Collection,
    symbols: Sequence[str],
    *,
    settings: Settings,
    start: str,
    end: str,
    period: str = "1m",
    target_db: str = "quant_data",
    target_collection: str = "minute_bars",
    rpc_timeout_seconds: float | None = None,
    download_timeout_seconds: float = 180.0,
    chunk_days: int = DEFAULT_CHUNK_DAYS,
    quote_rpc_address: str | None = None,
    quote_account_id: str | None = None,
    config_path: str | None = None,
    rpc_client: Any | None = None,
    repository: MinuteRepository | None = None,
) -> list[str]:
    """Download/upsert via quote RPC. Returns symbols with no new bars."""
    if not symbols:
        return []
    owned_client = rpc_client is None
    if rpc_client is None:
        address, account_id, timeout = resolve_quote_rpc(
            quote_rpc_address=quote_rpc_address,
            quote_account_id=quote_account_id,
            config_path=config_path,
            rpc_timeout_seconds=rpc_timeout_seconds,
        )
        print(
            f"quote RPC: {address} account_id={account_id or '-'} "
            f"timeout={timeout}s chunk_days={chunk_days}"
        )
        rpc_client = BigQmtZmqRpcClient(connect_address=address, account_id=account_id)
        rpc_timeout_seconds = timeout
    elif rpc_timeout_seconds is None:
        rpc_timeout_seconds = DEFAULT_RPC_TIMEOUT_SEC
    repo = repository or MinuteRepository(
        settings.mongo_uri,
        target_db,
        collection=target_collection,
    )
    failed: list[str] = []
    try:
        for index, symbol in enumerate(symbols, start=1):
            print(f"[{index}/{len(symbols)}] {symbol}")
            before = collection.count_documents({"symbol": symbol})
            try:
                written = backfill_symbol_via_bigqmt(
                    rpc_client,
                    repo,
                    symbol,
                    start=start,
                    end=end,
                    period=period,
                    rpc_timeout_seconds=float(rpc_timeout_seconds),
                    download_timeout_seconds=download_timeout_seconds,
                    chunk_days=chunk_days,
                )
            except Exception as exc:
                print(f"  error: {exc}")
                failed.append(symbol)
                continue
            after = collection.count_documents({"symbol": symbol})
            if written <= 0 and after <= before:
                failed.append(symbol)
                print(f"  warning: no new bars for {symbol} (count {before} -> {after})")
            else:
                print(f"  upserted={written} count {before} -> {after}")
    finally:
        if owned_client:
            try:
                rpc_client.close()
            except Exception:
                pass
    return failed
