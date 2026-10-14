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
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Sequence

from pymongo.collection import Collection

from quant_data_engine.bridges.bigqmt.constants import (
    DEFAULT_QUOTE_RPC_TRANSPORT,
    QUOTE_RPC_TRANSPORT_FILE,
    QUOTE_RPC_TRANSPORT_ZMQ,
)
from quant_data_engine.bridges.bigqmt.kline_source import KLINE_FIELDS
from quant_data_engine.bridges.bigqmt.rpc_client_factory import (
    create_quote_rpc_client,
    normalize_quote_rpc_transport,
)
from quant_data_engine.bridges.bigqmt.zmq_rpc_client import (
    RpcTransportError,
    RpcTransportTimeout,
    default_zmq_address,
)
from quant_data_engine.bridges.miniqmt.config import BridgeConfig
from quant_data_engine.bridges.miniqmt.kline_bars import (
    is_filler_bar,
    market_data_to_minute_records,
    split_real_days,
)
from quant_data_engine.config.settings import Settings
from quant_data_engine.repositories.minute_repository import MinuteRepository

logger = logging.getLogger(__name__)

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
        "--quote-rpc-transport",
        choices=["zmq", "file"],
        help="zmq=connect to the strategy (default); file=use its mailbox "
        "(zero sockets, required when the QMT client enforces the illegal-IP policy)",
    )
    parser.add_argument(
        "--quote-rpc-file-mailbox",
        help="quote-rpc-transport=file: this process's mailbox directory; must be "
        "unique per process and a subdirectory of the QMT-side mailbox root",
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


@dataclass(frozen=True)
class QuoteRpcTarget:
    """Everything needed to reach the quote terminal, however it is reached."""

    transport: str
    connect_address: str
    account_id: str
    timeout_seconds: float
    file_mailbox: str = ""

    @property
    def describe(self) -> str:
        if self.transport == QUOTE_RPC_TRANSPORT_FILE:
            return f"file {self.file_mailbox or '(no mailbox)'}"
        return f"zmq {self.connect_address or '(no address)'}"

    def create_client(self) -> Any:
        return create_quote_rpc_client(
            transport=self.transport,
            connect_address=self.connect_address,
            account_id=self.account_id,
            file_mailbox=self.file_mailbox,
        )


def resolve_quote_rpc(
    *,
    quote_rpc_address: str | None = None,
    quote_account_id: str | None = None,
    config_path: str | None = None,
    rpc_timeout_seconds: float | None = None,
    quote_rpc_transport: str | None = None,
    quote_rpc_file_mailbox: str | None = None,
) -> QuoteRpcTarget:
    """Resolve how to reach the quote terminal: CLI/env beat the bridge JSON."""
    address = (quote_rpc_address or os.getenv("QUOTE_RPC_ADDRESS") or "").strip()
    account_id = (quote_account_id or os.getenv("QUOTE_ACCOUNT_ID") or "").strip()
    transport = (quote_rpc_transport or os.getenv("QUOTE_RPC_TRANSPORT") or "").strip()
    mailbox = (quote_rpc_file_mailbox or os.getenv("QUOTE_RPC_FILE_MAILBOX") or "").strip()
    timeout = float(rpc_timeout_seconds) if rpc_timeout_seconds else 0.0
    path = (config_path or "").strip() or None
    if not path:
        guessed = default_watchlist_config_path()
        path = str(guessed) if guessed else None
    if path:
        cfg = BridgeConfig.from_json(path)
        address = address or (cfg.quote_rpc_address or "")
        account_id = account_id or (cfg.quote_account_id or "")
        transport = transport or (cfg.quote_rpc_transport or "")
        mailbox = mailbox or (cfg.quote_rpc_file_mailbox or "")
        # Do not inherit quote_rpc_timeout_sec from the live bridge JSON.
        # Path A files use 15s for get_ticks; historical 1m windows need more.
    if timeout <= 0:
        timeout = DEFAULT_RPC_TIMEOUT_SEC
    transport = normalize_quote_rpc_transport(transport or DEFAULT_QUOTE_RPC_TRANSPORT)
    if transport == QUOTE_RPC_TRANSPORT_FILE:
        if not mailbox:
            raise RuntimeError(
                "quote_rpc_transport=file requires --quote-rpc-file-mailbox, "
                "QUOTE_RPC_FILE_MAILBOX, or a config with quote_rpc_file_mailbox"
            )
    else:
        if not address and account_id:
            address = default_zmq_address(account_id)
        if not address:
            raise RuntimeError(
                "bigqmt_rpc requires --quote-rpc-address, --quote-account-id, or "
                f"--config with quote_account_id (tried scripts/{_WATCHLIST_CONFIG_NAME})"
            )
    return QuoteRpcTarget(
        transport=transport,
        connect_address=address,
        account_id=account_id,
        timeout_seconds=timeout,
        file_mailbox=mailbox,
    )


def _chunk_bar_count(chunk_start: str, chunk_end: str) -> int:
    start_dt = datetime.strptime(chunk_start, "%Y%m%d")
    end_dt = datetime.strptime(chunk_end, "%Y%m%d")
    days = max(1, (end_dt - start_dt).days + 1)
    return days * 240 + 80


def _in_window(trade_date: str, start: str, end: str) -> bool:
    day = str(trade_date or "")[:8]
    return bool(day) and start <= day <= end


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


def expected_days_in_chunk(
    day_set: set[str], chunk_start: str, chunk_end: str
) -> set[str]:
    return {day for day in day_set if chunk_start <= day <= chunk_end}


def missing_days_in_chunk(
    records: Sequence[dict[str, Any]],
    chunk_start: str,
    chunk_end: str,
    day_set: set[str],
) -> list[str]:
    """Open days the reply should have covered but did not.

    Filler days count as covered — they *are* returned, with placeholder bars —
    so this isolates the days a truncated reply dropped entirely. Empty
    ``day_set`` (no calendar) yields ``[]`` and disables the check.
    """
    if not day_set:
        return []
    returned = {str(row.get("trade_date") or "")[:8] for row in records}
    return sorted(expected_days_in_chunk(day_set, chunk_start, chunk_end) - returned)


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
    trading_days: Sequence[str] | None = None,
    force_download: bool = False,
) -> int:
    """Read+upsert one symbol. Returns number of records written (0 = still empty).

    **Only whole real days are ever upserted.** Without a local 1m cache QMT
    answers with placeholder bars rather than nothing, so treating any non-empty
    reply as "has data" both skips the download and writes filler over real
    rows. All-filler days count as a cache miss and are never written.

    ``trading_days`` is the run window's open days. With it, a chunk is also a
    cache miss when the reply does not cover every open day it should:
    ``get_market_data_ex`` returns a contiguous run anchored at ``end_time`` and
    **stops at a gap**, so a truncated reply hides the filler days behind that
    gap and would otherwise never trigger a download — the symbol stays broken
    forever while verify keeps failing. Without it, coverage is not checked and
    behaviour is unchanged.

    ``force_download`` skips the read pass and downloads unconditionally, for
    repairing days whose *other* bars are fine (e.g. a stale 15:00 close) and
    which therefore trip no cache-miss test.

    The download covers the whole range, so afterwards **every** chunk is
    re-read, not just the flagged ones: a partially-filled day sitting in an
    unflagged chunk would otherwise keep its stale placeholders for good.
    """
    day_set = {str(day) for day in (trading_days or ())}
    chunks = filter_session_chunks(
        iter_date_chunks(start, end, chunk_days),
        trading_day_checker=((lambda dt: dt.strftime("%Y%m%d") in day_set) if day_set else None),
    )
    written = 0
    need_download: list[tuple[str, str]] = []
    if force_download:
        need_download = list(chunks)
    else:
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
            if (
                not records
                or filler_days
                or missing_days_in_chunk(records, chunk_start, chunk_end, day_set)
            ):
                need_download.append((chunk_start, chunk_end))
    if not need_download:
        return written
    if force_download:
        print(f"  redownload: all {len(chunks)} session chunks {start}..{end}")
    else:
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
        not_covered = missing_days_in_chunk(records, chunk_start, chunk_end, day_set)
        if not records or filler_days or not_covered:
            if _is_weekend(chunk_start) and _is_weekend(chunk_end):
                continue
            detail = []
            if not records:
                detail.append("no bars")
            if filler_days:
                detail.append(f"{len(filler_days)} filler days")
            if not_covered:
                detail.append(f"{len(not_covered)} uncovered days")
            print(
                f"  still incomplete after download: {symbol} {chunk_start}..{chunk_end} "
                f"({', '.join(detail)}) (Issue #95 / no 1m cache)"
            )
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
    quote_rpc_transport: str | None = None,
    quote_rpc_file_mailbox: str | None = None,
    config_path: str | None = None,
    rpc_client: Any | None = None,
    repository: MinuteRepository | None = None,
    trading_days: Sequence[str] | None = None,
    force_download: bool = False,
) -> list[str]:
    """Download/upsert via quote RPC. Returns symbols with no new bars."""
    if not symbols:
        return []
    if force_download:
        print(
            f"redownload: forcing download_history_data for all {len(symbols)} symbols "
            f"{start}..{end} (existing rows are rewritten)"
        )
    elif trading_days:
        print(f"trading calendar: {len(trading_days)} open days in {start}..{end}")
    else:
        # Worth a warning rather than silence: without the calendar the coverage
        # check is off, so "no missing days" and "the check never ran" look
        # identical from the outside.
        logger.warning(
            "no trading calendar for %s..%s; coverage check disabled "
            "(a truncated reply will not trigger a download)",
            start,
            end,
        )
    owned_client = rpc_client is None
    if rpc_client is None:
        target = resolve_quote_rpc(
            quote_rpc_address=quote_rpc_address,
            quote_account_id=quote_account_id,
            config_path=config_path,
            rpc_timeout_seconds=rpc_timeout_seconds,
            quote_rpc_transport=quote_rpc_transport,
            quote_rpc_file_mailbox=quote_rpc_file_mailbox,
        )
        print(
            f"quote RPC: {target.describe} account_id={target.account_id or '-'} "
            f"timeout={target.timeout_seconds}s chunk_days={chunk_days}"
        )
        rpc_client = target.create_client()
        rpc_timeout_seconds = target.timeout_seconds
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
                    trading_days=trading_days,
                    force_download=force_download,
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
