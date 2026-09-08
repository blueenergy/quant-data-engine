"""Big QMT ZMQ RPC 1m K-line source (path B: ``get_market_data_ex``)."""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Callable, Mapping, Sequence
from zoneinfo import ZoneInfo

from .source import PollStats, SymbolDelta
from .zmq_rpc_client import BigQmtZmqRpcClient, RpcTransportTimeout

logger = logging.getLogger(__name__)
CN_TZ = ZoneInfo("Asia/Shanghai")
KLINE_FIELDS = ("open", "high", "low", "close", "volume")


class BigQmtRpcKlineSource:
    """Poll official 1m bars via ZMQ RPC ``get_market_data_ex``."""

    payload_kind = "kline"

    def __init__(
        self,
        symbols: Sequence[str],
        *,
        rpc_client: BigQmtZmqRpcClient | None = None,
        connect_address: str = "",
        account_id: str = "",
        poll_batch_size: int = 50,
        rpc_timeout_seconds: float = 15.0,
        period: str = "1m",
        count: int = 241,
        client_factory: Callable[..., BigQmtZmqRpcClient] | None = None,
    ) -> None:
        self.symbols = sorted(set(symbols))
        self.rpc_client = rpc_client
        self.connect_address = connect_address
        self.account_id = account_id
        self.poll_batch_size = int(poll_batch_size or 0)
        self.rpc_timeout_seconds = float(rpc_timeout_seconds)
        self.period = period
        self.count = int(count)
        self._client_factory = client_factory or BigQmtZmqRpcClient
        self.started = False
        self.last_poll_stats: PollStats | None = None

    def start(self) -> None:
        if self.started:
            return
        if self.rpc_client is None:
            if not self.connect_address:
                raise RuntimeError("quote RPC connect_address is required for bigqmt_rpc kline source")
            self.rpc_client = self._client_factory(
                connect_address=self.connect_address,
                account_id=self.account_id,
            )
        self.started = True
        logger.info(
            "Big QMT RPC 1m kline source ready for %d symbols (period=%s count=%d batch=%s address=%s)",
            len(self.symbols),
            self.period,
            self.count,
            self.poll_batch_size or "all",
            self.connect_address or getattr(self.rpc_client, "connect_address", ""),
        )

    def poll(self) -> Mapping[str, Any]:
        if not self.started:
            self.start()
        if not self.symbols or self.rpc_client is None:
            self.last_poll_stats = None
            return {}

        start_time = datetime.now(CN_TZ).strftime("%Y%m%d")
        batch_size = self.poll_batch_size
        if batch_size <= 0 or batch_size >= len(self.symbols):
            frames, skipped = self._fetch_with_retry(list(self.symbols), label="all", start_time=start_time, raise_after_retry=True)
            nonempty = _nonempty_frames(frames)
            self.last_poll_stats = PollStats(
                expected_symbols=len(self.symbols),
                synced_symbols=len(nonempty),
                batches_total=1,
                batches_ok=0 if skipped else 1,
                batches_skipped=1 if skipped else 0,
            )
            if not nonempty:
                logger.warning(
                    "get_market_data_ex period=%s returned 0 bars for %d symbols; "
                    "Guojin 1m cache is often empty (xtquant_big_convert Issue #95)",
                    self.period,
                    len(self.symbols),
                )
            return frames

        merged: dict[str, Any] = {}
        symbols = list(self.symbols)
        batches_total = (len(symbols) + batch_size - 1) // batch_size
        batches_ok = 0
        batches_skipped = 0
        for index in range(0, len(symbols), batch_size):
            batch = symbols[index : index + batch_size]
            part, skipped = self._fetch_with_retry(batch, label=str(index // batch_size), start_time=start_time)
            if skipped:
                batches_skipped += 1
            elif part:
                batches_ok += 1
                merged.update(part)

        nonempty = _nonempty_frames(merged)
        stats = PollStats(
            expected_symbols=len(symbols),
            synced_symbols=len(nonempty),
            batches_total=batches_total,
            batches_ok=batches_ok,
            batches_skipped=batches_skipped,
        )
        self.last_poll_stats = stats
        logger.info(
            "bigqmt 1m poll: synced=%d/%d coverage=%.1f%% batches_ok=%d batches_skipped=%d/%d",
            stats.synced_symbols,
            stats.expected_symbols,
            stats.coverage_ratio * 100.0,
            stats.batches_ok,
            stats.batches_skipped,
            stats.batches_total,
        )
        if not merged and batches_skipped > 0:
            raise RpcTransportTimeout(
                "zmq rpc timeout: get_market_data_ex (all %d batches skipped)" % batches_skipped
            )
        if not nonempty:
            logger.warning(
                "get_market_data_ex period=%s returned 0 bars; supplement 1m in QMT 数据管理 (Issue #95)",
                self.period,
            )
        return merged

    def _fetch_with_retry(
        self,
        batch: Sequence[str],
        *,
        label: str,
        start_time: str,
        raise_after_retry: bool = False,
    ) -> tuple[Mapping[str, Any], bool]:
        last_exc: BaseException | None = None
        for attempt in (1, 2):
            try:
                frames = self.rpc_client.get_market_data_ex(
                    field_list=list(KLINE_FIELDS),
                    stock_list=list(batch),
                    period=self.period,
                    count=self.count,
                    start_time=start_time,
                    timeout_seconds=self.rpc_timeout_seconds,
                )
                return frames or {}, False
            except RpcTransportTimeout as exc:
                last_exc = exc
                logger.warning("bigqmt get_market_data_ex batch %s timed out (attempt %d/2)", label, attempt)
            except Exception as exc:
                last_exc = exc
                logger.exception("bigqmt get_market_data_ex batch %s failed (attempt %d/2)", label, attempt)
                break
        if raise_after_retry and last_exc is not None:
            raise last_exc
        return {}, True

    def update_symbols(self, new_symbols: Sequence[str]) -> SymbolDelta:
        new_set = set(new_symbols)
        old_set = set(self.symbols)
        added = tuple(sorted(new_set - old_set))
        removed = tuple(sorted(old_set - new_set))
        self.symbols = sorted(new_set)
        if added or removed:
            logger.info("Big QMT RPC 1m symbols updated: +%d -%d total=%d", len(added), len(removed), len(self.symbols))
        return SymbolDelta(added=added, removed=removed)

    def stop(self) -> None:
        if self.rpc_client is not None:
            try:
                self.rpc_client.close()
            except Exception as exc:
                logger.warning("Big QMT RPC kline client close failed: %s", exc)
        self.started = False
        logger.info("Big QMT RPC 1m kline source stopped")


def _nonempty_frames(frames: Mapping[str, Any]) -> dict[str, Any]:
    from quant_data_engine.bridges.miniqmt.kline_bars import _frame_rows

    return {symbol: payload for symbol, payload in (frames or {}).items() if _frame_rows(payload)}
