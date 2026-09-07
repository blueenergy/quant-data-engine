"""Big QMT ZMQ RPC tick source for the standalone bridge."""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Sequence

from .tick_normalize import normalize_bigqmt_ticks
from .zmq_rpc_client import BigQmtZmqRpcClient, RpcTransportTimeout

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SymbolDelta:
    added: tuple[str, ...]
    removed: tuple[str, ...]


@dataclass(frozen=True)
class PollStats:
    """Summary of the most recent multi-batch ``poll()`` round."""

    expected_symbols: int
    synced_symbols: int
    batches_total: int
    batches_ok: int
    batches_skipped: int

    @property
    def coverage_ratio(self) -> float:
        if self.expected_symbols <= 0:
            return 0.0
        return self.synced_symbols / self.expected_symbols


class BigQmtRpcTickSource:
    """Poll Big QMT via ZMQ RPC ``get_ticks`` in batches (no subscribe/unsubscribe)."""

    def __init__(
        self,
        symbols: Sequence[str],
        *,
        rpc_client: BigQmtZmqRpcClient | None = None,
        connect_address: str = "",
        account_id: str = "",
        poll_batch_size: int = 400,
        rpc_timeout_seconds: float = 15.0,
        sleep_func: Callable[[float], None] = time.sleep,
        client_factory: Callable[..., BigQmtZmqRpcClient] | None = None,
    ) -> None:
        self.symbols = sorted(set(symbols))
        self.rpc_client = rpc_client
        self.connect_address = connect_address
        self.account_id = account_id
        self.poll_batch_size = int(poll_batch_size or 0)
        self.rpc_timeout_seconds = float(rpc_timeout_seconds)
        self.sleep_func = sleep_func
        self._client_factory = client_factory or BigQmtZmqRpcClient
        self.started = False
        self.last_poll_stats: PollStats | None = None

    def start(self) -> None:
        if self.started:
            return
        if self.rpc_client is None:
            if not self.connect_address:
                raise RuntimeError("quote RPC connect_address is required for bigqmt_rpc tick source")
            self.rpc_client = self._client_factory(
                connect_address=self.connect_address,
                account_id=self.account_id,
            )
        self.started = True
        logger.info(
            "Big QMT RPC tick source ready for %d symbols (poll_batch_size=%s, address=%s)",
            len(self.symbols),
            self.poll_batch_size or "all",
            self.connect_address or getattr(self.rpc_client, "connect_address", ""),
        )

    def poll(self) -> Mapping[str, Mapping[str, Any]]:
        if not self.started:
            self.start()
        if not self.symbols or self.rpc_client is None:
            self.last_poll_stats = None
            return {}

        batch_size = self.poll_batch_size
        if batch_size <= 0 or batch_size >= len(self.symbols):
            ticks, skipped = self._fetch_with_retry(list(self.symbols), label="all", raise_after_retry=True)
            normalized = normalize_bigqmt_ticks(ticks)
            self.last_poll_stats = PollStats(
                expected_symbols=len(self.symbols),
                synced_symbols=len(normalized),
                batches_total=1,
                batches_ok=0 if skipped else 1,
                batches_skipped=1 if skipped else 0,
            )
            return normalized

        merged: dict[str, Mapping[str, Any]] = {}
        symbols = list(self.symbols)
        batches_total = (len(symbols) + batch_size - 1) // batch_size
        batches_ok = 0
        batches_skipped = 0
        for index in range(0, len(symbols), batch_size):
            batch = symbols[index : index + batch_size]
            part, skipped = self._fetch_with_retry(batch, label=str(index // batch_size))
            if skipped:
                batches_skipped += 1
            elif part:
                batches_ok += 1
                merged.update(normalize_bigqmt_ticks(part))

        stats = PollStats(
            expected_symbols=len(symbols),
            synced_symbols=len(merged),
            batches_total=batches_total,
            batches_ok=batches_ok,
            batches_skipped=batches_skipped,
        )
        self.last_poll_stats = stats
        logger.info(
            "bigqmt poll: synced=%d/%d coverage=%.1f%% batches_ok=%d batches_skipped=%d/%d",
            stats.synced_symbols,
            stats.expected_symbols,
            stats.coverage_ratio * 100.0,
            stats.batches_ok,
            stats.batches_skipped,
            stats.batches_total,
        )
        if not merged and batches_skipped > 0:
            raise RpcTransportTimeout(
                "zmq rpc timeout: get_ticks (all %d batches skipped)" % batches_skipped
            )
        if batches_skipped > 0:
            logger.warning(
                "bigqmt poll partial: %d batch(es) skipped after retry; sink receives partial coverage",
                batches_skipped,
            )
        return merged

    def _fetch_with_retry(
        self,
        batch: Sequence[str],
        *,
        label: str,
        raise_after_retry: bool = False,
    ) -> tuple[Mapping[str, Any], bool]:
        """Fetch one batch with a single retry on transport timeout.

        The full-market poll splits into ~14 batches; letting one failed batch
        sink the round discards every batch already fetched. Retry the batch
        once and skip it if it still fails, so the sink receives partial
        coverage instead of nothing. A single-batch poll keeps the original
        raise-on-failure behaviour so the runner records the failure.
        """
        last_exc: BaseException | None = None
        for attempt in (1, 2):
            try:
                ticks = self.rpc_client.get_ticks(
                    list(batch),
                    timeout_seconds=self.rpc_timeout_seconds,
                )
                return ticks, False
            except RpcTransportTimeout as exc:
                last_exc = exc
                logger.warning("bigqmt get_ticks batch %s timed out (attempt %d/2)", label, attempt)
            except Exception as exc:
                last_exc = exc
                logger.exception("bigqmt get_ticks batch %s failed (attempt %d/2)", label, attempt)
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
            logger.info("Big QMT RPC symbols updated: +%d -%d total=%d", len(added), len(removed), len(self.symbols))
        return SymbolDelta(added=added, removed=removed)

    def stop(self) -> None:
        if self.rpc_client is not None:
            try:
                self.rpc_client.close()
            except Exception as exc:
                logger.warning("Big QMT RPC client close failed: %s", exc)
        self.started = False
        logger.info("Big QMT RPC tick source stopped")
