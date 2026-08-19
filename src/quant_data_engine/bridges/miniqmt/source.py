"""miniQMT tick source for the standalone bridge."""
from __future__ import annotations

from dataclasses import dataclass
import logging
import time
from typing import Any, Callable, Mapping, Sequence

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SymbolDelta:
    added: tuple[str, ...]
    removed: tuple[str, ...]


def _load_xtdata() -> Any:
    try:
        from xtquant import xtdata  # type: ignore
    except Exception as exc:  # pragma: no cover - xtquant is Windows/miniQMT only
        raise RuntimeError(
            "xtquant not available. Run this standalone bridge on Windows with miniQMT installed."
        ) from exc
    return xtdata


class MiniQMTTickSource:
    """Subscribe once to miniQMT full quote and poll snapshots repeatedly."""

    def __init__(
        self,
        symbols: Sequence[str],
        *,
        xtdata_module: Any = None,
        warmup_seconds: float = 0.5,
        poll_batch_size: int = 0,
        sleep_func: Callable[[float], None] = time.sleep,
    ) -> None:
        self.symbols = sorted(set(symbols))
        self.xtdata = xtdata_module
        self.warmup_seconds = warmup_seconds
        self.poll_batch_size = int(poll_batch_size or 0)
        self.sleep_func = sleep_func
        self.started = False
        self._subscription_ids: list[int] = []

    def start(self) -> None:
        if self.started:
            return
        if self.xtdata is None:
            self.xtdata = _load_xtdata()
        if self.symbols:
            self._subscribe(self.symbols)
            if self.warmup_seconds > 0:
                self.sleep_func(self.warmup_seconds)
        self.started = True
        logger.info(
            "miniQMT tick source subscribed to %d symbols (poll_batch_size=%s)",
            len(self.symbols),
            self.poll_batch_size or "all",
        )

    def poll(self) -> Mapping[str, Mapping[str, Any]]:
        if not self.started:
            self.start()
        if not self.symbols:
            return {}
        batch_size = self.poll_batch_size
        if batch_size <= 0 or batch_size >= len(self.symbols):
            ticks = self.xtdata.get_full_tick(list(self.symbols))
            return ticks or {}

        merged: dict[str, Mapping[str, Any]] = {}
        symbols = list(self.symbols)
        for index in range(0, len(symbols), batch_size):
            batch = symbols[index : index + batch_size]
            part = self.xtdata.get_full_tick(list(batch))
            if part:
                merged.update(part)
        return merged

    def update_symbols(self, new_symbols: Sequence[str]) -> SymbolDelta:
        new_set = set(new_symbols)
        old_set = set(self.symbols)
        added = tuple(sorted(new_set - old_set))
        removed = tuple(sorted(old_set - new_set))
        if not added and not removed:
            return SymbolDelta(added=(), removed=())

        if not self.started:
            self.symbols = sorted(new_set)
            return SymbolDelta(added=added, removed=removed)

        if self.xtdata is None:
            self.xtdata = _load_xtdata()
        if removed:
            self._unsubscribe_all()
            if new_set:
                self._subscribe(sorted(new_set))
        elif added:
            self._subscribe(list(added))
        self.symbols = sorted(new_set)
        logger.info("miniQMT symbols updated: +%d -%d total=%d", len(added), len(removed), len(self.symbols))
        return SymbolDelta(added=added, removed=removed)

    def stop(self) -> None:
        if self.xtdata is None:
            self.started = False
            return
        self._unsubscribe_all()
        self.started = False
        logger.info("miniQMT tick source stopped")

    def _subscribe(self, symbols: Sequence[str]) -> None:
        seq = self.xtdata.subscribe_whole_quote(list(symbols))
        if isinstance(seq, int):
            self._subscription_ids.append(seq)

    def _unsubscribe_all(self) -> None:
        for seq in list(self._subscription_ids):
            try:
                self.xtdata.unsubscribe_quote(seq)
            except Exception as exc:
                logger.warning("miniQMT unsubscribe failed for subscription %s: %s", seq, exc)
        self._subscription_ids.clear()
