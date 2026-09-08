"""Runner for the standalone miniQMT bridge."""
from __future__ import annotations

from datetime import datetime, time as dtime
import logging
import signal
import time
from typing import Any, Callable, Sequence
from zoneinfo import ZoneInfo

from .config import BridgeConfig

CN_TZ = ZoneInfo("Asia/Shanghai")
logger = logging.getLogger(__name__)


def is_cn_trading_day(
    now: datetime,
    xtdata_module: Any = None,
    *,
    trading_day_checker: Callable[[datetime], bool] | None = None,
) -> bool:
    """Check A-share trading day, preferring explicit checker, then miniQMT calendar."""
    if now.tzinfo is None:
        now = now.replace(tzinfo=CN_TZ)
    else:
        now = now.astimezone(CN_TZ)
    if now.weekday() >= 5:
        return False

    if trading_day_checker is not None:
        return trading_day_checker(now)

    get_dates = getattr(xtdata_module, "get_trading_dates", None) if xtdata_module is not None else None
    if not callable(get_dates):
        return True

    date_str = now.strftime("%Y%m%d")
    for market in ("SH", "SZ"):
        try:
            dates = get_dates(market, date_str, date_str)
            if dates:
                return True
        except Exception as exc:
            logger.debug("miniQMT trading calendar unavailable for %s: %s", market, exc)
            return True
    return False


def in_cn_stock_session(
    now: datetime,
    xtdata_module: Any = None,
    *,
    trading_day_checker: Callable[[datetime], bool] | None = None,
) -> bool:
    """A-share continuous auction window with trading-day guard."""
    if now.tzinfo is None:
        now = now.replace(tzinfo=CN_TZ)
    else:
        now = now.astimezone(CN_TZ)
    if not is_cn_trading_day(now, xtdata_module=xtdata_module, trading_day_checker=trading_day_checker):
        return False
    t = now.time()
    return dtime(9, 30) <= t <= dtime(11, 30) or dtime(13, 0) <= t <= dtime(15, 0)


class BridgeRunner:
    """Drive one miniQMT tick source and dispatch snapshots to configured sinks."""

    def __init__(
        self,
        *,
        config: BridgeConfig,
        universe: Any,
        source: Any,
        sinks: Sequence[Any],
        clock: Callable[[], datetime] | None = None,
        sleep_func: Callable[[float], None] = time.sleep,
        install_signal_handlers: bool = True,
        trading_day_checker: Callable[[datetime], bool] | None = None,
    ) -> None:
        self.config = config
        self.universe = universe
        self.source = source
        self.sinks = list(sinks)
        self.clock = clock or (lambda: datetime.now(CN_TZ))
        self.sleep_func = sleep_func
        self.trading_day_checker = trading_day_checker
        self.stop_requested = False
        self._last_refresh_ts: float | None = None
        if install_signal_handlers:
            self.install_signal_handlers()

    def install_signal_handlers(self) -> None:
        try:
            signal.signal(signal.SIGINT, self._handle_signal)
            signal.signal(signal.SIGTERM, self._handle_signal)
        except ValueError:
            logger.debug("signal handlers can only be installed from main thread")

    def _handle_signal(self, signum: int, frame: Any) -> None:
        logger.info("received signal %s, stopping miniQMT bridge", signum)
        self.stop_requested = True

    def run(self) -> None:
        if self.config.universe.is_full_market:
            # Load xtquant first so full-market universe can fall back to sector lists
            # when Mongo stock_info is empty.
            self.source.start()
            if hasattr(self.universe, "xtdata_module"):
                self.universe.xtdata_module = getattr(self.source, "xtdata", None)

        symbols = self.universe.resolve()
        if symbols:
            self.source.update_symbols(symbols)
            self.source.start()

        try:
            while not self.stop_requested:
                now = self._now_cn()
                trade_date = now.strftime("%Y%m%d")

                if not self.config.ignore_session and not in_cn_stock_session(
                    now,
                    xtdata_module=getattr(self.source, "xtdata", None),
                    trading_day_checker=self.trading_day_checker,
                ):
                    if not is_cn_trading_day(
                        now,
                        xtdata_module=getattr(self.source, "xtdata", None),
                        trading_day_checker=self.trading_day_checker,
                    ):
                        skip_reason = "non-trading day"
                        logger.info("non-trading day, skip")
                    else:
                        skip_reason = "outside CN stock session"
                        logger.info("outside CN stock session, sleep %.1fs", self.config.daily_k_interval)
                    if self.config.mode == "once":
                        for sink in self.sinks:
                            if hasattr(sink, "record_skipped"):
                                sink.record_skipped(trade_date=trade_date, reason=skip_reason, expected_symbols=0)
                        break
                    self.sleep_func(self.config.daily_k_interval)
                    continue

                symbols = self._refresh_symbols_if_needed(symbols, now)
                if not symbols:
                    logger.info("no target symbols from configured universe")
                    for sink in self.sinks:
                        if hasattr(sink, "record_skipped"):
                            sink.record_skipped(trade_date=trade_date, reason="no target symbols", expected_symbols=0)
                    if self.config.mode == "once":
                        break
                    self.sleep_func(self.config.daily_k_interval)
                    continue

                try:
                    payload = self.source.poll()
                    kind = getattr(self.source, "payload_kind", "ticks")
                    for sink in self.sinks:
                        if kind == "kline":
                            result = sink.process(
                                bars=payload,
                                symbols=symbols,
                                now=now,
                                force=self.config.mode == "once",
                            )
                        else:
                            result = sink.process(
                                ticks=payload,
                                symbols=symbols,
                                now=now,
                                force=self.config.mode == "once",
                            )
                        if result is not None:
                            logger.info(
                                "snapshot target_symbols=%d synced=%d daily_changed=%d latest_changed=%d",
                                result.expected_symbols,
                                result.synced_symbols,
                                result.daily_changed,
                                result.latest_changed,
                            )
                except Exception as exc:
                    logger.exception("miniQMT bridge iteration failed: %s", exc)
                    for sink in self.sinks:
                        if hasattr(sink, "record_failed"):
                            sink.record_failed(
                                trade_date=trade_date,
                                error=str(exc),
                                expected_symbols=len(symbols),
                            )
                    if self.config.mode == "once":
                        raise

                if self.config.mode == "once":
                    break
                self.sleep_func(self.config.poll_interval)
        finally:
            self.source.stop()

    def _refresh_symbols_if_needed(self, current_symbols: list[str], now: datetime) -> list[str]:
        if self.config.refresh <= 0:
            return current_symbols
        if not self.config.universe.auto and not self.config.universe.is_full_market:
            return current_symbols
        now_ts = now.timestamp()
        if self._last_refresh_ts is not None and now_ts - self._last_refresh_ts < self.config.refresh:
            return current_symbols
        self._last_refresh_ts = now_ts
        new_symbols = self.universe.refresh()
        if set(new_symbols) != set(current_symbols):
            delta = self.source.update_symbols(new_symbols)
            logger.info("symbol universe refreshed: +%d -%d total=%d", len(delta.added), len(delta.removed), len(new_symbols))
        return new_symbols

    def _now_cn(self) -> datetime:
        now = self.clock()
        if now.tzinfo is None:
            return now.replace(tzinfo=CN_TZ)
        return now.astimezone(CN_TZ)

