"""Tick → 1m aggregation for the standalone bridge ``minute-bars`` sink.

Mirrors ``MiniQMTRealtimeAdapter``: exchange timestamp floors to the minute,
then the bar is labeled at the **close** of that minute (09:30:xx → 09:31).
Volume is the delta of cumulative share volume after a per-symbol baseline
taken from the first tick seen after start / universe add.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, Mapping, Optional, Sequence
from zoneinfo import ZoneInfo

from quant_data_engine.bridges.miniqmt.kline_bars import (
    market_data_to_minute_records,
    split_real_days,
)
from quant_data_engine.utils.ingestion_job import record_ingestion_run

from .constants import (
    SOURCE_MODE_WATCHLIST_MINUTE,
    TASK_TYPE_WATCHLIST_MINUTE,
)
from .price_precision import round_price
from .sinks import SinkResult
from .universe import normalize_symbol

CN_TZ = ZoneInfo("Asia/Shanghai")
logger = logging.getLogger(__name__)


def _num(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _tick_dt(tick: Mapping[str, Any], now: datetime) -> datetime:
    raw = tick.get("time")
    n = _num(raw)
    if n is None or n <= 0:
        return now
    ts = n / 1000.0 if n >= 100000000000 else n
    return datetime.fromtimestamp(ts, tz=CN_TZ)


def _close_label(dt: datetime) -> str:
    floored = dt.replace(second=0, microsecond=0)
    close_dt = floored + timedelta(minutes=1)
    return close_dt.strftime("%Y-%m-%d %H:%M")


def _trade_date(label: str) -> str:
    return datetime.strptime(label, "%Y-%m-%d %H:%M").strftime("%Y%m%d%H%M")


def _price(tick: Mapping[str, Any]) -> Optional[float]:
    value = _num(tick.get("lastPrice"))
    if value is None:
        value = _num(tick.get("price"))
    if value is None:
        value = _num(tick.get("close"))
    if value is None or value <= 0:
        return None
    return value


class TickMinuteAggregator:
    """Stateful OHLCV buckets keyed by close-of-minute label + symbol."""

    def __init__(self) -> None:
        self._buckets: dict[tuple[str, str], dict[str, Any]] = {}
        self._baseline_volume: dict[str, int] = {}
        self._prev_adjusted: dict[str, int] = {}
        self._last_emitted_minute: str = ""

    def ingest(
        self,
        ticks: Mapping[str, Mapping[str, Any]],
        now: datetime,
        symbols: Sequence[str] | None = None,
    ) -> list[dict[str, Any]]:
        now = _ensure_cn(now)
        raw_symbols = list(symbols) if symbols is not None else list(ticks)
        wanted_norm = {normalize_symbol(symbol) or symbol for symbol in raw_symbols}
        self.drop_symbols(set(self._baseline_volume) - wanted_norm)
        for symbol in raw_symbols:
            tick = ticks.get(symbol)
            if not tick:
                continue
            self._process_tick(symbol, tick, now)
        current_minute = now.strftime("%Y-%m-%d %H:%M")
        closed: list[dict[str, Any]] = []
        if self._last_emitted_minute and current_minute > self._last_emitted_minute:
            closed = self._pop_label(current_minute)
        self._last_emitted_minute = current_minute
        return closed

    def snapshot_open(self) -> list[dict[str, Any]]:
        return [self._to_record(label, symbol, bucket) for (label, symbol), bucket in self._buckets.items()]

    def drop_symbols(self, removed: Sequence[str] | set[str]) -> None:
        gone = set(removed)
        if not gone:
            return
        for symbol in gone:
            self._baseline_volume.pop(symbol, None)
            self._prev_adjusted.pop(symbol, None)
        self._buckets = {
            key: bucket for key, bucket in self._buckets.items() if key[1] not in gone
        }

    def _process_tick(self, symbol: str, tick: Mapping[str, Any], now: datetime) -> None:
        price = _price(tick)
        if price is None:
            return
        ts_code = normalize_symbol(symbol) or symbol
        label = _close_label(_tick_dt(tick, now))
        cumulative = int(_num(tick.get("volume")) or 0)
        if ts_code not in self._baseline_volume:
            self._baseline_volume[ts_code] = cumulative
            self._prev_adjusted[ts_code] = 0
        adjusted = max(0, cumulative - self._baseline_volume[ts_code])
        prev = self._prev_adjusted.get(ts_code, 0)
        vol_delta = max(0, adjusted - prev)
        self._prev_adjusted[ts_code] = adjusted
        key = (label, ts_code)
        bucket = self._buckets.get(key)
        if bucket is None:
            self._buckets[key] = {
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "volume": vol_delta,
            }
            return
        bucket["high"] = max(bucket["high"], price)
        bucket["low"] = min(bucket["low"], price)
        bucket["close"] = price
        bucket["volume"] += vol_delta

    def _pop_label(self, label: str) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        for key in list(self._buckets):
            if key[0] != label:
                continue
            bucket = self._buckets.pop(key)
            records.append(self._to_record(key[0], key[1], bucket))
        return records

    @staticmethod
    def _to_record(label: str, symbol: str, bucket: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "symbol": symbol,
            "trade_date": _trade_date(label),
            "open": round_price(bucket["open"], symbol),
            "high": round_price(bucket["high"], symbol),
            "low": round_price(bucket["low"], symbol),
            "close": round_price(bucket["close"], symbol),
            "volume": float(bucket["volume"]),
        }


def _ensure_cn(now: datetime) -> datetime:
    if now.tzinfo is None:
        return now.replace(tzinfo=CN_TZ)
    return now.astimezone(CN_TZ)


class MinuteBarsSink:
    """Aggregate polled ticks into 1m bars and upsert ``quant_data.minute_bars``."""

    def __init__(
        self,
        *,
        repo: Any,
        settings: Any = None,
        write_interval: float = 5.0,
        universe_type: str,
        source_mode: str = SOURCE_MODE_WATCHLIST_MINUTE,
        task_type: str = TASK_TYPE_WATCHLIST_MINUTE,
        record_runs: bool = True,
        record_func: Any = record_ingestion_run,
        aggregator: TickMinuteAggregator | None = None,
    ) -> None:
        self.repo = repo
        self.settings = settings
        self.write_interval = write_interval
        self.universe_type = universe_type
        self.source_mode = source_mode
        self.task_type = task_type
        self.record_runs = record_runs
        self.record_func = record_func
        self.aggregator = aggregator or TickMinuteAggregator()
        self._last_write_ts: Optional[float] = None

    def should_write_forming(self, now: datetime, *, force: bool = False) -> bool:
        if force or self._last_write_ts is None:
            return True
        return now.timestamp() - self._last_write_ts >= self.write_interval

    def process(
        self,
        *,
        ticks: Mapping[str, Mapping[str, Any]] | None = None,
        bars: Mapping[str, Any] | None = None,
        symbols: Sequence[str],
        now: Optional[datetime] = None,
        force: bool = False,
    ) -> Optional[SinkResult]:
        now = _ensure_cn(now or datetime.now(CN_TZ))
        if bars is not None:
            records = market_data_to_minute_records(bars, symbols)
            if not records:
                return None
            # QMT answers a symbol with no local 1m cache with placeholder bars
            # rather than nothing, so a freshly added watchlist symbol would
            # otherwise write a whole day of flat prices over its real rows.
            # Drop days whose every bar is filler — a real day always has at
            # least one non-flat bar, so no real data can be discarded here.
            records, filler_days = split_real_days(records)
            if filler_days:
                logger.warning(
                    "minute sink dropped %d all-filler day(s): %s (no local 1m cache; "
                    "run download_history_data before trusting these symbols)",
                    len(filler_days),
                    ",".join(filler_days),
                )
            # Deliberately not `return None` when everything was filler: an
            # empty result would look exactly like "nothing polled", and the
            # sink would go quiet precisely when it is broken. Reporting an
            # empty SinkResult runs the normal path, where status becomes
            # "partial" and coverage_ratio 0.
            written = int(self.repo.upsert_bar_records(records) or 0) if records else 0
            self._last_write_ts = now.timestamp()
            result = SinkResult(
                records=records,
                daily_changed=written,
                latest_changed=0,
                expected_symbols=len(symbols),
            )
            self.record_success(
                result=result,
                trade_date=now.strftime("%Y%m%d"),
                snapshot_time=now.strftime("%Y%m%d%H%M"),
            )
            return result
        closed = self.aggregator.ingest(ticks or {}, now, symbols)
        records = list(closed)
        write_forming = self.should_write_forming(now, force=force)
        if write_forming:
            records.extend(self.aggregator.snapshot_open())
        if not records:
            return None
        written = int(self.repo.upsert_bar_records(records) or 0)
        self._last_write_ts = now.timestamp()
        result = SinkResult(
            records=records,
            daily_changed=written,
            latest_changed=0,
            expected_symbols=len(symbols),
        )
        self.record_success(result=result, trade_date=now.strftime("%Y%m%d"), snapshot_time=now.strftime("%Y%m%d%H%M"))
        return result

    def record_skipped(self, *, trade_date: str, reason: str, expected_symbols: int = 0) -> None:
        if not self.record_runs or self.settings is None:
            return
        self.record_func(
            self.settings,
            self.task_type,
            trade_date,
            status="skipped",
            reason=reason,
            expected_symbols=expected_symbols,
            synced_symbols=0,
            source_mode=self.source_mode,
            universe_type=self.universe_type,
        )

    def record_failed(self, *, trade_date: str, error: str, expected_symbols: int) -> None:
        if not self.record_runs or self.settings is None:
            return
        self.record_func(
            self.settings,
            self.task_type,
            trade_date,
            status="failed",
            error=error[:2000],
            expected_symbols=expected_symbols,
            synced_symbols=0,
            source_mode=self.source_mode,
            universe_type=self.universe_type,
        )

    def record_success(self, *, result: SinkResult, trade_date: str, snapshot_time: str) -> None:
        if not self.record_runs or self.settings is None:
            return
        synced = result.synced_symbols
        coverage = synced / result.expected_symbols if result.expected_symbols else 0.0
        self.record_func(
            self.settings,
            self.task_type,
            trade_date,
            status=result.status,
            records_changed=result.daily_changed,
            expected_symbols=result.expected_symbols,
            synced_symbols=synced,
            coverage_ratio=coverage,
            last_realtime_at=snapshot_time,
            source_health="healthy" if result.status == "complete" else "degraded",
            quality_passed=result.status == "complete",
            source_mode=self.source_mode,
            universe_type=self.universe_type,
        )
