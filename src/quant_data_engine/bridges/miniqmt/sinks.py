"""Sinks for the standalone miniQMT bridge."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping, Optional, Sequence
from zoneinfo import ZoneInfo

from quant_data_engine.utils.ingestion_job import record_ingestion_run

from .constants import SOURCE_MODE, SOURCE_TAG, TASK_TYPE
from .universe import normalize_symbol

CN_TZ = ZoneInfo("Asia/Shanghai")


def _num(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _int_num(value: Any) -> int:
    n = _num(value)
    return int(n) if n is not None else 0


def _pick(row: Mapping[str, Any], *names: str) -> Any:
    for name in names:
        if name in row and row.get(name) not in (None, ""):
            return row.get(name)
    return None


def _first_level(row: Mapping[str, Any], scalar_name: str, list_name: str) -> Any:
    scalar = row.get(scalar_name)
    if scalar not in (None, ""):
        return scalar
    values = row.get(list_name)
    if isinstance(values, (list, tuple)) and values:
        return values[0]
    return None


def _tick_time_string(tick: Mapping[str, Any]) -> Optional[str]:
    raw = _pick(tick, "time", "timetag")
    n = _num(raw)
    if n is None or n <= 0:
        return None
    ts = n / 1000.0 if n >= 100000000000 else n
    return datetime.fromtimestamp(ts, tz=CN_TZ).strftime("%Y-%m-%d %H:%M:%S")


def tick_to_realtime_daily_record(
    symbol: str,
    tick: Mapping[str, Any],
    trade_date: str,
    snapshot_time: str,
    *,
    universe_type: str,
) -> Optional[dict[str, Any]]:
    """Convert miniQMT full tick snapshot into a StockRepository daily row."""
    ts_code = normalize_symbol(symbol)
    if not ts_code:
        return None

    close = _num(_pick(tick, "lastPrice", "price", "close", "last_price"))
    if close is None or close <= 0:
        return None
    pre_close = _num(_pick(tick, "lastClose", "preClose", "pre_close", "preclose"))
    open_ = _num(_pick(tick, "open", "openPrice", "open_price")) or close
    high = _num(_pick(tick, "high", "highPrice", "high_price")) or max(open_, close)
    low = _num(_pick(tick, "low", "lowPrice", "low_price")) or min(open_, close)
    if high < max(open_, close):
        high = max(open_, close)
    if low <= 0 or low > min(open_, close):
        low = min(open_, close)

    change = close - pre_close if pre_close else None
    pct_chg = change / pre_close * 100.0 if change is not None and pre_close else None
    amount_yuan = _num(_pick(tick, "amount", "turnover", "money"))

    return {
        "symbol": ts_code,
        "ts_code": ts_code,
        "trade_date": trade_date,
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
        "pre_close": pre_close,
        "change": change,
        "pct_chg": pct_chg,
        "volume": _int_num(_pick(tick, "volume", "vol")),
        "amount": amount_yuan / 1000.0 if amount_yuan is not None else None,
        "realtime_amount_yuan": amount_yuan,
        "realtime_num": _int_num(_pick(tick, "transactionNum", "num")),
        "realtime_ask_price1": _num(_first_level(tick, "askPrice1", "askPrice")),
        "realtime_ask_volume1": _int_num(_first_level(tick, "askVol1", "askVol")),
        "realtime_bid_price1": _num(_first_level(tick, "bidPrice1", "bidPrice")),
        "realtime_bid_volume1": _int_num(_first_level(tick, "bidVol1", "bidVol")),
        "realtime_trade_time": _tick_time_string(tick),
        "realtime_snapshot_time": snapshot_time,
        "source": SOURCE_TAG,
        "source_mode": SOURCE_MODE,
        "universe_type": universe_type,
    }


@dataclass(frozen=True)
class SinkResult:
    records: list[dict[str, Any]]
    daily_changed: int
    latest_changed: int
    expected_symbols: int

    @property
    def synced_symbols(self) -> int:
        return len({record["symbol"] for record in self.records})

    @property
    def status(self) -> str:
        return "complete" if self.expected_symbols == self.synced_symbols else "partial"


class RealtimeDailyKSink:
    """Write miniQMT ticks as realtime daily-K rows."""

    def __init__(
        self,
        *,
        repo: Any,
        settings: Any = None,
        write_interval: float = 5.0,
        universe_type: str,
        record_runs: bool = True,
        record_func: Any = record_ingestion_run,
    ) -> None:
        self.repo = repo
        self.settings = settings
        self.write_interval = write_interval
        self.universe_type = universe_type
        self.record_runs = record_runs
        self.record_func = record_func
        self._last_write_ts: Optional[float] = None

    def should_write(self, now: datetime, *, force: bool = False) -> bool:
        if force or self._last_write_ts is None:
            return True
        return now.timestamp() - self._last_write_ts >= self.write_interval

    def process(
        self,
        *,
        ticks: Mapping[str, Mapping[str, Any]],
        symbols: Sequence[str],
        now: Optional[datetime] = None,
        force: bool = False,
    ) -> Optional[SinkResult]:
        now = now or datetime.now(CN_TZ)
        if now.tzinfo is None:
            now = now.replace(tzinfo=CN_TZ)
        else:
            now = now.astimezone(CN_TZ)
        if not self.should_write(now, force=force):
            return None

        trade_date = now.strftime("%Y%m%d")
        snapshot_time = now.strftime("%Y%m%d%H%M")
        records: list[dict[str, Any]] = []
        for symbol in symbols:
            tick = ticks.get(symbol)
            if not tick:
                continue
            record = tick_to_realtime_daily_record(
                symbol,
                tick,
                trade_date,
                snapshot_time,
                universe_type=self.universe_type,
            )
            if record:
                records.append(record)

        daily_changed = self.repo.upsert_daily(records) if records else 0
        latest_changed = self.repo.upsert_realtime_latest(records) if records else 0
        result = SinkResult(
            records=records,
            daily_changed=daily_changed,
            latest_changed=latest_changed,
            expected_symbols=len(symbols),
        )
        self._last_write_ts = now.timestamp()
        self.record_success(result=result, trade_date=trade_date, snapshot_time=snapshot_time)
        return result

    def record_skipped(self, *, trade_date: str, reason: str, expected_symbols: int = 0) -> None:
        if not self.record_runs or self.settings is None:
            return
        self.record_func(
            self.settings,
            TASK_TYPE,
            trade_date,
            status="skipped",
            reason=reason,
            expected_symbols=expected_symbols,
            synced_symbols=0,
            source_mode=SOURCE_MODE,
            universe_type=self.universe_type,
        )

    def record_failed(self, *, trade_date: str, error: str, expected_symbols: int) -> None:
        if not self.record_runs or self.settings is None:
            return
        self.record_func(
            self.settings,
            TASK_TYPE,
            trade_date,
            status="failed",
            error=error[:2000],
            expected_symbols=expected_symbols,
            synced_symbols=0,
            source_mode=SOURCE_MODE,
            universe_type=self.universe_type,
        )

    def record_success(self, *, result: SinkResult, trade_date: str, snapshot_time: str) -> None:
        if not self.record_runs or self.settings is None:
            return
        synced = result.synced_symbols
        coverage = synced / result.expected_symbols if result.expected_symbols else 0.0
        self.record_func(
            self.settings,
            TASK_TYPE,
            trade_date,
            status=result.status,
            records_changed=result.daily_changed,
            realtime_latest_changed=result.latest_changed,
            expected_symbols=result.expected_symbols,
            synced_symbols=synced,
            coverage_ratio=coverage,
            last_realtime_at=snapshot_time,
            source_health="healthy" if result.status == "complete" else "degraded",
            quality_passed=result.status == "complete",
            fallback_decision="targeted_only_no_eod_skip",
            source_mode=SOURCE_MODE,
            universe_type=self.universe_type,
            full_market_derivatives_enabled=False,
        )

