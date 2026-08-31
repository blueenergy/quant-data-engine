"""Shadow sink: compare coverage without writing ``volume_price``."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping, Optional, Sequence
from zoneinfo import ZoneInfo

from quant_data_engine.bridges.bigqmt.constants import SHADOW_COLLECTION
from quant_data_engine.bridges.miniqmt.sinks import RealtimeDailyKSink, SinkResult, tick_to_realtime_daily_record

CN_TZ = ZoneInfo("Asia/Shanghai")


@dataclass(frozen=True)
class ShadowSample:
    symbol: str
    bigqmt_close: float | None
    bigqmt_volume: int | None
    bigqmt_time: str | None


class ShadowRealtimeDailyKSink(RealtimeDailyKSink):
    """Write snapshot summaries to ``quant_realtime.bigqmt_shadow_snapshots`` only."""

    def __init__(
        self,
        *,
        shadow_db: Any,
        sample_size: int = 5,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.shadow_col = shadow_db[SHADOW_COLLECTION]
        self.sample_size = max(1, int(sample_size))

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
                source_mode=self.source_mode,
            )
            if record:
                records.append(record)

        expected = len(symbols)
        synced = len({record["symbol"] for record in records})
        coverage = synced / expected if expected else 0.0
        samples = [
            ShadowSample(
                symbol=record["symbol"],
                bigqmt_close=record.get("close"),
                bigqmt_volume=record.get("volume"),
                bigqmt_time=record.get("realtime_trade_time"),
            ).__dict__
            for record in records[: self.sample_size]
        ]
        doc = {
            "trade_date": trade_date,
            "snapshot_time": snapshot_time,
            "expected_symbols": expected,
            "synced_symbols": synced,
            "coverage_ratio": coverage,
            "source_mode": self.source_mode,
            "universe_type": self.universe_type,
            "task_type": self.task_type,
            "samples": samples,
            "status": "complete" if synced == expected else "partial",
        }
        self.shadow_col.insert_one(doc)

        result = SinkResult(
            records=records,
            daily_changed=0,
            latest_changed=0,
            expected_symbols=expected,
        )
        self._last_write_ts = now.timestamp()
        self.record_success(result=result, trade_date=trade_date, snapshot_time=snapshot_time)
        return result
