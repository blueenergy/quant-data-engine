"""Normalize Big QMT tick snapshots to miniQMT-compatible shapes."""
from __future__ import annotations

from datetime import datetime
from typing import Any, Mapping, MutableMapping
from zoneinfo import ZoneInfo

CN_TZ = ZoneInfo("Asia/Shanghai")


def _num(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_stime_to_epoch_ms(stime: Any) -> int | None:
    text = str(stime or "").strip()
    if not text:
        return None
    if len(text) == 14 and text.isdigit():
        try:
            dt = datetime.strptime(text, "%Y%m%d%H%M%S").replace(tzinfo=CN_TZ)
            return int(dt.timestamp() * 1000)
        except ValueError:
            return None
    if text.isdigit():
        n = int(text)
        if n >= 100000000000:
            return n
        if n >= 10000000000:
            return n * 1000
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S"):
        try:
            dt = datetime.strptime(text, fmt).replace(tzinfo=CN_TZ)
            return int(dt.timestamp() * 1000)
        except ValueError:
            continue
    return None


def normalize_bigqmt_tick(tick: Mapping[str, Any]) -> dict[str, Any]:
    """Return a shallow copy with ``time`` populated when only ``stime`` exists."""
    out: MutableMapping[str, Any] = dict(tick)
    if _num(out.get("time")) is None and out.get("stime") not in (None, ""):
        parsed = _parse_stime_to_epoch_ms(out.get("stime"))
        if parsed is not None:
            out["time"] = parsed
    return dict(out)


def normalize_bigqmt_ticks(ticks: Mapping[str, Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
    return {symbol: normalize_bigqmt_tick(tick) for symbol, tick in (ticks or {}).items()}
