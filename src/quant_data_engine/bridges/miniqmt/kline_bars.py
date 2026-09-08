"""Convert Big QMT ``get_market_data_ex`` 1m frames into ``minute_bars`` rows.

QMT native 1m timestamps are stored as-is (usually bar *start*, e.g. 09:30).
Do not apply the tick-aggregator close-of-minute shift here.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Iterable, Mapping, Optional, Sequence
from zoneinfo import ZoneInfo

from .universe import normalize_symbol

CN_TZ = ZoneInfo("Asia/Shanghai")
_TIME_KEYS = ("stime", "time", "datetime", "timetag", "index")
_META_KEYS = frozenset({"columns", "__bigqmt_type__"})


def _num(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _digits(raw: Any) -> str:
    return "".join(ch for ch in str(raw or "").strip() if ch.isdigit())


def row_trade_date(row: Mapping[str, Any]) -> Optional[str]:
    """Return ``YYYYMMDDHHMM`` from a QMT 1m row, or None if unusable."""
    for key in _TIME_KEYS:
        raw = row.get(key)
        if raw in (None, ""):
            continue
        digits = _digits(raw)
        if len(digits) in (12, 14) and digits[:2] in ("19", "20"):
            return digits[:12]
        if len(digits) == 8 and digits[:2] in ("19", "20"):
            return digits + "0000"
        n = _num(raw)
        if n is None or n <= 0:
            continue
        ts = n / 1000.0 if n >= 100000000000 else n
        try:
            return datetime.fromtimestamp(ts, tz=CN_TZ).strftime("%Y%m%d%H%M")
        except (OverflowError, OSError, ValueError):
            continue
    return None


def _columnar_rows(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Zip dict-of-arrays (QMT/pandas column-oriented frames) into row dicts.

    Do not treat DataFrame ``columns`` (a list of field *names*) as a data series.
    That produced the false ``raw_rows=6`` probe (stime + OHLCV names).
    """
    list_keys = [
        key
        for key, value in payload.items()
        if isinstance(value, (list, tuple))
        and not str(key).startswith("_")
        and str(key) not in _META_KEYS
    ]
    if not list_keys:
        return []
    length = max(len(payload[key]) for key in list_keys)
    rows = []
    for index in range(length):
        rows.append(
            {key: payload[key][index] if index < len(payload[key]) else None for key in list_keys}
        )
    return rows


def _frame_rows(payload: Any) -> list[dict[str, Any]]:
    if payload is None:
        return []
    if isinstance(payload, list):
        rows = [dict(row) for row in payload if isinstance(row, Mapping)]
        if rows:
            return rows
        return []
    if not isinstance(payload, Mapping):
        return []
    if payload.get("__bigqmt_type__") == "DataFrame" or "records" in payload or "columns" in payload:
        records = payload.get("records")
        columns = payload.get("columns")
        if isinstance(records, Mapping):
            if records.get("__bigqmt_type__") == "DataFrame" or "records" in records:
                return _frame_rows(records)
            return _columnar_rows(records)
        if isinstance(records, list):
            rows: list[dict[str, Any]] = []
            for item in records:
                if isinstance(item, Mapping):
                    rows.append(dict(item))
                elif isinstance(item, (list, tuple)) and isinstance(columns, Sequence):
                    rows.append(
                        {str(columns[i]): item[i] for i in range(min(len(columns), len(item)))}
                    )
            return rows
        return []
    return _columnar_rows(payload)


def _ohlcv(row: Mapping[str, Any]) -> Optional[dict[str, float]]:
    close = _num(row.get("close"))
    if close is None or close <= 0:
        return None
    open_ = _num(row.get("open")) or close
    high = _num(row.get("high"))
    low = _num(row.get("low"))
    volume = _num(row.get("volume") if row.get("volume") not in (None, "") else row.get("vol")) or 0.0
    if high is None:
        high = max(open_, close)
    if low is None or low <= 0:
        low = min(open_, close)
    return {
        "open": round(open_, 2),
        "high": round(high, 2),
        "low": round(low, 2),
        "close": round(close, 2),
        "volume": float(volume),
    }


def market_data_to_minute_records(
    frames: Mapping[str, Any],
    symbols: Sequence[str] | None = None,
) -> list[dict[str, Any]]:
    """Flatten per-symbol ``get_market_data_ex`` frames to minute_bars records."""
    wanted: Iterable[str]
    if symbols is not None:
        wanted = list(symbols)
    else:
        wanted = list(frames or {})
    records: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for raw_symbol in wanted:
        ts_code = normalize_symbol(raw_symbol) or str(raw_symbol)
        payload = frames.get(raw_symbol)
        if payload is None and ts_code != raw_symbol:
            payload = frames.get(ts_code)
        for row in _frame_rows(payload):
            trade_date = row_trade_date(row)
            ohlcv = _ohlcv(row)
            if not trade_date or ohlcv is None:
                continue
            key = (ts_code, trade_date)
            if key in seen:
                continue
            seen.add(key)
            records.append({"symbol": ts_code, "trade_date": trade_date, **ohlcv})
    return records
