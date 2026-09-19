"""Convert Big QMT ``get_market_data_ex`` 1m frames into ``minute_bars`` rows.

QMT native 1m timestamps are stored as-is (usually bar *start*, e.g. 09:30).
Do not apply the tick-aggregator close-of-minute shift here.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Iterable, Mapping, Optional, Sequence
from zoneinfo import ZoneInfo

from .price_precision import round_price
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


def _ohlcv(row: Mapping[str, Any], symbol: object) -> Optional[dict[str, float]]:
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
        "open": round_price(open_, symbol),
        "high": round_price(high, symbol),
        "low": round_price(low, symbol),
        "close": round_price(close, symbol),
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
            ohlcv = _ohlcv(row, ts_code)
            if not trade_date or ohlcv is None:
                continue
            key = (ts_code, trade_date)
            if key in seen:
                continue
            seen.add(key)
            records.append({"symbol": ts_code, "trade_date": trade_date, **ohlcv})
    return records


def is_filler_bar(row: Mapping[str, Any]) -> bool:
    """Whether one row is a QMT placeholder bar: four prices equal, no volume.

    QMT returns these for symbols with no local 1m cache, **even with
    ``fill_data=False``** — the flag does not suppress them, so a non-empty
    reply is not evidence of data.

    **Never filter individual bars with this.** A real day carries one stray
    flat bar at 14:59 (a bridge artifact), so per-bar filtering would shrink
    241-bar days to 240 and silently break the ``close`` horizon, which
    consumers read as the day's last bar. Classify whole days via
    :func:`split_real_days` instead.

    Limit-up/limit-down (一字板) rows are flat too but carry volume, so they
    are not misread as filler.

    Total function: anything unparseable is *not* filler, matching the Mongo
    ``is_flat`` predicate in ``watchlist_minute_backfill.verify_minute_bars``,
    which likewise evaluates to false rather than raising.
    """
    try:
        open_ = float(row["open"])
        high = float(row["high"])
        low = float(row["low"])
        close = float(row["close"])
        volume = float(row.get("volume") or 0)
    except (KeyError, TypeError, ValueError):
        return False
    if open_ != high or high != low or low != close:
        return False
    return volume == 0


def split_real_days(
    records: Sequence[Mapping[str, Any]],
) -> tuple[list[Mapping[str, Any]], list[str]]:
    """Split rows into ``(writable real rows, days whose every bar is filler)``.

    Grouped by ``(symbol, day)``, not by day alone: the live sink hands the
    whole poll batch (up to ``poll_batch_size`` symbols) to one call, so a
    day-only grouping would let one symbol's all-filler day through whenever
    any *other* symbol in the batch traded. Day-level rather than bar-level
    either way — see :func:`is_filler_bar`: a day is filler only when *all* of
    its bars are, so real days keep their stray 14:59 bar and their 241-bar
    shape.

    ``filler_days`` is the sorted, de-duplicated set of such days. Empty input
    yields ``([], [])``.
    """
    by_symbol_day: dict[tuple[str, str], list[Mapping[str, Any]]] = {}
    for row in records:
        day = str(row.get("trade_date") or "")[:8]
        if day:
            key = (str(row.get("symbol") or ""), day)
            by_symbol_day.setdefault(key, []).append(row)
    real: list[Mapping[str, Any]] = []
    filler_days: set[str] = set()
    for (_symbol, day), rows in by_symbol_day.items():
        if all(is_filler_bar(row) for row in rows):
            filler_days.add(day)
        else:
            real.extend(rows)
    return real, sorted(filler_days)
