"""Build the in-progress ("forming") daily bar for global markets.

Why this exists
---------------
``global_market_daily`` only ever holds **closed** sessions: it is written by
``run_global_market_daily_sync`` at 06:30 Beijing time, i.e. after the US close.
While a market is open the K-line chart therefore stops at the previous close
even though the market has moved a lot (the brief card shows the live move,
because it reads ``global_market_quotes``).

Yahoo's spark endpoint — already polled every cycle by
``run_global_market_sync`` — carries the live price plus the session's
high / low / volume, but neither the session's opening print nor its date
label.  Both come from Yahoo's chart endpoint (``interval=1d``), which returns
the current daily bar including the open.  That endpoint takes one symbol per
request, so it is only called when a symbol actually starts a new session
(see :func:`session_bar_is_stale`).

Date labelling
--------------
Yahoo labels a daily bar by its own timestamp, and yfinance (which writes
``global_market_daily``) derives the date from that same timestamp converted
into the exchange timezone.  We use exactly the same rule —
``date = ts + gmtoffset`` — so the forming bar always carries the date the
06:30 sync will later write for that session.  Verified symbol by symbol
against the stored series (US indices, ES/GC/NQ futures, BTC, HSI, N225, SSE).
"""
from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

# Yahoo sometimes keeps the previous session in ``currentTradingPeriod`` until
# the next one opens; the "regular" window is the one we care about.
_RETRIEVED_STALE_FLOOR_SECONDS = 900


def _num(value: Any) -> Optional[float]:
    """Coerce to a finite float, or ``None``."""
    if value is None:
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    return f if math.isfinite(f) else None


def exchange_local_date(ts: Any, gmtoffset: Any) -> Optional[str]:
    """Yahoo's own daily-bar date label: the timestamp shifted into the exchange timezone.

    ``gmtoffset`` is Yahoo's exchange offset in seconds (e.g. -18000 for EST).
    """
    if ts is None:
        return None
    try:
        offset = int(gmtoffset or 0)
        moment = datetime.fromtimestamp(int(ts), tz=timezone.utc) + timedelta(seconds=offset)
    except (TypeError, ValueError, OSError, OverflowError):
        return None
    return moment.strftime("%Y%m%d")


def session_state(meta: Dict[str, Any]) -> Dict[str, Any]:
    """Describe the current trading session from a Yahoo spark ``meta`` block.

    The market counts as *in session* when the last trade happened after the
    regular window opened and that window has not ended yet.  Both halves
    matter: before the open the last trade still belongs to the previous
    session, and after the close the session is final (not "forming").
    """
    meta = meta or {}
    gmtoffset = int(_num(meta.get("gmtoffset")) or 0)
    market_time = _num(meta.get("regularMarketTime"))
    market_time = int(market_time) if market_time is not None else None

    period = ((meta.get("currentTradingPeriod") or {}).get("regular") or {})
    start = _num(period.get("start"))
    end = _num(period.get("end"))
    start = int(start) if start is not None else None
    end = int(end) if end is not None else None

    in_session = bool(
        market_time is not None
        and start is not None
        and end is not None
        and start <= market_time < end
    )

    return {
        "in_session": in_session,
        "session_start": start,
        "session_end": end,
        "market_time": market_time,
        "gmtoffset": gmtoffset,
        # Exchange-local date of the last trade — the session we are in.
        "local_date": exchange_local_date(market_time, gmtoffset),
    }


def parse_chart_daily(result: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Pull the newest usable daily bar out of a Yahoo chart v8 result.

    The newest bar is skipped while Yahoo has not published its open yet (the
    bar is still being formed and every OHLC field can be null); the previous
    bar is returned in that case, which keeps the cache pointing at a real
    session instead of a half-empty one.
    """
    if not result:
        return None
    meta = result.get("meta") or {}
    gmtoffset = int(_num(meta.get("gmtoffset")) or 0)

    timestamps = result.get("timestamp") or []
    quote = ((result.get("indicators") or {}).get("quote") or [{}])[0] or {}

    def _series(name: str) -> list:
        values = quote.get(name)
        return values if isinstance(values, list) else []

    opens, highs, lows, closes, volumes = (
        _series("open"), _series("high"), _series("low"),
        _series("close"), _series("volume"),
    )

    for idx in range(len(timestamps) - 1, -1, -1):
        def _at(series: list) -> Optional[float]:
            if idx >= len(series):
                return None
            return _num(series[idx])

        open_ = _at(opens)
        if open_ is None:
            continue
        close_ = _at(closes)
        if close_ is None:
            continue
        date = exchange_local_date(timestamps[idx], gmtoffset)
        if date is None:
            continue
        return {
            "date": date,
            "open": open_ if open_ else None,
            "high": _at(highs),
            "low": _at(lows),
            "close": close_,
            "volume": int(_at(volumes) or 0),
        }
    return None


def futures_night_bar(
    symbol: str,
    meta: Dict[str, Any],
    bars: Optional[List[tuple]],
) -> Optional[Dict[str, Any]]:
    """Build the bar for a futures night session that already belongs to tomorrow.

    A CME trading day starts at 18:00 ET the evening before, but Yahoo labels
    its daily bar by calendar date.  Between 18:00 and midnight ET the running
    session is therefore labelled with the date the daily sync has already
    stored as a *closed* session, and the two collide.

    This returns the night session's bar dated to the next trading day, built
    from the 5-minute closes spark already sends (it carries no OHLC for them,
    so high/low come from the closes).  Returns ``None`` outside that window,
    for non-futures symbols, or when there is no night data yet.
    """
    if not symbol.endswith("=F"):
        return None
    meta = meta or {}
    gmtoffset = int(_num(meta.get("gmtoffset")) or 0)
    market_time = _num(meta.get("regularMarketTime"))
    if market_time is None:
        return None
    local = datetime.fromtimestamp(int(market_time), tz=timezone.utc) + timedelta(seconds=gmtoffset)
    if local.hour < 18:
        return None

    day = local.strftime("%Y%m%d")
    closes: List[float] = []
    for ts, close in bars or []:
        value = _num(close)
        if value is None or ts is None:
            continue
        moment = datetime.fromtimestamp(int(ts), tz=timezone.utc) + timedelta(seconds=gmtoffset)
        if moment.strftime("%Y%m%d") == day and moment.hour >= 18:
            closes.append(value)
    if not closes:
        return None

    return {
        "date": (local + timedelta(days=1)).strftime("%Y%m%d"),
        "open": closes[0],
        "high": max(closes),
        "low": min(closes),
        "volume": 0,
    }


def session_bar_is_stale(
    cache: Optional[Dict[str, Any]],
    session: Dict[str, Any],
    now: float,
) -> bool:
    """Decide whether the cached session bar must be fetched again.

    A refresh is needed when the symbol entered a new session, when the
    exchange-local date rolled over, or when the cached bar is older than the
    session we are in (Yahoo had not published it yet at fetch time).  The last
    case is rate limited so a symbol Yahoo lags on does not turn into one
    request per sync cycle.
    """
    if not cache or not cache.get("date"):
        return True
    if cache.get("session_start") != session.get("session_start"):
        return True
    expected = session.get("local_date")
    if expected and cache.get("local_date") != expected:
        return True
    if expected and cache["date"] < expected:
        return (now - float(cache.get("fetched_at") or 0)) >= _RETRIEVED_STALE_FLOOR_SECONDS
    return False


def compose_forming_bar(
    meta: Dict[str, Any],
    session_bar: Dict[str, Any],
    price: Optional[float],
    prev_close: Optional[float],
) -> Dict[str, Any]:
    """Assemble the bar for the session that is still open.

    The open comes from the chart endpoint (Yahoo's own session open); the
    high / low / volume come from spark, which tracks them live; the close is
    the live price, so the forming candle shows the same move the brief card
    shows.
    """
    meta = meta or {}
    session_bar = session_bar or {}
    close = _num(price)
    if close is None:
        close = _num(session_bar.get("close"))
    high = _num(meta.get("regularMarketDayHigh"))
    low = _num(meta.get("regularMarketDayLow"))
    volume = _num(meta.get("regularMarketVolume"))
    return {
        "date": session_bar.get("date"),
        "open": _num(session_bar.get("open")),
        "high": high,
        "low": low,
        "close": close,
        "volume": int(volume) if volume is not None else 0,
        "prev_close": _num(prev_close),
    }


__all__ = [
    "compose_forming_bar",
    "exchange_local_date",
    "futures_night_bar",
    "parse_chart_daily",
    "session_bar_is_stale",
    "session_state",
]
