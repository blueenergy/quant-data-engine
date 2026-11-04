"""Pure call-auction snapshot/feature logic.

Extracted from ``jupiter_script/learning/auction_backfill.py`` so the 09:15
live capture and the 21:30 post-close backfill cannot drift.  Everything here
is side-effect free: no Mongo, no RPC, no clock.
"""
from __future__ import annotations

from datetime import datetime
import math
from typing import Any, Iterable, Mapping, Sequence
from zoneinfo import ZoneInfo

import numpy as np

CN_TZ = ZoneInfo("Asia/Shanghai")
FEATURE_VERSION = "auction_v1"

PHASE_0915_0920 = "0915_0920"
PHASE_0920_0925 = "0920_0925"
PHASE_MATCH_0925 = "match_0925"
PHASE_0925_0930 = "0925_0930"
PHASES = (PHASE_0915_0920, PHASE_0920_0925, PHASE_MATCH_0925, PHASE_0925_0930)


def num(value: Any) -> float | None:
    """Finite float or ``None``; mirrors the backfill's tolerance."""
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number):
        return None
    return number


def hhmmss(stime: Any) -> int | None:
    """Return ``HHMMSS`` from ``YYYYMMDDHHMMSS[.mmm]`` (or epoch digits)."""
    digits = "".join(ch for ch in str(stime) if ch.isdigit())
    if len(digits) < 14:
        return None
    try:
        return int(digits[8:14])
    except ValueError:
        return None


def phase_of(value: int | None) -> str | None:
    if value is None:
        return None
    if 91500 <= value < 92000:
        return PHASE_0915_0920
    if 92000 <= value < 92500:
        return PHASE_0920_0925
    # QMT's first post-match snapshot is 09:25:01; keep it separate from the
    # 09:25-09:30 pre-open book.
    if 92500 <= value < 92600:
        return PHASE_MATCH_0925
    if 92600 <= value <= 93000:
        return PHASE_0925_0930
    return None


def ref_price(row: Mapping[str, Any]) -> float | None:
    """Auction reference price: ``lastPrice`` is 0 during the call auction."""
    last = row.get("last_price")
    if last is not None and last > 0:
        return last
    bid = row.get("bid_price") or []
    ask = row.get("ask_price") or []
    b1 = bid[0] if len(bid) > 0 else None
    a1 = ask[0] if len(ask) > 0 else None
    if b1 is not None and a1 is not None and b1 > 0 and a1 > 0:
        return (b1 + a1) / 2.0
    if b1 is not None and b1 > 0:
        return b1
    if a1 is not None and a1 > 0:
        return a1
    return None


def _parse_stime_ms(text: str) -> int | None:
    digits = "".join(ch for ch in str(text) if ch.isdigit())
    if len(digits) < 14:
        return None
    try:
        dt = datetime.strptime(digits[:14], "%Y%m%d%H%M%S").replace(tzinfo=CN_TZ)
    except ValueError:
        return None
    return int(dt.timestamp() * 1000)


def tick_ts_ms(tick: Mapping[str, Any]) -> int | None:
    """Best-effort tick timestamp in epoch milliseconds.

    Big QMT ``get_full_tick`` returns ``time`` as epoch ms and ``stime`` as
    ``None``; historical ``get_market_data_ex`` returns ``stime`` as
    ``YYYYMMDDHHMMSS.mmm``.  Support both.
    """
    raw = tick.get("time")
    if isinstance(raw, str):
        text = raw.strip()
        if len(text) >= 14 and text[:14].isdigit():
            return _parse_stime_ms(text[:14])
    number = num(raw)
    if number is not None:
        if number >= 1e12:
            return int(number)
        if number >= 1e9:
            return int(number * 1000)
    return _parse_stime_ms(str(tick.get("stime") or ""))


def ts_from_ms(ts_ms: int) -> str:
    """Format an epoch-ms timestamp exactly like the backfill's tick ``ts``."""
    dt = datetime.fromtimestamp(int(ts_ms) / 1000.0, CN_TZ)
    return dt.strftime("%Y%m%d%H%M%S") + ".%03d" % (int(ts_ms) % 1000)


def _as_list(value: Any) -> list[Any]:
    if isinstance(value, (list, tuple)):
        return list(value)
    return []


def snapshot_from_tick(
    tick: Mapping[str, Any],
    symbol: str,
    trade_date: str,
    *,
    ts: str | None = None,
    ts_ms: int | None = None,
    phase: str | None = None,
    source: str = "qmt_tick_live",
    prev_volume: float | None = None,
    prev_amount: float | None = None,
) -> dict[str, Any] | None:
    """Convert one live ``get_ticks`` tick into an ``auction_snapshots`` row.

    ``volume``/``amount`` from ``get_full_tick`` are day-cumulative, so the
    row is marked cumulative and ``*_delta`` is derived from the previous
    poll (in-memory).  Returns ``None`` outside the auction window.
    """
    ts_ms = ts_ms if ts_ms is not None else tick_ts_ms(tick)
    if ts_ms is None:
        return None
    ts = ts or ts_from_ms(ts_ms)
    if ts[:8] != str(trade_date):
        return None
    phase = phase or phase_of(hhmmss(ts))
    if phase is None:
        return None

    last_price = num(tick.get("lastPrice", tick.get("last_price")))
    bid_price = [num(x) for x in _as_list(tick.get("bidPrice", tick.get("bid_price")))[:5]]
    ask_price = [num(x) for x in _as_list(tick.get("askPrice", tick.get("ask_price")))[:5]]
    bid_volume = [num(x) for x in _as_list(tick.get("bidVol", tick.get("bid_volume")))[:5]]
    ask_volume = [num(x) for x in _as_list(tick.get("askVol", tick.get("ask_volume")))[:5]]

    transaction_num = num(tick.get("transactionNum", tick.get("transaction_num")))
    stock_status = num(tick.get("stockStatus", tick.get("stock_status")))

    row: dict[str, Any] = {
        "symbol": symbol,
        "trade_date": str(trade_date),
        "ts": ts,
        "ts_ms": int(ts_ms),
        "phase": phase,
        "last_price": last_price,
        "pvolume": num(tick.get("pvolume")),
        "transaction_num": int(transaction_num) if transaction_num is not None else None,
        "ask_price": ask_price,
        "bid_price": bid_price,
        "ask_volume": ask_volume,
        "bid_volume": bid_volume,
        "stock_status": int(stock_status) if stock_status is not None else None,
    }
    row["ref_price"] = ref_price(row)

    volume = num(tick.get("volume")) or 0.0
    amount = num(tick.get("amount")) or 0.0
    row["volume"] = volume
    row["volume_cum"] = volume
    row["volume_delta"] = (
        volume if prev_volume is None or volume < prev_volume else max(0.0, volume - prev_volume)
    )
    row["amount_cum"] = amount
    row["amount_delta"] = (
        amount if prev_amount is None or amount < prev_amount else max(0.0, amount - prev_amount)
    )
    row["volume_is_cumulative"] = True
    row["amount_is_cumulative"] = True
    row["source"] = source
    row["ingested_at"] = datetime.now()
    return row


def _oi(bid_vol: Sequence[Any], ask_vol: Sequence[Any]) -> tuple[float | None, float, float]:
    b = sum(v for v in (num(x) for x in (bid_vol or [])[:5]) if v is not None)
    a = sum(v for v in (num(x) for x in (ask_vol or [])[:5]) if v is not None)
    if b + a <= 0:
        return None, b, a
    return (b - a) / (b + a), b, a


def _micro_dev(
    ask: Sequence[Any],
    bid: Sequence[Any],
    ask_vol: Sequence[Any],
    bid_vol: Sequence[Any],
    last: float | None,
) -> float | None:
    a1 = num(ask[0]) if len(ask) > 0 else None
    b1 = num(bid[0]) if len(bid) > 0 else None
    av1 = num(ask_vol[0]) if len(ask_vol) > 0 else None
    bv1 = num(bid_vol[0]) if len(bid_vol) > 0 else None
    if (
        a1 is None or b1 is None or av1 is None or bv1 is None
        or av1 + bv1 <= 0 or last is None or last <= 0
    ):
        return None
    micro = (b1 * av1 + a1 * bv1) / (av1 + bv1)
    return micro / last - 1.0


def _spread(ask: Sequence[Any], bid: Sequence[Any]) -> float | None:
    a1 = num(ask[0]) if len(ask) > 0 else None
    b1 = num(bid[0]) if len(bid) > 0 else None
    if a1 is None or b1 is None or a1 <= 0 or b1 <= 0:
        return None
    mid = (a1 + b1) / 2.0
    return (a1 - b1) / mid if mid > 0 else None


def summarize_phase(rows: Iterable[Mapping[str, Any]], phase: str) -> dict[str, Any]:
    subset = [r for r in rows if r["phase"] == phase]
    if not subset:
        return {
            "volume": None, "amount": None, "oi1": None, "oi5": None,
            "spread": None, "micro": None, "ticks": 0, "missing": 0,
        }
    oi1 = []
    oi5 = []
    spreads = []
    micros = []
    missing = 0
    for row in subset:
        one, _, _ = _oi(row["bid_volume"][:1], row["ask_volume"][:1])
        five, _, _ = _oi(row["bid_volume"], row["ask_volume"])
        if one is None or five is None:
            missing += 1
        else:
            oi1.append(one)
            oi5.append(five)
        spread = _spread(row["ask_price"], row["bid_price"])
        if spread is not None:
            spreads.append(spread)
        micro = _micro_dev(
            row["ask_price"], row["bid_price"],
            row["ask_volume"], row["bid_volume"], row["ref_price"],
        )
        if micro is not None:
            micros.append(micro)
    return {
        "volume": float(sum(r["volume_delta"] for r in subset)),
        "amount": float(sum(r["amount_delta"] for r in subset)),
        "oi1": float(np.mean(oi1)) if oi1 else None,
        "oi5": float(np.mean(oi5)) if oi5 else None,
        "spread": float(np.mean(spreads)) if spreads else None,
        "micro": float(np.mean(micros)) if micros else None,
        "ticks": len(subset),
        "missing": missing,
    }


def build_features(
    rows: Sequence[Mapping[str, Any]],
    symbol: str,
    date: str,
    prev_close: float | None,
    avg_1m_amount: float | None,
    *,
    source: str = "qmt_tick_backfill",
    partial: bool | None = None,
) -> dict[str, Any] | None:
    """Build the ``auction_v1`` feature row (same fields as the backfill)."""
    if not rows:
        return None
    phases = {p: summarize_phase(rows, p) for p in PHASES}
    pre = [
        r for r in rows
        if r["phase"] in (PHASE_0915_0920, PHASE_0920_0925, PHASE_MATCH_0925)
    ]
    if not pre:
        return None
    first = pre[0]
    last = pre[-1]
    highs = [r["ref_price"] for r in pre if r["ref_price"] is not None]
    lows = [r["ref_price"] for r in pre if r["ref_price"] is not None]
    high = max(highs) if highs else None
    low = min(lows) if lows else None
    p1 = [r for r in rows if r["phase"] == PHASE_0915_0920]
    p2 = [r for r in rows if r["phase"] == PHASE_0920_0925]
    pm = [r for r in rows if r["phase"] == PHASE_MATCH_0925]
    price_0915 = (p1[0]["ref_price"] if p1 else first["ref_price"])
    price_0920 = (p2[-1]["ref_price"] if p2 else (p1[-1]["ref_price"] if p1 else price_0915))
    price_0925 = (pm[0]["ref_price"] if pm else last["ref_price"])
    if price_0925 is None:
        price_0925 = last["ref_price"]
    auction_amount = sum(
        (phases[p]["amount"] or 0.0)
        for p in (PHASE_0915_0920, PHASE_0920_0925, PHASE_MATCH_0925)
    )
    ratio = (
        auction_amount / (10.0 * avg_1m_amount)
        if avg_1m_amount and avg_1m_amount > 0
        else None
    )
    missing = sum(p.get("missing", 0) for p in phases.values() if p)
    total_ticks = sum(p.get("ticks", 0) for p in phases.values() if p)

    def pct(a: float | None, b: float | None) -> float | None:
        if a is None or b is None or b == 0:
            return None
        return (a / b - 1.0) * 100.0

    feature = {
        "symbol": symbol,
        "trade_date": date,
        "prev_close": prev_close,
        "open_price": price_0925,
        "gap_pct": pct(price_0925, prev_close),
        "price_0915": price_0915,
        "price_0920": price_0920,
        "price_0925": price_0925,
        "auction_high": high,
        "auction_low": low,
        "price_slope_pct": pct(price_0925, price_0915),
        "fade_from_high_pct": pct(price_0925, high),
        "reclaim_from_low_pct": pct(price_0925, low),
        "volume_0915_0920": phases[PHASE_0915_0920]["volume"] or 0.0,
        "volume_0920_0925": phases[PHASE_0920_0925]["volume"] or 0.0,
        "volume_match_0925": phases[PHASE_MATCH_0925]["volume"] or 0.0,
        "volume_0925_0930": phases[PHASE_0925_0930]["volume"] or 0.0,
        "amount_0915_0920": phases[PHASE_0915_0920]["amount"] or 0.0,
        "amount_0920_0925": phases[PHASE_0920_0925]["amount"] or 0.0,
        "amount_match_0925": phases[PHASE_MATCH_0925]["amount"] or 0.0,
        "amount_0925_0930": phases[PHASE_0925_0930]["amount"] or 0.0,
        "auction_volume": float(
            sum(
                (phases[p]["volume"] or 0.0)
                for p in (PHASE_0915_0920, PHASE_0920_0925, PHASE_MATCH_0925)
            )
        ),
        "auction_amount": float(auction_amount),
        "auction_amount_ratio_20d": ratio,
        "oi1_0915_0920": phases[PHASE_0915_0920]["oi1"],
        "oi5_0915_0920": phases[PHASE_0915_0920]["oi5"],
        "oi1_0920_0925": phases[PHASE_0920_0925]["oi1"],
        "oi5_0920_0925": phases[PHASE_0920_0925]["oi5"],
        "oi1_match_0925": phases[PHASE_MATCH_0925]["oi1"],
        "oi5_match_0925": phases[PHASE_MATCH_0925]["oi5"],
        "oi5_0925_0930": phases[PHASE_0925_0930]["oi5"],
        "spread_mean_0920_0925": phases[PHASE_0920_0925]["spread"],
        "micro_dev_mean_0920_0925": phases[PHASE_0920_0925]["micro"],
        "tick_count_0915_0920": phases[PHASE_0915_0920]["ticks"],
        "tick_count_0920_0925": phases[PHASE_0920_0925]["ticks"],
        "tick_count_match_0925": phases[PHASE_MATCH_0925]["ticks"],
        "tick_count_0925_0930": phases[PHASE_0925_0930]["ticks"],
        "missing_book_ratio": (missing / total_ticks) if total_ticks else None,
        "volume_is_cumulative": bool(rows[0].get("volume_is_cumulative")),
        "amount_is_cumulative": bool(rows[0].get("amount_is_cumulative")),
        "is_auction_complete": bool(price_0925 is not None and len(pm) > 0),
        "feature_version": FEATURE_VERSION,
        "source": source,
        "computed_at": datetime.now(),
    }
    if partial is not None:
        feature["partial"] = bool(partial)
    return feature
