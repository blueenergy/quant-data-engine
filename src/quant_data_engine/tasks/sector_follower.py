"""Pure trigger logic for the sector-leader / laggard watchdog.

No Mongo, no RPC, no clock: the I/O task lives in
``run_sector_follower_watch.py``.  Keep the thresholds here so the
pre-registration and the code cannot drift silently.

v0 thresholds (pre-registered 2026-11-06, do not tune before 20 triggers /
20 trading days):

  leader:  price_0925 >= limit_price - 0.005
  laggard: gap_pct < 0
           price_0925 / price_0920 - 1 >= +1.0%
           reclaim_from_low_pct >= +1.5%
           max(oi5_0920_0925, oi5_match_0925) >= 0.50
           09:31 bar: close >= high - 0.005, close > open,
                      volume >= 0.5 * auction_volume

The watchdog is alert-only.  It never writes trade_signals or order intents.
"""
from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
import math
from typing import Any, Mapping, Optional

MIN_LIFT_PCT = 1.0
MIN_RECLAIM_PCT = 1.5
MIN_OI5 = 0.50
FIRST_MINUTE_PRICE_TOL = 0.005
FIRST_MINUTE_MIN_VOLUME_RATIO = 0.5


def num(value: Any) -> Optional[float]:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def pct(value: Any, base: Any) -> Optional[float]:
    a = num(value)
    b = num(base)
    if a is None or b is None or b == 0:
        return None
    return (a / b - 1.0) * 100.0


def limit_pct(symbol: str, name: str = "") -> float:
    text = str(name or "").upper()
    if "ST" in text:
        return 0.05
    if symbol.endswith(".BJ"):
        return 0.30
    if symbol.startswith(("300", "301", "688")):
        return 0.20
    return 0.10


def limit_price(pre_close: Any, symbol: str, name: str = "") -> Optional[float]:
    base = num(pre_close)
    if base is None or base <= 0:
        return None
    pct_value = Decimal(str(limit_pct(symbol, name)))
    return float(
        (Decimal(str(base)) * (Decimal("1") + pct_value)).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )
    )


def auction_sealed(feature: Mapping[str, Any]) -> bool:
    price = num(feature.get("price_0925"))
    limit = limit_price(feature.get("prev_close"), str(feature.get("symbol") or ""),
                        str(feature.get("name") or ""))
    return bool(price is not None and limit is not None and price >= limit - 0.005)


def last_second_lift(feature: Mapping[str, Any]) -> tuple[Optional[float], Optional[float]]:
    """Return (price_0920 -> price_0925 lift %, low -> match reclaim %)."""
    p920 = num(feature.get("price_0920"))
    p925 = num(feature.get("price_0925"))
    lift = pct(p925, p920)
    reclaim = num(feature.get("reclaim_from_low_pct"))
    return lift, reclaim


def oi5_strength(feature: Mapping[str, Any]) -> Optional[float]:
    values = [
        num(feature.get("oi5_0920_0925")),
        num(feature.get("oi5_match_0925")),
    ]
    values = [v for v in values if v is not None]
    return max(values) if values else None


def first_minute_bullish(bar: Mapping[str, Any], auction_volume: Any) -> bool:
    open_price = num(bar.get("open"))
    high = num(bar.get("high"))
    close = num(bar.get("close"))
    volume = num(bar.get("volume"))
    auction_volume = num(auction_volume)
    if open_price is None or high is None or close is None or volume is None:
        return False
    if close < high - FIRST_MINUTE_PRICE_TOL:
        return False
    if close <= open_price:
        return False
    if auction_volume and auction_volume > 0:
        if volume < FIRST_MINUTE_MIN_VOLUME_RATIO * auction_volume:
            return False
    return True


def evaluate_laggard(
    feature: Mapping[str, Any],
    bar: Mapping[str, Any],
) -> tuple[bool, dict[str, Any]]:
    """Apply the pre-registered laggard thresholds to one feature + 09:31 bar."""
    lift, reclaim = last_second_lift(feature)
    oi5 = oi5_strength(feature)
    gap = num(feature.get("gap_pct"))
    metrics = {
        "gap_pct": gap,
        "lift_pct": lift,
        "reclaim_from_low_pct": reclaim,
        "oi5": oi5,
        "oi5_0920_0925": num(feature.get("oi5_0920_0925")),
        "oi5_match_0925": num(feature.get("oi5_match_0925")),
        "auction_volume": num(feature.get("auction_volume")),
        "first_minute_open": num(bar.get("open")),
        "first_minute_high": num(bar.get("high")),
        "first_minute_low": num(bar.get("low")),
        "first_minute_close": num(bar.get("close")),
        "first_minute_volume": num(bar.get("volume")),
        "first_minute_close_at_high": bool(
            num(bar.get("close")) is not None
            and num(bar.get("high")) is not None
            and float(bar["close"]) >= float(bar["high"]) - FIRST_MINUTE_PRICE_TOL
        ),
    }
    passed = bool(
        gap is not None and gap < 0
        and lift is not None and lift >= MIN_LIFT_PCT
        and reclaim is not None and reclaim >= MIN_RECLAIM_PCT
        and oi5 is not None and oi5 >= MIN_OI5
        and first_minute_bullish(bar, feature.get("auction_volume"))
    )
    return passed, metrics


def sector_key(member: Mapping[str, Any] | None) -> tuple[Optional[str], Optional[str], str]:
    """Return (code, name, level), preferring L3 and falling back to L2."""
    if not member:
        return None, None, ""
    if member.get("l3_code"):
        return str(member["l3_code"]), member.get("l3_name"), "L3"
    if member.get("l2_code"):
        return str(member["l2_code"]), member.get("l2_name"), "L2"
    return None, None, ""


def sector_key_l2(member: Mapping[str, Any] | None) -> tuple[Optional[str], Optional[str], str]:
    """Return the L2 sector (the leader/laggard matching level).

    The 2026-11-06 东方日升/永臻 case shows why L2, not L3: both are in
    光伏设备 (L2) but in different L3 sub-sectors (光伏电池组件 vs 光伏辅材).
    Matching on L3 would miss the sector leader entirely.
    """
    if not member:
        return None, None, ""
    if member.get("l2_code"):
        return str(member["l2_code"]), member.get("l2_name"), "L2"
    if member.get("l3_code"):
        return str(member["l3_code"]), member.get("l3_name"), "L3"
    return None, None, ""


def leader_has_tracked(
    member_map: Mapping[str, Mapping[str, Any]],
    tracked_symbols: Any,
    sector_code: str,
    sector_level: str,
) -> bool:
    """True when a watchlist/holding sits in the leader's L2 (or fallback L3)."""
    for symbol in tracked_symbols:
        member = member_map.get(str(symbol)) or {}
        if sector_level == "L2" and str(member.get("l2_code") or "") == sector_code:
            return True
        if sector_level == "L3" and str(member.get("l3_code") or "") == sector_code:
            return True
    return False


def tracked_in_sector(
    member_map: Mapping[str, Mapping[str, Any]],
    tracked_symbols: Any,
    sector_code: str,
    sector_level: str,
) -> list[str]:
    out: list[str] = []
    for symbol in sorted(str(s) for s in tracked_symbols):
        member = member_map.get(symbol) or {}
        code = member.get("l2_code") if sector_level == "L2" else member.get("l3_code")
        if str(code or "") == sector_code:
            out.append(symbol)
    return out


def seal_metrics(
    daily_limit_up: Mapping[str, Any] | None,
    volume_price: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Normalize the live seal-quality fields for the track mode."""
    row = daily_limit_up or {}
    price = volume_price or {}
    seal_amount = num(row.get("seal_amount"))
    day_amount = num(price.get("amount"))
    # volume_price.amount is thousand yuan; daily_limit_up.seal_amount is yuan.
    day_amount_yuan = day_amount * 1000.0 if day_amount is not None else None
    seal_ratio = (
        seal_amount / day_amount_yuan
        if seal_amount is not None and day_amount_yuan and day_amount_yuan > 0
        else None
    )
    status = str(row.get("status") or "")
    limit_status = str(row.get("limit_up_status") or "")
    if status == "limit_up" and limit_status == "sealed":
        state = "sealed"
    elif status == "broken" or limit_status == "open":
        state = "broken"
    elif row:
        state = "unknown"
    else:
        state = "not_sealed"
    return {
        "state": state,
        "status": status or None,
        "limit_up_status": limit_status or None,
        "limit_up_time": row.get("limit_up_time"),
        "last_seal_time": row.get("last_seal_time"),
        "open_count": row.get("open_count"),
        "turnover_ratio": num(row.get("turnover_ratio")),
        "seal_amount": seal_amount,
        "day_amount": day_amount_yuan,
        "seal_ratio": seal_ratio,
        "close": num(price.get("close")),
        "pct_chg": num(price.get("pct_chg")),
    }
