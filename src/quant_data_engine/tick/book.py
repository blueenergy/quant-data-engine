"""Order-book helpers for the tick feature builder.

Pure functions: they take the five-level arrays QMT returns with each tick
snapshot and return plain numbers (or ``None`` when the book is missing).

The formulas are the same ones ``quant_data_engine.auction.core`` uses for
the 09:15-09:30 window, so the auction features and the intraday features
report the same book metrics on one definition.
"""
from __future__ import annotations

from typing import Any, Sequence

BOOK_LEVELS = 5


def num(value: Any) -> float | None:
    """Coerce to a finite float, or ``None``."""
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number != number or number in (float("inf"), float("-inf")):
        return None
    return number


def levels(values: Sequence[Any] | None, count: int = BOOK_LEVELS) -> list[float]:
    """Numeric book levels (max ``count``), unusable entries dropped."""
    out: list[float] = []
    for value in (values or [])[:count]:
        number = num(value)
        if number is not None:
            out.append(number)
    return out


def order_imbalance(
    bid_volume: Sequence[Any] | None,
    ask_volume: Sequence[Any] | None,
    count: int = BOOK_LEVELS,
) -> float | None:
    """``(bid - ask) / (bid + ask)`` over the first ``count`` levels.

    Positive means more size resting on the bid side of the book.
    """
    bid = sum(levels(bid_volume, count))
    ask = sum(levels(ask_volume, count))
    if bid + ask <= 0:
        return None
    return (bid - ask) / (bid + ask)


def relative_spread(
    bid_price: Sequence[Any] | None,
    ask_price: Sequence[Any] | None,
) -> float | None:
    """``(ask1 - bid1) / mid`` — the relative bid-ask spread."""
    bids = levels(bid_price, 1)
    asks = levels(ask_price, 1)
    if not bids or not asks:
        return None
    bid, ask = bids[0], asks[0]
    if bid <= 0 or ask <= 0:
        return None
    mid = (bid + ask) / 2.0
    if mid <= 0:
        return None
    return (ask - bid) / mid


def micro_price(
    bid_price: Sequence[Any] | None,
    ask_price: Sequence[Any] | None,
    bid_volume: Sequence[Any] | None,
    ask_volume: Sequence[Any] | None,
) -> float | None:
    """Size-weighted microprice ``(bid1*askVol1 + ask1*bidVol1) / (askVol1 + bidVol1)``."""
    bids = levels(bid_price, 1)
    asks = levels(ask_price, 1)
    bid_vols = levels(bid_volume, 1)
    ask_vols = levels(ask_volume, 1)
    if not (bids and asks and bid_vols and ask_vols):
        return None
    total = ask_vols[0] + bid_vols[0]
    if total <= 0:
        return None
    return (bids[0] * ask_vols[0] + asks[0] * bid_vols[0]) / total


def micro_deviation(
    bid_price: Sequence[Any] | None,
    ask_price: Sequence[Any] | None,
    bid_volume: Sequence[Any] | None,
    ask_volume: Sequence[Any] | None,
    last: Any,
) -> float | None:
    """``microprice / last - 1`` — signed book pressure in units of the last price."""
    price = micro_price(bid_price, ask_price, bid_volume, ask_volume)
    reference = num(last)
    if price is None or reference is None or reference <= 0:
        return None
    return price / reference - 1.0


def mid_price(
    bid_price: Sequence[Any] | None,
    ask_price: Sequence[Any] | None,
) -> float | None:
    """Mid of the best bid and best ask."""
    bids = levels(bid_price, 1)
    asks = levels(ask_price, 1)
    if not bids or not asks or bids[0] <= 0 or asks[0] <= 0:
        return None
    return (bids[0] + asks[0]) / 2.0
