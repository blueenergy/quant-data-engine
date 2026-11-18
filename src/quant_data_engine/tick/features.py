"""Intraday minute features from QMT 3-second tick snapshots.

**This is not order flow.**  The broker account has no Level-2 permission
(``period='l2quote' / 'l2order' / 'l2transaction'`` all return ``{}``), so
there is no per-trade aggressor tag and no order/cancel stream to read.  From
3-second five-level snapshots we can only build:

* book state per minute — order imbalance (1 and 5 levels), relative spread,
  microprice deviation;
* an **inferred** buy/sell split of the traded volume, using the classic
  Lee-Ready tick rule (a trade above the mid is buyer initiated, below is
  seller initiated, a trade at the mid inherits the previous direction).

Two caveats are surfaced as columns rather than footnotes:

1. The split is **interval level**.  One 3-second snapshot can bundle many
   trades, so the whole interval's volume takes the direction of its last
   price versus the mid — not the direction of each trade.
2. The tick rule **inherits** direction when the price did not move, which
   is autocorrelated.  ``flow_uncertain_ratio`` reports the share of volume
   whose direction was inherited or guessed rather than observed, so
   research can filter on it instead of trusting a clean-looking flow number.

Units: QMT reports tick ``volume`` in lots (手, 100 shares), ``amount`` in
yuan and ``transactionNum`` as the running count of trades.

**Minute labelling**: the ``minute`` key is the **start** of the minute
(``0930`` covers 09:30:00-09:30:59), which is what ``tick_study.py`` expects
and where the auction match lands (the 09:25 snapshot to the 09:30 snapshot
interval is booked to ``0930``, the same minute Tushare's 09:30 bar holds).
The native 1m bars in ``quant_data.minute_bars`` are labelled at the **end**
of the minute, so joining the two needs ``bar_minute = minute + 1 minute`` —
the reconciliation script prints the shift it found.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Mapping, Sequence

from .book import (
    mid_price,
    micro_deviation,
    num,
    order_imbalance,
    relative_spread,
)

FEATURE_VERSION = "tick_1m_v1"
LOT_SIZE = 100  # QMT tick volume is in 手 (100 shares per lot)

TICK_FIELDS = [
    "time", "stime", "lastPrice", "volume", "amount", "transactionNum",
    "askPrice", "bidPrice", "askVol", "bidVol", "stockStatus",
]

# Kept windows as HHMMSS (inclusive).  09:15-09:30 stays in so the auction
# match volume is not silently dropped from the daily total; 09:30+ buckets
# are the continuous session.  The 15:05-15:30 after-hours fixed-price
# session is dropped on purpose.
MORNING_WINDOW = (91500, 113059)
AFTERNOON_WINDOW = (130000, 150059)

# Direction of the inferred flow.
DIRECTION_BUY = 1
DIRECTION_SELL = -1

# How the direction of an interval was decided.
SOURCE_TICK = "tick"      # last price above/below the mid
SOURCE_INHERIT = "inherit"  # price unchanged, previous direction reused
SOURCE_QUOTE = "quote"    # no previous direction, Lee-Ready quote rule
SOURCE_NONE = "none"      # no book and no previous direction: unclassified


@dataclass
class TickFrame:
    """One aligned QMT snapshot."""

    ts: str
    ts_ms: int
    minute: str
    last: float | None
    volume: float | None
    amount: float | None
    txn: float | None
    bid_price: list[float] = field(default_factory=list)
    ask_price: list[float] = field(default_factory=list)
    bid_volume: list[float] = field(default_factory=list)
    ask_volume: list[float] = field(default_factory=list)


def hhmmss(stime: Any) -> int | None:
    """``YYYYMMDDHHMMSS`` -> ``HHMMSS`` as an int."""
    digits = "".join(ch for ch in str(stime or "") if ch.isdigit())
    if len(digits) < 14:
        return None
    try:
        return int(digits[8:14])
    except ValueError:
        return None


def _in_session(value: int) -> bool:
    return (
        MORNING_WINDOW[0] <= value <= MORNING_WINDOW[1]
        or AFTERNOON_WINDOW[0] <= value <= AFTERNOON_WINDOW[1]
    )


def _minute_label(trade_date: str, value: int) -> str:
    hour = value // 10000
    minute = (value // 100) % 100
    return f"{trade_date}{hour:02d}{minute:02d}"


def _ts_ms(value: Any) -> int:
    number = num(value)
    if number is None or number <= 0:
        return 0
    if number >= 100000000000:
        return int(number)
    if number >= 10000000000:
        return int(number * 1000)
    return int(number * 1000)


def parse_frames(
    records: Mapping[str, Any],
    trade_date: str,
) -> list[TickFrame]:
    """Align the parallel QMT tick arrays into per-tick frames.

    Frames outside the kept session windows are dropped.  The result is
    sorted by timestamp so a duplicated ``stime`` cannot reorder the deltas.
    """
    if not isinstance(records, Mapping):
        return []
    stimes = records.get("stime") or []
    times = records.get("time") or []
    last = records.get("lastPrice") or []
    volume = records.get("volume") or []
    amount = records.get("amount") or []
    txn = records.get("transactionNum") or []
    asks = records.get("askPrice") or []
    bids = records.get("bidPrice") or []
    ask_vols = records.get("askVol") or []
    bid_vols = records.get("bidVol") or []

    def pick(values: Sequence[Any], index: int) -> Any:
        return values[index] if index < len(values) else None

    frames: list[TickFrame] = []
    for index, stime in enumerate(stimes):
        value = hhmmss(stime)
        if value is None or not _in_session(value):
            continue
        frames.append(
            TickFrame(
                ts=str(stime),
                ts_ms=_ts_ms(pick(times, index)),
                minute=_minute_label(trade_date, value),
                last=num(pick(last, index)),
                volume=num(pick(volume, index)),
                amount=num(pick(amount, index)),
                txn=num(pick(txn, index)),
                bid_price=list(pick(bids, index) or []) if isinstance(pick(bids, index), list) else [],
                ask_price=list(pick(asks, index) or []) if isinstance(pick(asks, index), list) else [],
                bid_volume=list(pick(bid_vols, index) or []) if isinstance(pick(bid_vols, index), list) else [],
                ask_volume=list(pick(ask_vols, index) or []) if isinstance(pick(ask_vols, index), list) else [],
            )
        )
    frames.sort(key=lambda frame: (frame.ts_ms, frame.ts))
    return frames


def _classify(
    frame: TickFrame,
    previous_direction: int | None,
) -> tuple[int | None, str]:
    """Classify the direction of the interval ending at ``frame``.

    Lee-Ready style: the last price above the mid means the trade was
    buyer initiated, below the mid seller initiated.  A trade at the mid
    inherits the previous direction (tick rule); with no previous direction
    the quote rule applies (last at or above the mid counts as a buy).
    """
    mid = mid_price(frame.bid_price, frame.ask_price)
    last = frame.last
    if mid is not None and last is not None and last > 0:
        if last > mid:
            return DIRECTION_BUY, SOURCE_TICK
        if last < mid:
            return DIRECTION_SELL, SOURCE_TICK
        if previous_direction is not None:
            return previous_direction, SOURCE_INHERIT
        return DIRECTION_BUY, SOURCE_QUOTE
    if previous_direction is not None:
        return previous_direction, SOURCE_INHERIT
    return None, SOURCE_NONE


def build_minute_features(
    records: Mapping[str, Any],
    symbol: str,
    trade_date: str,
    *,
    source: str = "qmt_tick_backfill",
) -> list[dict[str, Any]]:
    """Build one feature row per minute from a QMT tick records dict.

    The rows are keyed by ``(symbol, minute)`` where ``minute`` is
    ``YYYYMMDDHHMM``.
    """
    frames = parse_frames(records, trade_date)
    if not frames:
        return []

    buckets: dict[str, dict[str, Any]] = {}
    previous_direction: int | None = None
    previous_volume = 0.0
    previous_amount = 0.0
    previous_txn = 0.0

    for index, frame in enumerate(frames):
        bucket = buckets.get(frame.minute)
        if bucket is None:
            bucket = {
                "oi1": [],
                "oi5": [],
                "spread": [],
                "micro": [],
                "mids": [],
                "volume_sum": 0.0,
                "amount_sum": 0.0,
                "txn_sum": 0.0,
                "buy_vol": 0.0,
                "sell_vol": 0.0,
                "buy_amount": 0.0,
                "sell_amount": 0.0,
                "uncertain_vol": 0.0,
                "unclassified_vol": 0.0,
                "tick_count": 0,
                "missing_book": 0,
                "last_price": frame.last,
            }
            buckets[frame.minute] = bucket

        bucket["tick_count"] += 1
        if frame.last is not None:
            bucket["last_price"] = frame.last

        oi1 = order_imbalance(frame.bid_volume[:1], frame.ask_volume[:1], 1)
        oi5 = order_imbalance(frame.bid_volume, frame.ask_volume)
        if oi1 is None or oi5 is None:
            bucket["missing_book"] += 1
        else:
            bucket["oi1"].append(oi1)
            bucket["oi5"].append(oi5)
        spread = relative_spread(frame.bid_price, frame.ask_price)
        if spread is not None:
            bucket["spread"].append(spread)
        micro = micro_deviation(
            frame.bid_price, frame.ask_price, frame.bid_volume, frame.ask_volume,
            frame.last,
        )
        if micro is not None:
            bucket["micro"].append(micro)
        mid = mid_price(frame.bid_price, frame.ask_price)
        if mid is not None:
            bucket["mids"].append(mid)

        # Interval deltas: QMT volume/amount/transactionNum are day cumulative.
        volume = frame.volume if frame.volume is not None else previous_volume
        amount = frame.amount if frame.amount is not None else previous_amount
        txn = frame.txn if frame.txn is not None else previous_txn
        delta_volume = max(0.0, volume - previous_volume)
        delta_amount = max(0.0, amount - previous_amount)
        delta_txn = max(0.0, txn - previous_txn)
        previous_volume, previous_amount, previous_txn = volume, amount, txn

        direction, direction_source = _classify(frame, previous_direction)
        if delta_volume > 0 or delta_amount > 0 or delta_txn > 0:
            bucket["volume_sum"] += delta_volume
            bucket["amount_sum"] += delta_amount
            bucket["txn_sum"] += delta_txn
            if direction is None:
                bucket["unclassified_vol"] += delta_volume
            else:
                if direction > 0:
                    bucket["buy_vol"] += delta_volume
                    bucket["buy_amount"] += delta_amount
                else:
                    bucket["sell_vol"] += delta_volume
                    bucket["sell_amount"] += delta_amount
                if direction_source in (SOURCE_INHERIT, SOURCE_QUOTE):
                    bucket["uncertain_vol"] += delta_volume
        if direction is not None:
            previous_direction = direction

    rows: list[dict[str, Any]] = []
    computed_at = datetime.now()
    for minute in sorted(buckets):
        bucket = buckets[minute]
        tick_count = bucket["tick_count"] or 1
        volume_sum = bucket["volume_sum"]
        flow_total = bucket["buy_vol"] + bucket["sell_vol"]
        row = {
            "symbol": symbol,
            "trade_date": trade_date,
            "minute": minute,
            "tick_count": bucket["tick_count"],
            "book_missing_ratio": bucket["missing_book"] / tick_count,
            "oi1_mean": _mean(bucket["oi1"]),
            "oi5_mean": _mean(bucket["oi5"]),
            "spread_mean": _mean(bucket["spread"]),
            "micro_dev_mean": _mean(bucket["micro"]),
            "volume_sum": volume_sum,
            "amount_sum": bucket["amount_sum"],
            "txn_sum": bucket["txn_sum"],
            "buy_vol": bucket["buy_vol"],
            "sell_vol": bucket["sell_vol"],
            "buy_amount": bucket["buy_amount"],
            "sell_amount": bucket["sell_amount"],
            "net_flow_ratio": (
                (bucket["buy_vol"] - bucket["sell_vol"]) / flow_total
                if flow_total > 0
                else None
            ),
            "flow_uncertain_vol": bucket["uncertain_vol"],
            "flow_uncertain_ratio": (
                bucket["uncertain_vol"] / volume_sum if volume_sum > 0 else None
            ),
            "flow_unclassified_vol": bucket["unclassified_vol"],
            "flow_unclassified_ratio": (
                bucket["unclassified_vol"] / volume_sum if volume_sum > 0 else None
            ),
            "avg_trade_size": (
                bucket["amount_sum"] / bucket["txn_sum"] if bucket["txn_sum"] > 0 else None
            ),
            "mid_mean": _mean(bucket["mids"]),
            "last_price": bucket["last_price"],
            "vwap": (
                bucket["amount_sum"] / (volume_sum * LOT_SIZE) if volume_sum > 0 else None
            ),
            "lot_size": LOT_SIZE,
            "feature_version": FEATURE_VERSION,
            "source": source,
            "computed_at": computed_at,
        }
        rows.append(row)
    return rows


def _mean(values: Sequence[float]) -> float | None:
    if not values:
        return None
    return float(sum(values)) / len(values)
