"""Realtime sector strength aggregation for the market insight radar.

This module materializes compact per-sector snapshots from the full-market
realtime daily K latest table. It intentionally stores sector-level time
series instead of full-market per-symbol minute history.
"""
from __future__ import annotations

from datetime import datetime
from statistics import median
from typing import Any, Dict, Iterable, List, Optional, Sequence

from pymongo import ASCENDING, DESCENDING, MongoClient, UpdateOne

from quant_data_engine.tasks.sentiment_analyzer import _get_trading_days_lookback


def _num(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_ratio(numerator: float, denominator: float) -> Optional[float]:
    if denominator == 0:
        return None
    return numerator / denominator


def _mean(values: Iterable[Optional[float]]) -> Optional[float]:
    vals = [v for v in values if v is not None]
    if not vals:
        return None
    return sum(vals) / len(vals)


def _amount_yuan(row: Dict[str, Any]) -> float:
    raw = _num(row.get("realtime_amount_yuan"))
    if raw is not None:
        return raw
    amount_k = _num(row.get("amount"))
    return amount_k * 1000.0 if amount_k is not None else 0.0


def _bid_ask_imbalance(row: Dict[str, Any]) -> Optional[float]:
    bid = _num(row.get("realtime_bid_volume1")) or 0.0
    ask = _num(row.get("realtime_ask_volume1")) or 0.0
    total = bid + ask
    if total <= 0:
        return None
    return (bid - ask) / total


def _is_narrow_spread(row: Dict[str, Any]) -> Optional[bool]:
    close = _num(row.get("close"))
    bid = _num(row.get("realtime_bid_price1"))
    ask = _num(row.get("realtime_ask_price1"))
    if close in (None, 0) or bid is None or ask is None:
        return None
    return (ask - bid) / close <= 0.003


def _sum_prev_closes_by_symbol(
    data_db,
    trade_date: str,
    symbols: List[str],
    ma_period: int,
) -> Dict[str, Optional[float]]:
    """Sum of the last (ma_period-1) daily closes strictly before trade_date.

    Aligns with quantFinance ``get_market_spectrum_sectors`` rolling MA rule:
    for trade date T, daily ``above_ma`` uses the last ``ma_period`` closes
    including T's close from ``volume_price``. Intraday we treat today's close
    as the realtime snapshot ``P`` and must not include a same-day
    ``volume_price`` row (often stale or partial) inside that window.

    With ``S`` = sum of prior ``ma_period-1`` closes, the equivalent check is
    ``P > (S + P) / ma_period`` (same algebra as including ``P`` in the mean).
    """
    if not symbols or ma_period < 2:
        return {}
    volume_price = data_db["volume_price"]
    need_prev = ma_period - 1
    window_start = _get_trading_days_lookback(volume_price, trade_date, ma_period + 30)
    cursor = volume_price.find(
        {
            "symbol": {"$in": list(set(symbols))},
            "trade_date": {"$gte": window_start, "$lt": trade_date},
            "close": {"$exists": True, "$ne": None},
        },
        {"_id": 0, "symbol": 1, "trade_date": 1, "close": 1},
    ).sort([("symbol", ASCENDING), ("trade_date", ASCENDING)])
    closes_by_symbol: Dict[str, List[float]] = {}
    for doc in cursor:
        close = _num(doc.get("close"))
        if close is None:
            continue
        closes_by_symbol.setdefault(str(doc.get("symbol")), []).append(close)
    out: Dict[str, Optional[float]] = {}
    for symbol in set(symbols):
        seq = closes_by_symbol.get(symbol, [])
        if len(seq) < need_prev:
            out[symbol] = None
        else:
            out[symbol] = float(sum(seq[-need_prev:]))
    return out


def _member_map(data_db, symbols: List[str]) -> Dict[str, Dict[str, Any]]:
    if not symbols:
        return {}
    cursor = data_db["sw_index_member"].find(
        {"src": "SW2021", "is_new": "Y", "con_code": {"$in": symbols}},
        {
            "_id": 0,
            "con_code": 1,
            "name": 1,
            "l1_code": 1,
            "l1_name": 1,
            "l2_code": 1,
            "l2_name": 1,
            "l3_code": 1,
            "l3_name": 1,
        },
    )
    return {str(doc.get("con_code")): doc for doc in cursor if doc.get("con_code")}


def _empty_group(level: str, code: str, name: str) -> Dict[str, Any]:
    return {"level": level, "sector_code": str(code), "sector_name": str(name), "rows": []}


def _group_key(member: Dict[str, Any], level: str) -> Optional[tuple]:
    prefix = level.lower()
    code = member.get(f"{prefix}_code")
    name = member.get(f"{prefix}_name")
    if not code or not name:
        return None
    return (level, str(code), str(name))


def calculate_realtime_sector_strength(
    client: MongoClient,
    data_db_name: str,
    realtime_db_name: str,
    snapshot_time: str,
    latest_collection_name: str = "volume_price_realtime_latest",
    output_collection_name: str = "realtime_sector_strength",
    ma_period: int = 5,
    data_source: str = "tushare_rt_k_daily",
    verbose: bool = True,
    latest_rows: Optional[Sequence[Dict[str, Any]]] = None,
    sum_prev_by_symbol: Optional[Dict[str, Optional[float]]] = None,
    member_by_symbol: Optional[Dict[str, Dict[str, Any]]] = None,
    ensure_indexes: bool = True,
) -> Dict[str, Any]:
    """Aggregate L1/L2/L3 sector strength snapshots into quant_realtime.

    Per-symbol ``above_ma`` matches the daily sector spectrum definition
    (``quantFinance`` ``get_market_spectrum_sectors``): compare today's close
    to the mean of the last ``ma_period`` closes **including** today, while
    today's close is taken from the realtime snapshot (not an intraday
    ``volume_price`` row for the same ``trade_date``).
    """
    trade_date = str(snapshot_time)[:8]
    data_db = client[data_db_name]
    realtime_db = client[realtime_db_name]
    latest_col = realtime_db[latest_collection_name]
    output_col = realtime_db[output_collection_name]

    if latest_rows is None:
        latest_rows = list(latest_col.find({"trade_date": trade_date}, {"_id": 0}))
    if not latest_rows:
        return {"success": False, "error": "No realtime latest rows", "written": 0, "results": []}

    symbols = [str(row.get("symbol")) for row in latest_rows if row.get("symbol")]
    if sum_prev_by_symbol is None:
        sum_prev_by_symbol = _sum_prev_closes_by_symbol(data_db, trade_date, symbols, ma_period)
    if member_by_symbol is None:
        member_by_symbol = _member_map(data_db, symbols)

    market_positive_count = 0
    market_valid_count = 0
    groups: Dict[tuple, Dict[str, Any]] = {}
    for row in latest_rows:
        symbol = str(row.get("symbol") or "")
        if not symbol:
            continue
        pct_chg = _num(row.get("pct_chg"))
        if pct_chg is not None:
            market_valid_count += 1
            if pct_chg > 0:
                market_positive_count += 1

        member = member_by_symbol.get(symbol)
        if not member:
            continue
        close = _num(row.get("close"))
        sum_prev = sum_prev_by_symbol.get(symbol)
        if close is None or sum_prev is None:
            ma_line: Optional[float] = None
            above_ma = False
        else:
            ma_line = (sum_prev + close) / ma_period
            above_ma = close > ma_line
        enriched = {
            **row,
            "ma": ma_line,
            "above_ma": above_ma,
            "amount_yuan": _amount_yuan(row),
            "bid_ask_imbalance": _bid_ask_imbalance(row),
            "narrow_spread": _is_narrow_spread(row),
        }
        for level in ("L1", "L2", "L3"):
            key = _group_key(member, level)
            if not key:
                continue
            if key not in groups:
                groups[key] = _empty_group(*key)
            groups[key]["rows"].append(enriched)

    market_positive_ratio = _safe_ratio(market_positive_count, market_valid_count) or 0.0
    updated_at = datetime.now().isoformat()
    docs: List[Dict[str, Any]] = []
    for group in groups.values():
        rows = group["rows"]
        if not rows:
            continue
        pct_values = [_num(row.get("pct_chg")) for row in rows if _num(row.get("pct_chg")) is not None]
        positive_count = sum(1 for row in rows if (_num(row.get("pct_chg")) or 0.0) > 0)
        negative_count = sum(1 for row in rows if (_num(row.get("pct_chg")) or 0.0) < 0)
        flat_count = len(rows) - positive_count - negative_count
        above_ma_count = sum(1 for row in rows if row.get("above_ma"))
        total_amount_yuan = sum(float(row.get("amount_yuan") or 0.0) for row in rows)
        total_volume = sum(int(row.get("volume") or 0) for row in rows)
        total_num = sum(int(row.get("realtime_num") or 0) for row in rows)
        narrow_known = [row.get("narrow_spread") for row in rows if row.get("narrow_spread") is not None]
        positive_ratio = _safe_ratio(positive_count, len(rows)) or 0.0
        doc = {
            "snapshot_time": snapshot_time,
            "trade_date": trade_date,
            "level": group["level"],
            "sector_code": group["sector_code"],
            "sector_name": group["sector_name"],
            "ma_period": int(ma_period),
            "stock_count": len(rows),
            "positive_count": positive_count,
            "negative_count": negative_count,
            "flat_count": flat_count,
            "positive_ratio": positive_ratio,
            # Legacy field names kept for API compatibility; values use ma_period.
            "above_ma5_count": above_ma_count,
            "above_ma5_ratio": _safe_ratio(above_ma_count, len(rows)),
            "above_ma_count": above_ma_count,
            "above_ma_ratio": _safe_ratio(above_ma_count, len(rows)),
            "avg_pct_chg": _mean(pct_values),
            "median_pct_chg": median(pct_values) if pct_values else None,
            "total_amount_yuan": total_amount_yuan,
            "total_volume": total_volume,
            "total_num": total_num,
            "avg_trade_size_yuan": _safe_ratio(total_amount_yuan, total_num),
            "avg_bid_ask_imbalance": _mean(row.get("bid_ask_imbalance") for row in rows),
            "narrow_spread_ratio": _safe_ratio(sum(1 for v in narrow_known if v), len(narrow_known)) if narrow_known else None,
            "market_positive_ratio": market_positive_ratio,
            "relative_positive_ratio": positive_ratio - market_positive_ratio,
            "data_source": data_source,
            "updated_at": updated_at,
        }
        docs.append(doc)

    if not docs:
        return {"success": False, "error": "No sector groups", "written": 0, "results": []}

    ops = [
        UpdateOne(
            {
                "snapshot_time": d["snapshot_time"],
                "level": d["level"],
                "sector_code": d["sector_code"],
                "ma_period": d["ma_period"],
            },
            {"$set": d},
            upsert=True,
        )
        for d in docs
    ]
    try:
        result = output_col.bulk_write(ops, ordered=False)
        written = (result.upserted_count or 0) + (result.modified_count or 0)
    except TypeError:
        # mongomock can lag behind PyMongo's UpdateOne signature; keep tests
        # focused on the aggregation semantics while production uses bulk write.
        for d in docs:
            output_col.update_one(
                {
                    "snapshot_time": d["snapshot_time"],
                    "level": d["level"],
                    "sector_code": d["sector_code"],
                    "ma_period": d["ma_period"],
                },
                {"$set": d},
                upsert=True,
            )
        written = len(docs)
    if ensure_indexes:
        # Drop legacy unique key so MA5/MA30 can coexist for the same snapshot/sector.
        try:
            output_col.drop_index("snapshot_level_sector_unique")
        except Exception:
            pass
        output_col.create_index(
            [("snapshot_time", DESCENDING), ("level", ASCENDING)],
            name="snapshot_level_idx",
            background=True,
        )
        output_col.create_index(
            [("trade_date", ASCENDING), ("level", ASCENDING), ("sector_code", ASCENDING), ("snapshot_time", DESCENDING)],
            name="trade_level_sector_time_idx",
            background=True,
        )
        output_col.create_index(
            [("level", ASCENDING), ("sector_code", ASCENDING), ("snapshot_time", DESCENDING)],
            name="level_sector_time_idx",
            background=True,
        )
        output_col.create_index(
            [
                ("snapshot_time", ASCENDING),
                ("level", ASCENDING),
                ("sector_code", ASCENDING),
                ("ma_period", ASCENDING),
            ],
            name="snapshot_level_sector_ma_unique",
            unique=True,
            background=True,
        )
    if verbose:
        print(f"📡 {snapshot_time}: MA{ma_period} sector strength groups={len(docs)} written={written}")
    return {"success": True, "written": written, "results": docs}
