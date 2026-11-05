"""Score yesterday's first-board pool against today's auction features.

This is the first-board consumer skeleton: it writes a ranked candidate list
to ``quant_data.first_board_candidates`` and **never submits orders**.  The
score is a transparent v0 heuristic, not a validated edge:

- 0.5 × auction-volume ratio (3% of yesterday's volume → 0.25, 12% → 1.0)
- 0.3 × gap (0..6% → 0..1)
- 0.2 × yesterday's seal amount / amount (5% → 1.0)

Run at 09:27 on trading days, after the 09:26 auction feature write::

    python -m quant_data_engine.tasks.run_first_board_candidates
    python -m quant_data_engine.tasks.run_first_board_candidates --dry-run

Probe replay before production deployment::

    python -m quant_data_engine.tasks.run_first_board_candidates \
        --trade-date 20261105 --features-collection auction_features_probe \
        --candidates-collection first_board_candidates_probe \
        --runs-collection first_board_candidate_runs_probe

QMT tick ``volume`` and Tushare ``volume_price.volume`` (``daily.vol``) are
both in lots (手), so the volume ratio compares them directly.
``volume_price.amount`` is in thousand yuan, while ``daily_limit_up.seal_amount``
is in yuan; the seal ratio multiplies the amount by 1000 first.
"""
from __future__ import annotations

import argparse
from datetime import datetime
import json
import logging
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo

from pymongo import MongoClient

from quant_data_engine.auction.universe import load_first_board_pool_with_date
from quant_data_engine.config.settings import get_settings

CST = ZoneInfo("Asia/Shanghai")
logger = logging.getLogger(__name__)

CANDIDATES = "first_board_candidates"
RUNS = "first_board_candidate_runs"
MODEL_VERSION = "first_board_v0_skeleton"
DEFAULT_PARAMS: dict[str, float] = {
    "min_gap_pct": 0.0,
    "max_gap_pct": 6.0,
    "min_volume_ratio": 0.03,
    "lot_size": 100.0,
    "seal_ratio_target": 0.05,
}


def normalize_trade_date(value: str | None) -> str:
    text = str(value or "").strip().replace("-", "")
    if len(text) != 8 or not text.isdigit():
        raise ValueError("trade date must be YYYYMMDD or YYYY-MM-DD")
    return text


def _num(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def load_auction_features(
    db: Any, symbols: Sequence[str], trade_date: str, collection: str = "auction_features"
) -> dict[str, dict[str, Any]]:
    if not symbols:
        return {}
    return {
        str(doc["symbol"]): doc
        for doc in db[collection].find(
            {"symbol": {"$in": list(symbols)}, "trade_date": trade_date}
        )
    }


def load_prev_volume_price(
    db: Any, symbols: Sequence[str], prev_date: str
) -> dict[str, dict[str, Any]]:
    if not symbols:
        return {}
    return {
        str(doc["symbol"]): doc
        for doc in db.volume_price.find(
            {"symbol": {"$in": list(symbols)}, "trade_date": prev_date}
        )
    }


def load_limit_up_docs(
    db: Any, prev_date: str, symbols: Sequence[str]
) -> dict[str, dict[str, Any]]:
    if not symbols:
        return {}
    return {
        str(doc["symbol"]): doc
        for doc in db.daily_limit_up.find(
            {"date": prev_date, "symbol": {"$in": list(symbols)}}
        )
    }


def score_candidate(
    *,
    symbol: str,
    trade_date: str,
    prev_limit_up_date: str,
    feature: Mapping[str, Any],
    prev_price: Mapping[str, Any] | None,
    limit_up: Mapping[str, Any] | None,
    params: Mapping[str, float],
) -> dict[str, Any] | None:
    """Return a candidate document, or ``None`` when the auction filter fails."""
    if feature.get("is_auction_complete") is False:
        return None
    gap = _num(feature.get("gap_pct"))
    if gap is None or not (params["min_gap_pct"] < gap <= params["max_gap_pct"]):
        return None
    auction_volume = _num(feature.get("auction_volume"))
    if auction_volume is None or auction_volume <= 0:
        return None
    prev_volume = _num((prev_price or {}).get("volume"))
    if prev_volume is None or prev_volume <= 0:
        return None

    auction_volume_shares = auction_volume * params["lot_size"]
    # Both QMT tick volume and Tushare daily.vol are lots (手).
    volume_ratio = auction_volume / prev_volume
    if volume_ratio < params["min_volume_ratio"]:
        return None

    prev_amount_k = _num((prev_price or {}).get("amount"))
    prev_amount_yuan = prev_amount_k * 1000.0 if prev_amount_k else None
    seal_amount = _num((limit_up or {}).get("seal_amount"))
    seal_ratio = (
        seal_amount / prev_amount_yuan
        if seal_amount and prev_amount_yuan
        else None
    )

    max_gap = params["max_gap_pct"]
    gap_score = min(max(gap, 0.0) / max_gap, 1.0) if max_gap > 0 else 0.0
    volume_ceiling = max(params["min_volume_ratio"] * 4.0, 1e-9)
    volume_score = min(volume_ratio / volume_ceiling, 1.0)
    seal_score = min((seal_ratio or 0.0) / params["seal_ratio_target"], 1.0)
    score = round(100.0 * (0.5 * volume_score + 0.3 * gap_score + 0.2 * seal_score), 2)

    return {
        "trade_date": trade_date,
        "symbol": symbol,
        "name": (limit_up or {}).get("name"),
        "prev_limit_up_date": prev_limit_up_date,
        "gap_pct": round(gap, 3),
        "auction_volume_lots": auction_volume,
        "auction_volume_shares": auction_volume_shares,
        "prev_volume_lots": prev_volume,
        "auction_volume_ratio": round(volume_ratio, 4),
        "volume_unit": "lots",
        "auction_amount": _num(feature.get("auction_amount")),
        "auction_amount_ratio_20d": _num(feature.get("auction_amount_ratio_20d")),
        "price_0915": _num(feature.get("price_0915")),
        "price_0920": _num(feature.get("price_0920")),
        "price_0925": _num(feature.get("price_0925")),
        "price_slope_pct": _num(feature.get("price_slope_pct")),
        "seal_amount_yuan": seal_amount,
        "prev_amount_k": prev_amount_k,
        "prev_amount_yuan": prev_amount_yuan,
        "amount_unit": "thousand_yuan",
        "seal_ratio": round(seal_ratio, 6) if seal_ratio is not None else None,
        "score": score,
        "score_parts": {
            "volume": round(volume_score, 4),
            "gap": round(gap_score, 4),
            "seal": round(seal_score, 4),
        },
        "model_version": MODEL_VERSION,
        "feature_version": feature.get("feature_version"),
        "feature_source": feature.get("source"),
        "partial": feature.get("partial"),
        "generated_at": datetime.now(),
    }


def run_scoring(
    db: Any,
    *,
    trade_date: str,
    dry_run: bool = False,
    params: Mapping[str, float] | None = None,
    features_collection: str = "auction_features",
    candidates_collection: str = CANDIDATES,
    runs_collection: str = RUNS,
    include_bj: bool = False,
) -> dict[str, Any]:
    effective = {**DEFAULT_PARAMS, **(params or {})}
    prev_date, pool = load_first_board_pool_with_date(db, trade_date)
    excluded_bj = [] if include_bj else [s for s in pool if s.endswith(".BJ")]
    if excluded_bj:
        pool = [s for s in pool if not s.endswith(".BJ")]
    summary: dict[str, Any] = {
        "trade_date": trade_date,
        "prev_limit_up_date": prev_date,
        "pool_size": len(pool),
        "excluded_bj": excluded_bj,
        "features_found": 0,
        "candidates": 0,
        "model_version": MODEL_VERSION,
        "params": effective,
        "dry_run": dry_run,
        "top": [],
    }
    if not prev_date or not pool:
        return summary

    features = load_auction_features(db, pool, trade_date, features_collection)
    prev_prices = load_prev_volume_price(db, pool, prev_date)
    limit_ups = load_limit_up_docs(db, prev_date, pool)
    summary["features_found"] = len(features)
    summary["missing_features"] = sorted(set(pool) - set(features))

    candidates: list[dict[str, Any]] = []
    for symbol in pool:
        feature = features.get(symbol)
        if not feature:
            continue
        doc = score_candidate(
            symbol=symbol,
            trade_date=trade_date,
            prev_limit_up_date=prev_date,
            feature=feature,
            prev_price=prev_prices.get(symbol),
            limit_up=limit_ups.get(symbol),
            params=effective,
        )
        if doc:
            candidates.append(doc)
    candidates.sort(key=lambda doc: (-doc["score"], doc["symbol"]))
    summary["candidates"] = len(candidates)
    summary["top"] = candidates[:10]

    if not dry_run:
        collection = db[candidates_collection]
        collection.create_index([("trade_date", 1), ("symbol", 1)], unique=True)
        for doc in candidates:
            collection.update_one(
                {"trade_date": doc["trade_date"], "symbol": doc["symbol"]},
                {"$set": doc},
                upsert=True,
            )
        runs = db[runs_collection]
        runs.create_index([("trade_date", 1), ("model_version", 1)], unique=True)
        runs.update_one(
            {"trade_date": trade_date, "model_version": MODEL_VERSION},
            {
                "$set": {
                    "trade_date": trade_date,
                    "prev_limit_up_date": prev_date,
                    "model_version": MODEL_VERSION,
                    "pool_size": len(pool),
                    "features_found": len(features),
                    "candidates": len(candidates),
                    "params": effective,
                    "finished_at": datetime.now(),
                },
                "$setOnInsert": {"started_at": datetime.now()},
            },
            upsert=True,
        )
    return summary


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="First-board auction scorer (candidate list only)")
    parser.add_argument("--trade-date", default="", help="YYYYMMDD (default: today CST)")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--min-gap-pct", type=float, default=DEFAULT_PARAMS["min_gap_pct"])
    parser.add_argument("--max-gap-pct", type=float, default=DEFAULT_PARAMS["max_gap_pct"])
    parser.add_argument("--min-volume-ratio", type=float, default=DEFAULT_PARAMS["min_volume_ratio"])
    parser.add_argument("--lot-size", type=float, default=DEFAULT_PARAMS["lot_size"])
    parser.add_argument("--seal-ratio-target", type=float, default=DEFAULT_PARAMS["seal_ratio_target"])
    parser.add_argument("--features-collection", default="auction_features",
                        help="auction feature source; use auction_features_probe for a replay")
    parser.add_argument("--candidates-collection", default=CANDIDATES)
    parser.add_argument("--runs-collection", default=RUNS)
    parser.add_argument("--include-bj", action="store_true",
                        help="include Beijing Stock Exchange symbols (default: exclude)")
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s [first_board] %(levelname)s %(message)s",
    )
    trade_date = normalize_trade_date(
        args.trade_date or datetime.now(CST).strftime("%Y%m%d")
    )
    params = {
        "min_gap_pct": args.min_gap_pct,
        "max_gap_pct": args.max_gap_pct,
        "min_volume_ratio": args.min_volume_ratio,
        "lot_size": args.lot_size,
        "seal_ratio_target": args.seal_ratio_target,
    }
    settings = get_settings()
    client = MongoClient(settings.mongo_uri)
    try:
        summary = run_scoring(
            client[settings.data_db],
            trade_date=trade_date,
            dry_run=args.dry_run,
            params=params,
            features_collection=args.features_collection,
            candidates_collection=args.candidates_collection,
            runs_collection=args.runs_collection,
            include_bj=args.include_bj,
        )
    finally:
        client.close()
    print(json.dumps(summary, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
