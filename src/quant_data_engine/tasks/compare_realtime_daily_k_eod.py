"""Compare realtime daily K snapshots with final EOD Tushare daily bars."""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, Optional

from pymongo import MongoClient

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from quant_data_engine.config.settings import get_settings
from quant_data_engine.utils.ingestion_job import record_ingestion_run


CN_TZ = timezone(timedelta(hours=8))
TASK_TYPE = "a_daily_realtime_eod_compare"
logger = logging.getLogger("realtime_daily_k_compare")


def _num(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _pct_diff(a: Any, b: Any) -> Optional[float]:
    left = _num(a)
    right = _num(b)
    if left is None or right is None:
        return None
    base = abs(right) if right != 0 else abs(left)
    if base == 0:
        return 0.0
    return abs(left - right) / base * 100.0


def _max_diff(doc: Dict[str, Any], snapshot: Dict[str, Any], fields: Iterable[str]) -> float:
    values = [_pct_diff(snapshot.get(field), doc.get(field)) for field in fields]
    valid = [v for v in values if v is not None]
    return max(valid) if valid else 0.0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare realtime daily K snapshot with EOD daily data")
    parser.add_argument("--date", default=None, help="Trade date YYYYMMDD, default: today CN")
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [realtime_daily_k_compare] %(message)s")
    args = parse_args()
    settings = get_settings()
    trade_date = (args.date or datetime.now(CN_TZ).strftime("%Y%m%d")).replace("-", "")[:8]

    client = MongoClient(settings.mongo_uri)
    try:
        col = client[settings.data_db]["volume_price"]
        docs = list(
            col.find(
                {
                    "trade_date": trade_date,
                    "realtime_daily_snapshot.snapshot_time": {"$regex": f"^{trade_date}"},
                },
                {
                    "_id": 0,
                    "symbol": 1,
                    "open": 1,
                    "high": 1,
                    "low": 1,
                    "close": 1,
                    "volume": 1,
                    "amount": 1,
                    "pre_close": 1,
                    "pct_chg": 1,
                    "realtime_daily_snapshot": 1,
                },
            )
        )
    finally:
        client.close()

    total = len(docs)
    if total == 0:
        record_ingestion_run(
            settings,
            TASK_TYPE,
            trade_date,
            status="failed",
            error="No realtime_daily_snapshot rows found for comparison",
            compared_symbols=0,
            equivalence_passed=False,
        )
        logger.error("No realtime_daily_snapshot rows found for %s", trade_date)
        return

    price_bad = 0
    volume_bad = 0
    amount_bad = 0
    missing_core = 0
    max_price_diff = 0.0
    max_volume_diff = 0.0
    max_amount_diff = 0.0
    examples = []

    for doc in docs:
        snapshot = doc.get("realtime_daily_snapshot") or {}
        if not all(snapshot.get(field) is not None for field in ("open", "high", "low", "close", "volume")):
            missing_core += 1
        price_diff = _max_diff(doc, snapshot, ("open", "high", "low", "close"))
        volume_diff = _max_diff(doc, snapshot, ("volume",))
        amount_diff = _max_diff(doc, snapshot, ("amount",))
        max_price_diff = max(max_price_diff, price_diff)
        max_volume_diff = max(max_volume_diff, volume_diff)
        max_amount_diff = max(max_amount_diff, amount_diff)
        bad = False
        if price_diff > settings.realtime_daily_k_price_tolerance_pct:
            price_bad += 1
            bad = True
        if volume_diff > settings.realtime_daily_k_volume_tolerance_pct:
            volume_bad += 1
            bad = True
        if amount_diff > settings.realtime_daily_k_volume_tolerance_pct:
            amount_bad += 1
            bad = True
        if bad and len(examples) < 10:
            examples.append(
                {
                    "symbol": doc.get("symbol"),
                    "price_diff_pct": price_diff,
                    "volume_diff_pct": volume_diff,
                    "amount_diff_pct": amount_diff,
                    "snapshot_time": snapshot.get("snapshot_time"),
                }
            )

    missing_core_ratio = missing_core / total
    price_bad_ratio = price_bad / total
    volume_bad_ratio = volume_bad / total
    amount_bad_ratio = amount_bad / total
    equivalence_passed = (
        missing_core_ratio <= (1 - settings.realtime_daily_k_min_field_coverage)
        and price_bad_ratio <= 0.01
        and volume_bad_ratio <= 0.02
        and amount_bad_ratio <= 0.02
    )
    status = "complete" if equivalence_passed else "partial"
    record_ingestion_run(
        settings,
        TASK_TYPE,
        trade_date,
        status=status,
        records_changed=total,
        compared_symbols=total,
        equivalence_passed=equivalence_passed,
        missing_core=missing_core,
        missing_core_ratio=missing_core_ratio,
        price_bad=price_bad,
        price_bad_ratio=price_bad_ratio,
        volume_bad=volume_bad,
        volume_bad_ratio=volume_bad_ratio,
        amount_bad=amount_bad,
        amount_bad_ratio=amount_bad_ratio,
        max_price_diff_pct=max_price_diff,
        max_volume_diff_pct=max_volume_diff,
        max_amount_diff_pct=max_amount_diff,
        examples=examples,
    )
    logger.info(
        "compared=%d equivalence=%s price_bad=%.2f%% volume_bad=%.2f%% amount_bad=%.2f%%",
        total,
        equivalence_passed,
        price_bad_ratio * 100,
        volume_bad_ratio * 100,
        amount_bad_ratio * 100,
    )


if __name__ == "__main__":
    main()
