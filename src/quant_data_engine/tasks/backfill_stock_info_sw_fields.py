#!/usr/bin/env python3
"""Backfill Shenwan industry fields into quant_data.stock_info.

Source of truth stays in ``sw_index_member`` / ``sw_index_classify``.  This task
materializes the latest SW2021 membership into ``stock_info`` so read-heavy
endpoints can use ``sw_l1/sw_l2/sw_l3`` without repeating the join.

Usage:
  python -m quant_data_engine.tasks.backfill_stock_info_sw_fields --src SW2021
  python -m quant_data_engine.tasks.backfill_stock_info_sw_fields --dry-run --limit 20
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime
from typing import Any, Dict, Iterable, List

from pymongo import MongoClient, UpdateOne

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from quant_data_engine import get_settings
from quant_data_engine.utils.ingestion_job import record_ingestion_run


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill SW industry fields into stock_info")
    parser.add_argument("--src", default="SW2021", help="申万版本，默认 SW2021")
    parser.add_argument(
        "--include-inactive",
        action="store_true",
        help="Include inactive member rows instead of only is_new=Y. Defaults to active only.",
    )
    parser.add_argument("--limit", type=int, default=0, help="Limit processed member rows for smoke tests")
    parser.add_argument("--dry-run", action="store_true", help="Print summary without writing stock_info")
    return parser.parse_args()


def _symbol_variants(ts_code: str) -> List[str]:
    text = str(ts_code or "").strip()
    if not text:
        return []
    variants = [text]
    if "." in text:
        variants.append(text.split(".", 1)[0])
    return list(dict.fromkeys(variants))


def _stock_info_match(ts_code: str) -> Dict[str, Any]:
    variants = _symbol_variants(ts_code)
    return {"$or": [{"ts_code": v} for v in variants] + [{"symbol": v} for v in variants]}


def _sw_update_doc(member: Dict[str, Any], *, src: str, now: datetime) -> Dict[str, Any]:
    return {
        "sw_l1": member.get("l1_name"),
        "sw_l1_code": member.get("l1_code"),
        "sw_l2": member.get("l2_name"),
        "sw_l2_code": member.get("l2_code"),
        "sw_l3": member.get("l3_name"),
        "sw_l3_code": member.get("l3_code"),
        "sw_src": src,
        "sw_is_new": member.get("is_new"),
        "sw_in_date": member.get("in_date"),
        "sw_out_date": member.get("out_date"),
        "sw_updated_at": now,
    }


def _iter_member_docs(member_coll: Any, *, src: str, include_inactive: bool) -> Iterable[Dict[str, Any]]:
    query: Dict[str, Any] = {"src": src}
    if not include_inactive:
        query["is_new"] = "Y"
    projection = {
        "_id": 0,
        "con_code": 1,
        "name": 1,
        "l1_code": 1,
        "l1_name": 1,
        "l2_code": 1,
        "l2_name": 1,
        "l3_code": 1,
        "l3_name": 1,
        "is_new": 1,
        "in_date": 1,
        "out_date": 1,
    }
    return member_coll.find(query, projection).sort([("con_code", 1), ("in_date", -1)])


def build_stock_info_sw_ops(
    members: Iterable[Dict[str, Any]], *, src: str, now: datetime, limit: int = 0
) -> List[UpdateOne]:
    ops: List[UpdateOne] = []
    seen: set[str] = set()
    for member in members:
        ts_code = str(member.get("con_code") or "").strip()
        if not ts_code or ts_code in seen:
            continue
        seen.add(ts_code)
        ops.append(UpdateOne(_stock_info_match(ts_code), {"$set": _sw_update_doc(member, src=src, now=now)}))
        if limit and len(ops) >= limit:
            break
    return ops


def main() -> None:
    args = parse_args()
    settings = get_settings()
    job_date = datetime.today().strftime("%Y%m%d")
    now = datetime.now()

    client = MongoClient(settings.mongo_uri)
    db = client[settings.data_db]
    stock_info = db["stock_info"]
    member_coll = db["sw_index_member"]
    stock_info.create_index("sw_l1", background=True)
    stock_info.create_index("sw_l2", background=True)
    stock_info.create_index("sw_l3", background=True)
    stock_info.create_index("sw_l2_code", background=True)

    members = _iter_member_docs(member_coll, src=args.src, include_inactive=args.include_inactive)
    ops = build_stock_info_sw_ops(members, src=args.src, now=now, limit=args.limit)

    print("🟡 Backfill stock_info SW fields")
    print(f"   src: {args.src}")
    print(f"   include_inactive: {args.include_inactive}")
    print(f"   candidate updates: {len(ops)}")
    if args.dry_run:
        print("   dry-run: no writes")
        record_ingestion_run(
            settings,
            "stock_info_sw_fields_backfill",
            job_date,
            status="dry_run",
            records_changed=0,
            src=args.src,
            candidate_updates=len(ops),
        )
        return
    if not ops:
        print("⚠️  No member rows to update")
        record_ingestion_run(
            settings,
            "stock_info_sw_fields_backfill",
            job_date,
            status="skipped",
            records_changed=0,
            src=args.src,
        )
        return

    result = stock_info.bulk_write(ops, ordered=False)
    changed = int(result.upserted_count or 0) + int(result.modified_count or 0)
    print("✅ Backfill completed")
    print(f"   Matched: {result.matched_count}")
    print(f"   Modified: {result.modified_count}")
    print(f"   Upserted: {result.upserted_count}")

    record_ingestion_run(
        settings,
        "stock_info_sw_fields_backfill",
        job_date,
        records_changed=changed,
        src=args.src,
        candidate_updates=len(ops),
    )


if __name__ == "__main__":
    main()
