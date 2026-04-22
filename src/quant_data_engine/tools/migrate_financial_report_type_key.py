"""
Set / normalize report_type on legacy financial_income / financial_cashflow documents.

1) 缺 report_type 的补为 1（合并表），以便与新的 upsert 键 (ts_code, end_date, report_type) 对齐。

2) Mongo 里整数 1 与字符串 "1" 在 filter 中不相等，旧数据若为字符串，sync 时无法用同一键覆盖，
   会多出一条「同周期、同含义」的重复行。`--fix-strings` 将字符串 "1"/"2" 等写回为 int。

3) 若已同时存在 int 1 与 string "1" 对应的两条文档，在字符串已改为 int 后，键相同则 `--dedupe`
   会按 `synced_at` 较新者保留，删除重复 `_id`（不必整库重拉）。

Run **once** (or with `--fix-strings` / `--dedupe` / `--full`) when upgrading keys or after seeing
duplicate rows for the same (ts_code, end_date) with mixed report_type types.

Usage (from quant-data-engine root, with MONGO_URI + DATA_DB_NAME in env):

  python -m quant_data_engine.tools.migrate_financial_report_type_key
  python -m quant_data_engine.tools.migrate_financial_report_type_key --dry-run
  python -m quant_data_engine.tools.migrate_financial_report_type_key --full
  python -m quant_data_engine.tools.migrate_financial_report_type_key --fix-strings --dedupe
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))

from quant_data_engine.config.settings import get_settings  # noqa: E402


def _fix_missing_report_type(coll, dry_run: bool) -> int:
    filt = {"report_type": {"$exists": False}}
    n = coll.count_documents(filt)
    print(f"  without report_type: {n}")
    if n == 0 or dry_run:
        return 0
    r = coll.update_many(filt, {"$set": {"report_type": 1}})
    print(f"  -> set report_type=1: modified {r.modified_count}")
    return r.modified_count or 0


def _fix_string_report_types(coll, dry_run: bool) -> int:
    n_str = coll.count_documents({"report_type": {"$type": "string"}})
    print(f"  report_type is string: {n_str}")
    if n_str == 0 or dry_run:
        return 0
    total = 0
    for s in ("1", "2", "3", "4"):
        r = coll.update_many({"report_type": s}, {"$set": {"report_type": int(s)}})
        total += r.modified_count or 0
    str_docs = list(coll.find({"report_type": {"$type": "string"}}, {"_id": 1, "report_type": 1}))
    for doc in str_docs:
        v = doc.get("report_type")
        try:
            ival = int(str(v).strip())
        except (TypeError, ValueError):
            continue
        coll.update_one({"_id": doc["_id"]}, {"$set": {"report_type": ival}})
        total += 1
    print(f"  -> normalized string report_type updates: {total}")
    return total


def _dedupe_ts_end_rt(coll, dry_run: bool) -> int:
    """对 (ts_code, end_date, report_type) 完全相同的文档去重，保留 synced_at 最新。"""
    pipeline = [
        {
            "$match": {
                "ts_code": {"$exists": True, "$ne": None},
                "end_date": {"$exists": True, "$ne": None},
                "report_type": {"$exists": True, "$ne": None},
            }
        },
        {
            "$group": {
                "_id": {
                    "t": "$ts_code",
                    "e": "$end_date",
                    "r": "$report_type",
                },
                "n": {"$sum": 1},
                "ids": {"$push": "$_id"},
            }
        },
        {"$match": {"n": {"$gt": 1}}},
    ]
    dups = list(coll.aggregate(pipeline, allowDiskUse=True))
    print(f"  duplicate (ts, end, report_type) groups: {len(dups)}")
    if dry_run or not dups:
        return 0
    deleted = 0
    for g in dups:
        ids = g["ids"]
        if len(ids) < 2:
            continue
        docs = list(
            coll.find(
                {"_id": {"$in": ids}},
                {"_id": 1, "synced_at": 1},
            )
        )
        docs.sort(
            key=lambda d: d.get("synced_at") or datetime.min,
            reverse=True,
        )
        keep = docs[0]["_id"]
        remove = [d["_id"] for d in docs[1:]]
        r = coll.delete_many({"_id": {"$in": remove}})
        deleted += r.deleted_count
    print(f"  -> deleted {deleted} duplicate documents")
    return deleted


def main() -> None:
    p = argparse.ArgumentParser(description="Migrate / repair report_type on financial income & cashflow")
    p.add_argument("--dry-run", action="store_true", help="Print counts only, do not update or delete")
    p.add_argument(
        "--full",
        action="store_true",
        help="Run: fill missing, fix string report_type, then dedupe",
    )
    p.add_argument(
        "--fix-strings",
        action="store_true",
        help="Set string '1'/'2'/… to int (use with --dedupe if duplicates were int+str)",
    )
    p.add_argument(
        "--dedupe",
        action="store_true",
        help="Delete duplicate documents for same (ts_code, end_date, report_type)",
    )
    args = p.parse_args()
    s = get_settings()
    from pymongo import MongoClient  # local import

    if args.full:
        do_missing, do_fix_strings, do_dedupe = True, True, True
    elif args.fix_strings or args.dedupe:
        do_missing = False
        do_fix_strings = args.fix_strings
        do_dedupe = args.dedupe
    else:
        # 默认与旧版一致：仅给缺少 report_type 的文档补 1
        do_missing, do_fix_strings, do_dedupe = True, False, False

    client = MongoClient(s.mongo_uri)
    db = client[s.data_db]
    for coll_name in ("financial_income", "financial_cashflow"):
        print(f"{coll_name}:")
        coll = db[coll_name]
        if do_missing:
            _fix_missing_report_type(coll, args.dry_run)
        if do_fix_strings:
            _fix_string_report_types(coll, args.dry_run)
        if do_dedupe:
            _dedupe_ts_end_rt(coll, args.dry_run)
    print("Done.")


if __name__ == "__main__":
    main()
