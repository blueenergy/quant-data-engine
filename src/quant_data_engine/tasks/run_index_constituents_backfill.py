"""Backfill historical index_weight snapshots into Mongo index_constituents.

Tushare ``index_weight`` is typically available at monthly rebalance dates. This task
walks month-end SSE open days from ``--start-date`` through ``--end-date`` and writes
one snapshot per ``(store_key, weight_trade_date)``.

Usage:
  python -m quant_data_engine.tasks.run_index_constituents_backfill --dry-run
  python -m quant_data_engine.tasks.run_index_constituents_backfill --start-date 20230101 --end-date 20251231 --execute
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Sequence, Tuple

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from quant_data_engine.config.settings import get_settings
from quant_data_engine.constants.index_constituents_sync import (
    ConstituentSyncProfile,
    load_constituent_sync_profiles,
)
from quant_data_engine.repositories.index_constituents_repository import (
    IndexConstituentsRepository,
    build_documents,
)
from quant_data_engine.tasks.run_index_constituents_sync import (
    _fetch_stock_basic_map,
    _probe_index_weight,
    _trade_cal_open_days,
)


def _month_end_open_days(pro: Any, start_date: str, end_date: str) -> List[str]:
    days = _trade_cal_open_days(pro, start_date, end_date)
    if not days:
        return []
    by_month: Dict[str, str] = {}
    for d in days:
        by_month[d[:6]] = d
    return sorted(by_month.values())


def _fetch_index_weight_for_date(
    pro: Any,
    profile: ConstituentSyncProfile,
    trade_date: str,
    min_interval_sec: float,
) -> Tuple[Optional[Any], Optional[str], Optional[str]]:
    df, used_ts, used_td = _probe_index_weight(
        pro,
        profile,
        [trade_date],
        min_interval_sec=min_interval_sec,
    )
    return df, used_ts, used_td


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Backfill historical index constituents snapshots")
    p.add_argument("--start-date", default="20230101", help="Inclusive YYYYMMDD")
    p.add_argument("--end-date", default="", help="Inclusive YYYYMMDD; default today")
    p.add_argument(
        "--store-keys",
        nargs="*",
        default=["csi1000"],
        help="Mongo index_code store keys to backfill (default: csi1000)",
    )
    p.add_argument("--min-interval", type=float, default=0.25, help="Seconds between Tushare calls")
    p.add_argument("--dry-run", action="store_true", help="Fetch only; do not write Mongo")
    p.add_argument("--execute", action="store_true", help="Write snapshots to Mongo")
    p.add_argument("--no-record-job", action="store_true", help="Skip job_status recording")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    if not args.dry_run and not args.execute:
        print("Use --dry-run to probe or --execute to write Mongo snapshots.", file=sys.stderr)
        return 1

    settings = get_settings()
    if not settings.tushare_token:
        print("ERROR: TUSHARE_TOKEN missing (set in .env)", file=sys.stderr)
        return 1

    try:
        profiles = load_constituent_sync_profiles()
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    wanted = {str(k).strip() for k in args.store_keys if str(k).strip()}
    profiles = [p for p in profiles if p["store_key"] in wanted]
    if not profiles:
        print(f"ERROR: no profiles matched store keys: {sorted(wanted)}", file=sys.stderr)
        return 1

    end_date = (args.end_date or datetime.now().strftime("%Y%m%d")).replace("-", "")[:8]
    start_date = str(args.start_date).replace("-", "")[:8]

    import tushare as ts

    ts.set_token(settings.tushare_token)
    pro = ts.pro_api()

    probe_days = _month_end_open_days(pro, start_date, end_date)
    if not probe_days:
        print("ERROR: no SSE open days in requested range", file=sys.stderr)
        return 1

    print(f"Backfill range {start_date}~{end_date}; month-end probe days={len(probe_days)}")
    basic_map = _fetch_stock_basic_map(pro, min_interval_sec=args.min_interval)

    repo: Optional[IndexConstituentsRepository] = None
    if args.execute and not args.dry_run:
        repo = IndexConstituentsRepository(settings.mongo_uri, settings.data_db)

    summary: List[Dict[str, Any]] = []
    total_rows = 0
    for prof in profiles:
        store_key = prof["store_key"]
        written = 0
        for probe_day in probe_days:
            df, used_ts, used_td = _fetch_index_weight_for_date(
                pro,
                prof,
                probe_day,
                min_interval_sec=args.min_interval,
            )
            if df is None or used_td is None:
                print(f"WARN {store_key}: no index_weight for probe {probe_day}")
                continue
            docs = build_documents(
                store_key=store_key,
                update_date=used_td,
                weight_trade_date=used_td,
                df_rows=df.to_dict("records"),
                basic_by_ts=basic_map,
            )
            print(f"OK {store_key}: weight_trade_date={used_td} rows={len(docs)}")
            summary.append(
                {
                    "store_key": store_key,
                    "weight_trade_date": used_td,
                    "rows": len(docs),
                    "ts_code": used_ts,
                }
            )
            total_rows += len(docs)
            if args.execute and not args.dry_run and repo is not None:
                written += repo.replace_for_weight_trade_date(store_key, used_td, docs)
        print(f"Done {store_key}: snapshots={len([s for s in summary if s['store_key'] == store_key])} rows={written or total_rows}")

    print(f"Summary snapshots={len(summary)} total_rows={total_rows} dry_run={args.dry_run}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
