"""Sync Tushare index_weight for core indices -> MongoDB index_constituents (daily job).

Reads TUSHARE_TOKEN / MONGO_URI / DATA_DB_NAME via get_settings (same .env as other tasks).

Default index set: see ``constants/index_constituents_sync.DEFAULT_SYNC_PROFILES``.
Override with env ``INDEX_CONSTITUENTS_PROFILES`` (JSON array of
{ts_code, store_key, alternates?}). If set, this **replaces** the default list unless
``INDEX_CONSTITUENTS_PROFILES_MERGE_DEFAULTS=1`` (see that module’s docstring).

Cron: after index daily (see crontab / daily_market_sync.sh index_constituents).
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from contextlib import contextmanager, nullcontext
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

import pandas as pd

from quant_data_engine.config.settings import get_settings
from quant_data_engine.constants.index_constituents_sync import (
    ConstituentSyncProfile,
    load_constituent_sync_profiles,
)
from quant_data_engine.repositories.index_constituents_repository import (
    IndexConstituentsRepository,
    build_documents,
)
from quant_data_engine.revision_ledger import (
    INDEX_CONSTITUENTS_SOURCE,
    revision_ledger_job,
)
from quant_data_engine.utils.ingestion_job import record_ingestion_run


class _NoSuccessfulConstituentWrite(RuntimeError):
    pass


@contextmanager
def _abort_ledger_without_success(ledger_job: Any, succeeded: Any):
    try:
        with ledger_job:
            yield
            if not succeeded():
                raise _NoSuccessfulConstituentWrite
    except _NoSuccessfulConstituentWrite:
        pass


def _trade_cal_open_days(pro: Any, start_date: str, end_date: str) -> List[str]:
    df = pro.trade_cal(
        exchange="SSE",
        start_date=start_date,
        end_date=end_date,
        is_open="1",
    )
    if df is None or df.empty:
        return []
    df = df.sort_values("cal_date", ascending=True)
    return [str(x) for x in df["cal_date"].tolist()]


def _probe_dates_newest_first(pro: Any, max_days: int) -> List[str]:
    """SSE open days up to yesterday, newest first (avoid empty same-day index_weight)."""
    end = date.today() - timedelta(days=1)
    start = end - timedelta(days=120)
    days = _trade_cal_open_days(
        pro, start.strftime("%Y%m%d"), end.strftime("%Y%m%d")
    )
    if not days:
        return []
    tail = days[-max_days:]
    return list(reversed(tail))


def _probe_index_weight(
    pro: Any,
    profile: ConstituentSyncProfile,
    dates_newest_first: List[str],
    min_interval_sec: float,
) -> Tuple[Optional[pd.DataFrame], Optional[str], Optional[str]]:
    codes = [profile["ts_code"]] + list(profile.get("alternates") or [])
    last_call = 0.0
    for d in dates_newest_first:
        for ts in codes:
            if min_interval_sec > 0:
                elapsed = time.monotonic() - last_call
                if elapsed < min_interval_sec and last_call > 0:
                    time.sleep(min_interval_sec - elapsed)
            try:
                df = pro.index_weight(index_code=ts, trade_date=d)
            except Exception:
                df = None
            last_call = time.monotonic()
            if df is not None and not df.empty:
                return df, ts, str(d)
    return None, None, None


def _fetch_stock_basic_map(pro: Any, min_interval_sec: float) -> Dict[str, Dict[str, str]]:
    if min_interval_sec > 0:
        time.sleep(min_interval_sec)
    df = pro.stock_basic(exchange="", list_status="L")
    out: Dict[str, Dict[str, str]] = {}
    if df is None or df.empty:
        return out
    for _, row in df.iterrows():
        tc = row.get("ts_code")
        if not tc:
            continue
        out[str(tc)] = {
            "name": str(row.get("name") or ""),
            "industry": str(row.get("industry") or ""),
        }
    return out


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Sync index constituents (index_weight) into Mongo index_constituents"
    )
    p.add_argument(
        "--max-probe-days",
        type=int,
        default=40,
        help="Max SSE open days to try per index (newest first, default 40)",
    )
    p.add_argument(
        "--min-interval",
        type=float,
        default=0.25,
        help="Min seconds between Tushare calls (soft rate limit, default 0.25)",
    )
    p.add_argument("--dry-run", action="store_true", help="Fetch only; do not write Mongo")
    p.add_argument("--no-record-job", action="store_true", help="Skip job_status recording")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    settings = get_settings()
    if not settings.tushare_token:
        print("ERROR: TUSHARE_TOKEN missing (set in .env)", file=sys.stderr)
        return 1

    try:
        profiles = load_constituent_sync_profiles()
    except ValueError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 1

    import tushare as ts

    ts.set_token(settings.tushare_token)
    pro = ts.pro_api()

    dates = _probe_dates_newest_first(pro, max_days=max(10, args.max_probe_days))
    if not dates:
        print("ERROR: trade_cal returned no open days; cannot probe index_weight", file=sys.stderr)
        return 1

    today = datetime.now().strftime("%Y%m%d")
    print(f"Sync calendar update_date={today}; probe up to {len(dates)} SSE open days (newest {dates[0]})")
    print(f"Profiles: {len(profiles)}")

    basic_map = _fetch_stock_basic_map(pro, min_interval_sec=args.min_interval)
    print(f"stock_basic map size: {len(basic_map)}")

    repo: Optional[IndexConstituentsRepository] = None
    if not args.dry_run:
        repo = IndexConstituentsRepository(settings.mongo_uri, settings.data_db)

    total_rows = 0
    errors: List[str] = []
    ledger_job = (
        _abort_ledger_without_success(
            revision_ledger_job(
                INDEX_CONSTITUENTS_SOURCE,
                "index_constituents_sync",
                settings.mongo_uri,
            ),
            lambda: total_rows > 0,
        )
        if not args.dry_run
        else nullcontext()
    )
    with ledger_job:
        for prof in profiles:
            sk = prof["store_key"]
            df, used_ts, used_td = _probe_index_weight(
                pro, prof, dates, min_interval_sec=args.min_interval
            )
            if df is None or used_ts is None or used_td is None:
                msg = f"{sk}: no index_weight data after probing {len(dates)} day(s)"
                print(f"WARN {msg}")
                errors.append(msg)
                continue

            records = df.to_dict("records")
            docs = build_documents(
                store_key=sk,
                update_date=today,
                weight_trade_date=used_td,
                df_rows=records,
                basic_by_ts=basic_map,
            )
            print(f"OK {sk}: ts_code={used_ts} weight_trade_date={used_td} rows={len(docs)}")

            if args.dry_run:
                total_rows += len(docs)
                continue

            assert repo is not None
            n = repo.replace_for_sync_day(sk, today, docs)
            total_rows += n

    if total_rows == 0:
        err = "; ".join(errors) or "no index constituents rows written"
        if not args.no_record_job and not args.dry_run:
            record_ingestion_run(
                settings,
                "index_constituents_sync",
                today,
                status="error",
                error=err[:2000],
                records_changed=0,
                codes=[p["store_key"] for p in profiles],
            )
        print(f"FAILED: {err}", file=sys.stderr)
        return 2

    if errors:
        print(f"WARN partial failures: {'; '.join(errors)}")

    if not args.no_record_job and not args.dry_run:
        err = "; ".join(errors) if errors else None
        st = "complete"
        if errors:
            st = "partial"
        record_ingestion_run(
            settings,
            "index_constituents_sync",
            today,
            status=st,
            error=err[:2000] if err else None,
            records_changed=total_rows,
            codes=[p["store_key"] for p in profiles],
        )

    print(f"Done. total_rows_written={total_rows} dry_run={args.dry_run}")
    return 0 if not errors else 3


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
