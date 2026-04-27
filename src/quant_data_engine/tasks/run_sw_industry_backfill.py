"""
申万 Tushare 数据回补：index_classify、按 L1 拉 index_member_all、按交易日拉 sw_daily。

用法::

  python -m quant_data_engine.tasks.run_sw_industry_backfill --sync classify --src SW2021
  python -m quant_data_engine.tasks.run_sw_industry_backfill --sync members --src SW2021
  python -m quant_data_engine.tasks.run_sw_industry_backfill --sync daily --start 20160425 --end 20260425

断点：sw_daily 成功写入后更新 meta key ``sw_daily_backfill`` 为最后交易日（可用于 --resume）。
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timedelta
from typing import List

import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from quant_data_engine.config.settings import get_settings
from quant_data_engine.adapters.sw_industry_adapter import SwIndustryAdapter
from quant_data_engine.repositories.sw_industry_repository import SwIndustryRepository
from quant_data_engine.utils.ingestion_job import record_ingestion_run


def get_open_days(sa, start: str, end: str) -> List[str]:
    from quant_data_engine.adapters.stock_adapter import StockAdapter

    assert sa is not None
    df = sa.fetch_trade_cal(exchange="SSE", start_date=start, end_date=end)
    if df is None or df.empty:
        return []
    o = df[df["is_open"] == 1]["cal_date"].astype(str).str.replace("-", "")
    return sorted(o.tolist())


def _records_from_df(df):
    if df is None or df.empty:
        return []
    return df.to_dict(orient="records")


def main() -> None:
    p = argparse.ArgumentParser(description="申万 Tushare 分类/成分/sw_daily 回补")
    p.add_argument("--sync", choices=("classify", "members", "daily", "weekly", "monthly", "all"), default="all")
    p.add_argument("--src", default="SW2021", help="申万版本: SW2021 或 SW2014")
    p.add_argument("--start", default="", help="sw_daily 起始日 YYYYMMDD")
    p.add_argument("--end", default="", help="sw_daily 结束日 YYYYMMDD，默认今天")
    p.add_argument("--resume", action="store_true", help="从 meta 中上次下一天继续")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    settings = get_settings()
    ad = SwIndustryAdapter(
        settings.tushare_token, rate_limit_per_sec=settings.rate_limit_per_sec
    )
    from quant_data_engine.adapters.stock_adapter import StockAdapter

    st = StockAdapter(settings.tushare_token, rate_limit_per_sec=settings.rate_limit_per_sec)
    repo = SwIndustryRepository(settings.mongo_uri, settings.data_db)
    n_total = 0

    if args.sync in ("classify", "all"):
        for level in ("L1", "L2", "L3"):
            df = ad.fetch_index_classify(src=args.src, level=level)
            recs = _records_from_df(df)
            if not args.dry_run:
                n = repo.upsert_classify(recs, src=args.src)
            else:
                n = len(recs)
            print(f"classify {args.src} {level} rows={len(recs)}")
            n_total += n

    if args.sync in ("members", "all"):
        df_l1 = ad.fetch_index_classify(src=args.src, level="L1")
        l1_codes: List[str] = []
        if df_l1 is not None and not df_l1.empty and "index_code" in df_l1.columns:
            l1_codes = [str(x) for x in df_l1["index_code"].dropna().unique().tolist() if str(x).strip()]
        mtotal = 0
        for code in l1_codes:
            mdf = ad.fetch_index_member_all_paged(l1_code=code)
            recs = _records_from_df(mdf)
            for r in recs:
                r["parent_l1"] = code
            if not args.dry_run and recs:
                mtotal += repo.upsert_members(recs, src=args.src)
        print(f"members L1 groups={len(l1_codes)} total_upsert={mtotal}")
        n_total += mtotal

    if args.sync in ("daily", "all"):
        end = args.end or datetime.now().strftime("%Y%m%d")
        start = args.start or (datetime.now() - timedelta(days=3650)).strftime("%Y%m%d")
        if args.resume:
            last = repo.get_meta("sw_daily_backfill")
            if last and len(last) == 8 and last < end:
                from datetime import datetime as _dt

                t = _dt.strptime(last, "%Y%m%d") + timedelta(days=1)
                start = t.strftime("%Y%m%d")
        open_days = get_open_days(st, start, end)
        print(f"sw_daily: {len(open_days)} open days {start}..{end}")
        for i, td in enumerate(open_days):
            dfa = ad.fetch_sw_daily_by_trade_date_all(td)
            recs = _records_from_df(dfa)
            if not recs:
                print(f"  {td} empty")
                continue
            if not args.dry_run:
                n = repo.upsert_sw_daily(recs)
                n_total += n
                repo.set_meta("sw_daily_backfill", td)
            if (i + 1) % 20 == 0 or i == len(open_days) - 1:
                print(f"  ... to {td} records={len(recs)}")

    if args.sync in ("weekly", "all"):
        n_total += _resample_daily_to_period(
            repo,
            period="weekly",
            start=args.start or "",
            end=args.end or "",
            resume=args.resume,
            dry_run=args.dry_run,
        )

    if args.sync in ("monthly", "all"):
        n_total += _resample_daily_to_period(
            repo,
            period="monthly",
            start=args.start or "",
            end=args.end or "",
            resume=args.resume,
            dry_run=args.dry_run,
        )

    today = datetime.now().strftime("%Y%m%d")
    record_ingestion_run(
        settings, "sw_industry_backfill", today, status="complete", records_changed=n_total
    )


def _resample_daily_to_period(
    repo: "SwIndustryRepository",
    period: str,
    start: str,
    end: str,
    resume: bool,
    dry_run: bool = False,
) -> int:
    """Convert sw_daily records to weekly or monthly bars via pandas resample.

    Args:
        repo: SwIndustryRepository instance.
        period: ``"weekly"`` or ``"monthly"``.
        start: Optional start date YYYYMMDD (inclusive).
        end: Optional end date YYYYMMDD (inclusive).
        resume: If True, load last checkpoint from meta and advance start.
        dry_run: If True, skip writes.

    Returns:
        Total upserted record count.
    """
    meta_key = f"sw_{period}_backfill"
    freq = "W-FRI" if period == "weekly" else "ME"

    effective_start = start
    effective_end = end or datetime.now().strftime("%Y%m%d")

    if resume:
        last = repo.get_meta(meta_key)
        if last and len(last) == 8:
            from datetime import datetime as _dt
            t = _dt.strptime(last, "%Y%m%d") + timedelta(days=1)
            resumed = t.strftime("%Y%m%d")
            if not effective_start or resumed > effective_start:
                effective_start = resumed
            print(f"{period}: resume from {effective_start} (last checkpoint: {last})")

    # Build date filter for MongoDB query
    date_filt: dict = {}
    if effective_start:
        date_filt["$gte"] = effective_start
    if effective_end:
        date_filt["$lte"] = effective_end

    # Get all distinct ts_codes present in sw_daily
    all_codes: list = repo.daily_coll.distinct("ts_code")
    all_codes = [c for c in all_codes if c]
    print(f"{period}: found {len(all_codes)} distinct ts_codes in sw_daily")

    agg_rules = {
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "vol": "sum",
        "amount": "sum",
        "change": "last",
        "pct_change": "last",
        "float_mv": "last",
        "total_mv": "last",
        "pe": "last",
        "pb": "last",
        "name": "first",
    }

    n_total = 0
    last_code_done: str = ""

    for i, ts_code in enumerate(all_codes):
        # Load daily records for this ts_code
        filt: dict = {"ts_code": ts_code}
        if date_filt:
            filt["trade_date"] = date_filt
        cursor = repo.daily_coll.find(filt, projection={"_id": 0}).sort("trade_date", 1)
        rows = list(cursor)
        if not rows:
            continue

        df = pd.DataFrame(rows)
        if "trade_date" not in df.columns:
            continue

        # Parse trade_date as datetime index
        df["trade_date"] = pd.to_datetime(df["trade_date"], format="%Y%m%d", errors="coerce")
        df = df.dropna(subset=["trade_date"])
        df = df.set_index("trade_date").sort_index()

        # Only resample columns that exist
        present_rules = {col: agg for col, agg in agg_rules.items() if col in df.columns}
        if not present_rules:
            continue

        resampled = df[list(present_rules.keys())].resample(freq, label="right", closed="right").agg(present_rules)
        resampled = resampled.dropna(how="all")

        if resampled.empty:
            continue

        # Format trade_date back to YYYYMMDD string
        resampled = resampled.reset_index()
        resampled["trade_date"] = resampled["trade_date"].dt.strftime("%Y%m%d")
        resampled["ts_code"] = ts_code
        resampled["source"] = "derived"
        resampled["update_time"] = datetime.now().isoformat()

        records = resampled.to_dict(orient="records")
        print(f"  Resampling ts_code={ts_code} to {period}: {len(rows)} daily -> {len(records)} {period} records")

        if not dry_run:
            if period == "weekly":
                n = repo.upsert_weekly_batch(records)
            else:
                n = repo.upsert_monthly_batch(records)
            n_total += n
            last_code_done = ts_code
        else:
            n_total += len(records)

        if (i + 1) % 50 == 0 or i == len(all_codes) - 1:
            print(f"  ... processed {i + 1}/{len(all_codes)} codes, upserted so far={n_total}")

    if not dry_run and last_code_done:
        # Checkpoint: store effective_end as the boundary we processed up to
        repo.set_meta(meta_key, effective_end)
        print(f"{period}: checkpoint saved {meta_key}={effective_end}")

    print(f"{period}: done. total_upserted={n_total}")
    return n_total


if __name__ == "__main__":
    main()
