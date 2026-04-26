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
    p.add_argument("--sync", choices=("classify", "members", "daily", "all"), default="all")
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

    today = datetime.now().strftime("%Y%m%d")
    record_ingestion_run(
        settings, "sw_industry_backfill", today, status="complete", records_changed=n_total
    )


if __name__ == "__main__":
    main()
