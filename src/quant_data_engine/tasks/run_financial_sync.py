"""CLI task to run financial data synchronization.

Collects financial statements and indicators from Tushare:
  - Income statement
  - Balance sheet
  - Cash flow statement
  - Financial indicators

Environment:
  MONGO_URI, DATA_DB_NAME, TUSHARE_TOKEN via ``get_settings()`` (see ``quantFinance/.env``).

Usage:
  # Full market, one report period (Tushare *_vip; no per-stock loop). Requires VIP permission.
  python -m quant_data_engine.tasks.run_financial_sync --all --period 20240930

  # 全市场：一段日期内所有**季末** (0331/0630/0930/1231) 连续同步（如补 income/cashflow 含 report_type=2）
  python -m quant_data_engine.tasks.run_financial_sync --all --period-range 20200630:20251231 --data-types income,cashflow

  # Sync specific stocks (still uses *_vip per ``ts_code`` + date filters)
  python -m quant_data_engine.tasks.run_financial_sync --codes 000001.SZ 000002.SZ --start 20230101

  # Sync all listed stocks (test mode: first 10 codes only — not used with ``--all --period``)
  python -m quant_data_engine.tasks.run_financial_sync --all --test

  # Sync only specific data types
  python -m quant_data_engine.tasks.run_financial_sync --codes 000001.SZ --data-types income,balance

  # Sync dividend records for DDM validation
  python -m quant_data_engine.tasks.run_financial_sync --codes 000001.SZ --data-types dividend

  # Sync main business composition (按产品/地区/行业; per-stock only, not full-market period mode)
  python -m quant_data_engine.tasks.run_financial_sync --codes 000001.SZ --data-types mainbz

  # After upgrading: one-time set report_type=1 on legacy rows, then backfill type 2
  python -m quant_data_engine.tools.migrate_financial_report_type_key
  ./scripts/backfill_report_type2.sh 20240930

  # Sync recent IPOs
  python -m quant_data_engine.tasks.run_financial_sync --recent-ipo 90
"""
from __future__ import annotations
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
import argparse
from datetime import date, datetime, timedelta
from typing import Any

from quant_data_engine.config.settings import get_settings
from quant_data_engine.adapters.stock_adapter import StockAdapter
from quant_data_engine.repositories.financial_repository import FinancialRepository
from quant_data_engine.pipelines.financial_pipeline import FinancialPipeline
from quant_data_engine.utils.ingestion_job import record_ingestion_run


def parse_args():
    p = argparse.ArgumentParser(description="Run financial data synchronization")
    
    # Stock selection
    stock_group = p.add_mutually_exclusive_group(required=True)
    stock_group.add_argument("--codes", nargs="+", help="Stock ts_codes (e.g., 000001.SZ 000002.SZ)")
    stock_group.add_argument("--all", action="store_true", help="Sync all listed stocks")
    stock_group.add_argument("--recent-ipo", type=int, help="Sync stocks listed in recent N days")
    
    # Date/period
    p.add_argument("--start", type=str, help="Start date YYYYMMDD")
    p.add_argument("--end", type=str, help="End date YYYYMMDD")
    p.add_argument("--period", type=str, help="Specific report period YYYYMMDD (full market with --all)")
    p.add_argument(
        "--period-range",
        type=str,
        metavar="START:END",
        help=(
            "With --all, sync every quarter-end in [START,END] (YYYYMMDD:YYYYMMDD e.g. 20200630:20251231). "
            "Fetches income/cashflow report_type 1+2 unless --no-single-quarter. Mutually exclusive with --period."
        ),
    )
    
    # Data types
    p.add_argument("--data-types", type=str,
                   help="Comma-separated data types: income,balance,cashflow,financial_indicator,dividend,mainbz "
                        "(default: income,balance,cashflow,financial_indicator). dividend/mainbz are per-stock only.")
    
    # Options
    p.add_argument("--test", action="store_true", help="Test mode: only sync first 10 stocks")
    p.add_argument("--force", action="store_true", help="Force update (currently not used)")
    p.add_argument("--verbose", action="store_true", help="Verbose output")
    p.add_argument(
        "--no-single-quarter",
        action="store_true",
        help="Do not fetch report_type=2 (单季合并) for income/cashflow; only type 1 (合并报表).",
    )

    return p.parse_args()


def _parse_yyyymmdd(s: str) -> date:
    raw = s.replace("-", "")[:8]
    if len(raw) != 8 or not raw.isdigit():
        raise ValueError(f"Not YYYYMMDD: {s!r}")
    y, m, d = int(raw[0:4]), int(raw[4:6]), int(raw[6:8])
    return date(y, m, d)


def quarter_ends_inclusive(ymd_start: str, ymd_end: str) -> list[str]:
    """All A-share report quarter-ends in [ymd_start, ymd_end]: 0331, 0630, 0930, 1231 per year."""
    a = _parse_yyyymmdd(ymd_start)
    b = _parse_yyyymmdd(ymd_end)
    if a > b:
        raise ValueError(f"Start after end: {ymd_start} > {ymd_end}")
    out: list[str] = []
    for y in range(a.year, b.year + 1):
        for month, day in (3, 31), (6, 30), (9, 30), (12, 31):
            dt = date(y, month, day)
            if a <= dt <= b:
                out.append(dt.strftime("%Y%m%d"))
    return out


def _parse_period_range(s: str) -> tuple[str, str]:
    s = s.strip()
    if ":" in s:
        a, b = s.split(":", 1)
    else:
        raise ValueError("Use YYYYMMDD:YYYMMDD, e.g. 20200630:20251231")
    return a.strip(), b.strip()


def _merge_full_market_results(accum: dict[str, Any] | None, one: dict[str, Any]) -> dict[str, Any]:
    if accum is None:
        out = dict(one)
        out["periods_completed"] = [one.get("period", "")]
        return out
    m: dict[str, Any] = dict(accum)
    m["total_processed"] = (m.get("total_processed") or 0) + (one.get("total_processed") or 0)
    m["success"] = (m.get("success") or 0) + (one.get("success") or 0)
    m["failed"] = (m.get("failed") or 0) + (one.get("failed") or 0)
    for k in (
        "income_records",
        "balance_records",
        "cashflow_records",
        "indicator_records",
        "dividend_records",
        "mainbz_records",
        "income_type1",
        "income_type2",
        "cashflow_type1",
        "cashflow_type2",
    ):
        m[k] = (m.get(k) or 0) + (one.get(k) or 0)
    m["results"] = (m.get("results") or []) + (one.get("results") or [])
    m["periods_completed"] = m.get("periods_completed", []) + [one.get("period", "")]
    m["mode"] = "full_market_by_period_range"
    p0 = m["periods_completed"][0]
    p1 = m["periods_completed"][-1]
    m["period"] = f"{p0}..{p1} ({len(m['periods_completed'])} periods)"
    m["sync_single_quarter"] = m.get("sync_single_quarter", one.get("sync_single_quarter"))
    return m


def main():
    args = parse_args()
    if args.period_range and not args.all:
        print("❌ --period-range 仅能与 --all 同用（全市场按季末拉取，需 Tushare VIP 接口）")
        return
    settings = get_settings()
    job_date = datetime.today().strftime("%Y%m%d")

    # Initialize components
    adapter = StockAdapter(settings.tushare_token, rate_limit_per_sec=settings.rate_limit_per_sec)
    repository = FinancialRepository(settings.mongo_uri, settings.data_db)
    pipeline = FinancialPipeline(adapter=adapter, repository=repository)
    
    ts_codes: list[str] = []
    if args.all and args.period and args.period_range:
        print("❌ 请只使用 --period 或 --period-range 之一")
        return
    use_full_market_period = bool(args.all and (args.period or args.period_range))
    use_full_market_range = bool(args.all and args.period_range)

    if use_full_market_period:
        if args.test:
            print("ℹ️  --test 与 --all --period 全市场 VIP 同时使用：不裁剪股票列表（全市场按季一次拉取）。")
    else:
        # Get stock list
        if args.codes:
            ts_codes = args.codes
        elif args.all:
            df = adapter.fetch_stock_basic(list_status='L')
            if df is None or df.empty:
                print("❌ Failed to fetch stock list")
                return
            ts_codes = df['ts_code'].tolist()
        elif args.recent_ipo:
            cutoff_date = (datetime.today() - timedelta(days=args.recent_ipo)).strftime("%Y%m%d")
            df = adapter.fetch_stock_basic(list_status='L')
            if df is None or df.empty:
                print("❌ Failed to fetch stock list")
                return
            # Filter by list_date
            if 'list_date' in df.columns:
                df = df[df['list_date'] >= cutoff_date]
            ts_codes = df['ts_code'].tolist()
        else:
            print("❌ Must specify stock selection method")
            return

        # Test mode: limit to first 10
        if args.test:
            ts_codes = ts_codes[:10]
            print(f"🧪 Test mode: limiting to {len(ts_codes)} stocks")
    
    # Parse data types
    data_types = None
    if args.data_types:
        data_types = [dt.strip() for dt in args.data_types.split(',')]
        valid_types = ['income', 'balance', 'cashflow', 'financial_indicator', 'dividend', 'mainbz']
        for dt in data_types:
            if dt not in valid_types:
                print(f"❌ Invalid data type: {dt}. Valid: {valid_types}")
                return
    
    print("🟡 Financial Data Synchronization")
    if use_full_market_period:
        print(f"   Mode: full-market VIP by period (no per-stock loop)")
        if use_full_market_range:
            try:
                pr = _parse_period_range(args.period_range)
            except ValueError as e:
                print(f"❌ --period-range: {e}")
                return
            try:
                qe = quarter_ends_inclusive(pr[0], pr[1])
            except ValueError as e:
                print(f"❌ {e}")
                return
            if not qe:
                print("❌ 该区间内无季末日期。")
                return
            print(f"   Period range: {pr[0]} → {pr[1]}  (共 {len(qe)} 个季末)")
        else:
            print(f"   Period: {args.period.replace('-', '')[:8]}")
    else:
        print(f"   Stocks: {len(ts_codes)}")
        print(f"   Date range: {args.start or 'N/A'} -> {args.end or 'N/A'}")
        print(f"   Period: {args.period or 'N/A'}")
    print(f"   Data types: {data_types or 'all'}")
    if not args.no_single_quarter:
        print("   report_type: income/cashflow fetch 1 (合并) + 2 (单季)")

    try:
        sq = not args.no_single_quarter
        if use_full_market_range:
            pr = _parse_period_range(args.period_range)
            periods = quarter_ends_inclusive(pr[0], pr[1])
            result: dict[str, Any] | None = None
            for i, per in enumerate(periods):
                print(f"\n--- 季末 {i + 1}/{len(periods)}: {per} ---")
                r = pipeline.run_full_market_by_period(
                    per,
                    data_types=data_types,
                    sync_single_quarter=sq,
                )
                result = _merge_full_market_results(result, r)
            assert result is not None
        elif use_full_market_period:
            result = pipeline.run_full_market_by_period(
                args.period.replace("-", "")[:8],
                data_types=data_types,
                sync_single_quarter=sq,
            )
        else:
            result = pipeline.run(
                ts_codes=ts_codes,
                start_date=args.start,
                end_date=args.end,
                period=args.period,
                data_types=data_types,
                sync_single_quarter=sq,
            )
    except Exception as e:
        record_ingestion_run(settings, "financial_sync", job_date, status="failed", error=str(e))
        raise
    
    print(f"\n✅ Synchronization completed:")
    print(f"   Total processed: {result['total_processed']}")
    print(f"   Success: {result['success']}")
    print(f"   Failed: {result['failed']}")
    print(f"   Income records: {result['income_records']}")
    if result.get("income_type1") is not None:
        print(f"     (type1 / type2): {result.get('income_type1', 0)} / {result.get('income_type2', 0)}")
    print(f"   Balance records: {result['balance_records']}")
    print(f"   Cashflow records: {result['cashflow_records']}")
    if result.get("cashflow_type1") is not None:
        print(f"     (type1 / type2): {result.get('cashflow_type1', 0)} / {result.get('cashflow_type2', 0)}")
    print(f"   Indicator records: {result['indicator_records']}")
    print(f"   Dividend records: {result.get('dividend_records', 0)}")
    print(f"   Mainbz records: {result.get('mainbz_records', 0)}")
    
    # Show errors if any（默认也打印，避免全市场模式下 balance 等抛错却只看到 income 有数）
    failed_results = [r for r in result['results'] if r.issues]
    if failed_results:
        cap = 20 if args.verbose else 5
        print(f"\n⚠️  含 issues 的结果（{len(failed_results)} 条，最多展示 {cap} 条）:")
        for r in failed_results[:cap]:
            print(f"   {r.ts_code}: {'; '.join(r.issues)}")
        if len(failed_results) > cap:
            print(f"   ... 共 {len(failed_results)} 条，加 --verbose 可看更多")
    touches = (
        int(result.get("income_records", 0))
        + int(result.get("balance_records", 0))
        + int(result.get("cashflow_records", 0))
        + int(result.get("indicator_records", 0))
        + int(result.get("dividend_records", 0))
        + int(result.get("mainbz_records", 0))
    )
    record_ingestion_run(
        settings,
        "financial_sync",
        job_date,
        records_changed=touches,
        mode=result.get("mode"),
        period=result.get("period"),
    )


if __name__ == "__main__":
    main()
