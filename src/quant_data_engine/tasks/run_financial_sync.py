"""CLI task to run financial data synchronization.

Collects financial statements and indicators from Tushare:
  - Income statement
  - Balance sheet
  - Cash flow statement
  - Financial indicators

Environment:
  MONGO_URL / MONGO_DB / TUSHARE_TOKEN via settings
  
Usage:
  # Sync specific stocks
  python -m quant_data_engine.tasks.run_financial_sync --codes 000001.SZ 000002.SZ --start 20230101
  
  # Sync all listed stocks (test mode)
  python -m quant_data_engine.tasks.run_financial_sync --all --test
  
  # Sync only specific data types
  python -m quant_data_engine.tasks.run_financial_sync --codes 000001.SZ --data-types income,balance
  
  # Sync recent IPOs
  python -m quant_data_engine.tasks.run_financial_sync --recent-ipo 90
"""
from __future__ import annotations
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
import argparse
from datetime import datetime, timedelta
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
    p.add_argument("--period", type=str, help="Specific report period YYYYMMDD")
    
    # Data types
    p.add_argument("--data-types", type=str,
                   help="Comma-separated data types: income,balance,cashflow,financial_indicator (default: all)")
    
    # Options
    p.add_argument("--test", action="store_true", help="Test mode: only sync first 10 stocks")
    p.add_argument("--force", action="store_true", help="Force update (currently not used)")
    p.add_argument("--verbose", action="store_true", help="Verbose output")
    
    return p.parse_args()


def main():
    args = parse_args()
    settings = get_settings()
    job_date = datetime.today().strftime("%Y%m%d")

    # Initialize components
    adapter = StockAdapter(settings.tushare_token, rate_limit_per_sec=settings.rate_limit_per_sec)
    repository = FinancialRepository(settings.mongo_uri, settings.data_db)
    pipeline = FinancialPipeline(adapter=adapter, repository=repository)
    
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
        valid_types = ['income', 'balance', 'cashflow', 'financial_indicator']
        for dt in data_types:
            if dt not in valid_types:
                print(f"❌ Invalid data type: {dt}. Valid: {valid_types}")
                return
    
    print(f"🟡 Financial Data Synchronization")
    print(f"   Stocks: {len(ts_codes)}")
    print(f"   Data types: {data_types or 'all'}")
    print(f"   Date range: {args.start or 'N/A'} -> {args.end or 'N/A'}")
    print(f"   Period: {args.period or 'N/A'}")
    
    try:
        result = pipeline.run(
            ts_codes=ts_codes,
            start_date=args.start,
            end_date=args.end,
            period=args.period,
            data_types=data_types,
        )
    except Exception as e:
        record_ingestion_run(settings, "financial_sync", job_date, status="failed", error=str(e))
        raise
    
    print(f"\n✅ Synchronization completed:")
    print(f"   Total processed: {result['total_processed']}")
    print(f"   Success: {result['success']}")
    print(f"   Failed: {result['failed']}")
    print(f"   Income records: {result['income_records']}")
    print(f"   Balance records: {result['balance_records']}")
    print(f"   Cashflow records: {result['cashflow_records']}")
    print(f"   Indicator records: {result['indicator_records']}")
    
    # Show some errors if any
    failed_results = [r for r in result['results'] if r.issues]
    if failed_results and args.verbose:
        print(f"\n⚠️  Failed stocks:")
        for r in failed_results[:10]:  # Show first 10
            print(f"   {r.ts_code}: {', '.join(r.issues)}")
    record_ingestion_run(
        settings,
        "financial_sync",
        job_date,
        records_changed=int(result.get("total_processed") or 0),
    )


if __name__ == "__main__":
    main()
