"""CLI task to run index daily pipeline."""
from __future__ import annotations
import argparse
from datetime import datetime, timedelta
from quant_data_engine import create_index_pipeline


def parse_args():
    p = argparse.ArgumentParser(description="Run index daily pipeline")
    p.add_argument("--codes", nargs="+", required=True, help="Index ts_codes e.g. 000300.SH 000905.SH")
    p.add_argument("--backfill-days", type=int, default=3, help="Backfill days tolerance")
    p.add_argument("--start", type=str, default=None, help="Start date YYYYMMDD (optional)")
    p.add_argument("--end", type=str, default=None, help="End date YYYYMMDD (optional)")
    return p.parse_args()


def main():
    args = parse_args()
    today = datetime.today()
    end = args.end or today.strftime("%Y%m%d")
    start = args.start or (today - timedelta(days=365)).strftime("%Y%m%d")
    pipeline = create_index_pipeline()
    out = pipeline.run(args.codes, start_date=start, end_date=end, backfill_days=args.backfill_days)
    print(out)

if __name__ == "__main__":
    main()
