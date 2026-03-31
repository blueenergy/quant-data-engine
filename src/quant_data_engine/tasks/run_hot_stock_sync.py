"""CLI task to sync hot stock data (THS/DC) from Tushare to quant_data.hot_stock_raw.

Usage:
  python -m quant_data_engine.tasks.run_hot_stock_sync --source ths
  python -m quant_data_engine.tasks.run_hot_stock_sync --source dc
  python -m quant_data_engine.tasks.run_hot_stock_sync --source all
"""
from __future__ import annotations
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
import argparse
import logging
from datetime import datetime, timedelta
from pymongo import MongoClient
from quant_data_engine.config.settings import get_settings
from quant_data_engine.adapters.tushare_adapter import TushareAdapter

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

SOURCE_CONFIG = {
    "ths": {"data_type": "热股", "source_name": "同花顺", "fetch_method": "fetch_ths_hot"},
    "dc": {"data_type": "A股市场", "source_name": "东方财富", "fetch_method": "fetch_dc_hot"},
}


def sync_hot_stock(adapter: TushareAdapter, source: str, data_db) -> dict:
    """Fetch hot stock data from Tushare and store in quant_data.hot_stock_raw."""
    cfg = SOURCE_CONFIG.get(source)
    if not cfg:
        return {"success": False, "error": f"Unknown source: {source}"}

    fetch_fn = getattr(adapter, cfg["fetch_method"])

    # Deterministic date fallback: try today, yesterday, ... up to 4 days back
    df = None
    latest_date = None
    for offset in range(0, 5):
        trade_date = (datetime.now() - timedelta(days=offset)).strftime("%Y%m%d")
        try:
            df_try = fetch_fn(trade_date=trade_date)
            if df_try is not None and not df_try.empty and len(df_try) >= 20:
                df = df_try
                latest_date = trade_date
                logger.info(f"[HotStock] Found {len(df)} rows for {source} on {trade_date}")
                break
        except Exception as e:
            logger.debug(f"[HotStock] trade_date={trade_date} failed: {e}")

    if df is None or df.empty:
        return {"success": False, "error": f"No data found for {source} in last 5 days"}

    # Filter by data_type
    if "data_type" in df.columns:
        filtered = df[df["data_type"] == cfg["data_type"]]
        if not filtered.empty:
            df = filtered

    # Deduplicate by ts_code (keep latest rank_time)
    rank_time = None
    if "rank_time" in df.columns:
        rank_time = df["rank_time"].max()
        if "ts_code" in df.columns:
            df = df.sort_values("rank_time", ascending=False).drop_duplicates(
                subset=["ts_code"], keep="first"
            ).copy()

    trade_date = latest_date or datetime.now().strftime("%Y%m%d")
    rows = df.to_dict("records")

    # Upsert into hot_stock_raw
    raw_col = data_db["hot_stock_raw"]
    raw_col.update_one(
        {"source": source, "trade_date": trade_date},
        {"$set": {
            "source": source,
            "source_name": cfg["source_name"],
            "trade_date": trade_date,
            "rank_time": rank_time,
            "data_type": cfg["data_type"],
            "stock_count": len(rows),
            "rows": rows,
            "fetched_at": datetime.now(),
        }},
        upsert=True,
    )

    logger.info(f"[HotStock] Stored {len(rows)} {source} stocks (rank_time={rank_time})")
    return {
        "success": True,
        "source": source,
        "trade_date": trade_date,
        "rank_time": rank_time,
        "stock_count": len(rows),
    }


def parse_args():
    p = argparse.ArgumentParser(description="Sync hot stock data from Tushare")
    p.add_argument("--source", choices=["ths", "dc", "all"], default="all",
                   help="Data source: ths, dc, or all (default: all)")
    return p.parse_args()


def main():
    args = parse_args()
    settings = get_settings()

    adapter = TushareAdapter(settings.tushare_token, settings.rate_limit_per_sec)
    client = MongoClient(settings.mongo_uri)
    data_db = client[settings.data_db]

    sources = ["ths", "dc"] if args.source == "all" else [args.source]

    for src in sources:
        logger.info(f"=== Syncing {src.upper()} hot stock data ===")
        result = sync_hot_stock(adapter, src, data_db)
        if result["success"]:
            logger.info(f"[{src.upper()}] Done: {result['stock_count']} stocks, rank_time={result['rank_time']}")
        else:
            logger.error(f"[{src.upper()}] Failed: {result['error']}")

    client.close()


if __name__ == "__main__":
    main()
