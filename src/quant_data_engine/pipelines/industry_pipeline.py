"""Pipeline for industry and ETF data sync and aggregation."""
from __future__ import annotations
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import pandas as pd

from quant_data_engine.adapters.industry_adapter import IndustryAdapter
from quant_data_engine.repositories.industry_repository import IndustryRepository
from quant_data_engine.repositories.ladder_repository import LadderRepository

logger = logging.getLogger(__name__)

class IndustryPipeline:
    def __init__(self, adapter: IndustryAdapter, repository: IndustryRepository, ladder_repo: LadderRepository):
        self.adapter = adapter
        self.repository = repository
        self.ladder_repo = ladder_repo

    def sync_industry_list(self) -> int:
        """Discover industry list from existing limit-up data."""
        industries = self.ladder_repo.daily_limit_up.distinct("primary_sector")
        if not industries:
            logger.warning("No industries found in daily_limit_up collection.")
            return 0
        
        logger.info(f"Discovered {len(industries)} industries from limit-up data.")
        # For our purposes, the 'list' is now dynamic.
        return len(industries)

    def sync_industries_daily(self, start_date: str, end_date: str) -> int:
        """Sync daily OHLC for industry boards (Currently disabled/skipped)."""
        logger.info("Industry index OHLC sync is currently disabled (No reliable source).")
        return 0

    def sync_etfs_daily(self, etf_map: Dict[str, str], start_date: str, end_date: str) -> int:
        """Sync daily OHLC for a list of ETFs using Baostock."""
        total_upserted = 0
        for code, name in etf_map.items():
            logger.info(f"Syncing daily data for ETF: {name} ({code}) using Baostock")
            df = self.adapter.fetch_etf_daily(code, start_date, end_date)
            if not df.empty:
                records = []
                for _, r in df.iterrows():
                    # Baostock columns: date,open,high,low,close,pctChg,vol,amount
                    records.append({
                        "etf_code": code,
                        "etf_name": name,
                        "date": r["date"].replace("-", ""),
                        "open": float(r["open"]),
                        "close": float(r["close"]),
                        "high": float(r["high"]),
                        "low": float(r["low"]),
                        "pct_chg": float(r["pctChg"]),
                        "volume": float(r["volume"]),
                        "amount": float(r["amount"])
                    })
                total_upserted += self.repository.upsert_etf_daily(records)
        return total_upserted

    def run_aggregation(self, date: str):
        """Run limit-up aggregation for weekly/monthly statistics."""
        # 1. Discover industries from DB
        industries = self.ladder_repo.daily_limit_up.distinct("primary_sector")
        if not industries:
            return
        
        # 2. Define windows
        dt = datetime.strptime(date, "%Y%m%d")
        windows = {
            "weekly": 5,
            "monthly": 20
        }
        
        from pymongo import UpdateOne
        all_stats = []
        for period_name, days in windows.items():
            # Range buffer
            start_dt = dt - timedelta(days=days * 3) 
            start_date = start_dt.strftime("%Y%m%d")
            
            for name in industries:
                if not name: continue
                
                # Query daily_limit_up for this industry and date range
                # primary_sector or limit_reason? LadderRepository uses primary_sector.
                pipeline = [
                    {
                        "$match": {
                            "date": {"$gte": start_date, "$lte": date},
                            "primary_sector": name,
                            "status": {"$ne": "broken"}
                        }
                    },
                    {
                        "$group": {
                            "_id": "$date",
                            "count": {"$sum": 1}
                        }
                    },
                    {
                        "$sort": {"_id": -1}
                    },
                    {
                        "$limit": days
                    }
                ]
                results = list(self.ladder_repo.daily_limit_up.aggregate(pipeline))
                
                if results:
                    counts = [r["count"] for r in results]
                    avg_count = sum(counts) / len(counts)
                    max_count = max(counts)
                    total_limitups = sum(counts)
                    
                    stats_doc = {
                        "industry_name": name,
                        "period": period_name,
                        "date": date,
                        "avg_limitups": round(avg_count, 2),
                        "max_limitups": max_count,
                        "total_limitups": total_limitups,
                        "days_active": len(results),
                        "updated_at": datetime.now()
                    }
                    all_stats.append(UpdateOne(
                        {"industry_name": name, "period": period_name, "date": date},
                        {"$set": stats_doc},
                        upsert=True
                    ))
        
        if all_stats:
            from pymongo import UpdateOne # Local import just in case
            self.repository.stats_coll.bulk_write(all_stats, ordered=False)
            logger.info(f"Aggregated statistics for {len(all_stats)} industry-periods on {date}")

__all__ = ["IndustryPipeline"]
