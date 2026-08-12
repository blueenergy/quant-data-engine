"""Pipeline for limit-up aggregation by industry."""
from __future__ import annotations
import logging
from datetime import datetime, timedelta

from quant_data_engine.repositories.industry_repository import IndustryRepository
from quant_data_engine.repositories.ladder_repository import LadderRepository

logger = logging.getLogger(__name__)

class IndustryPipeline:
    def __init__(self, repository: IndustryRepository, ladder_repo: LadderRepository):
        self.repository = repository
        self.ladder_repo = ladder_repo

    def run_aggregation(self, date: str) -> int:
        """Run limit-up aggregation for weekly/monthly statistics."""
        # 1. Discover industries from DB
        industries = self.ladder_repo.daily_limit_up.distinct("primary_sector")
        if not industries:
            logger.warning("No industries found in daily_limit_up; nothing to aggregate.")
            return 0
        
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
        
        if not all_stats:
            logger.warning(f"No limit-up rows in range for any industry on {date}")
            return 0

        self.repository.stats_coll.bulk_write(all_stats, ordered=False)
        logger.info(f"Aggregated statistics for {len(all_stats)} industry-periods on {date}")
        return len(all_stats)

__all__ = ["IndustryPipeline"]
