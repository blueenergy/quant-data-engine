from pymongo import MongoClient
from datetime import datetime, timedelta
from collections import defaultdict
import os

def generate_mock_report():
    mongo_uri = "mongodb://admin:veryStrongPassword@180.184.28.170:27017"
    db_name = "finance"
    client = MongoClient(mongo_uri)
    db = client[db_name]
    limit_coll = db["daily_limit_up"]
    
    # 1. Find recent dates
    recent_docs = list(limit_coll.find({}, {"date": 1}).sort("date", -1).limit(100))
    if not recent_docs:
        print("No limit-up data found in daily_limit_up collection.")
        return
        
    dates = sorted(list(set(d["date"] for d in recent_docs)), reverse=True)
    latest_date = dates[0]
    print(f"Latest data found for date: {latest_date}")
    
    # 2. Simulate aggregation for the latest 3 active industries
    # Find most active industries today
    pipeline = [
        {"$match": {"date": latest_date, "status": {"$ne": "broken"}}},
        {"$group": {"_id": "$primary_sector", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 5}
    ]
    top_industries = list(limit_coll.aggregate(pipeline))
    
    print("\n--- Multi-Dimensional Aggregate Simulation (MOCK REPORT) ---")
    print(f"Base Date: {latest_date}")
    print("-" * 50)
    
    for ind in top_industries:
        name = ind["_id"]
        today_count = ind["count"]
        
        # Calculate Weekly (5 days)
        # We'll just use the last 5 dates we found
        window_dates = dates[:5]
        
        weekly_counts = []
        for d in window_dates:
            c = limit_coll.count_documents({"date": d, "primary_sector": name, "status": {"$ne": "broken"}})
            weekly_counts.append(c)
            
        avg_weekly = sum(weekly_counts) / len(weekly_counts)
        total_weekly = sum(weekly_counts)
        
        # Signal Logic (Rough simulation)
        signal = "⚪ NORMAL"
        if today_count > avg_weekly * 2 and avg_weekly > 0:
            signal = "🟢 BREAKOUT"
        elif today_count < avg_weekly * 0.5 and avg_weekly > 2:
            signal = "🔴 DECAY"
            
        print(f"Industry: {name}")
        print(f"  - Today Count: {today_count}")
        print(f"  - Weekly Avg:  {avg_weekly:.2f}")
        print(f"  - Weekly Total: {total_weekly}")
        print(f"  - Signal:      {signal}")
        print("-" * 30)

if __name__ == "__main__":
    generate_mock_report()
