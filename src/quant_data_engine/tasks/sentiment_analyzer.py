"""Shared sentiment analysis logic for market spectrum calculation.

This module provides reusable functions for calculating yang/yin spectrum
across different contexts (standalone CLI, embedded in sync loops, etc.).
"""

from datetime import datetime
import pandas as pd
from pymongo import MongoClient


def calculate_sentiment(
    client: MongoClient,
    db_name: str,
    source_collection_name: str,
    output_collection_name: str,
    start_time: str,
    end_time: str,
    ma_period: int,
    granularity: str,
    min_stocks: int = 100,
    verbose: bool = True
) -> dict:
    """Calculate market sentiment for a time range.
    
    Args:
        client: MongoDB client
        db_name: Database name
        source_collection_name: Source data collection
        output_collection_name: Output collection for results
        start_time: Start time (YYYYMMDD or YYYYMMDDHHMM)
        end_time: End time (YYYYMMDD or YYYYMMDDHHMM)
        ma_period: Moving average period
        granularity: "daily" or "minute"
        min_stocks: Minimum stocks required for valid calculation
        verbose: Print progress messages
        
    Returns:
        dict with keys: 'success', 'results' (list of result docs), 'error' (if failed)
    """
    try:
        db = client[db_name]
        source_collection = db[source_collection_name]
        output_collection = db[output_collection_name]
        
        # Calculate window start for MA
        window_start_time = _calculate_window_start(start_time, ma_period, granularity)
        
        # Query all data in range
        query = {
            "trade_date": {"$gte": window_start_time, "$lte": end_time},
            "close": {"$exists": True, "$ne": None},
            "volume": {"$exists": True, "$ne": None}
        }
        projection = {"symbol": 1, "close": 1, "trade_date": 1, "_id": 0}
        
        all_data = list(source_collection.find(query, projection))
        
        if not all_data:
            if verbose:
                print("⚠️  No data found for sentiment analysis")
            return {"success": False, "error": "No data found", "results": []}
        
        # Build DataFrame and calculate MA
        df = pd.DataFrame(all_data)
        df["close"] = pd.to_numeric(df["close"])
        df = df.sort_values(["symbol", "trade_date"]).reset_index(drop=True)
        
        ma_col = f"ma{ma_period}"
        df[ma_col] = (
            df.groupby("symbol")["close"]
            .rolling(ma_period)
            .mean()
            .reset_index(level=0, drop=True)
        )
        
        # Get list of unique trade_dates in target range
        target_df = df[
            (df["trade_date"] >= start_time) & 
            (df["trade_date"] <= end_time)
        ]
        
        if target_df.empty:
            if verbose:
                print("⚠️  No data in target range for sentiment analysis")
            return {"success": False, "error": "No data in target range", "results": []}
        
        unique_times = sorted(target_df["trade_date"].unique())
        
        # Calculate sentiment for each time period
        results = []
        for trade_date in unique_times:
            period_df = df[df["trade_date"] == trade_date]
            
            # Only count stocks with valid MA (have enough history)
            valid_df = period_df[~period_df[ma_col].isna()]
            total = len(valid_df)
            
            if total < min_stocks:
                if verbose:
                    print(f"⏭️  {trade_date}: Only {total} stocks (< {min_stocks}), skipping")
                continue
            
            # Count stocks above MA
            above_ma = (valid_df["close"] > valid_df[ma_col]).sum()
            yang_ratio = above_ma / total
            yin_ratio = 1 - yang_ratio
            
            if verbose:
                print(
                    f"📊 {trade_date}: "
                    f"Yang {yang_ratio:.2%} ({above_ma}/{total}), "
                    f"Yin {yin_ratio:.2%}"
                )
            
            # Prepare result document
            result_doc = {
                "trade_date": trade_date,
                "yang_spectrum": float(yang_ratio),
                "yin_spectrum": float(yin_ratio),
                "total_stocks": int(total),
                "above_ma_count": int(above_ma),
                "below_ma_count": int(total - above_ma),
                "ma_period": ma_period,
                "granularity": granularity,
                "updated_at": datetime.now().isoformat(),
            }
            
            results.append(result_doc)
            
            # Write to database
            output_collection.update_one(
                {
                    "trade_date": trade_date,
                    "ma_period": ma_period,
                    "granularity": granularity
                },
                {"$set": result_doc},
                upsert=True
            )
        
        return {"success": True, "results": results}
        
    except Exception as e:
        if verbose:
            print(f"❌ Sentiment analysis error: {e}")
            import traceback
            traceback.print_exc()
        return {"success": False, "error": str(e), "results": []}


def _calculate_window_start(start_time: str, ma_period: int, granularity: str) -> str:
    """Calculate earlier start time needed for MA calculation.
    
    Args:
        start_time: Target start time (YYYYMMDD or YYYYMMDDHHMM)
        ma_period: MA period
        granularity: "daily" or "minute"
        
    Returns:
        Earlier start time in same format as input
    """
    from datetime import timedelta
    
    if granularity == "minute":
        dt = datetime.strptime(start_time, "%Y%m%d%H%M")
        window_dt = dt - timedelta(minutes=ma_period * 2)
        return window_dt.strftime("%Y%m%d%H%M")
    else:  # daily
        dt = datetime.strptime(start_time, "%Y%m%d")
        window_dt = dt - timedelta(days=ma_period * 2)
        return window_dt.strftime("%Y%m%d")


def calculate_daily_realtime_sentiment(
    client: MongoClient,
    db_name: str,
    daily_collection_name: str,
    minute_collection_name: str,
    start_time: str,
    end_time: str,
    ma_period: int,
    min_stocks: int = 100,
    verbose: bool = True
) -> dict:
    """Calculate daily-based real-time sentiment using minute closes.

    Uses daily K data to compute MA (e.g. MA5) and compares latest minute close
    against that daily MA for each stock at each minute snapshot.

    Assumptions:
      - start_time / end_time are within the same trading day (YYYYMMDDHHMM)
      - daily_collection has fields: symbol, trade_date (YYYYMMDD), close
      - minute_collection has fields: symbol, trade_date (YYYYMMDDHHMM), close

    Results are written into output collection by the caller.
    """
    try:
        db = client[db_name]
        daily_col = db[daily_collection_name]
        minute_col = db[minute_collection_name]

        # Derive day part from start_time
        day = start_time[:8]

        # 1) Load daily data to build daily MA per symbol
        window_start_day = _calculate_window_start(day, ma_period, "daily")
        daily_query = {
            "trade_date": {"$gte": window_start_day, "$lte": day},
            "close": {"$exists": True, "$ne": None},
        }
        daily_proj = {"symbol": 1, "close": 1, "trade_date": 1, "_id": 0}
        daily_docs = list(daily_col.find(daily_query, daily_proj))

        if not daily_docs:
            if verbose:
                print("⚠️  No daily data found for realtime sentiment")
            return {"success": False, "error": "No daily data", "results": []}

        daily_df = pd.DataFrame(daily_docs)
        daily_df["close"] = pd.to_numeric(daily_df["close"])
        daily_df = daily_df.sort_values(["symbol", "trade_date"]).reset_index(drop=True)

        ma_col = f"ma{ma_period}"
        daily_df[ma_col] = (
            daily_df.groupby("symbol")["close"]
            .rolling(ma_period)
            .mean()
            .reset_index(level=0, drop=True)
        )

        # Take latest daily row per symbol (<= day)
        latest_daily = (
            daily_df.dropna(subset=[ma_col])
            .sort_values(["symbol", "trade_date"])
            .groupby("symbol")
            .tail(1)
        )
        if latest_daily.empty:
            if verbose:
                print("⚠️  No symbols with valid daily MA")
            return {"success": False, "error": "No valid daily MA", "results": []}

        daily_ma_map = latest_daily.set_index("symbol")[ma_col].to_dict()

        # 2) Load minute data for the intraday window
        minute_query = {
            "trade_date": {"$gte": start_time, "$lte": end_time},
            "close": {"$exists": True, "$ne": None},
        }
        minute_proj = {"symbol": 1, "close": 1, "trade_date": 1, "_id": 0}
        minute_docs = list(minute_col.find(minute_query, minute_proj))

        if not minute_docs:
            if verbose:
                print("⚠️  No minute data found for realtime sentiment")
            return {"success": False, "error": "No minute data", "results": []}

        minute_df = pd.DataFrame(minute_docs)
        minute_df["close"] = pd.to_numeric(minute_df["close"])

        # Attach daily MA to each minute row
        minute_df["daily_ma"] = minute_df["symbol"].map(daily_ma_map)
        minute_df = minute_df.dropna(subset=["daily_ma"])

        if minute_df.empty:
            if verbose:
                print("⚠️  No overlapping symbols between daily MA and minute data")
            return {"success": False, "error": "No overlapping symbols", "results": []}

        # 3) Aggregate by minute trade_date
        results = []
        for ts, group in minute_df.groupby("trade_date"):
            total = len(group)
            if total < min_stocks:
                if verbose:
                    print(f"⏭️  {ts}: Only {total} stocks (< {min_stocks}), skipping")
                continue

            above = (group["close"] > group["daily_ma"]).sum()
            yang_ratio = above / total
            yin_ratio = 1 - yang_ratio

            if verbose:
                print(
                    f"📊 {ts}: "
                    f"Yang {yang_ratio:.2%} ({above}/{total}), "
                    f"Yin {yin_ratio:.2%}"
                )

            results.append(
                {
                    "trade_date": ts,
                    "yang_spectrum": float(yang_ratio),
                    "yin_spectrum": float(yin_ratio),
                    "total_stocks": int(total),
                    "above_ma_count": int(above),
                    "below_ma_count": int(total - above),
                    "ma_period": ma_period,
                    "granularity": "daily_realtime",
                    "updated_at": datetime.now().isoformat(),
                }
            )

        return {"success": True, "results": results}

    except Exception as e:
        if verbose:
            print(f"❌ Daily realtime sentiment error: {e}")
            import traceback
            traceback.print_exc()
        return {"success": False, "error": str(e), "results": []}
