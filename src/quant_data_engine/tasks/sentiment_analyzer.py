"""Shared sentiment analysis logic for market spectrum calculation.

This module provides reusable functions for calculating yang/yin spectrum
across different contexts (standalone CLI, embedded in sync loops, etc.).
"""

from datetime import datetime
import pandas as pd
from pymongo import MongoClient


from pymongo import MongoClient


def _get_trading_days_lookback(collection, target_date: str, n: int) -> str:
    """Get the date N trading days before target_date by querying unique dates in DB."""
    pipeline = [
        {"$match": {"trade_date": {"$lte": target_date}}},
        {"$group": {"_id": "$trade_date"}},
        {"$sort": {"_id": -1}},
        {"$limit": n + 5}  # Buffer to ensure we have enough data for MA
    ]
    dates = list(collection.aggregate(pipeline))
    if not dates:
        return target_date
    return dates[-1]["_id"]


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
        projection = {"symbol": 1, "close": 1, "trade_date": 1, "pct_chg": 1, "_id": 0}
        
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
        limit_up_col = db["daily_limit_up"]
        limit_down_col = db["daily_limit_down"]

        for trade_date in unique_times:
            period_df = df[df["trade_date"] == trade_date]
            
            # Fetch limit-up and limit-down symbols for this date to be precise
            # Only count stocks with status='limit_up' (exclude 炸板 stocks)
            date_nodash = trade_date.replace("-", "")
            lup_symbols = set(d["symbol"] for d in limit_up_col.find({"date": date_nodash, "status": "limit_up"}, {"symbol": 1}))
            ldn_symbols = set(d["symbol"] for d in limit_down_col.find({"date": date_nodash, "status": "limit_down"}, {"symbol": 1}))

            # Only count stocks with valid MA (have enough history)
            valid_df = period_df[~period_df[ma_col].isna()].copy()
            total = len(valid_df)
            
            if total < min_stocks:
                if verbose:
                    print(f"⏭️  {trade_date}: Only {total} stocks (< {min_stocks}), skipping")
                continue
            
            # 11-bin Distribution Calculation
            # 1. Limit Up
            is_lup = valid_df["symbol"].isin(lup_symbols)
            lup_count = is_lup.sum()
            
            # 2. Limit Down
            is_ldn = valid_df["symbol"].isin(ldn_symbols)
            ldn_count = is_ldn.sum()
            
            # 3. Categorize others by pct_chg
            dist = {
                "limit_up": int(lup_count),
                "gt_7": int(((valid_df["pct_chg"] > 7) & ~is_lup).sum()),
                "p5_7": int(((valid_df["pct_chg"] > 5) & (valid_df["pct_chg"] <= 7)).sum()),
                "p2_5": int(((valid_df["pct_chg"] > 2) & (valid_df["pct_chg"] <= 5)).sum()),
                "p0_2": int(((valid_df["pct_chg"] > 0) & (valid_df["pct_chg"] <= 2)).sum()),
                "flat": int((valid_df["pct_chg"] == 0).sum()),
                "m2_0": int(((valid_df["pct_chg"] < 0) & (valid_df["pct_chg"] >= -2)).sum()),
                "m5_2": int(((valid_df["pct_chg"] < -2) & (valid_df["pct_chg"] >= -5)).sum()),
                "m7_5": int(((valid_df["pct_chg"] < -5) & (valid_df["pct_chg"] >= -7)).sum()),
                "lt_m7": int(((valid_df["pct_chg"] < -7) & ~is_ldn).sum()),
                "limit_down": int(ldn_count)
            }

            # Count stocks above MA
            above_ma = (valid_df["close"] > valid_df[ma_col]).sum()
            yang_ratio = above_ma / total
            yin_ratio = 1 - yang_ratio
            
            if verbose:
                print(
                    f"📊 {trade_date}: "
                    f"Yang {yang_ratio:.2%} ({above_ma}/{total}), "
                    f"LimitUp: {lup_count}, LimitDown: {ldn_count}"
                )
            
            # Prepare result document
            result_doc = {
                "trade_date": trade_date,
                "yang_spectrum": float(yang_ratio),
                "yin_spectrum": float(yin_ratio),
                "total_stocks": int(total),
                "above_ma_count": int(above_ma),
                "below_ma_count": int(total - above_ma),
                "distribution": dist,
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
        window_dt = dt - timedelta(days=ma_period * 4) # 4x period covers most holidays
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

        # 1) Load daily data to build daily MA per symbol using actual trading days
        window_start_day = _get_trading_days_lookback(daily_col, day, ma_period)
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
        # We also need pre_close from the target day to calculate intraday pct_chg
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
        # Find pre_close for the target day (which is 'close' of the previous trading day)
        # Actually, volume_price has 'pre_close' for the day itself. 
        # Let's get the record for 'day' specifically.
        target_day_docs = [d for d in daily_docs if d["trade_date"] == day]
        pre_close_map = {d["symbol"]: d.get("pre_close") or d.get("close") for d in target_day_docs}

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

        # Attach daily MA and pre_close to each minute row
        minute_df["daily_ma"] = minute_df["symbol"].map(daily_ma_map)
        minute_df["pre_close"] = minute_df["symbol"].map(pre_close_map)
        minute_df = minute_df.dropna(subset=["daily_ma", "pre_close"])

        if minute_df.empty:
            if verbose:
                print("⚠️  No overlapping symbols between daily MA and minute data")
            return {"success": False, "error": "No overlapping symbols", "results": []}

        # Calculate intraday pct_chg
        minute_df["pct_chg"] = (minute_df["close"] - minute_df["pre_close"]) / minute_df["pre_close"] * 100

        # 3) Aggregate by minute trade_date
        results = []
        limit_up_col = db["daily_limit_up"]
        limit_down_col = db["daily_limit_down"]
        
        # Cache limit symbols for the day
        # Only count stocks with status='limit_up' (exclude 炸板 stocks)
        lup_symbols = set(d["symbol"] for d in limit_up_col.find({"date": day, "status": "limit_up"}, {"symbol": 1}))
        ldn_symbols = set(d["symbol"] for d in limit_down_col.find({"date": day, "status": "limit_down"}, {"symbol": 1}))

        for ts, group in minute_df.groupby("trade_date"):
            total = len(group)
            if total < min_stocks:
                if verbose:
                    print(f"⏭️  {ts}: Only {total} stocks (< {min_stocks}), skipping")
                continue

            above = (group["close"] > group["daily_ma"]).sum()
            yang_ratio = above / total
            yin_ratio = 1 - yang_ratio

            # 11-bin Distribution for Realtime
            is_lup = group["symbol"].isin(lup_symbols)
            is_ldn = group["symbol"].isin(ldn_symbols)
            
            dist = {
                "limit_up": int(is_lup.sum()),
                "gt_7": int(((group["pct_chg"] > 7) & ~is_lup).sum()),
                "p5_7": int(((group["pct_chg"] > 5) & (group["pct_chg"] <= 7)).sum()),
                "p2_5": int(((group["pct_chg"] > 2) & (group["pct_chg"] <= 5)).sum()),
                "p0_2": int(((group["pct_chg"] > 0) & (group["pct_chg"] <= 2)).sum()),
                "flat": int((group["pct_chg"] == 0).sum()),
                "m2_0": int(((group["pct_chg"] < 0) & (group["pct_chg"] >= -2)).sum()),
                "m5_2": int(((group["pct_chg"] < -2) & (group["pct_chg"] >= -5)).sum()),
                "m7_5": int(((group["pct_chg"] < -5) & (group["pct_chg"] >= -7)).sum()),
                "lt_m7": int(((group["pct_chg"] < -7) & ~is_ldn).sum()),
                "limit_down": int(is_ldn.sum())
            }

            if verbose:
                print(
                    f"📊 {ts}: "
                    f"Yang {yang_ratio:.2%} ({above}/{total}), "
                    f"LU: {dist['limit_up']}, LD: {dist['limit_down']}"
                )

            results.append(
                {
                    "trade_date": ts,
                    "yang_spectrum": float(yang_ratio),
                    "yin_spectrum": float(yin_ratio),
                    "total_stocks": int(total),
                    "above_ma_count": int(above),
                    "below_ma_count": int(total - above),
                    "distribution": dist,
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
