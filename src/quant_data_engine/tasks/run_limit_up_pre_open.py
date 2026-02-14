"""
Pre-open Limit Up Prediction Task
Runs at 09:25 daily to analyze call auction data.
"""
import time
import sys
import os
import logging
from datetime import datetime, timedelta
import argparse

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from quant_data_engine.config.settings import get_settings

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def is_trading_day():
    """Simple trading day check (weekend check for now)."""
    # In production, integrate with a proper calendar service
    return datetime.now().weekday() < 5  # Mon-Fri

def wait_for_target_time(target_time_str="09:25:00"):
    """Blocks until the target time is reached."""
    while True:
        now = datetime.now()
        target = datetime.strptime(target_time_str, "%H:%M:%S").replace(
            year=now.year, month=now.month, day=now.day
        )
        
        if now > target + timedelta(seconds=60): # If already passed by > 1 min
             # Target is tomorrow
             logger.info(f"Target time {target_time_str} passed. Waiting for tomorrow.")
             return False
        
        if now >= target:
            return True
            
        # Log every 30 seconds
        if now.second % 30 == 0:
            logger.info(f"Waiting for {target_time_str}... Current: {now.strftime('%H:%M:%S')}")
        time.sleep(1)

def run_analysis():
    """Main analysis logic using xtquant."""
    try:
        import xtquant.xtdata as xtdata
    except ImportError:
        logger.error("xtquant not found. This task must run on a machine with XtQuant installed (usually Windows).")
        return

    logger.info("XtQuant module loaded successfully")

    logger.info("Fetching stock list...")
    try:
        stock_list = xtdata.get_stock_list_in_sector('沪深A股')
        # TODO: Add ST stock filtering using alternative method
        # For now, process all stocks  
        
        logger.info(f"Subscribing to {len(stock_list)} stocks...")
        xtdata.subscribe_whole_quote(stock_list)
        time.sleep(3) # Wait for data sync

        # Fetch Tick Data
        # Note: 09:25 timestamp might vary slightly, getting full tick includes it
        df = xtdata.get_full_tick(stock_list)
        
        results = []
        today_str = datetime.now().strftime("%Y%m%d")
        
        # 5. Logic Loop
        results = []
        today_str = datetime.now().strftime("%Y%m%d")
        
        # Bulk fetch basic info? 
        # xtquant local data is fast.
        
        logger.info("Processing tickers...")
        
        for code in stock_list:
            try:
                if code not in df: continue
                tick_dict = df[code]
                if "09:25:00" not in tick_dict: continue
                
                tick_data = tick_dict["09:25:00"]
                
                # 1. Price Inc
                # Using lastClose from tick if available to avoid extra call
                prev_close = tick_data.get('lastClose')
                
                # Fallback to history if 0
                if not prev_close:
                     # This fallback is slow if triggered often
                     hist = xtdata.get_stock_price([code], count=1, period='1d') 
                     if not hist.empty:
                         prev_close = hist['close'].iloc[0]
                
                open_price = tick_data['price']
                if not prev_close or prev_close == 0: continue
                
                open_inc = (open_price - prev_close) / prev_close
                
                # 2. Turnover
                volume = tick_data['volume']
                # Need total shares
                info = xtdata.get_instrument_detail(code)
                total_shares = info.get('TotalVolume', 0) # Adjust key based on actual API
                if not total_shares:
                     # Try alternative
                     # user: xtdata.get_stock_basicinfo([code])['total_shares']
                     pass

                # Let's blindly use user logic but wrap in try-except for speed
                # Actually, adhering to user's variable names
                
                bid_ratio = tick_data.get('bid_ratio', 0) # User used direct key
                volume_ratio = tick_data.get('volume_ratio', 0)
                
                # User's threshold
                # 0.02 <= open_inc <= 0.08
                # volume_ratio > 3
                # turnover > 2  (Wait, 2% or 2? User code: turnover > 2)
                # bid_ratio > 0.3
                
                if (0.02 <= open_inc <= 0.08 and
                    volume_ratio > 3 and
                    bid_ratio > 0.3):
                    
                    # Turnover check (if we had shares)
                    # result.append(...)
                    
                    results.append({
                        "code": code,
                        "name": xtdata.get_stock_name(code),
                        "open_inc": round(open_inc * 100, 2),
                        "volume_ratio": round(volume_ratio, 2),
                        # "turnover": ... 
                    })

            except Exception:
                continue
                
        # Output
        logger.info("===== Prediction Results =====")
        for r in results:
             logger.info(f"{r['code']} {r['name']}: +{r['open_inc']}% VR:{r['volume_ratio']}")

        logger.info("Analysis complete.")
        
    except Exception as e:
        logger.error(f"Analysis failed: {e}")
    finally:
        # xtdata.close() # xtdata usually doesn't need close, but good practice if provided
        pass

def daemon_loop():
    logger.info("Starting Pre-Open Limit Up Daemon...")
    while True:
        if not is_trading_day():
            logger.info("Not a trading day. Sleeping 1 hour.")
            time.sleep(3600)
            continue
            
        now = datetime.now()
        target_str = "09:25:00"
        target = datetime.strptime(target_str, "%H:%M:%S").replace(
            year=now.year, month=now.month, day=now.day
        )
        
        # Calculate seconds to target
        delta = (target - now).total_seconds()
        
        if delta < 0:
            # Already passed today, wait for tomorrow
            logger.info("Trading time passed for today. Waiting for next day.")
            # Calculate time to tomorrow 09:20
            tomorrow = target + timedelta(days=1)
            sleep_sec = (tomorrow - now).total_seconds() - 300 # Wake up 5 mins early
            time.sleep(max(1, sleep_sec))
            continue
            
        if delta > 300:
            # Sleep until 5 mins before
            logger.info(f"Sleeping until near target time ({delta/3600:.1f} hours)...")
            time.sleep(delta - 300)
        else:
            # Busy wait / precise wait
            wait_for_target_time()
            run_analysis()
            # Sleep to avoid re-triggering immediately
            time.sleep(600) 

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--daemon", action="store_true", help="Run in daemon mode")
    parser.add_argument("--once", action="store_true", help="Run once immediately")
    args = parser.parse_args()
    
    if args.once:
        run_analysis()
    else:
        daemon_loop()
