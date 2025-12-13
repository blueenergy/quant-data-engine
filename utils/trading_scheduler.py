"""Trading time scheduler for A-share market."""

import time
from datetime import datetime, time as dt_time


class TradingScheduler:
    """A-share market trading hours scheduler.
    
    Trading hours:
    - Morning session: 09:30 - 11:30
    - Afternoon session: 13:00 - 15:00
    - Weekends: Closed
    
    Note: This does not account for holidays. For production use,
    integrate with a calendar library (e.g., chinese_calendar).
    """
    
    # Trading hours (hour, minute)
    MORNING_START = (9, 30)
    MORNING_END = (11, 30)
    AFTERNOON_START = (13, 0)
    AFTERNOON_END = (15, 0)
    
    def is_trading_time(self, dt: datetime = None) -> bool:
        """Check if current time is within trading hours.
        
        Args:
            dt: Datetime to check. If None, uses current time.
            
        Returns:
            bool: True if within trading hours, False otherwise
        """
        if dt is None:
            dt = datetime.now()
        
        # Check if weekend
        if dt.weekday() >= 5:  # Saturday = 5, Sunday = 6
            return False
        
        current_time = (dt.hour, dt.minute)
        
        # Morning session
        if self.MORNING_START <= current_time <= self.MORNING_END:
            return True
        
        # Afternoon session
        if self.AFTERNOON_START <= current_time <= self.AFTERNOON_END:
            return True
        
        return False
    
    def is_trading_day(self, dt: datetime = None) -> bool:
        """Check if current day is a trading day.
        
        Args:
            dt: Date to check. If None, uses current date.
            
        Returns:
            bool: True if trading day, False otherwise
        """
        if dt is None:
            dt = datetime.now()
        
        # Simple check: not weekend
        # TODO: Add holiday calendar integration
        return dt.weekday() < 5
    
    def wait_until_trading_time(self):
        """Block until next trading session starts.
        
        This method will sleep until the market opens, checking
        every 60 seconds. It prints status messages periodically.
        """
        while not self.is_trading_time():
            now = datetime.now()
            
            # Calculate next trading time
            if now.hour < 9 or (now.hour == 9 and now.minute < 30):
                next_hour, next_minute = self.MORNING_START
                wait_msg = f"morning open ({next_hour:02d}:{next_minute:02d})"
            elif now.hour < 13:
                next_hour, next_minute = self.AFTERNOON_START
                wait_msg = f"afternoon open ({next_hour:02d}:{next_minute:02d})"
            else:
                # Next trading day morning
                import datetime as dt_module
                next_day = now + dt_module.timedelta(days=1)
                # Skip weekends
                while next_day.weekday() >= 5:
                    next_day += dt_module.timedelta(days=1)
                next_hour, next_minute = self.MORNING_START
                wait_msg = f"next trading day ({next_day.strftime('%Y-%m-%d')} {next_hour:02d}:{next_minute:02d})"
            
            print(f"⏰ Non-trading time. Waiting for {wait_msg}")
            print(f"   Current: {now.strftime('%Y-%m-%d %H:%M:%S')}")
            
            # Sleep in 60s intervals
            time.sleep(60)
    
    def get_next_trading_time(self, dt: datetime = None) -> datetime:
        """Get the next trading time.
        
        Args:
            dt: Reference datetime. If None, uses current time.
            
        Returns:
            datetime: Next trading time
        """
        if dt is None:
            dt = datetime.now()
        
        # If already in trading time, return current time
        if self.is_trading_time(dt):
            return dt
        
        # Calculate next trading time
        import datetime as dt_module
        
        # Same day afternoon session
        if dt.hour < 13 and dt.weekday() < 5:
            return dt.replace(
                hour=self.AFTERNOON_START[0],
                minute=self.AFTERNOON_START[1],
                second=0,
                microsecond=0
            )
        
        # Next trading day morning session
        next_day = dt + dt_module.timedelta(days=1)
        while next_day.weekday() >= 5:  # Skip weekends
            next_day += dt_module.timedelta(days=1)
        
        return next_day.replace(
            hour=self.MORNING_START[0],
            minute=self.MORNING_START[1],
            second=0,
            microsecond=0
        )
