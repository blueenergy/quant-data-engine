"""Repository for storing minute-level data in MongoDB."""
from pymongo import MongoClient, UpdateOne
import pandas as pd

class MinuteRepository:
    def __init__(self, mongo_uri: str, mongo_db: str, collection: str = 'minute_bars'):
        self.client = MongoClient(mongo_uri)
        self.db = self.client[mongo_db]
        self.collection = self.db[collection]

    def upsert_minute_bars(self, code: str, df: pd.DataFrame):
        """Upsert minute bars for a given code."""
        # 统一处理 DataFrame 字段
        df = df.copy()
        
        # Handle different time column names from different sources
        time_col = None
        for col in df.columns:
            if col.lower() in ['datetime', 'time', 'timestamp']:
                time_col = col
                break
        
        if time_col is None:
            # Try to use index if it's datetime
            if isinstance(df.index, pd.DatetimeIndex):
                df = df.reset_index()
                time_col = df.columns[0]
        
        if time_col and time_col != 'datetime':
            df.rename(columns={time_col: 'datetime'}, inplace=True)
        
        if 'datetime' not in df.columns:
            raise ValueError(f"No datetime column found. Available columns: {df.columns.tolist()}")
        
        # Convert time column
        if df['datetime'].dtype in ['float64', 'int64', 'float', 'int']:
            # Detect if it's millisecond or second timestamp
            # Timestamps < 100000000000 (100 billion) are likely seconds
            # Timestamps >= 100000000000 are likely milliseconds
            is_ms = (df['datetime'] >= 100000000000).any()
            
            if is_ms:
                # Convert from milliseconds to Beijing time (UTC+8)
                df['datetime'] = pd.to_datetime(df['datetime'], unit='ms') + pd.Timedelta(hours=8)
            else:
                # Convert from seconds to Beijing time (UTC+8)
                df['datetime'] = pd.to_datetime(df['datetime'], unit='s') + pd.Timedelta(hours=8)
        else:
            df['datetime'] = pd.to_datetime(df['datetime'])
        
        df['trade_date'] = df['datetime'].dt.strftime('%Y%m%d%H%M')
        df['symbol'] = code
        if 'vol' in df.columns:
            df.rename(columns={'vol': 'volume'}, inplace=True)
        # 只保留需要的字段
        keep_cols = ['symbol', 'trade_date', 'open', 'high', 'low', 'close', 'volume']
        df = df[[col for col in keep_cols if col in df.columns]]
        records = df.to_dict('records')
        print(f"  [Repository] First record sample: {records[0] if records else 'empty'}")
        ops = [
            UpdateOne(
                {"symbol": record["symbol"], "trade_date": record["trade_date"]},
                {"$set": record},
                upsert=True
            )
            for record in records
        ]
        
        # Check before write
        count_before = self.collection.count_documents({'symbol': code})
        
        # Execute bulk write and verify
        if ops:
            try:
                result = self.collection.bulk_write(ops)
                # Calculate total written: inserted + modified records
                written = result.inserted_count + result.modified_count
                print(f"  [Repository] Wrote {written} records to database")
                print(f"  [Repository] Details - Inserted: {result.inserted_count}, Upserted: {result.upserted_count}, Modified: {result.modified_count}")
            except Exception as e:
                print(f"  [Repository] ERROR in bulk_write: {e}")
        else:
            print(f"  [Repository] No records to write")
        
        # Check after write
        count_after = self.collection.count_documents({'symbol': code})
        print(f"  [Repository] Records before: {count_before}, after: {count_after}")
