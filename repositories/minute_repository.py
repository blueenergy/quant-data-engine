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
        df['datetime'] = pd.to_datetime(df['datetime'])
        df['trade_date'] = df['datetime'].dt.strftime('%Y%m%d%H%M')
        df['symbol'] = code
        if 'vol' in df.columns:
            df.rename(columns={'vol': 'volume'}, inplace=True)
        # 只保留需要的字段
        keep_cols = ['symbol', 'trade_date', 'open', 'high', 'low', 'close', 'volume']
        df = df[[col for col in keep_cols if col in df.columns]]
        records = df.to_dict('records')
        ops = [
            UpdateOne(
                {"symbol": record["symbol"], "trade_date": record["trade_date"]},
                {"$set": record},
                upsert=True
            )
            for record in records
        ]
        self.collection.bulk_write(ops)
