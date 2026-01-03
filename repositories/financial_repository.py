"""FinancialRepository - handles persistence for financial statements and indicators."""
from __future__ import annotations
from typing import Sequence, Dict, Any, Optional
from pymongo import MongoClient, UpdateOne
from datetime import datetime
import pandas as pd


class FinancialRepository:
    """Repository for financial statements (income, balance, cashflow) and indicators."""
    
    def __init__(self, mongo_uri: str, db_name: str):
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.income_coll = self.db["financial_income"]
        self.balance_coll = self.db["financial_balance"]
        self.cashflow_coll = self.db["financial_cashflow"]
        self.indicator_coll = self.db["financial_indicator"]
        self._ensure_indexes()

    def _ensure_indexes(self):
        """Create necessary indexes for all financial collections."""
        indexes = [
            [('ts_code', 1), ('end_date', -1)],  # Query by stock and report period
            [('end_date', -1)],                   # Query by report period
            [('ann_date', -1)],                   # Query by announcement date
        ]
        
        for collection in [self.income_coll, self.balance_coll, self.cashflow_coll, self.indicator_coll]:
            for index_keys in indexes:
                try:
                    collection.create_index(index_keys, background=True)
                except Exception:
                    pass

    def _upsert_dataframe(self, collection, df: pd.DataFrame, sync_time: datetime) -> int:
        """Helper to upsert DataFrame to a collection.
        
        Args:
            collection: MongoDB collection
            df: DataFrame to upsert
            sync_time: Timestamp for synced_at field
            
        Returns:
            Number of upserted + modified documents
        """
        if df is None or df.empty:
            return 0
        
        operations = []
        for _, row in df.iterrows():
            doc = row.to_dict()
            # Clean NaN values
            doc = {k: (None if pd.isna(v) else v) for k, v in doc.items()}
            doc['synced_at'] = sync_time
            doc['data_source'] = 'tushare'
            
            # Use ts_code + end_date as unique key
            key = {'ts_code': doc['ts_code'], 'end_date': doc['end_date']}
            operations.append(UpdateOne(key, {'$set': doc}, upsert=True))
        
        if operations:
            result = collection.bulk_write(operations, ordered=False)
            return (result.upserted_count or 0) + (result.modified_count or 0)
        return 0

    # ==================== Income Statement ====================
    
    def upsert_income(self, records: Sequence[Dict[str, Any]] = None, df: pd.DataFrame = None) -> int:
        """Upsert income statement data.
        
        Args:
            records: List of income statement dicts (alternative to df)
            df: DataFrame with income statement data
            
        Returns:
            Number of upserted + modified documents
        """
        if df is not None:
            return self._upsert_dataframe(self.income_coll, df, datetime.now())
        
        if not records:
            return 0
        
        ops = []
        sync_time = datetime.now()
        for r in records:
            doc = dict(r)
            doc['synced_at'] = sync_time
            doc['data_source'] = 'tushare'
            key = {'ts_code': doc['ts_code'], 'end_date': doc['end_date']}
            ops.append(UpdateOne(key, {'$set': doc}, upsert=True))
        
        if ops:
            result = self.income_coll.bulk_write(ops, ordered=False)
            return (result.upserted_count or 0) + (result.modified_count or 0)
        return 0

    # ==================== Balance Sheet ====================
    
    def upsert_balance(self, records: Sequence[Dict[str, Any]] = None, df: pd.DataFrame = None) -> int:
        """Upsert balance sheet data.
        
        Args:
            records: List of balance sheet dicts (alternative to df)
            df: DataFrame with balance sheet data
            
        Returns:
            Number of upserted + modified documents
        """
        if df is not None:
            return self._upsert_dataframe(self.balance_coll, df, datetime.now())
        
        if not records:
            return 0
        
        ops = []
        sync_time = datetime.now()
        for r in records:
            doc = dict(r)
            doc['synced_at'] = sync_time
            doc['data_source'] = 'tushare'
            key = {'ts_code': doc['ts_code'], 'end_date': doc['end_date']}
            ops.append(UpdateOne(key, {'$set': doc}, upsert=True))
        
        if ops:
            result = self.balance_coll.bulk_write(ops, ordered=False)
            return (result.upserted_count or 0) + (result.modified_count or 0)
        return 0

    # ==================== Cash Flow Statement ====================
    
    def upsert_cashflow(self, records: Sequence[Dict[str, Any]] = None, df: pd.DataFrame = None) -> int:
        """Upsert cash flow statement data.
        
        Args:
            records: List of cash flow dicts (alternative to df)
            df: DataFrame with cash flow data
            
        Returns:
            Number of upserted + modified documents
        """
        if df is not None:
            return self._upsert_dataframe(self.cashflow_coll, df, datetime.now())
        
        if not records:
            return 0
        
        ops = []
        sync_time = datetime.now()
        for r in records:
            doc = dict(r)
            doc['synced_at'] = sync_time
            doc['data_source'] = 'tushare'
            key = {'ts_code': doc['ts_code'], 'end_date': doc['end_date']}
            ops.append(UpdateOne(key, {'$set': doc}, upsert=True))
        
        if ops:
            result = self.cashflow_coll.bulk_write(ops, ordered=False)
            return (result.upserted_count or 0) + (result.modified_count or 0)
        return 0

    # ==================== Financial Indicators ====================
    
    def upsert_indicators(self, records: Sequence[Dict[str, Any]] = None, df: pd.DataFrame = None) -> int:
        """Upsert financial indicators data.
        
        Args:
            records: List of indicator dicts (alternative to df)
            df: DataFrame with financial indicators
            
        Returns:
            Number of upserted + modified documents
        """
        if df is not None:
            return self._upsert_dataframe(self.indicator_coll, df, datetime.now())
        
        if not records:
            return 0
        
        ops = []
        sync_time = datetime.now()
        for r in records:
            doc = dict(r)
            doc['synced_at'] = sync_time
            doc['data_source'] = 'tushare'
            key = {'ts_code': doc['ts_code'], 'end_date': doc['end_date']}
            ops.append(UpdateOne(key, {'$set': doc}, upsert=True))
        
        if ops:
            result = self.indicator_coll.bulk_write(ops, ordered=False)
            return (result.upserted_count or 0) + (result.modified_count or 0)
        return 0

    # ==================== Query Methods ====================
    
    def get_income_data(self, ts_code: str, periods: int = 8) -> pd.DataFrame:
        """Get income statement data for a stock.
        
        Args:
            ts_code: Stock code
            periods: Number of recent periods to retrieve
            
        Returns:
            DataFrame with income data
        """
        cursor = self.income_coll.find(
            {'ts_code': ts_code},
            sort=[('end_date', -1)],
            limit=periods
        )
        docs = list(cursor)
        return pd.DataFrame(docs) if docs else pd.DataFrame()

    def get_balance_data(self, ts_code: str, periods: int = 8) -> pd.DataFrame:
        """Get balance sheet data for a stock."""
        cursor = self.balance_coll.find(
            {'ts_code': ts_code},
            sort=[('end_date', -1)],
            limit=periods
        )
        docs = list(cursor)
        return pd.DataFrame(docs) if docs else pd.DataFrame()

    def get_cashflow_data(self, ts_code: str, periods: int = 8) -> pd.DataFrame:
        """Get cash flow data for a stock."""
        cursor = self.cashflow_coll.find(
            {'ts_code': ts_code},
            sort=[('end_date', -1)],
            limit=periods
        )
        docs = list(cursor)
        return pd.DataFrame(docs) if docs else pd.DataFrame()

    def get_indicator_data(self, ts_code: str, periods: int = 8) -> pd.DataFrame:
        """Get financial indicator data for a stock."""
        cursor = self.indicator_coll.find(
            {'ts_code': ts_code},
            sort=[('end_date', -1)],
            limit=periods
        )
        docs = list(cursor)
        return pd.DataFrame(docs) if docs else pd.DataFrame()


__all__ = ["FinancialRepository"]
