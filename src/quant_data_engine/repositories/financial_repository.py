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
        self.dividend_coll = self.db["financial_dividend"]
        self.mainbz_coll = self.db["financial_mainbz"]
        self._ensure_indexes()

    def _ensure_indexes(self):
        """Create necessary indexes for all financial collections."""
        # income / cashflow: one document per (ts_code, end_date, report_type) e.g. 1=合并 2=单季合并
        income_cash_indexes = [
            [("ts_code", 1), ("end_date", -1), ("report_type", 1)],
            [("ts_code", 1), ("end_date", -1)],
            [("end_date", -1)],
            [("ann_date", -1)],
        ]
        # balance / indicator: legacy key (ts_code, end_date) only
        simple_indexes = [
            [("ts_code", 1), ("end_date", -1)],
            [("end_date", -1)],
            [("ann_date", -1)],
        ]
        for index_keys in income_cash_indexes:
            try:
                self.income_coll.create_index(index_keys, background=True)
            except Exception:
                pass
            try:
                self.cashflow_coll.create_index(index_keys, background=True)
            except Exception:
                pass
        for index_keys in simple_indexes:
            try:
                self.balance_coll.create_index(index_keys, background=True)
            except Exception:
                pass
            try:
                self.indicator_coll.create_index(index_keys, background=True)
            except Exception:
                pass
        dividend_indexes = [
            [("ts_code", 1), ("end_date", -1), ("div_proc", 1)],
            [("ts_code", 1), ("ex_date", -1)],
            [("ann_date", -1)],
        ]
        for index_keys in dividend_indexes:
            try:
                self.dividend_coll.create_index(index_keys, background=True)
            except Exception:
                pass
        # mainbz: one document per (ts_code, end_date, bz_type, bz_item)
        mainbz_indexes = [
            [("ts_code", 1), ("end_date", -1), ("bz_type", 1)],
            [("ts_code", 1), ("end_date", -1)],
            [("end_date", -1)],
        ]
        for index_keys in mainbz_indexes:
            try:
                self.mainbz_coll.create_index(index_keys, background=True)
            except Exception:
                pass

    @staticmethod
    def _normalize_report_type(doc: dict) -> int:
        v = doc.get("report_type")
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return 1
        try:
            return int(v)
        except (TypeError, ValueError):
            return 1

    def _income_cashflow_upsert_key(self, doc: dict) -> dict:
        return {
            "ts_code": doc["ts_code"],
            "end_date": doc["end_date"],
            "report_type": self._normalize_report_type(doc),
        }

    @staticmethod
    def _default_ts_end_key(doc: dict) -> dict:
        return {"ts_code": doc["ts_code"], "end_date": doc["end_date"]}

    def _upsert_dataframe(
        self,
        collection,
        df: pd.DataFrame,
        sync_time: datetime,
        *,
        include_report_type_in_key: bool = False,
    ) -> int:
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
            doc = {k: (None if pd.isna(v) else v) for k, v in doc.items()}
            doc["synced_at"] = sync_time
            doc["data_source"] = "tushare"
            if include_report_type_in_key:
                # 强制写入为 int，避免 Mongo 中 1 与 "1" 为不同 key 导致无法 upsert 合并
                doc["report_type"] = int(self._normalize_report_type(doc))
                key = self._income_cashflow_upsert_key(doc)
            else:
                key = self._default_ts_end_key(doc)
            operations.append(UpdateOne(key, {"$set": doc}, upsert=True))
        
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
            return self._upsert_dataframe(
                self.income_coll, df, datetime.now(), include_report_type_in_key=True
            )

        if not records:
            return 0

        ops = []
        sync_time = datetime.now()
        for r in records:
            doc = dict(r)
            doc["synced_at"] = sync_time
            doc["data_source"] = "tushare"
            doc["report_type"] = int(self._normalize_report_type(doc))
            key = self._income_cashflow_upsert_key(doc)
            ops.append(UpdateOne(key, {"$set": doc}, upsert=True))
        
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
            return self._upsert_dataframe(
                self.cashflow_coll, df, datetime.now(), include_report_type_in_key=True
            )

        if not records:
            return 0

        ops = []
        sync_time = datetime.now()
        for r in records:
            doc = dict(r)
            doc["synced_at"] = sync_time
            doc["data_source"] = "tushare"
            doc["report_type"] = int(self._normalize_report_type(doc))
            key = self._income_cashflow_upsert_key(doc)
            ops.append(UpdateOne(key, {"$set": doc}, upsert=True))

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

    # ==================== Dividends ====================

    @staticmethod
    def _dividend_upsert_key(doc: dict) -> dict:
        return {
            "ts_code": doc["ts_code"],
            "end_date": doc.get("end_date"),
            "ann_date": doc.get("ann_date"),
            "div_proc": doc.get("div_proc"),
            "ex_date": doc.get("ex_date"),
        }

    def upsert_dividends(self, records: Sequence[Dict[str, Any]] = None, df: pd.DataFrame = None) -> int:
        """Upsert dividend and bonus share records."""
        if df is not None:
            if df is None or df.empty:
                return 0
            ops = []
            sync_time = datetime.now()
            for _, row in df.iterrows():
                doc = row.to_dict()
                doc = {k: (None if pd.isna(v) else v) for k, v in doc.items()}
                doc["synced_at"] = sync_time
                doc["data_source"] = "tushare"
                ops.append(UpdateOne(self._dividend_upsert_key(doc), {"$set": doc}, upsert=True))
            if ops:
                result = self.dividend_coll.bulk_write(ops, ordered=False)
                return (result.upserted_count or 0) + (result.modified_count or 0)
            return 0

        if not records:
            return 0
        ops = []
        sync_time = datetime.now()
        for r in records:
            doc = dict(r)
            doc["synced_at"] = sync_time
            doc["data_source"] = "tushare"
            ops.append(UpdateOne(self._dividend_upsert_key(doc), {"$set": doc}, upsert=True))
        if ops:
            result = self.dividend_coll.bulk_write(ops, ordered=False)
            return (result.upserted_count or 0) + (result.modified_count or 0)
        return 0

    # ==================== Main Business Composition ====================

    # upsert key 的必备字段：缺任一会让多条坏行塌缩到同一个 (None) key 互相覆盖
    _MAINBZ_KEY_FIELDS = ("ts_code", "end_date", "bz_type", "bz_item")

    @classmethod
    def _mainbz_upsert_key(cls, doc: dict) -> Optional[dict]:
        key = {field: doc.get(field) for field in cls._MAINBZ_KEY_FIELDS}
        if any(value is None or value == "" for value in key.values()):
            return None
        return key

    def upsert_mainbz(self, records: Sequence[Dict[str, Any]] = None, df: pd.DataFrame = None) -> int:
        """Upsert main business composition (revenue/profit/cost by segment).

        Rows missing any upsert-key field (ts_code/end_date/bz_type/bz_item) are
        skipped so malformed rows cannot collapse onto a shared key.
        """
        if df is not None:
            if df.empty:
                return 0
            rows = (row.to_dict() for _, row in df.iterrows())
            return self._upsert_mainbz_docs(rows, normalize_nan=True)

        if not records:
            return 0
        return self._upsert_mainbz_docs((dict(r) for r in records), normalize_nan=False)

    def _upsert_mainbz_docs(self, rows, *, normalize_nan: bool) -> int:
        ops = []
        sync_time = datetime.now()
        for doc in rows:
            if normalize_nan:
                doc = {k: (None if pd.isna(v) else v) for k, v in doc.items()}
            key = self._mainbz_upsert_key(doc)
            if key is None:
                continue
            doc["synced_at"] = sync_time
            doc["data_source"] = "tushare"
            ops.append(UpdateOne(key, {"$set": doc}, upsert=True))
        if ops:
            result = self.mainbz_coll.bulk_write(ops, ordered=False)
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
