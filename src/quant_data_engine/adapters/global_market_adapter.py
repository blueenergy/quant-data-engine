"""GlobalMarketAdapter — reads US/global market daily OHLCV from MongoDB.

This adapter mirrors the interface of ``TushareAdapter`` so that
``signals.py`` functions can be called unchanged for US sectors.

Data source: ``global_market_daily`` collection (written by
``tasks/run_global_market_daily_sync.py``).

Column name alignment with TushareAdapter:
    trade_date, open, high, low, close, vol, amount (amount may be NaN for indices)
"""
from __future__ import annotations

import logging
from typing import Optional

import pandas as pd
from pymongo import MongoClient

logger = logging.getLogger(__name__)


class GlobalMarketAdapter:
    """Read US/global market OHLCV history from ``global_market_daily`` collection.

    Args:
        mongo_uri: MongoDB connection string.
        db_name: Database name (e.g. ``"quant_data"``).

    Example::

        adapter = GlobalMarketAdapter(mongo_uri, "quant_data")
        df = adapter.fetch_etf_daily("SOXX", "20250101", "20260430")
    """

    COLLECTION = "global_market_daily"

    def __init__(self, mongo_uri: str, db_name: str) -> None:
        self._client = MongoClient(mongo_uri)
        self._coll = self._client[db_name][self.COLLECTION]

    # ------------------------------------------------------------------
    # Public interface — mirrors TushareAdapter method signatures
    # ------------------------------------------------------------------

    def fetch_etf_daily(
        self,
        ts_code: str,
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        """Fetch daily OHLCV for an ETF symbol (e.g. ``"SOXX"``).

        Returns a DataFrame with columns:
            trade_date (str YYYYMMDD), open, high, low, close, vol, amount
        Sorted ascending by trade_date.
        """
        return self._fetch(ts_code, start_date, end_date)

    def fetch_index_daily(
        self,
        ts_code: str,
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        """Fetch daily OHLCV for an index symbol (e.g. ``"^SOX"``).

        Same return schema as ``fetch_etf_daily``.
        """
        return self._fetch(ts_code, start_date, end_date)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _fetch(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        query = {
            "symbol": symbol,
            "date": {"$gte": start_date, "$lte": end_date},
        }
        projection = {"_id": 0, "date": 1, "open": 1, "high": 1, "low": 1,
                      "close": 1, "volume": 1}
        docs = list(self._coll.find(query, projection).sort("date", 1))

        if not docs:
            logger.warning("GlobalMarketAdapter: no data for %s %s-%s", symbol, start_date, end_date)
            return pd.DataFrame(columns=["trade_date", "open", "high", "low", "close", "vol", "amount"])

        df = pd.DataFrame(docs)
        df.rename(columns={"date": "trade_date", "volume": "vol"}, inplace=True)
        df["amount"] = float("nan")   # not available for most global symbols
        df["trade_date"] = df["trade_date"].astype(str)
        for col in ("open", "high", "low", "close", "vol"):
            df[col] = pd.to_numeric(df[col], errors="coerce")
        return df.sort_values("trade_date").reset_index(drop=True)

    def close(self) -> None:
        self._client.close()


__all__ = ["GlobalMarketAdapter"]
