"""Stock weekly / monthly bars: Tushare ``weekly`` / ``monthly`` → quant_data."""
from __future__ import annotations

from typing import Any, Dict, List, Optional

import pandas as pd

PAGE_SIZE = 6000


def _norm_trade_date(v: Any) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""
    if hasattr(v, "strftime"):
        return v.strftime("%Y%m%d")
    s = str(v).replace("-", "")[:8]
    return s if len(s) == 8 else ""


class StockWmBarsPipeline:
    """Ingest full-market weekly or monthly bars for one Tushare ``trade_date``."""

    def __init__(self, adapter, repository):
        self.adapter = adapter
        self.repository = repository

    def _fetch_all_pages_weekly(self, trade_date: str) -> pd.DataFrame:
        return self._fetch_all_pages("weekly", trade_date)

    def _fetch_all_pages_monthly(self, trade_date: str) -> pd.DataFrame:
        return self._fetch_all_pages("monthly", trade_date)

    def _fetch_all_pages(self, kind: str, trade_date: str) -> pd.DataFrame:
        chunks: List[pd.DataFrame] = []
        offset = 0
        while True:
            if kind == "weekly":
                df = self.adapter.fetch_weekly(
                    trade_date=trade_date, offset=offset, limit=PAGE_SIZE
                )
            else:
                df = self.adapter.fetch_monthly(
                    trade_date=trade_date, offset=offset, limit=PAGE_SIZE
                )
            if df is None or df.empty:
                break
            chunks.append(df)
            if len(df) < PAGE_SIZE:
                break
            offset += PAGE_SIZE
        if not chunks:
            return pd.DataFrame()
        return pd.concat(chunks, ignore_index=True)

    def _df_to_records(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for _, row in df.iterrows():
            ts = row.get("ts_code") or ""
            td = _norm_trade_date(row.get("trade_date"))
            if not ts or not td:
                continue
            out.append(
                {
                    "ts_code": ts,
                    "trade_date": td,
                    "open": row.get("open"),
                    "high": row.get("high"),
                    "low": row.get("low"),
                    "close": row.get("close"),
                    "pre_close": row.get("pre_close"),
                    "change": row.get("change"),
                    "pct_chg": row.get("pct_chg"),
                    "vol": row.get("vol"),
                    "amount": row.get("amount"),
                }
            )
        return out

    def ingest_weekly_trade_date(self, trade_date: str) -> Dict[str, Any]:
        df = self._fetch_all_pages_weekly(trade_date)
        if df is None or df.empty:
            return {"trade_date": trade_date, "count": 0, "freq": "week"}
        recs = self._df_to_records(df)
        n = self.repository.upsert_weekly_bars(recs)
        return {"trade_date": trade_date, "count": n, "rows": len(recs), "freq": "week"}

    def ingest_monthly_trade_date(self, trade_date: str) -> Dict[str, Any]:
        df = self._fetch_all_pages_monthly(trade_date)
        if df is None or df.empty:
            return {"trade_date": trade_date, "count": 0, "freq": "month"}
        recs = self._df_to_records(df)
        n = self.repository.upsert_monthly_bars(recs)
        return {"trade_date": trade_date, "count": n, "rows": len(recs), "freq": "month"}

    def ingest_weekly_ts_range(
        self, ts_code: str, start_date: str, end_date: str
    ) -> Dict[str, Any]:
        """Single-stock (or explicit code) range backfill."""
        df = self.adapter.fetch_weekly(ts_code=ts_code, start_date=start_date, end_date=end_date)
        if df is None or df.empty:
            return {"ts_code": ts_code, "count": 0, "freq": "week"}
        recs = self._df_to_records(df)
        n = self.repository.upsert_weekly_bars(recs)
        return {"ts_code": ts_code, "count": n, "rows": len(recs), "freq": "week"}

    def ingest_monthly_ts_range(
        self, ts_code: str, start_date: str, end_date: str
    ) -> Dict[str, Any]:
        df = self.adapter.fetch_monthly(ts_code=ts_code, start_date=start_date, end_date=end_date)
        if df is None or df.empty:
            return {"ts_code": ts_code, "count": 0, "freq": "month"}
        recs = self._df_to_records(df)
        n = self.repository.upsert_monthly_bars(recs)
        return {"ts_code": ts_code, "count": n, "rows": len(recs), "freq": "month"}


__all__ = ["StockWmBarsPipeline", "PAGE_SIZE"]
