"""ETF daily pipeline: fetch -> validate -> upsert -> summary."""
from __future__ import annotations
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import pandas as pd

logger = logging.getLogger(__name__)

# Tushare 单次限量（与文档一致）
_FUND_DAILY_MAX = 5000
_FUND_ADJ_PAGE = 2000
_FUND_SHARE_PAGE = 2000


def _yyyymmdd_range(start: str, end: str) -> List[str]:
    ds = datetime.strptime(start, "%Y%m%d")
    de = datetime.strptime(end, "%Y%m%d")
    out: List[str] = []
    d = ds
    while d <= de:
        out.append(d.strftime("%Y%m%d"))
        d += timedelta(days=1)
    return out


def _norm_td_cell(v: Any) -> Any:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        return str(int(v))
    return v


def _paginate_fund_adj_for_trade_date(adapter, trade_date: str) -> pd.DataFrame:
    parts: List[pd.DataFrame] = []
    offset = 0
    while True:
        df = adapter.fetch_fund_adj(
            trade_date=trade_date, offset=offset, limit=_FUND_ADJ_PAGE
        )
        if df is None or df.empty:
            break
        parts.append(df)
        if len(df) < _FUND_ADJ_PAGE:
            break
        offset += _FUND_ADJ_PAGE
    if not parts:
        return pd.DataFrame()
    out = pd.concat(parts, ignore_index=True)
    if "ts_code" in out.columns and "trade_date" in out.columns:
        return out.drop_duplicates(subset=["ts_code", "trade_date"], keep="last")
    return out


def _paginate_fund_share_for_trade_date(adapter, trade_date: str) -> pd.DataFrame:
    parts: List[pd.DataFrame] = []
    offset = 0
    while True:
        df = adapter.fetch_fund_share(
            trade_date=trade_date, offset=offset, limit=_FUND_SHARE_PAGE
        )
        if df is None or df.empty:
            break
        parts.append(df)
        if len(df) < _FUND_SHARE_PAGE:
            break
        offset += _FUND_SHARE_PAGE
    if not parts:
        return pd.DataFrame()
    out = pd.concat(parts, ignore_index=True)
    if "ts_code" in out.columns and "trade_date" in out.columns:
        return out.drop_duplicates(subset=["ts_code", "trade_date"], keep="last")
    return out


def _paginate_fund_adj_for_ts_range(
    adapter, ts_code: str, start_date: str, end_date: str
) -> pd.DataFrame:
    """单代码 + 日期区间：Tushare 单次最多 2000 行，需 offset 分页。"""
    parts: List[pd.DataFrame] = []
    offset = 0
    while True:
        df = adapter.fetch_fund_adj(
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
            offset=offset,
            limit=_FUND_ADJ_PAGE,
        )
        if df is None or df.empty:
            break
        parts.append(df)
        if len(df) < _FUND_ADJ_PAGE:
            break
        offset += _FUND_ADJ_PAGE
    if not parts:
        return pd.DataFrame()
    out = pd.concat(parts, ignore_index=True)
    if "ts_code" in out.columns and "trade_date" in out.columns:
        return out.drop_duplicates(subset=["ts_code", "trade_date"], keep="last")
    return out


def _paginate_fund_share_for_ts_range(
    adapter, ts_code: str, start_date: str, end_date: str
) -> pd.DataFrame:
    parts: List[pd.DataFrame] = []
    offset = 0
    while True:
        df = adapter.fetch_fund_share(
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
            offset=offset,
            limit=_FUND_SHARE_PAGE,
        )
        if df is None or df.empty:
            break
        parts.append(df)
        if len(df) < _FUND_SHARE_PAGE:
            break
        offset += _FUND_SHARE_PAGE
    if not parts:
        return pd.DataFrame()
    out = pd.concat(parts, ignore_index=True)
    if "ts_code" in out.columns and "trade_date" in out.columns:
        return out.drop_duplicates(subset=["ts_code", "trade_date"], keep="last")
    return out


@dataclass
class EtfDailyResult:
    ts_code: str
    inserted: int
    issues: List[str]


class EtfDailyPipeline:
    def __init__(self, adapter, repository, validator):
        self.adapter = adapter
        self.repository = repository
        self.validator = validator

    def run(
        self,
        codes: Optional[List[str]],
        start_date: str,
        end_date: str,
    ) -> Dict[str, Any]:
        """
        - ``codes is None``：不传 ``ts_code``，按 **trade_date** 逐日拉 ``fund_daily`` / ``fund_adj`` / ``fund_share``
          当日全市场（与 Tushare 可选参数语义一致）；``etf_basic`` 仍用一次 ``fund_basic(market=E,status=L)`` 全量写入。
        - ``codes`` 非空：按代码 + 区间逐只拉取；``fund_adj`` / ``fund_share`` 对单代码长区间按 **offset** 分页（2000/次），
          避免 Tushare 单次截断。
        """
        if codes is None:
            return self._run_by_trade_dates(start_date, end_date)
        return self._run_by_ts_codes(codes, start_date, end_date)

    def _run_by_trade_dates(self, start_date: str, end_date: str) -> Dict[str, Any]:
        try:
            df_basic = self.adapter.fetch_fund_basic(market="E", status="L")
            if df_basic is not None and not df_basic.empty:
                self.repository.upsert_basic(df_basic.to_dict("records"))
        except Exception as e:
            logger.warning("ETF bulk: fund_basic upsert failed: %s", e)

        days = _yyyymmdd_range(start_date, end_date)
        day_results: List[Dict[str, Any]] = []
        for j, td in enumerate(days, 1):
            if j == 1 or j == len(days) or j % 5 == 0:
                print(f"  [{j}/{len(days)}] fund_daily / adj / share  trade_date={td} (all ts_code)...")
            try:
                df_daily = self.adapter.fetch_etf_daily(trade_date=td)
            except Exception as ex:
                day_results.append(
                    {"trade_date": td, "inserted": 0, "issues": [f"fund_daily:{ex}"]}
                )
                continue
            if df_daily is None or df_daily.empty:
                day_results.append({"trade_date": td, "inserted": 0, "issues": []})
                continue
            if len(df_daily) >= _FUND_DAILY_MAX:
                logger.warning(
                    "fund_daily trade_date=%s returned %s rows (>= %s); may be truncated",
                    td,
                    len(df_daily),
                    _FUND_DAILY_MAX,
                )
            v = self.validator.validate(df_daily)
            if not v["is_valid"]:
                day_results.append(
                    {"trade_date": td, "inserted": 0, "issues": list(v["issues"])}
                )
                continue

            daily_records: List[Dict[str, Any]] = []
            for _, row in df_daily.iterrows():
                tc = row.get("ts_code")
                if tc is None or (isinstance(tc, float) and pd.isna(tc)):
                    continue
                tc = str(tc).strip()
                tdr = _norm_td_cell(row.get("trade_date"))
                daily_records.append(
                    {
                        "ts_code": tc,
                        "trade_date": tdr,
                        "open": float(row.open) if pd.notna(row.open) else None,
                        "high": float(row.high) if pd.notna(row.high) else None,
                        "low": float(row.low) if pd.notna(row.low) else None,
                        "close": float(row.close) if pd.notna(row.close) else None,
                        "vol": float(row.vol) if "vol" in row and pd.notna(row.vol) else None,
                        "amount": float(row.amount)
                        if "amount" in row and pd.notna(row.amount)
                        else None,
                        "source": "tushare",
                    }
                )
            count = self.repository.upsert_daily_prices(daily_records)

            df_adj = _paginate_fund_adj_for_trade_date(self.adapter, td)
            if df_adj is not None and not df_adj.empty:
                self.repository.upsert_adj_factors(df_adj.to_dict("records"))

            df_share = _paginate_fund_share_for_trade_date(self.adapter, td)
            if df_share is not None and not df_share.empty:
                self.repository.upsert_shares(df_share.to_dict("records"))

            day_results.append({"trade_date": td, "inserted": count, "issues": []})

        return {
            "mode": "by_trade_date",
            "start_date": start_date,
            "end_date": end_date,
            "days": len(days),
            "results": day_results,
        }

    def _run_by_ts_codes(
        self, codes: List[str], start_date: str, end_date: str
    ) -> Dict[str, Any]:
        results: List[EtfDailyResult] = []

        try:
            df_basic = self.adapter.fetch_fund_basic(market="E", status="L")
            if df_basic is not None and not df_basic.empty:
                df_basic = df_basic[df_basic["ts_code"].isin(codes)]
                if not df_basic.empty:
                    basic_records = df_basic.to_dict("records")
                    self.repository.upsert_basic(basic_records)
        except Exception as e:
            print(f"Warning: Failed to fetch ETF basic info: {e}")

        for i, code in enumerate(codes, 1):
            if i % 50 == 0 or i == 1:
                print(f"  [{i}/{len(codes)}] Processing {code}...")

            df_daily = self.adapter.fetch_etf_daily(code, start_date=start_date, end_date=end_date)
            df_adj = _paginate_fund_adj_for_ts_range(
                self.adapter, code, start_date, end_date
            )
            df_share = _paginate_fund_share_for_ts_range(
                self.adapter, code, start_date, end_date
            )

            if df_daily is None or df_daily.empty:
                results.append(EtfDailyResult(code, 0, ["no_daily_data"]))
                continue

            v = self.validator.validate(df_daily)
            if not v["is_valid"]:
                results.append(EtfDailyResult(code, 0, v["issues"]))
                continue

            daily_records = []
            for _, row in df_daily.iterrows():
                daily_records.append(
                    {
                        "ts_code": code,
                        "trade_date": row.trade_date,
                        "open": float(row.open) if pd.notna(row.open) else None,
                        "high": float(row.high) if pd.notna(row.high) else None,
                        "low": float(row.low) if pd.notna(row.low) else None,
                        "close": float(row.close) if pd.notna(row.close) else None,
                        "vol": float(row.vol) if "vol" in row and pd.notna(row.vol) else None,
                        "amount": float(row.amount)
                        if "amount" in row and pd.notna(row.amount)
                        else None,
                        "source": "tushare",
                    }
                )

            count = self.repository.upsert_daily_prices(daily_records)

            if df_adj is not None and not df_adj.empty:
                adj_records = df_adj.to_dict("records")
                self.repository.upsert_adj_factors(adj_records)

            if df_share is not None and not df_share.empty:
                share_records = df_share.to_dict("records")
                self.repository.upsert_shares(share_records)

            results.append(EtfDailyResult(code, count, v["issues"]))
        return {"mode": "by_ts_code", "run_codes": codes, "results": [r.__dict__ for r in results]}


__all__ = ["EtfDailyPipeline"]
