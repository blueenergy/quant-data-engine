"""Market regime scanner for A-share breadth and valuation signals."""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import pandas as pd

from .breadth_signals import (
    aggregate_regime_alert,
    cape_band_from_pct,
    classify_cape,
    compute_breadth_table,
    compute_cape_series,
    compute_hindenburg,
    compute_mcl,
)
from .instruments import BREADTH_THRESHOLDS, CAPE_INDEX_PROFILES
from .models import CAPESignal, MarketRegimeReport

logger = logging.getLogger(__name__)


def _offset_date(date_str: str, days: int) -> str:
    return (datetime.strptime(date_str, "%Y%m%d") + timedelta(days=days)).strftime("%Y%m%d")


class MarketRegimeScanner:
    """Orchestrates MCL + Hindenburg + CAPE for one scan date."""

    def __init__(
        self,
        repo,
        *,
        lookback_calendar_days: int = 700,
        cape_start_date: str = "20100101",
    ):
        self.repo = repo
        self.db = repo.db
        self.lookback_calendar_days = lookback_calendar_days
        self.cape_start_date = cape_start_date
        self.last_breadth_doc: Optional[Dict[str, object]] = None
        self.last_cape_docs: List[Dict[str, object]] = []
        self._stock_daily_cache: Dict[str, pd.DataFrame] = {}
        self._stock_info_cache: Optional[pd.DataFrame] = None
        self._cpi_cache: Optional[pd.DataFrame] = None

    def scan(self, trade_date: str) -> MarketRegimeReport:
        """Run all regime signals for *trade_date*."""
        warnings: List[str] = []
        self.last_breadth_doc = None
        self.last_cape_docs = []
        start_date = _offset_date(trade_date, -self.lookback_calendar_days)
        lookback_trading_days = int(BREADTH_THRESHOLDS["breadth_lookback_trading_days"])
        exclude_ipo_trading_days = int(BREADTH_THRESHOLDS["breadth_exclude_ipo_trading_days"])
        recent_trade_dates = self._load_recent_trade_dates(
            trade_date,
            lookback_trading_days + 210,
        )
        if recent_trade_dates:
            start_date = recent_trade_dates[0]

        stock_daily = self._load_stock_daily(recent_trade_dates or (start_date, trade_date), trade_date)
        stock_info = self._load_stock_info()
        trade_cal = self._load_trade_cal_from_dates(recent_trade_dates) if recent_trade_dates else self._load_trade_cal_from_daily(stock_daily)
        breadth_today = compute_breadth_table(
            stock_daily,
            stock_info,
            trade_date,
            trade_cal,
            lookback_trading_days=lookback_trading_days,
            exclude_ipo_trading_days=exclude_ipo_trading_days,
            warnings=warnings,
        )
        self.last_breadth_doc = dict(breadth_today)

        breadth_history = self.repo.get_breadth_range(start_date, trade_date)
        breadth_history = self._append_today_breadth(breadth_history, breadth_today)
        mcl = compute_mcl(
            breadth_history,
            trade_date,
            fast=int(BREADTH_THRESHOLDS["mcl_fast"]),
            slow=int(BREADTH_THRESHOLDS["mcl_slow"]),
            signal_period=int(BREADTH_THRESHOLDS["mcl_signal_period"]),
            ratio_adjusted=bool(BREADTH_THRESHOLDS["mcl_ratio_adjusted"]),
            warnings=warnings,
        )
        if mcl is not None and self.last_breadth_doc is not None:
            self.last_breadth_doc.update({
                "net_advance_ratio": mcl.net_advance_ratio,
                "ema19": mcl.ema19,
                "ema39": mcl.ema39,
                "mcl": mcl.mcl,
                "mcl_ma9": mcl.mcl_ma9,
                "mcl_pct_rank_to_date": mcl.pct_rank_to_date,
            })

        hindenburg = None
        if mcl is not None:
            hs300_close = self._load_index_close(
                str(BREADTH_THRESHOLDS["hindenburg_benchmark"]),
                start_date,
                trade_date,
            )
            recent_triggers = self.repo.get_hindenburg_triggers(
                trade_date,
                window_days=int(BREADTH_THRESHOLDS["hindenburg_cluster_window_days"]),
            )
            hindenburg = compute_hindenburg(
                breadth_today,
                mcl,
                hs300_close,
                recent_triggers,
                trade_date,
                high_low_pct=float(BREADTH_THRESHOLDS["hindenburg_high_low_pct"]),
                ratio_max=float(BREADTH_THRESHOLDS["hindenburg_ratio_max"]),
                ma_period=int(BREADTH_THRESHOLDS["hindenburg_ma_period"]),
                cluster_window_days=int(BREADTH_THRESHOLDS["hindenburg_cluster_window_days"]),
                cluster_count=int(BREADTH_THRESHOLDS["hindenburg_cluster_count"]),
                warnings=warnings,
            )

        cape_signals = self._compute_cape_signals(trade_date, warnings)
        composite_alert, aggregate_warnings = aggregate_regime_alert(mcl, hindenburg, cape_signals)
        warnings.extend(aggregate_warnings)

        return MarketRegimeReport(
            scan_date=trade_date,
            generated_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            mcl=mcl,
            hindenburg=hindenburg,
            cape=cape_signals,
            composite_alert=composite_alert,
            warnings=warnings,
        )

    def _load_stock_daily(self, start_date_or_dates, end_date: str) -> pd.DataFrame:
        if isinstance(start_date_or_dates, list):
            requested_dates = list(dict.fromkeys(str(date) for date in start_date_or_dates))
            missing_dates = [date for date in requested_dates if date not in self._stock_daily_cache]
            if missing_dates:
                docs = list(self.db["volume_price"].find(
                    {"trade_date": {"$in": missing_dates}},
                    {"_id": 0, "symbol": 1, "ts_code": 1, "trade_date": 1, "pct_chg": 1, "close": 1, "high": 1, "low": 1},
                ).sort("trade_date", 1))
                loaded = pd.DataFrame(docs)
                if not loaded.empty:
                    for date, frame in loaded.groupby("trade_date", sort=False):
                        self._stock_daily_cache[str(date)] = frame.reset_index(drop=True)
                for date in missing_dates:
                    self._stock_daily_cache.setdefault(date, pd.DataFrame())
            if requested_dates:
                first_date = requested_dates[0]
                for cached_date in list(self._stock_daily_cache):
                    if cached_date < first_date:
                        del self._stock_daily_cache[cached_date]
            frames = [self._stock_daily_cache[date] for date in requested_dates if not self._stock_daily_cache[date].empty]
            return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        else:
            query = {"trade_date": {"$gte": start_date_or_dates, "$lte": end_date}}
            docs = list(self.db["volume_price"].find(
                query,
                {"_id": 0, "symbol": 1, "ts_code": 1, "trade_date": 1, "pct_chg": 1, "close": 1, "high": 1, "low": 1},
            ).sort("trade_date", 1))
            return pd.DataFrame(docs)

    def _load_recent_trade_dates(self, trade_date: str, count: int) -> List[str]:
        docs = list(self.db["index_prices"].find(
            {"ts_code": str(BREADTH_THRESHOLDS["hindenburg_benchmark"]), "trade_date": {"$lte": trade_date}},
            {"_id": 0, "trade_date": 1},
        ).sort("trade_date", -1).limit(count))
        return sorted(str(doc["trade_date"]) for doc in docs if doc.get("trade_date"))

    def _load_stock_info(self) -> pd.DataFrame:
        if self._stock_info_cache is not None:
            return self._stock_info_cache.copy()
        docs = list(self.db["stock_info"].find(
            {},
            {"_id": 0, "symbol": 1, "ts_code": 1, "name": 1, "list_date": 1, "list_status": 1, "delist_date": 1},
        ))
        self._stock_info_cache = pd.DataFrame(docs)
        return self._stock_info_cache.copy()

    @staticmethod
    def _load_trade_cal_from_daily(stock_daily: pd.DataFrame) -> pd.DataFrame:
        if stock_daily is None or stock_daily.empty or "trade_date" not in stock_daily.columns:
            return pd.DataFrame(columns=["cal_date", "is_open"])
        dates = sorted(stock_daily["trade_date"].astype(str).str.replace("-", "", regex=False).unique())
        return pd.DataFrame({"cal_date": dates, "is_open": 1})

    @staticmethod
    def _load_trade_cal_from_dates(dates: List[str]) -> pd.DataFrame:
        return pd.DataFrame({"cal_date": dates, "is_open": 1})

    def _load_index_close(self, ts_code: str, start_date: str, end_date: str) -> pd.Series:
        docs = list(self.db["index_prices"].find(
            {"ts_code": ts_code, "trade_date": {"$gte": start_date, "$lte": end_date}},
            {"_id": 0, "trade_date": 1, "close": 1},
        ).sort("trade_date", 1))
        if not docs:
            return pd.Series(dtype=float)
        df = pd.DataFrame(docs)
        series = pd.to_numeric(df["close"], errors="coerce")
        series.index = df["trade_date"].astype(str)
        return series.dropna()

    @staticmethod
    def _append_today_breadth(history: pd.DataFrame, breadth_today: Dict[str, object]) -> pd.DataFrame:
        today = pd.DataFrame([breadth_today])
        if history is None or history.empty:
            return today
        combined = pd.concat([history, today], ignore_index=True)
        return combined.drop_duplicates("trade_date", keep="last").sort_values("trade_date")

    def _compute_cape_signals(self, trade_date: str, warnings: List[str]) -> List[CAPESignal]:
        signals: List[CAPESignal] = []
        cpi = self._load_cpi()
        if cpi.empty:
            warnings.append("CAPE: CPI 数据为空，跳过")
            return signals
        for profile in CAPE_INDEX_PROFILES.values():
            ts_code = profile["ts_code"]
            try:
                index_daily = self._load_index_daily(ts_code, self.cape_start_date, trade_date)
                index_basic = self.repo.get_index_basic_range(ts_code, self.cape_start_date, trade_date)
                cape_df = compute_cape_series(
                    index_daily,
                    index_basic,
                    cpi,
                    pe_field=str(BREADTH_THRESHOLDS["cape_pe_field"]),
                    window_years=float(BREADTH_THRESHOLDS["cape_window_years"]),
                    min_coverage=float(BREADTH_THRESHOLDS["cape_min_coverage"]),
                )
                cape_col = f"cape_{float(BREADTH_THRESHOLDS['cape_window_years']):g}y"
                docs = self._cape_docs_from_frame(
                    cape_df,
                    ts_code,
                    cape_col,
                    target_date=trade_date,
                    pct_cheap=float(BREADTH_THRESHOLDS["cape_pct_cheap"]),
                    pct_rich=float(BREADTH_THRESHOLDS["cape_pct_rich"]),
                    pct_bubble=float(BREADTH_THRESHOLDS["cape_pct_bubble"]),
                )
                self.last_cape_docs.extend(docs)
                valid = cape_df[cape_df["trade_date"] <= trade_date].dropna(subset=[cape_col])
                if valid.empty:
                    warnings.append(f"CAPE: {ts_code} 无有效 CAPE 样本")
                    continue
                latest = valid.iloc[-1]
                if str(latest["trade_date"]) != trade_date:
                    warnings.append(f"CAPE: {ts_code} 最新有效样本为 {latest['trade_date']}，非扫描日 {trade_date}")
                signals.append(classify_cape(
                    latest,
                    ts_code,
                    profile["name"],
                    pct_cheap=float(BREADTH_THRESHOLDS["cape_pct_cheap"]),
                    pct_rich=float(BREADTH_THRESHOLDS["cape_pct_rich"]),
                    pct_bubble=float(BREADTH_THRESHOLDS["cape_pct_bubble"]),
                ))
            except Exception as exc:
                warnings.append(f"CAPE计算失败 {ts_code}: {exc}")
        return signals

    def _load_index_daily(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        docs = list(self.db["index_prices"].find(
            {"ts_code": ts_code, "trade_date": {"$gte": start_date, "$lte": end_date}},
            {"_id": 0, "ts_code": 1, "trade_date": 1, "close": 1},
        ).sort("trade_date", 1))
        return pd.DataFrame(docs)

    def _load_cpi(self) -> pd.DataFrame:
        if self._cpi_cache is not None:
            return self._cpi_cache.copy()
        docs = list(self.db["cn_macro_monthly"].find(
            {"data_type": "cpi"},
            {"_id": 0, "month": 1, "nt_mom": 1, "cpi_date": 1, "cpi_index": 1},
        ).sort("month", 1))
        self._cpi_cache = pd.DataFrame(docs)
        return self._cpi_cache.copy()

    @staticmethod
    def _cape_docs_from_frame(
        cape_df: pd.DataFrame,
        ts_code: str,
        cape_col: str,
        *,
        target_date: Optional[str] = None,
        pct_cheap: float = 20.0,
        pct_rich: float = 80.0,
        pct_bubble: float = 90.0,
    ) -> List[Dict[str, object]]:
        docs: List[Dict[str, object]] = []
        pct_col = f"{cape_col}_pct_rank_to_date"
        valid = cape_df.dropna(subset=[cape_col])
        if target_date is not None:
            valid = valid[valid["trade_date"].astype(str) == str(target_date)]
        for row in valid.to_dict("records"):
            pct_rank = float(row[pct_col])
            docs.append({
                "index_code": ts_code,
                "trade_date": row["trade_date"],
                "close": float(row["close"]),
                "pe_ttm": float(row["pe_ttm"]),
                "pe_source_date": str(row.get("pe_source_date", row["trade_date"])),
                "earnings_proxy": float(row["earnings_proxy"]),
                "cpi_index": float(row["cpi_index"]),
                "real_avg_earnings": float(row["real_avg_earnings"]),
                "cape": float(row[cape_col]),
                "window_years": float(cape_col.removeprefix("cape_").removesuffix("y")),
                "pct_rank_to_date": pct_rank,
                "valuation_band": cape_band_from_pct(
                    pct_rank,
                    pct_cheap=pct_cheap,
                    pct_rich=pct_rich,
                    pct_bubble=pct_bubble,
                ),
            })
        return docs


__all__ = ["MarketRegimeScanner"]