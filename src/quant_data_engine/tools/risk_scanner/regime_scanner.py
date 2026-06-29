"""Market regime scanner for A-share breadth and valuation signals."""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import pandas as pd

from .breadth_builder import MarketBreadthBuilder
from .breadth_signals import (
    aggregate_regime_alert,
    cape_band_from_pct,
    classify_cape,
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
        self.breadth_builder = MarketBreadthBuilder(repo, lookback_calendar_days=lookback_calendar_days)
        self.last_breadth_doc: Optional[Dict[str, object]] = None
        self.last_cape_docs: List[Dict[str, object]] = []
        self._cpi_cache: Optional[pd.DataFrame] = None

    def scan(self, trade_date: str) -> MarketRegimeReport:
        """Run all regime signals for *trade_date*."""
        warnings: List[str] = []
        self.last_breadth_doc = None
        self.last_cape_docs = []
        start_date = _offset_date(trade_date, -self.lookback_calendar_days)

        breadth_today = self.repo.get_breadth(trade_date)
        if breadth_today:
            breadth_today = {k: v for k, v in breadth_today.items() if k != "updated_at"}
        else:
            warnings.append(
                f"广度: {trade_date} 未找到预聚合行，现场计算（建议先运行 run_market_breadth_daily）"
            )
            breadth_today = self.breadth_builder.build(trade_date, warnings=warnings)
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