"""MarketRiskScanner — orchestrates signal computation for any sector and date."""
from __future__ import annotations

import logging
from datetime import datetime
from typing import List, Optional

import pandas as pd

from .instruments import SECTOR_PROFILES, US_SECTOR_PROFILES, SectorProfile
from .models import FactorCrowdingSignal, RiskReport
from .reporter import print_report, compare_reports
from .signals import (
    aggregate_risk_score,
    compute_factor_crowding,
    compute_ma_deviation,
    compute_rsi,
    compute_sector_relative_strength,
    compute_volume_surge,
    offset_date,
)

logger = logging.getLogger(__name__)


class MarketRiskScanner:
    """Orchestrates A-share market risk scanning for any sector and any date.

    Args:
        adapter: ``TushareAdapter`` — provides ETF daily + index daily data.
        stock_adapter: Optional ``StockAdapter`` — provides full-market stock
            daily + daily_basic for the factor-crowding signal (Signal 4).
            If omitted, Signal 4 is skipped gracefully.
        lookback_days: Calendar days of history fetched for percentile
            baselines.  252 ≈ 1 trading year; 504 ≈ 2 years.

    Example::

        from quant_data_engine.tools.risk_scanner import MarketRiskScanner
        scanner = MarketRiskScanner(adapter, stock_adapter=stock_adapter)

        # Scan any date, any sector
        report = scanner.scan("20260502", sector="nev")
        scanner.print_report(report)

        # Compare multiple historical dates
        reports = scanner.compare(["20250115", "20250701", "20260502"], sector="semicon")
        scanner.compare_reports(reports)
    """

    def __init__(self, adapter, stock_adapter=None, global_adapter=None, lookback_days: int = 252):
        self.adapter = adapter
        self.stock_adapter = stock_adapter
        self.global_adapter = global_adapter
        self.lookback_days = lookback_days

    # ------------------------------------------------------------------
    # Primary API
    # ------------------------------------------------------------------

    def scan(self, trade_date: str, sector: str = "semicon") -> RiskReport:
        """Run all five risk signals for *sector* on *trade_date*.

        Args:
            trade_date: Target date in ``YYYYMMDD`` format.  Any historical
                date is supported — pass any past trading day for backtesting
                or comparison analysis.
            sector: Key into ``SECTOR_PROFILES`` (e.g. ``"semicon"``,
                ``"finance"``, ``"consumer"``, ``"healthcare"``, ``"nev"``).

        Returns:
            ``RiskReport`` containing all signal results and a composite score.
        """
        profile = self._resolve_profile(sector)
        effective_adapter = self._resolve_adapter(sector)
        start_date = offset_date(trade_date, -self.lookback_days)

        logger.info(
            "MarketRiskScanner.scan: sector=%s date=%s lookback_start=%s",
            sector, trade_date, start_date,
        )

        warnings: List[str] = []
        is_us = sector.startswith("us_")

        ma_signals = compute_ma_deviation(
            effective_adapter, profile.etfs, start_date, trade_date, warnings
        )
        rsi_signals = compute_rsi(
            effective_adapter, profile.extra_rsi_indices, start_date, trade_date, warnings,
            include_broad=not is_us,
        )
        spec_signals = compute_volume_surge(
            effective_adapter, profile.etfs, start_date, trade_date, warnings
        )

        if is_us:
            # Signal 4: use VIX level as crowding proxy for US sectors
            crowding = self._compute_vix_crowding(start_date, trade_date, warnings)
        else:
            crowding = compute_factor_crowding(
                self.stock_adapter, trade_date, warnings
            )

        rel_strength = compute_sector_relative_strength(
            effective_adapter,
            profile.sector_index,
            profile.benchmark_index,
            start_date,
            trade_date,
            warnings,
        )

        score, level = aggregate_risk_score(
            ma_signals, rsi_signals, spec_signals, crowding, rel_strength
        )

        return RiskReport(
            scan_date=trade_date,
            generated_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            sector=sector,
            overall_risk_score=round(score, 1),
            overall_risk_level=level,
            ma_deviation=ma_signals,
            rsi_signals=rsi_signals,
            speculation_heat=spec_signals,
            factor_crowding=crowding,
            tech_relative_strength=rel_strength,
            warnings=warnings,
        )

    def compare(self, trade_dates: List[str], sector: str = "semicon") -> List[RiskReport]:
        """Scan multiple historical dates for side-by-side comparison.

        Args:
            trade_dates: List of dates in ``YYYYMMDD`` format.  Can be any
                historical dates — past, recent, or a mix.
            sector: Sector profile to apply uniformly across all dates.

        Returns:
            List of ``RiskReport`` objects in the same order as *trade_dates*.
        """
        return [self.scan(d, sector=sector) for d in trade_dates]

    # ------------------------------------------------------------------
    # Convenience wrappers for report output
    # ------------------------------------------------------------------

    @staticmethod
    def print_report(report: RiskReport) -> None:
        """Print a formatted single-report dashboard."""
        print_report(report)

    @staticmethod
    def compare_reports(reports: List[RiskReport]) -> None:
        """Print a side-by-side comparison table across multiple dates."""
        compare_reports(reports)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _resolve_profile(self, sector: str) -> SectorProfile:
        if sector.startswith("us_"):
            if sector not in US_SECTOR_PROFILES:
                available = ", ".join(US_SECTOR_PROFILES.keys())
                raise ValueError(f"Unknown US sector '{sector}'. Available: {available}")
            return US_SECTOR_PROFILES[sector]
        if sector not in SECTOR_PROFILES:
            available = ", ".join(SECTOR_PROFILES.keys())
            raise ValueError(f"Unknown sector '{sector}'. Available: {available}")
        return SECTOR_PROFILES[sector]

    def _resolve_adapter(self, sector: str):
        """Return the appropriate data adapter for the sector."""
        if sector.startswith("us_"):
            if self.global_adapter is None:
                raise ValueError(
                    f"Sector '{sector}' requires a GlobalMarketAdapter. "
                    "Pass global_adapter= when constructing MarketRiskScanner."
                )
            return self.global_adapter
        return self.adapter

    def _compute_vix_crowding(
        self,
        start_date: str,
        trade_date: str,
        warnings: List[str],
    ) -> Optional["FactorCrowdingSignal"]:
        """VIX-level proxy for US market crowding / sentiment extremes.

        Maps current VIX percentile (vs 252-day history) to a crowding signal:
          - VIX > 25 (high fear) or VIX < 15 (complacency) → elevated crowding
          - risk_level mirrors A-share crowding thresholds via percentile rank
        """
        if self.global_adapter is None:
            warnings.append("VIX拥挤代理: global_adapter 未提供，跳过")
            return None
        try:
            df = self.global_adapter.fetch_index_daily("^VIX", start_date, trade_date)
            if df.empty or "close" not in df.columns:
                warnings.append("VIX拥挤代理: ^VIX 数据不可得，跳过")
                return None

            df["close"] = pd.to_numeric(df["close"], errors="coerce")
            df = df.dropna(subset=["close"])
            if len(df) < 20:
                warnings.append("VIX拥挤代理: 数据量不足，跳过")
                return None

            vix_now = float(df["close"].iloc[-1])
            # Percentile of current VIX vs history
            pct = float((df["close"] < vix_now).sum() / len(df) * 100)

            # Risk level: extreme fear/complacency = high crowding risk
            if vix_now > 30 or vix_now < 13:
                risk_level = "extreme"
            elif vix_now > 25 or vix_now < 15:
                risk_level = "high"
            elif vix_now > 20:
                risk_level = "medium"
            else:
                risk_level = "low"

            return FactorCrowdingSignal(
                momentum_20d_pct_rank=round(pct, 1),
                volatility_20d_pct_rank=round(pct, 1),
                turnover_pct_rank=round(pct, 1),
                composite_crowding_score=round(pct, 1),
                signal=risk_level,
                top_crowded_stocks=[f"^VIX={vix_now:.1f}"],
            )
        except Exception as exc:
            warnings.append(f"VIX拥挤代理: 计算失败 ({exc})")
            return None


__all__ = ["MarketRiskScanner"]
