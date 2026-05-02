"""MarketRiskScanner — orchestrates signal computation for any sector and date."""
from __future__ import annotations

import logging
from datetime import datetime
from typing import List, Optional

from .instruments import SECTOR_PROFILES, SectorProfile
from .models import RiskReport
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

    def __init__(self, adapter, stock_adapter=None, lookback_days: int = 252):
        self.adapter = adapter
        self.stock_adapter = stock_adapter
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
        start_date = offset_date(trade_date, -self.lookback_days)

        logger.info(
            "MarketRiskScanner.scan: sector=%s date=%s lookback_start=%s",
            sector, trade_date, start_date,
        )

        warnings: List[str] = []

        ma_signals = compute_ma_deviation(
            self.adapter, profile.etfs, start_date, trade_date, warnings
        )
        rsi_signals = compute_rsi(
            self.adapter, profile.extra_rsi_indices, start_date, trade_date, warnings
        )
        spec_signals = compute_volume_surge(
            self.adapter, profile.etfs, start_date, trade_date, warnings
        )
        crowding = compute_factor_crowding(
            self.stock_adapter, trade_date, warnings
        )
        rel_strength = compute_sector_relative_strength(
            self.adapter,
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

    @staticmethod
    def _resolve_profile(sector: str) -> SectorProfile:
        if sector not in SECTOR_PROFILES:
            available = ", ".join(SECTOR_PROFILES.keys())
            raise ValueError(
                f"Unknown sector '{sector}'. Available: {available}"
            )
        return SECTOR_PROFILES[sector]


__all__ = ["MarketRiskScanner"]
