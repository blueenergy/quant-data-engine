"""risk_scanner — A-share market risk scanner package.

Public API::

    from quant_data_engine.tools.risk_scanner import MarketRiskScanner, SECTOR_PROFILES
    from quant_data_engine.tools.risk_scanner.reporter import compare_reports
"""
from .scanner import MarketRiskScanner
from .models import (
    MADeviationSignal,
    RSISignal,
    SpeculationHeatSignal,
    FactorCrowdingSignal,
    TechRelativeStrengthSignal,
    RiskReport,
)
from .instruments import SECTOR_PROFILES, SectorProfile, RISK_THRESHOLDS, BROAD_INDICES
from .reporter import print_report, compare_reports

__all__ = [
    "MarketRiskScanner",
    "RiskReport",
    "MADeviationSignal",
    "RSISignal",
    "SpeculationHeatSignal",
    "FactorCrowdingSignal",
    "TechRelativeStrengthSignal",
    "SECTOR_PROFILES",
    "SectorProfile",
    "RISK_THRESHOLDS",
    "BROAD_INDICES",
    "print_report",
    "compare_reports",
]
