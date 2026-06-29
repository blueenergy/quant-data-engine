"""risk_scanner — A-share market risk scanner package.

Public API::

    from quant_data_engine.tools.risk_scanner import MarketRiskScanner, SECTOR_PROFILES
    from quant_data_engine.tools.risk_scanner.reporter import compare_reports
"""
from .scanner import MarketRiskScanner
from .regime_scanner import MarketRegimeScanner
from .models import (
    CAPESignal,
    HindenburgSignal,
    MADeviationSignal,
    MCLSignal,
    MarketRegimeReport,
    RSISignal,
    SpeculationHeatSignal,
    FactorCrowdingSignal,
    TechRelativeStrengthSignal,
    RiskReport,
)
from .instruments import (
    BREADTH_THRESHOLDS,
    BROAD_INDICES,
    CAPE_INDEX_PROFILES,
    RISK_THRESHOLDS,
    SECTOR_PROFILES,
    US_SECTOR_PROFILES,
    SectorProfile,
)
from .reporter import print_report, compare_reports

__all__ = [
    "MarketRiskScanner",
    "MarketRegimeScanner",
    "MarketRegimeReport",
    "RiskReport",
    "CAPESignal",
    "HindenburgSignal",
    "MADeviationSignal",
    "MCLSignal",
    "RSISignal",
    "SpeculationHeatSignal",
    "FactorCrowdingSignal",
    "TechRelativeStrengthSignal",
    "SECTOR_PROFILES",
    "US_SECTOR_PROFILES",
    "SectorProfile",
    "BREADTH_THRESHOLDS",
    "CAPE_INDEX_PROFILES",
    "RISK_THRESHOLDS",
    "BROAD_INDICES",
    "print_report",
    "compare_reports",
]
