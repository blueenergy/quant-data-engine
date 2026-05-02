"""Backward-compatibility shim for market_risk_scanner.

The implementation has been split into the ``risk_scanner`` sub-package:

    src/quant_data_engine/tools/risk_scanner/
        __init__.py      — public API
        models.py        — dataclasses
        instruments.py   — sector profiles & thresholds
        signals.py       — pure signal-computation functions
        scanner.py       — MarketRiskScanner class
        reporter.py      — print_report / compare_reports

Import from either location — both are equivalent::

    from quant_data_engine.tools.market_risk_scanner import MarketRiskScanner
    from quant_data_engine.tools.risk_scanner import MarketRiskScanner
"""
# Re-export everything from the new package so existing imports keep working.
from quant_data_engine.tools.risk_scanner import (  # noqa: F401
    MarketRiskScanner,
    RiskReport,
    MADeviationSignal,
    RSISignal,
    SpeculationHeatSignal,
    FactorCrowdingSignal,
    TechRelativeStrengthSignal,
    SECTOR_PROFILES,
    SectorProfile,
    RISK_THRESHOLDS,
    BROAD_INDICES,
    print_report,
    compare_reports,
)

# Legacy name aliases used in tests / scripts
from quant_data_engine.tools.risk_scanner.instruments import (  # noqa: F401
    BROAD_INDICES as BROAD_INDICES,
    RISK_THRESHOLDS as RISK_THRESHOLDS,
)

# Keep SEMICON_ETFS and TECH_INDICES as top-level names for backward compat
from quant_data_engine.tools.risk_scanner.instruments import SECTOR_PROFILES as _SP
SEMICON_ETFS = _SP["semicon"].etfs
TECH_INDICES = {_SP["semicon"].sector_index[0]: _SP["semicon"].sector_index[1]}

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
    "SEMICON_ETFS",
    "TECH_INDICES",
    "print_report",
    "compare_reports",
]
