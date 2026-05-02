"""Data-model dataclasses for the market risk scanner."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class MADeviationSignal:
    """Distance of a sector ETF from its 20-day MA (SOX analog)."""
    ts_code: str
    name: str
    close: float
    ma20: float
    deviation_pct: float        # (close - ma20) / ma20
    rolling_pct_rank: float     # percentile among lookback window
    risk_level: str             # "normal" | "watch" | "high" | "extreme"


@dataclass
class RSISignal:
    """RSI(14) overbought proxy for an index (CTA trend analog)."""
    ts_code: str
    name: str
    rsi: float
    rolling_pct_rank: float     # historical percentile of this RSI value
    signal: str                 # "neutral" | "overbought" | "oversold"


@dataclass
class SpeculationHeatSignal:
    """Volume surge ratio proxy for speculative activity.

    ``turnover_rate`` stores the vol-surge ratio (latest vol / 20-day mean vol)
    rather than an actual turnover-rate percentage, because Tushare ``fund_daily``
    does not expose turnover_rate for ETFs.
    """
    ts_code: str
    name: str
    turnover_rate: float        # vol surge ratio (x× 20-day mean)
    rolling_pct_rank: float
    signal: str                 # "normal" | "elevated" | "extreme"


@dataclass
class FactorCrowdingSignal:
    """Cross-sectional factor crowding (Meme crowding analog).

    Computes momentum, volatility, and turnover percentiles across all
    A-share stocks available via ``daily`` + ``daily_basic`` on the scan date.
    """
    momentum_20d_pct_rank: float    # 90th-pct of cross-sectional momentum ranks
    volatility_20d_pct_rank: float  # 90th-pct of cross-sectional volatility ranks
    turnover_pct_rank: float        # 90th-pct of cross-sectional turnover ranks
    composite_crowding_score: float # 0-100 composite
    signal: str                     # "low" | "medium" | "high" | "extreme"
    top_crowded_stocks: List[str]   # sorted by composite score desc


@dataclass
class TechRelativeStrengthSignal:
    """Sector-vs-benchmark relative strength (hedge-fund selling proxy)."""
    sector_name: str            # e.g. "半导体科技", "金融"
    period_days: int
    tech_return: float          # sector N-day return
    broad_return: float         # benchmark N-day return
    relative_return: float      # sector - benchmark
    rolling_pct_rank: float     # percentile of relative return in lookback
    signal: str                 # "outperform" | "neutral" | "underperform" | "rapid_sell"


@dataclass
class RiskReport:
    """Consolidated risk report for a single scan date + sector."""
    scan_date: str
    generated_at: str
    sector: str                 # sector profile key, e.g. "semicon"
    overall_risk_score: float   # 0-100 composite
    overall_risk_level: str     # "low" | "medium" | "high" | "extreme"
    ma_deviation: List[MADeviationSignal] = field(default_factory=list)
    rsi_signals: List[RSISignal] = field(default_factory=list)
    speculation_heat: List[SpeculationHeatSignal] = field(default_factory=list)
    factor_crowding: Optional[FactorCrowdingSignal] = None
    tech_relative_strength: Optional[TechRelativeStrengthSignal] = None
    warnings: List[str] = field(default_factory=list)


__all__ = [
    "MADeviationSignal",
    "RSISignal",
    "SpeculationHeatSignal",
    "FactorCrowdingSignal",
    "TechRelativeStrengthSignal",
    "RiskReport",
]
