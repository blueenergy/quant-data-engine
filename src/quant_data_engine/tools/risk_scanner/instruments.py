"""Instrument constants and sector profile definitions.

Adding a new sector is as simple as adding an entry to ``SECTOR_PROFILES``::

    SECTOR_PROFILES["media"] = SectorProfile(
        name="传媒数字",
        etfs={"512980.SH": "传媒ETF", "159941.SZ": "数字经济ETF"},
        sector_index=("399006.SZ", "创业板"),
        benchmark_index=("000300.SH", "沪深300"),
        extra_rsi_indices={},
    )
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Tuple


# ---------------------------------------------------------------------------
# Broad-market indices used for RSI / CTA proxy (common to all sectors)
# ---------------------------------------------------------------------------
BROAD_INDICES: Dict[str, str] = {
    "000300.SH": "沪深300",
    "000905.SH": "中证500",
    "399006.SZ": "创业板",
    "000016.SH": "上证50",
}

# ---------------------------------------------------------------------------
# Risk-level thresholds (shared)
# ---------------------------------------------------------------------------
RISK_THRESHOLDS: Dict[str, float] = {
    "ma_deviation_warn": 0.10,   # ≥10% above/below MA20 → watch
    "ma_deviation_high": 0.15,   # ≥15% → high-risk
    "rsi_overbought": 70.0,
    "rsi_oversold": 30.0,
    "percentile_crowded": 90.0,  # ≥90th pct = crowded
    "percentile_extreme": 97.0,  # ≥97th pct = extreme
    "rel_strength_warn": -0.05,  # underperforms benchmark by ≥5% → warning
}


# ---------------------------------------------------------------------------
# Sector profile
# ---------------------------------------------------------------------------
@dataclass
class SectorProfile:
    """Defines which instruments to watch for a given sector.

    Attributes:
        name: Human-readable sector label shown in reports.
        etfs: ETFs used for MA-deviation (Signal 1) and vol-surge (Signal 3).
              Dict[ts_code, display_name].
        sector_index: ``(ts_code, name)`` used as the sector proxy in the
            relative-strength signal (Signal 5).
        benchmark_index: ``(ts_code, name)`` used as the benchmark in Signal 5.
        extra_rsi_indices: Additional sector-specific indices merged into
            ``BROAD_INDICES`` for the RSI signal (Signal 2).
    """
    name: str
    etfs: Dict[str, str]
    sector_index: Tuple[str, str]
    benchmark_index: Tuple[str, str]
    extra_rsi_indices: Dict[str, str] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Built-in sector profiles
# ---------------------------------------------------------------------------
SECTOR_PROFILES: Dict[str, SectorProfile] = {

    "semicon": SectorProfile(
        name="半导体芯片",
        etfs={
            "512760.SH": "半导体ETF",
            "159995.SZ": "芯片ETF",
        },
        sector_index=("000688.SH", "科创50"),
        benchmark_index=("000300.SH", "沪深300"),
        extra_rsi_indices={"000688.SH": "科创50"},
    ),

    # 半导体设备：跟踪晶圆厂资本开支周期，与芯片设计/制造周期存在阶段性背离
    "semicon_eq": SectorProfile(
        name="半导体设备",
        etfs={
            "159516.SZ": "半导体设备ETF",
            "561980.SH": "半导体设备ETF招商",
        },
        sector_index=("000688.SH", "科创50"),
        benchmark_index=("000300.SH", "沪深300"),
        extra_rsi_indices={"000688.SH": "科创50"},
    ),

    "finance": SectorProfile(
        name="金融",
        etfs={
            "512000.SH": "证券ETF",
            "512800.SH": "银行ETF",
        },
        sector_index=("000016.SH", "上证50"),
        benchmark_index=("000905.SH", "中证500"),
        extra_rsi_indices={},
    ),

    "consumer": SectorProfile(
        name="消费",
        etfs={
            "512690.SH": "白酒ETF",
            "159928.SZ": "消费ETF",
        },
        sector_index=("000300.SH", "沪深300"),
        benchmark_index=("000905.SH", "中证500"),
        extra_rsi_indices={},
    ),

    "healthcare": SectorProfile(
        name="医药医疗",
        etfs={
            "159847.SZ": "医疗ETF",
            "159929.SZ": "医药ETF"
        },
        sector_index=("000905.SH", "中证500"),
        benchmark_index=("000300.SH", "沪深300"),
        extra_rsi_indices={},
    ),

    "nev": SectorProfile(
        name="新能源",
        etfs={
            "515790.SH": "光伏ETF",
            "516160.SH": "新能源ETF",
        },
        sector_index=("399006.SZ", "创业板"),
        benchmark_index=("000300.SH", "沪深300"),
        extra_rsi_indices={"399006.SZ": "创业板"},
    ),

    # 机器人：用科创50作为板块代理指数（机器人/AI硬件主要在科创板上市）
    "robot": SectorProfile(
        name="机器人",
        etfs={
            "159272.SZ": "机器人ETF",
        },
        sector_index=("000688.SH", "科创50"),
        benchmark_index=("000300.SH", "沪深300"),
        extra_rsi_indices={"000688.SH": "科创50"},
    ),

    # 旅游：用中证500作为板块代理指数（旅游/酒店/景区多为中小盘）
    "tourism": SectorProfile(
        name="旅游",
        etfs={
            "159766.SZ": "旅游ETF",
        },
        sector_index=("000905.SH", "中证500"),
        benchmark_index=("000300.SH", "沪深300"),
        extra_rsi_indices={},
    ),

    # 航空航天：用科创50作为板块代理指数（航天/卫星/低空主要集中在科创板）
    "aerospace": SectorProfile(
        name="航空航天",
        etfs={
            "159227.SZ": "航空航天ETF",
            "563230.SH": "卫星ETF",
        },
        sector_index=("000688.SH", "科创50"),
        benchmark_index=("000300.SH", "沪深300"),
        extra_rsi_indices={"000688.SH": "科创50"},
    ),
}


__all__ = [
    "BROAD_INDICES",
    "RISK_THRESHOLDS",
    "SectorProfile",
    "SECTOR_PROFILES",
]
