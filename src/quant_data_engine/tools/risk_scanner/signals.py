"""Pure signal-computation functions.

Each function takes an adapter (or stock_adapter) plus date range arguments
and returns the relevant signal object(s).  No class state — easy to test
and reuse independently.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from .models import (
    FactorCrowdingSignal,
    MADeviationSignal,
    RSISignal,
    SpeculationHeatSignal,
    TechRelativeStrengthSignal,
)
from .instruments import BROAD_INDICES, RISK_THRESHOLDS, SectorProfile

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Signal 1: MA deviation  (SOX analog)
# ---------------------------------------------------------------------------

def compute_ma_deviation(
    adapter,
    etfs: Dict[str, str],
    start_date: str,
    trade_date: str,
    warnings: List[str],
) -> List[MADeviationSignal]:
    """Compute price deviation from 20-day MA for each ETF in *etfs*."""
    signals: List[MADeviationSignal] = []

    for ts_code, name in etfs.items():
        try:
            df = adapter.fetch_etf_daily(ts_code=ts_code, start_date=start_date, end_date=trade_date)
            if df is None or df.empty:
                warnings.append(f"MA偏离: {ts_code} 无数据")
                continue

            df = _prepare_ohlcv(df)
            if len(df) < 22:
                warnings.append(f"MA偏离: {ts_code} 数据不足 ({len(df)} 行)")
                continue

            df["ma20"] = df["close"].rolling(20).mean()
            df["deviation"] = (df["close"] - df["ma20"]) / df["ma20"]
            df.dropna(subset=["ma20"], inplace=True)

            latest = df.iloc[-1]
            deviation = float(latest["deviation"])
            abs_dev_series = df["deviation"].abs()
            pct_rank = float((abs_dev_series < abs(deviation)).sum() / len(abs_dev_series) * 100)
            risk_level = _ma_risk_level(abs(deviation))

            if abs(deviation) >= RISK_THRESHOLDS["ma_deviation_high"]:
                direction = "高于" if deviation > 0 else "低于"
                warnings.append(
                    f"【高风险】{name}({ts_code}) 收盘价{direction}20日均线"
                    f" {abs(deviation)*100:.1f}% (历史{pct_rank:.1f}%分位)"
                )

            signals.append(MADeviationSignal(
                ts_code=ts_code,
                name=name,
                close=float(latest["close"]),
                ma20=round(float(latest["ma20"]), 4),
                deviation_pct=round(deviation, 6),
                rolling_pct_rank=round(pct_rank, 1),
                risk_level=risk_level,
            ))
        except Exception as exc:
            warnings.append(f"MA偏离计算失败 {ts_code}: {exc}")

    return signals


# ---------------------------------------------------------------------------
# Signal 2: RSI overbought  (CTA trend proxy)
# ---------------------------------------------------------------------------

def compute_rsi(
    adapter,
    extra_indices: Dict[str, str],
    start_date: str,
    trade_date: str,
    warnings: List[str],
    include_broad: bool = True,
) -> List[RSISignal]:
    """Compute RSI(14) for BROAD_INDICES merged with *extra_indices*.

    Args:
        include_broad: If False, skip BROAD_INDICES (A-share) and only use
            *extra_indices*.  Set to False for US-sector scans.
    """
    all_indices = {**BROAD_INDICES, **extra_indices} if include_broad else dict(extra_indices)
    signals: List[RSISignal] = []

    for ts_code, name in all_indices.items():
        try:
            df = adapter.fetch_index_daily(ts_code, start_date=start_date, end_date=trade_date)
            if df is None or df.empty:
                warnings.append(f"RSI: {ts_code} 无数据")
                continue

            df = _prepare_ohlcv(df)
            if len(df) < 20:
                warnings.append(f"RSI: {ts_code} 数据不足")
                continue

            df["rsi"] = _rsi(df["close"], period=14)
            clean = df["rsi"].dropna()
            if clean.empty:
                continue

            latest_rsi = float(clean.iloc[-1])
            pct_rank = float((clean < latest_rsi).sum() / len(clean) * 100)

            signal = "neutral"
            if latest_rsi >= RISK_THRESHOLDS["rsi_overbought"]:
                signal = "overbought"
                warnings.append(
                    f"【超买告警】{name}({ts_code}) RSI={latest_rsi:.1f}"
                    f" (历史{pct_rank:.1f}%分位)"
                )
            elif latest_rsi <= RISK_THRESHOLDS["rsi_oversold"]:
                signal = "oversold"

            signals.append(RSISignal(
                ts_code=ts_code,
                name=name,
                rsi=round(latest_rsi, 2),
                rolling_pct_rank=round(pct_rank, 1),
                signal=signal,
            ))
        except Exception as exc:
            warnings.append(f"RSI计算失败 {ts_code}: {exc}")

    return signals


# ---------------------------------------------------------------------------
# Signal 3: Volume surge  (speculative options volume proxy)
# ---------------------------------------------------------------------------

def compute_volume_surge(
    adapter,
    etfs: Dict[str, str],
    start_date: str,
    trade_date: str,
    warnings: List[str],
) -> List[SpeculationHeatSignal]:
    """Compute vol-surge ratio (latest vol / 20-day mean) for each ETF.

    Tushare ``fund_daily`` does not expose ``turnover_rate`` for ETFs, so we
    use the volume surge ratio as a proxy for speculative activity.
    """
    signals: List[SpeculationHeatSignal] = []

    for ts_code, name in etfs.items():
        try:
            df = adapter.fetch_etf_daily(ts_code=ts_code, start_date=start_date, end_date=trade_date)
            if df is None or df.empty:
                continue

            df = _prepare_ohlcv(df)
            if "vol" not in df.columns:
                warnings.append(f"投机热度: {ts_code} 缺少 vol 字段")
                continue

            df["vol"] = pd.to_numeric(df["vol"], errors="coerce")
            df.dropna(subset=["vol"], inplace=True)

            if len(df) < 22:
                warnings.append(f"投机热度: {ts_code} 数据不足 ({len(df)} 行)")
                continue

            df["vol_ma20"] = df["vol"].rolling(20).mean()
            df["vol_surge"] = df["vol"] / df["vol_ma20"]
            df.dropna(subset=["vol_surge"], inplace=True)

            latest_surge = float(df["vol_surge"].iloc[-1])
            pct_rank = float((df["vol_surge"] < latest_surge).sum() / len(df["vol_surge"]) * 100)

            signal = "normal"
            if pct_rank >= RISK_THRESHOLDS["percentile_extreme"]:
                signal = "extreme"
                warnings.append(
                    f"【极端投机热度】{name}({ts_code}) 成交量是20日均量的{latest_surge:.2f}倍"
                    f" (历史{pct_rank:.1f}%分位)"
                )
            elif pct_rank >= RISK_THRESHOLDS["percentile_crowded"]:
                signal = "elevated"

            signals.append(SpeculationHeatSignal(
                ts_code=ts_code,
                name=name,
                turnover_rate=round(latest_surge, 4),
                rolling_pct_rank=round(pct_rank, 1),
                signal=signal,
            ))
        except Exception as exc:
            warnings.append(f"投机热度计算失败 {ts_code}: {exc}")

    return signals


# ---------------------------------------------------------------------------
# Signal 4: Factor crowding  (JPM Meme crowding analog)
# ---------------------------------------------------------------------------

def compute_factor_crowding(
    stock_adapter,
    trade_date: str,
    warnings: List[str],
) -> Optional[FactorCrowdingSignal]:
    """Cross-sectional factor crowding across all A-share stocks.

    Requires a ``StockAdapter`` with ``fetch_daily`` and ``fetch_daily_basic``.
    Returns ``None`` and appends a warning when the adapter is not available.
    """
    if stock_adapter is None:
        warnings.append("因子拥挤度: 未提供 stock_adapter，跳过 (需传入 StockAdapter 实例)")
        return None

    fetch_basic = getattr(stock_adapter, "fetch_daily_basic", None)
    fetch_daily = getattr(stock_adapter, "fetch_daily", None)

    if fetch_basic is None or fetch_daily is None:
        warnings.append("因子拥挤度: stock_adapter 不支持 fetch_daily_basic/fetch_daily，跳过")
        return None

    try:
        df_basic = fetch_basic(trade_date=trade_date)
        df_daily = fetch_daily(trade_date=trade_date)

        if df_basic is None or df_basic.empty:
            warnings.append(f"因子拥挤度: {trade_date} daily_basic 无数据")
            return None
        if df_daily is None or df_daily.empty:
            warnings.append(f"因子拥挤度: {trade_date} daily 无数据")
            return None

        for col in ["turnover_rate"]:
            if col in df_basic.columns:
                df_basic[col] = pd.to_numeric(df_basic[col], errors="coerce")
        for col in ["pct_chg"]:
            if col in df_daily.columns:
                df_daily[col] = pd.to_numeric(df_daily[col], errors="coerce")

        if "ts_code" not in df_daily.columns or "ts_code" not in df_basic.columns:
            warnings.append("因子拥挤度: ts_code 字段缺失，无法合并")
            return None

        df = pd.merge(
            df_daily[["ts_code", "pct_chg"]].dropna(),
            df_basic[["ts_code", "turnover_rate"]].dropna(),
            on="ts_code",
            how="inner",
        )

        if len(df) < 100:
            warnings.append(f"因子拥挤度: 合并后数据量不足 ({len(df)} 行)")
            return None

        pct_chg_rank = df["pct_chg"].rank(pct=True) * 100
        volatility_rank = df["pct_chg"].abs().rank(pct=True) * 100
        turnover_rank = df["turnover_rate"].rank(pct=True) * 100

        df["_crowding_score"] = pct_chg_rank + volatility_rank + turnover_rank

        mask_crowded = (pct_chg_rank >= 80) & (volatility_rank >= 80) & (turnover_rank >= 80)
        crowded_stocks = (
            df.loc[mask_crowded]
            .sort_values("_crowding_score", ascending=False)["ts_code"]
            .tolist()
        )

        mom_pct = float(pct_chg_rank.quantile(0.90))
        vol_pct = float(volatility_rank.quantile(0.90))
        tr_pct = float(turnover_rank.quantile(0.90))
        composite = (mom_pct + vol_pct + tr_pct) / 3.0

        signal = "low"
        if composite >= RISK_THRESHOLDS["percentile_extreme"]:
            signal = "extreme"
            warnings.append(
                f"【极端因子拥挤】复合拥挤度={composite:.1f}"
                f" (动量{mom_pct:.1f}% / 波动{vol_pct:.1f}% / 换手{tr_pct:.1f}%)"
            )
        elif composite >= RISK_THRESHOLDS["percentile_crowded"]:
            signal = "high"
        elif composite >= 75:
            signal = "medium"

        return FactorCrowdingSignal(
            momentum_20d_pct_rank=round(mom_pct, 1),
            volatility_20d_pct_rank=round(vol_pct, 1),
            turnover_pct_rank=round(tr_pct, 1),
            composite_crowding_score=round(composite, 1),
            signal=signal,
            top_crowded_stocks=crowded_stocks[:10],
        )

    except Exception as exc:
        warnings.append(f"因子拥挤度计算失败: {exc}")
        return None


# ---------------------------------------------------------------------------
# Signal 5: Sector relative strength  (hedge-fund selling proxy)
# ---------------------------------------------------------------------------

def compute_sector_relative_strength(
    adapter,
    sector_index: Tuple[str, str],
    benchmark_index: Tuple[str, str],
    start_date: str,
    trade_date: str,
    warnings: List[str],
    period: int = 20,
) -> Optional[TechRelativeStrengthSignal]:
    """Compute N-day relative return of *sector_index* vs *benchmark_index*."""
    sector_code, sector_name = sector_index
    broad_code, broad_name = benchmark_index

    try:
        df_sector = adapter.fetch_index_daily(sector_code, start_date=start_date, end_date=trade_date)
        df_broad = adapter.fetch_index_daily(broad_code, start_date=start_date, end_date=trade_date)

        if df_sector is None or df_sector.empty or df_broad is None or df_broad.empty:
            warnings.append(f"相对强弱: {sector_name} 或 {broad_name} 数据获取失败")
            return None

        s = _prepare_ohlcv(df_sector).set_index("trade_date")["close"]
        b = _prepare_ohlcv(df_broad).set_index("trade_date")["close"]

        df = pd.DataFrame({"sector": s, "broad": b}).dropna()
        if len(df) < period + 2:
            warnings.append(f"相对强弱: 数据不足 ({len(df)} 行)")
            return None

        df["sector_ret"] = df["sector"].pct_change(period)
        df["broad_ret"] = df["broad"].pct_change(period)
        df["rel_ret"] = df["sector_ret"] - df["broad_ret"]
        df.dropna(inplace=True)

        latest = df.iloc[-1]
        sector_ret = float(latest["sector_ret"])
        broad_ret = float(latest["broad_ret"])
        rel_ret = float(latest["rel_ret"])
        pct_rank = float((df["rel_ret"] < rel_ret).sum() / len(df["rel_ret"]) * 100)

        signal = "neutral"
        if pct_rank <= 5:
            signal = "rapid_sell"
            warnings.append(
                f"【极端抛售信号】{sector_name} vs {broad_name} 相对收益"
                f" {rel_ret*100:+.2f}% 处于历史{pct_rank:.1f}%分位 — 疑似机构加速抛售"
            )
        elif rel_ret <= RISK_THRESHOLDS["rel_strength_warn"]:
            signal = "underperform"
        elif rel_ret >= 0.05:
            signal = "outperform"

        return TechRelativeStrengthSignal(
            sector_name=sector_name,
            period_days=period,
            tech_return=round(sector_ret, 6),
            broad_return=round(broad_ret, 6),
            relative_return=round(rel_ret, 6),
            rolling_pct_rank=round(pct_rank, 1),
            signal=signal,
        )

    except Exception as exc:
        warnings.append(f"相对强弱计算失败: {exc}")
        return None


# ---------------------------------------------------------------------------
# Composite score aggregation
# ---------------------------------------------------------------------------

def aggregate_risk_score(
    ma_signals: List[MADeviationSignal],
    rsi_signals: List[RSISignal],
    spec_signals: List[SpeculationHeatSignal],
    crowding: Optional[FactorCrowdingSignal],
    rel_strength: Optional[TechRelativeStrengthSignal],
) -> Tuple[float, str]:
    """Combine sub-signals into a 0-100 composite risk score."""
    components: List[float] = []

    if ma_signals:
        components.append(max(s.rolling_pct_rank for s in ma_signals))

    if rsi_signals:
        overbought = [s for s in rsi_signals if s.signal == "overbought"]
        if overbought:
            components.append(max(s.rolling_pct_rank for s in overbought))
        else:
            components.append(max(s.rolling_pct_rank for s in rsi_signals) * 0.5)

    if spec_signals:
        components.append(max(s.rolling_pct_rank for s in spec_signals))

    if crowding:
        components.append(crowding.composite_crowding_score)

    if rel_strength:
        # Low pct_rank = sector badly underperforming = high risk
        components.append(100.0 - rel_strength.rolling_pct_rank)

    if not components:
        return 0.0, "unknown"

    score = float(np.mean(components))
    level = "low"
    if score >= 80:
        level = "extreme"
    elif score >= 65:
        level = "high"
    elif score >= 45:
        level = "medium"

    return score, level


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _prepare_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "trade_date" in df.columns:
        df["trade_date"] = df["trade_date"].astype(str)
        df = df.sort_values("trade_date").reset_index(drop=True)
    for col in ["open", "high", "low", "close", "vol", "amount"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def _rsi(close: pd.Series, period: int = 14) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)
    avg_gain = gain.ewm(com=period - 1, min_periods=period).mean()
    avg_loss = loss.ewm(com=period - 1, min_periods=period).mean()
    rs = avg_gain / avg_loss.replace(0, float("nan"))
    return 100.0 - (100.0 / (1.0 + rs))


def _ma_risk_level(abs_dev: float) -> str:
    if abs_dev >= 0.20:
        return "extreme"
    if abs_dev >= RISK_THRESHOLDS["ma_deviation_high"]:
        return "high"
    if abs_dev >= RISK_THRESHOLDS["ma_deviation_warn"]:
        return "watch"
    return "normal"


def offset_date(date_str: str, offset_days: int) -> str:
    dt = datetime.strptime(date_str, "%Y%m%d") + timedelta(days=offset_days)
    return dt.strftime("%Y%m%d")


__all__ = [
    "compute_ma_deviation",
    "compute_rsi",
    "compute_volume_surge",
    "compute_factor_crowding",
    "compute_sector_relative_strength",
    "aggregate_risk_score",
    "offset_date",
]
