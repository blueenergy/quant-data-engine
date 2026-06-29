"""Pure signal functions for A-share breadth and valuation regimes."""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from .models import CAPESignal, HindenburgSignal, MCLSignal


def _code_col(df: pd.DataFrame) -> str:
    candidates = [col for col in ("ts_code", "symbol") if col in df.columns]
    if candidates:
        return max(candidates, key=lambda col: df[col].fillna("").astype(str).str.strip().ne("").sum())
    raise ValueError("DataFrame requires ts_code or symbol column")


def _clean_trade_date(series: pd.Series) -> pd.Series:
    return series.astype(str).str.replace("-", "", regex=False)


def _to_date(value: str) -> datetime:
    return datetime.strptime(str(value).replace("-", ""), "%Y%m%d")


def _expanding_percentile_rank(values: pd.Series) -> pd.Series:
    ranks: List[float] = []
    seen: List[float] = []
    for value in values:
        if pd.isna(value):
            ranks.append(float("nan"))
            continue
        current = float(value)
        seen.append(current)
        ranks.append(sum(v <= current for v in seen) / len(seen) * 100.0)
    return pd.Series(ranks, index=values.index)


def _prepare_cpi_level(cpi_index: pd.DataFrame) -> pd.DataFrame:
    if cpi_index is None or cpi_index.empty:
        raise ValueError("CPI data is empty")
    df = cpi_index.copy()
    if "cpi_date" in df.columns and "cpi_index" in df.columns:
        df["cpi_date"] = pd.to_datetime(df["cpi_date"], errors="coerce")
        df["cpi_index"] = pd.to_numeric(df["cpi_index"], errors="coerce")
        return df.dropna(subset=["cpi_date", "cpi_index"])[["cpi_date", "cpi_index"]].sort_values("cpi_date")
    if "month" not in df.columns:
        raise ValueError("CPI data requires month plus nt_mom, or cpi_date plus cpi_index")
    df["month"] = df["month"].astype(str)
    df = df.sort_values("month").reset_index(drop=True)
    if "nt_mom" not in df.columns:
        raise ValueError("CPI level must be built from nt_mom when cpi_index is absent")
    mom = pd.to_numeric(df["nt_mom"], errors="coerce").fillna(0.0)
    df["cpi_index"] = (1.0 + mom / 100.0).cumprod() * 100.0
    df["cpi_date"] = pd.to_datetime(df["month"] + "01", format="%Y%m%d", errors="coerce")
    return df.dropna(subset=["cpi_date", "cpi_index"])[["cpi_date", "cpi_index"]].sort_values("cpi_date")


def build_normal_universe(
    stock_daily: pd.DataFrame,
    stock_info: pd.DataFrame,
    trade_date: str,
    trade_cal: pd.DataFrame,
    *,
    exclude_ipo_trading_days: int = 250,
    warnings: Optional[List[str]] = None,
) -> pd.DataFrame:
    """Return normal-trading stock rows for *trade_date* after universe filters."""
    warnings = warnings if warnings is not None else []
    if stock_daily is None or stock_daily.empty:
        warnings.append(f"广度统计: {trade_date} 无 stock_daily 数据")
        return pd.DataFrame()

    daily = stock_daily.copy()
    code_col = _code_col(daily)
    daily["trade_date"] = _clean_trade_date(daily["trade_date"])
    today = daily[daily["trade_date"] == trade_date].copy()
    if today.empty:
        warnings.append(f"广度统计: {trade_date} 当日无 volume_price 行")
        return today

    today["ts_code"] = today[code_col].astype(str)
    if stock_info is not None and not stock_info.empty:
        info = stock_info.copy()
        info_col = _code_col(info)
        info["ts_code"] = info[info_col].astype(str)
        keep_cols = [col for col in ["ts_code", "name", "list_date", "list_status", "delist_date"] if col in info.columns]
        today = today.merge(info[keep_cols].drop_duplicates("ts_code"), on="ts_code", how="left")

    if "list_status" in today.columns:
        today = today[today["list_status"].fillna("L").astype(str).str.upper().eq("L")]
    if "delist_date" in today.columns:
        today = today[today["delist_date"].isna() | today["delist_date"].astype(str).isin(["", "nan", "None"])]
    if "name" in today.columns:
        name = today["name"].fillna("").astype(str).str.upper()
        today = today[~name.str.contains("ST", regex=False)]

    if "list_date" in today.columns and trade_cal is not None and not trade_cal.empty:
        cal = trade_cal.copy()
        if "cal_date" in cal.columns:
            cal["cal_date"] = _clean_trade_date(cal["cal_date"])
            if "is_open" in cal.columns:
                cal = cal[pd.to_numeric(cal["is_open"], errors="coerce") == 1]
            open_dates = sorted(cal["cal_date"].astype(str).unique())

            def listed_long_enough(list_date: object) -> bool:
                if pd.isna(list_date) or not str(list_date).strip():
                    return True
                listed = str(list_date).replace("-", "")
                count = sum(listed <= d <= trade_date for d in open_dates)
                return count >= exclude_ipo_trading_days

            before = len(today)
            today = today[today["list_date"].apply(listed_long_enough)]
            dropped = before - len(today)
            if dropped:
                warnings.append(f"广度统计: 剔除上市未满{exclude_ipo_trading_days}个交易日股票 {dropped} 只")

    return today.reset_index(drop=True)


def compute_breadth_table(
    stock_daily: pd.DataFrame,
    stock_info: pd.DataFrame,
    trade_date: str,
    trade_cal: pd.DataFrame,
    *,
    lookback_trading_days: int = 250,
    exclude_ipo_trading_days: int = 250,
    warnings: Optional[List[str]] = None,
) -> Dict[str, object]:
    """Compute one-day breadth stats for *trade_date*."""
    warnings = warnings if warnings is not None else []
    universe = build_normal_universe(
        stock_daily,
        stock_info,
        trade_date,
        trade_cal,
        exclude_ipo_trading_days=exclude_ipo_trading_days,
        warnings=warnings,
    )
    if universe.empty:
        return {
            "trade_date": trade_date,
            "total_normal": 0,
            "advances": 0,
            "declines": 0,
            "unchanged": 0,
            "new_high_count": 0,
            "new_low_count": 0,
            "new_high_pct": 0.0,
            "new_low_pct": 0.0,
        }

    total_normal = int(len(universe))

    daily = stock_daily.copy()
    code_col = _code_col(daily)
    daily["ts_code"] = daily[code_col].astype(str)
    daily["trade_date"] = _clean_trade_date(daily["trade_date"])
    daily["close"] = pd.to_numeric(daily["close"], errors="coerce")
    daily = daily[daily["trade_date"] <= trade_date].dropna(subset=["close"])
    current_codes = set(universe["ts_code"].astype(str))
    history = daily[daily["ts_code"].isin(current_codes)].sort_values(["ts_code", "trade_date"])

    # Prefer explicit pct_chg values; fall back per stock to close-to-close
    # change when older volume_price rows store only OHLC.
    pct_by_code = {}
    if "pct_chg" in universe.columns:
        pct_frame = universe[["ts_code", "pct_chg"]].copy()
        pct_frame["pct_chg"] = pd.to_numeric(pct_frame["pct_chg"], errors="coerce")
        pct_by_code = pct_frame.dropna(subset=["pct_chg"]).set_index("ts_code")["pct_chg"].to_dict()
    recent = history.groupby("ts_code", group_keys=False, sort=False).tail(lookback_trading_days).copy()
    recent["prev_close"] = recent.groupby("ts_code", sort=False)["close"].shift(1)
    today_rows = recent[recent["trade_date"] == trade_date].drop_duplicates("ts_code", keep="last").copy()
    today_rows["pct_chg"] = today_rows["ts_code"].map(pct_by_code)
    today_rows["move"] = np.select(
        [
            today_rows["pct_chg"].notna() & (today_rows["pct_chg"] > 0),
            today_rows["pct_chg"].notna() & (today_rows["pct_chg"] < 0),
            today_rows["pct_chg"].isna() & today_rows["prev_close"].notna() & (today_rows["close"] > today_rows["prev_close"]),
            today_rows["pct_chg"].isna() & today_rows["prev_close"].notna() & (today_rows["close"] < today_rows["prev_close"]),
        ],
        [1, -1, 1, -1],
        default=0,
    )
    advances = int((today_rows["move"] > 0).sum())
    declines = int((today_rows["move"] < 0).sum())
    unchanged = total_normal - advances - declines

    grouped_recent = recent.groupby("ts_code", sort=False)
    stats = grouped_recent["close"].agg(row_count="size", max_close="max", min_close="min")
    latest = today_rows.set_index("ts_code")["close"].rename("latest_close")
    stats = stats.join(latest, how="inner")
    eligible_stats = stats[stats["row_count"] >= lookback_trading_days]
    eligible = int(len(eligible_stats))
    new_high_count = int((eligible_stats["latest_close"] >= eligible_stats["max_close"]).sum())
    new_low_count = int((eligible_stats["latest_close"] <= eligible_stats["min_close"]).sum())
    if eligible < total_normal:
        warnings.append(f"广度统计: {total_normal - eligible} 只股票52周窗口不足，跳过新高新低判定")

    return {
        "trade_date": trade_date,
        "total_normal": total_normal,
        "advances": advances,
        "declines": declines,
        "unchanged": unchanged,
        "new_high_count": int(new_high_count),
        "new_low_count": int(new_low_count),
        "new_high_pct": round(new_high_count / total_normal, 6) if total_normal else 0.0,
        "new_low_pct": round(new_low_count / total_normal, 6) if total_normal else 0.0,
    }


def compute_mcl(
    breadth_history: pd.DataFrame,
    trade_date: str,
    *,
    fast: int = 19,
    slow: int = 39,
    signal_period: int = 9,
    ratio_adjusted: bool = True,
    warnings: Optional[List[str]] = None,
) -> Optional[MCLSignal]:
    """Compute ratio-adjusted McClellan Oscillator for *trade_date*."""
    warnings = warnings if warnings is not None else []
    if breadth_history is None or breadth_history.empty:
        warnings.append("MCL: 广度历史为空")
        return None
    df = breadth_history.copy()
    df["trade_date"] = _clean_trade_date(df["trade_date"])
    df = df[df["trade_date"] <= trade_date].sort_values("trade_date").reset_index(drop=True)
    if trade_date not in set(df["trade_date"]):
        warnings.append(f"MCL: {trade_date} 缺少广度行")
        return None
    if len(df[df["trade_date"] < trade_date]) < slow + 1:
        warnings.append(f"MCL: EMA预热不足，需要至少 {slow + 1} 行历史")
        return None

    adv = pd.to_numeric(df["advances"], errors="coerce").fillna(0.0)
    dec = pd.to_numeric(df["declines"], errors="coerce").fillna(0.0)
    denom = adv + dec
    if ratio_adjusted:
        df["net_advance_ratio"] = np.where(denom > 0, (adv - dec) / denom * 1000.0, 0.0)
    else:
        df["net_advance_ratio"] = adv - dec
    df["ema_fast"] = df["net_advance_ratio"].ewm(span=fast, adjust=False).mean()
    df["ema_slow"] = df["net_advance_ratio"].ewm(span=slow, adjust=False).mean()
    df["mcl"] = df["ema_fast"] - df["ema_slow"]
    df["mcl_ma"] = df["mcl"].rolling(signal_period, min_periods=1).mean()
    df["pct_rank_to_date"] = _expanding_percentile_rank(df["mcl"])
    latest = df[df["trade_date"] == trade_date].iloc[-1]
    pct_rank = float(latest["pct_rank_to_date"])
    if pct_rank >= 90:
        signal = "overbought"
    elif pct_rank <= 10:
        signal = "oversold"
    elif float(latest["mcl"]) < 0:
        signal = "weak"
    else:
        signal = "healthy"

    return MCLSignal(
        trade_date=trade_date,
        advances=int(latest["advances"]),
        declines=int(latest["declines"]),
        unchanged=int(latest.get("unchanged", 0)),
        net_advance_ratio=round(float(latest["net_advance_ratio"]), 4),
        ema19=round(float(latest["ema_fast"]), 4),
        ema39=round(float(latest["ema_slow"]), 4),
        mcl=round(float(latest["mcl"]), 4),
        mcl_ma9=round(float(latest["mcl_ma"]), 4),
        pct_rank_to_date=round(pct_rank, 2),
        signal=signal,
    )


def count_hindenburg_cluster(
    prior_trigger_dates: List[str],
    scan_date: str,
    triggered_today: bool,
    *,
    window_days: int = 30,
) -> int:
    """Count triggers in closed calendar-day window including today only when triggered."""
    scan_dt = _to_date(scan_date)
    start_dt = scan_dt - timedelta(days=window_days)
    count = 0
    for date_str in prior_trigger_dates:
        dt = _to_date(date_str)
        if start_dt <= dt <= scan_dt:
            count += 1
    return count + (1 if triggered_today else 0)


def compute_hindenburg(
    breadth_today: Dict[str, object],
    mcl: MCLSignal,
    hs300_close: pd.Series,
    recent_trigger_dates: List[str],
    scan_date: str,
    *,
    high_low_pct: float = 0.018,
    ratio_max: float = 2.0,
    ma_period: int = 50,
    cluster_window_days: int = 30,
    cluster_count: int = 2,
    warnings: Optional[List[str]] = None,
) -> HindenburgSignal:
    """Evaluate A-share Hindenburg Omen conditions for *scan_date*."""
    warnings = warnings if warnings is not None else []
    close = hs300_close.copy()
    if not isinstance(close.index, pd.Index):
        close.index = close.index.astype(str)
    if not pd.api.types.is_datetime64_any_dtype(close.index):
        close.index = pd.to_datetime(close.index.astype(str), format="%Y%m%d", errors="coerce")
    close = pd.to_numeric(close, errors="coerce").dropna().sort_index()
    scan_dt = _to_date(scan_date)
    close = close[close.index <= scan_dt]
    if len(close) < ma_period + 1:
        warnings.append(f"兴登堡: 沪深300 MA{ma_period} 数据不足")
        cond_uptrend = False
    else:
        ma = close.rolling(ma_period).mean()
        cond_uptrend = bool(close.iloc[-1] > ma.iloc[-1] and ma.iloc[-1] > ma.iloc[-2])

    new_high_count = int(breadth_today.get("new_high_count", 0) or 0)
    new_low_count = int(breadth_today.get("new_low_count", 0) or 0)
    total_normal = int(breadth_today.get("total_normal", 0) or 0)
    new_high_pct = float(breadth_today.get("new_high_pct", 0.0) or 0.0)
    new_low_pct = float(breadth_today.get("new_low_pct", 0.0) or 0.0)
    cond_high_low = bool(total_normal > 0 and new_high_pct >= high_low_pct and new_low_pct >= high_low_pct)
    cond_ratio = bool(new_low_count > 0 and new_high_count <= new_low_count * ratio_max)
    cond_mcl_negative = bool(mcl is not None and mcl.mcl < 0)
    triggered = bool(cond_uptrend and cond_high_low and cond_ratio and cond_mcl_negative)
    cluster_total = count_hindenburg_cluster(
        recent_trigger_dates,
        scan_date,
        triggered,
        window_days=cluster_window_days,
    )
    if cluster_total >= cluster_count:
        alert_level = "cluster"
    elif cluster_total == 1 and triggered:
        alert_level = "single"
    else:
        alert_level = "none"

    return HindenburgSignal(
        trade_date=scan_date,
        total_normal=total_normal,
        new_high_count=new_high_count,
        new_low_count=new_low_count,
        new_high_pct=round(new_high_pct, 6),
        new_low_pct=round(new_low_pct, 6),
        cond_uptrend=cond_uptrend,
        cond_high_low=cond_high_low,
        cond_ratio=cond_ratio,
        cond_mcl_negative=cond_mcl_negative,
        triggered=triggered,
        cluster_count_30d=cluster_total,
        alert_level=alert_level,
    )


def compute_cape_series(
    index_daily: pd.DataFrame,
    index_basic: pd.DataFrame,
    cpi_index: pd.DataFrame,
    *,
    pe_field: str = "pe_ttm",
    window_years: float = 5.0,
    min_coverage: float = 0.80,
) -> pd.DataFrame:
    """Compute CPI-adjusted CAPE series using index-level earnings proxy."""
    if index_daily is None or index_daily.empty:
        raise ValueError("index_daily returned no rows")
    if index_basic is None or index_basic.empty:
        raise ValueError("index_dailybasic returned no rows")
    if pe_field not in index_basic.columns:
        raise ValueError(f"index_dailybasic missing required '{pe_field}' column")

    prices = index_daily.copy()
    basics = index_basic.copy()
    for df in (prices, basics):
        df["trade_date"] = _clean_trade_date(df["trade_date"])
    prices = prices[["ts_code", "trade_date", "close"]]
    basics = basics[["ts_code", "trade_date", pe_field]].rename(columns={"trade_date": "pe_source_date"})
    prices["date"] = pd.to_datetime(prices["trade_date"], format="%Y%m%d", errors="coerce")
    basics["pe_date"] = pd.to_datetime(basics["pe_source_date"], format="%Y%m%d", errors="coerce")
    prices["close"] = pd.to_numeric(prices["close"], errors="coerce")
    basics[pe_field] = pd.to_numeric(basics[pe_field], errors="coerce")
    prices = prices.dropna(subset=["date", "close"]).sort_values(["ts_code", "date"])
    basics = basics.dropna(subset=["pe_date", pe_field]).query(f"{pe_field} > 0").sort_values(["ts_code", "pe_date"])
    if prices.empty or basics.empty:
        raise ValueError("no usable index price or positive PE rows")

    merged_parts = []
    for ts_code, price_group in prices.groupby("ts_code", sort=False):
        basic_group = basics[basics["ts_code"] == ts_code]
        if basic_group.empty:
            continue
        merged_parts.append(pd.merge_asof(
            price_group.sort_values("date"),
            basic_group[["pe_date", "pe_source_date", pe_field]].sort_values("pe_date"),
            left_on="date",
            right_on="pe_date",
            direction="backward",
        ))
    if not merged_parts:
        raise ValueError("index_daily and index_dailybasic have no overlapping rows")
    df = pd.concat(merged_parts, ignore_index=True)
    cpi = _prepare_cpi_level(cpi_index)
    df = pd.merge_asof(
        df.sort_values("date"),
        cpi.sort_values("cpi_date"),
        left_on="date",
        right_on="cpi_date",
        direction="backward",
    ).dropna(subset=["cpi_index"])
    if df.empty:
        raise ValueError("no rows could be matched with CPI history")

    window_days = int(round(window_years * 252))
    min_periods = max(1, int(round(window_days * min_coverage)))
    cape_col = f"cape_{window_years:g}y"
    df["earnings_proxy"] = df["close"] / df[pe_field]
    df["real_earnings_base"] = df["earnings_proxy"] / df["cpi_index"]
    df["rolling_real_earnings_base"] = df.groupby("ts_code")["real_earnings_base"].transform(
        lambda series: series.rolling(window=window_days, min_periods=min_periods).mean()
    )
    df["real_avg_earnings"] = df["rolling_real_earnings_base"] * df["cpi_index"]
    df[cape_col] = df["close"] / df["real_avg_earnings"]
    df[f"{cape_col}_pct_rank_to_date"] = df.groupby("ts_code")[cape_col].transform(_expanding_percentile_rank)
    return df[
        [
            "ts_code",
            "trade_date",
            "close",
            pe_field,
            "pe_source_date",
            "earnings_proxy",
            "cpi_index",
            "real_avg_earnings",
            cape_col,
            f"{cape_col}_pct_rank_to_date",
        ]
    ]


def cape_band_from_pct(
    pct_rank: float,
    *,
    pct_cheap: float = 20.0,
    pct_rich: float = 80.0,
    pct_bubble: float = 90.0,
) -> str:
    """Map an expanding percentile rank to a CAPE valuation band."""
    if pct_rank >= pct_bubble:
        return "bubble"
    if pct_rank >= pct_rich:
        return "rich"
    if pct_rank <= pct_cheap:
        return "cheap"
    return "fair"


def classify_cape(
    cape_row: pd.Series,
    index_code: str,
    index_name: str,
    *,
    pct_cheap: float = 20.0,
    pct_rich: float = 80.0,
    pct_bubble: float = 90.0,
) -> CAPESignal:
    """Map one CAPE row to a CAPESignal."""
    cape_col = next((col for col in cape_row.index if col.startswith("cape_") and not col.endswith("pct_rank_to_date")), None)
    if cape_col is None:
        raise ValueError("cape_row requires a cape_* column")
    pct_col = f"{cape_col}_pct_rank_to_date"
    pct_rank = float(cape_row[pct_col])
    band = cape_band_from_pct(pct_rank, pct_cheap=pct_cheap, pct_rich=pct_rich, pct_bubble=pct_bubble)
    return CAPESignal(
        trade_date=str(cape_row["trade_date"]),
        index_code=index_code,
        index_name=index_name,
        close=round(float(cape_row["close"]), 4),
        pe_ttm=round(float(cape_row["pe_ttm"]), 4),
        pe_source_date=str(cape_row.get("pe_source_date", cape_row["trade_date"])),
        cape=round(float(cape_row[cape_col]), 4),
        window_years=float(cape_col.removeprefix("cape_").removesuffix("y")),
        pct_rank_to_date=round(pct_rank, 2),
        valuation_band=band,
    )


def aggregate_regime_alert(
    mcl: Optional[MCLSignal],
    hindenburg: Optional[HindenburgSignal],
    cape_signals: List[CAPESignal],
    *,
    benchmark_index: str = "000300.SH",
) -> tuple[str, List[str]]:
    """Combine sub-signals into a composite alert and human warnings."""
    warnings: List[str] = []
    benchmark_cape = next((signal for signal in cape_signals if signal.index_code == benchmark_index), None)
    cape_pct = benchmark_cape.pct_rank_to_date if benchmark_cape else 0.0
    hind_level = hindenburg.alert_level if hindenburg else "none"

    if hindenburg and hindenburg.alert_level == "cluster":
        warnings.append(f"【兴登堡集群】30日内第{hindenburg.cluster_count_30d}次触发")
    elif hindenburg and hindenburg.alert_level == "single":
        warnings.append("【兴登堡单次】今日四条件同时满足")
    if benchmark_cape and cape_pct >= 80:
        warnings.append(f"【CAPE高估】{benchmark_cape.index_name} CAPE 处 {cape_pct:.1f}% 分位")
    if mcl and mcl.signal in {"weak", "oversold"}:
        warnings.append(f"【MCL走弱】MCL={mcl.mcl:.2f}，分位 {mcl.pct_rank_to_date:.1f}%")

    if hind_level == "cluster" and cape_pct >= 80:
        level = "extreme"
    elif hind_level == "cluster" or cape_pct >= 90:
        level = "high"
    elif hind_level == "single" or cape_pct >= 80:
        level = "medium"
    else:
        level = "low"
    return level, warnings


__all__ = [
    "aggregate_regime_alert",
    "build_normal_universe",
    "cape_band_from_pct",
    "classify_cape",
    "compute_breadth_table",
    "compute_cape_series",
    "compute_hindenburg",
    "compute_mcl",
    "count_hindenburg_cluster",
]