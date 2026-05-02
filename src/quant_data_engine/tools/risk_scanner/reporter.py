"""Report printer and multi-date comparison for the market risk scanner."""
from __future__ import annotations

from typing import List

from .models import RiskReport
from .instruments import SECTOR_PROFILES


# ---------------------------------------------------------------------------
# Single-report printer
# ---------------------------------------------------------------------------

def print_report(report: RiskReport) -> None:
    """Print a human-readable risk dashboard for a single ``RiskReport``."""
    profile = SECTOR_PROFILES.get(report.sector)
    sector_label = profile.name if profile else report.sector

    divider = "=" * 65
    print(divider)
    print(f"  市场风险扫描报告  [{sector_label}]  |  扫描日期: {report.scan_date}")
    print(f"  生成时间: {report.generated_at}")
    print(divider)
    print(f"  综合风险评分: {report.overall_risk_score:.1f}/100  [{report.overall_risk_level.upper()}]")
    print()

    print("【信号1】板块ETF偏离20日均线")
    print(f"  {'代码':<14} {'名称':<12} {'偏离%':>8} {'历史分位':>9} {'风险等级':>8}")
    for s in report.ma_deviation:
        print(
            f"  {s.ts_code:<14} {s.name:<12}"
            f" {s.deviation_pct*100:>+7.2f}% {s.rolling_pct_rank:>8.1f}%  {s.risk_level}"
        )
    print()

    print("【信号2】主要指数RSI超买 (CTA趋势超买代理)")
    print(f"  {'代码':<14} {'名称':<12} {'RSI(14)':>8} {'历史分位':>9} {'信号':>10}")
    for s in report.rsi_signals:
        print(
            f"  {s.ts_code:<14} {s.name:<12}"
            f" {s.rsi:>8.2f} {s.rolling_pct_rank:>8.1f}%  {s.signal}"
        )
    print()

    print("【信号3】板块ETF成交量倍数 (投机热度代理)")
    print(f"  {'代码':<14} {'名称':<12} {'量/均量':>9} {'历史分位':>9} {'信号':>10}")
    for s in report.speculation_heat:
        print(
            f"  {s.ts_code:<14} {s.name:<12}"
            f" {s.turnover_rate:>8.2f}x {s.rolling_pct_rank:>8.1f}%  {s.signal}"
        )
    print()

    if report.factor_crowding:
        fc = report.factor_crowding
        print("【信号4】全市场因子拥挤度 (摩根大通Meme拥挤度代理)")
        print(f"  动量分位:       {fc.momentum_20d_pct_rank:.1f}%")
        print(f"  波动率分位:     {fc.volatility_20d_pct_rank:.1f}%")
        print(f"  换手率分位:     {fc.turnover_pct_rank:.1f}%")
        print(f"  综合拥挤度:     {fc.composite_crowding_score:.1f}/100  [{fc.signal}]")
        if fc.top_crowded_stocks:
            print(f"  三因子共振个股: {', '.join(fc.top_crowded_stocks)}")
        print()

    if report.tech_relative_strength:
        rs = report.tech_relative_strength
        print(f"【信号5】{rs.sector_name} vs 大盘相对强弱 (机构抛售代理)")
        print(f"  观测周期:       {rs.period_days} 日")
        print(f"  板块收益:       {rs.tech_return*100:+.2f}%")
        print(f"  基准收益:       {rs.broad_return*100:+.2f}%")
        print(f"  超额收益:       {rs.relative_return*100:+.2f}%  (历史{rs.rolling_pct_rank:.1f}%分位)")
        print(f"  信号:           {rs.signal}")
        print()

    if report.warnings:
        print("⚠  告警事项:")
        for w in report.warnings:
            print(f"   - {w}")
        print()

    print(divider)


# ---------------------------------------------------------------------------
# Multi-date comparison
# ---------------------------------------------------------------------------

def compare_reports(reports: List[RiskReport]) -> None:
    """Print a side-by-side comparison table for multiple scan dates.

    Useful for historical context: run ``scanner.compare(["20250115", "20260502"])``
    then pass the resulting list here.
    """
    if not reports:
        print("(无报告可比较)")
        return

    dates = [r.scan_date for r in reports]
    profile = SECTOR_PROFILES.get(reports[0].sector)
    sector_label = profile.name if profile else reports[0].sector

    col_w = 12   # width of each date column
    label_w = 32  # width of metric label column

    divider = "=" * (label_w + col_w * len(dates) + 2)
    header_dates = "".join(f"{d:>{col_w}}" for d in dates)

    print(divider)
    print(f"  多期对比分析  [{sector_label}]")
    print(divider)
    print(f"  {'指标':<{label_w}}{header_dates}")
    print("-" * (label_w + col_w * len(dates) + 2))

    # ── 综合评分 ──────────────────────────────
    scores = "".join(f"{r.overall_risk_score:>{col_w}.1f}" for r in reports)
    levels = "".join(f"{r.overall_risk_level.upper():>{col_w}}" for r in reports)
    print(f"  {'综合风险评分':<{label_w}}{scores}")
    print(f"  {'风险等级':<{label_w}}{levels}")
    print()

    # ── 信号1: MA偏离 ────────────────────────
    print(f"  [信号1] MA偏离")
    # Collect union of all ts_codes appearing in any report
    ma_codes: dict[str, str] = {}
    for r in reports:
        for s in r.ma_deviation:
            ma_codes[s.ts_code] = s.name
    for ts_code, name in ma_codes.items():
        row = f"  {name}({ts_code}) 偏离%"
        vals = ""
        for r in reports:
            hit = next((s for s in r.ma_deviation if s.ts_code == ts_code), None)
            vals += f"{(hit.deviation_pct*100):>+{col_w}.2f}" if hit else f"{'N/A':>{col_w}}"
        print(f"  {row:<{label_w}}{vals}")
    print()

    # ── 信号2: RSI ───────────────────────────
    print(f"  [信号2] RSI(14)")
    rsi_codes: dict[str, str] = {}
    for r in reports:
        for s in r.rsi_signals:
            rsi_codes[s.ts_code] = s.name
    for ts_code, name in rsi_codes.items():
        row = f"  {name}"
        vals = ""
        for r in reports:
            hit = next((s for s in r.rsi_signals if s.ts_code == ts_code), None)
            vals += f"{hit.rsi:>{col_w}.2f}" if hit else f"{'N/A':>{col_w}}"
        print(f"  {row:<{label_w}}{vals}")
    print()

    # ── 信号3: 成交量倍数 ────────────────────
    print(f"  [信号3] 成交量倍数 (x20日均量)")
    vol_codes: dict[str, str] = {}
    for r in reports:
        for s in r.speculation_heat:
            vol_codes[s.ts_code] = s.name
    for ts_code, name in vol_codes.items():
        row = f"  {name}"
        vals = ""
        for r in reports:
            hit = next((s for s in r.speculation_heat if s.ts_code == ts_code), None)
            vals += f"{hit.turnover_rate:>{col_w}.2f}" if hit else f"{'N/A':>{col_w}}"
        print(f"  {row:<{label_w}}{vals}")
    print()

    # ── 信号4: 因子拥挤度 ────────────────────
    print(f"  [信号4] 因子拥挤度")
    for label, attr in [("动量分位%", "momentum_20d_pct_rank"),
                        ("波动率分位%", "volatility_20d_pct_rank"),
                        ("换手率分位%", "turnover_pct_rank"),
                        ("综合拥挤度", "composite_crowding_score")]:
        vals = ""
        for r in reports:
            fc = r.factor_crowding
            vals += f"{getattr(fc, attr):>{col_w}.1f}" if fc else f"{'N/A':>{col_w}}"
        print(f"  {label:<{label_w}}{vals}")
    print()

    # ── 信号5: 相对强弱 ──────────────────────
    print(f"  [信号5] 板块相对强弱 (vs 基准 {reports[0].tech_relative_strength.period_days if reports[0].tech_relative_strength else '?'}日)")
    for label, attr, fmt in [
        ("超额收益%", "relative_return", lambda v: f"{v*100:>+{col_w}.2f}"),
        ("历史分位%", "rolling_pct_rank", lambda v: f"{v:>{col_w}.1f}"),
    ]:
        vals = ""
        for r in reports:
            rs = r.tech_relative_strength
            vals += fmt(getattr(rs, attr)) if rs else f"{'N/A':>{col_w}}"
        print(f"  {label:<{label_w}}{vals}")
    print()

    print(divider)


__all__ = ["print_report", "compare_reports"]
