"""CNINFO report disclosure calendar adapter."""

from __future__ import annotations

import logging
import re
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
import requests

from quant_data_engine.market_events.filters import MarketEventsFilterConfig
from quant_data_engine.market_events.identity import normalize_symbol, parse_shanghai_datetime
from quant_data_engine.market_events.normalizer import score_report_importance

logger = logging.getLogger(__name__)

_CNINFO_URL = "http://www.cninfo.com.cn/new/information/getPrbookInfo"
_MARKET_MAP = {
    "沪深京": "szsh",
    "深市": "sz",
    "深主板": "szmb",
    "创业板": "szcn",
    "沪市": "sh",
    "沪主板": "shmb",
    "科创板": "shkcp",
    "北交所": "bj",
}
_HTTP_TIMEOUT = 30


def _report_periods_for_year(year: int) -> List[str]:
    return [f"{year}一季", f"{year}半年报", f"{year}三季", f"{year}年报"]


def _period_section_time(period: str) -> str:
    year = period[:4]
    mapping = {
        f"{year}一季": f"{year}-03-31",
        f"{year}半年报": f"{year}-06-30",
        f"{year}三季": f"{year}-09-30",
        f"{year}年报": f"{year}-12-31",
    }
    if period not in mapping:
        raise ValueError(f"unsupported report period: {period}")
    return mapping[period]


def _pick_disclosure_date(row: pd.Series) -> Optional[str]:
    for col in ("三次变更", "二次变更", "初次变更", "首次预约", "实际披露"):
        val = row.get(col)
        if val is None or (isinstance(val, float) and pd.isna(val)):
            continue
        s = str(val).strip()
        if s and s not in {"NaT", "nan", "--", "None"}:
            return s.replace("/", "-")[:10]
    return None


def _period_to_ref(period_label: str) -> str:
    m = re.match(r"(20\d{2})(一季|半年报|三季|年报)", period_label)
    if not m:
        return period_label
    y, p = m.group(1), m.group(2)
    mapping = {"一季": "Q1", "半年报": "Q2", "三季": "Q3", "年报": "Q4"}
    return f"{y}{mapping.get(p, p)}"


def _fetch_cninfo_period(period: str, *, market: str = "沪深京") -> pd.DataFrame:
    """Fetch one CNINFO reservation table; empty/null payload → empty DataFrame."""
    market_code = _MARKET_MAP.get(market)
    if not market_code:
        raise ValueError(f"unsupported market: {market}")
    resp = requests.post(
        _CNINFO_URL,
        params={
            "sectionTime": _period_section_time(period),
            "firstTime": "",
            "lastTime": "",
            "market": market_code,
            "stockCode": "",
            "orderClos": "",
            "isDesc": "",
            "pagesize": "10000",
            "pagenum": "1",
        },
        timeout=_HTTP_TIMEOUT,
    )
    resp.raise_for_status()
    payload = resp.json() or {}
    books = payload.get("prbookinfos")
    if not books:
        return pd.DataFrame(
            columns=["股票代码", "股票简称", "首次预约", "初次变更", "二次变更", "三次变更", "实际披露"]
        )

    df = pd.DataFrame(books)
    rename = {
        "seccode": "股票代码",
        "secname": "股票简称",
        "f002d_0102": "首次预约",
        "f006d_0102": "实际披露",
        "f003d_0102": "初次变更",
        "f004d_0102": "二次变更",
        "f005d_0102": "三次变更",
    }
    if set(rename).issubset(df.columns):
        df = df.rename(columns=rename)
    elif df.shape[1] >= 7:
        # Fallback for unexpected list-shaped payloads (legacy akshare path).
        ordered = list(df.columns)[:10]
        mapped = {
            ordered[0]: "股票代码",
            ordered[1]: "股票简称",
            ordered[2]: "首次预约",
            ordered[3]: "实际披露",
            ordered[4]: "初次变更",
            ordered[5]: "二次变更",
            ordered[6]: "三次变更",
        }
        df = df.rename(columns=mapped)
    keep = ["股票代码", "股票简称", "首次预约", "初次变更", "二次变更", "三次变更", "实际披露"]
    missing = [c for c in keep if c not in df.columns]
    if missing:
        raise ValueError(f"unexpected cninfo columns, missing {missing}; got {list(df.columns)}")
    out = df[keep].copy()
    for col in keep[2:]:
        out[col] = pd.to_datetime(out[col], errors="coerce").dt.date
    return out


def fetch_cn_report(cfg: MarketEventsFilterConfig, window_start: datetime, window_end: datetime) -> List[Dict[str, Any]]:
    years = {window_start.year, window_end.year}
    raw_rows: List[Dict[str, Any]] = []
    aggregate_by_date: Dict[str, int] = defaultdict(int)
    empty_periods: List[str] = []

    for year in sorted(years):
        for period in _report_periods_for_year(year):
            try:
                df = _fetch_cninfo_period(period)
            except Exception as e:
                logger.warning("stock_report_disclosure failed %s: %s", period, e)
                continue
            if df is None or df.empty:
                empty_periods.append(period)
                continue
            ref_period = _period_to_ref(period)
            for _, row in df.iterrows():
                date_s = _pick_disclosure_date(row)
                if not date_s:
                    continue
                try:
                    event_time = parse_shanghai_datetime(date_s)
                except ValueError:
                    continue
                if event_time < window_start or event_time > window_end:
                    continue
                code = normalize_symbol(str(row.get("股票代码") or ""))
                name = str(row.get("股票简称") or "")
                if not code:
                    continue
                if not cfg.symbol_allowed(code):
                    if cfg.report_aggregate_outside_whitelist:
                        aggregate_by_date[date_s] += 1
                    continue
                raw_rows.append(
                    {
                        "identity_kind": "cn_report",
                        "source_name": "cninfo_disclosure",
                        "title": f"{name}({code}) {period}预约披露",
                        "event_time": event_time,
                        "region": "CN",
                        "category": "earnings",
                        "event_subtype": "report_disclosure",
                        "symbol": code,
                        "symbol_name": name,
                        "ref_period": ref_period,
                        "importance": score_report_importance(code, cfg),
                    }
                )

    if empty_periods:
        logger.info("cn_report: empty reservation tables for %s", ", ".join(empty_periods))

    if cfg.report_aggregate_outside_whitelist:
        for date_s, count in aggregate_by_date.items():
            if count < 5:
                continue
            try:
                event_time = parse_shanghai_datetime(date_s)
            except ValueError:
                continue
            if event_time < window_start or event_time > window_end:
                continue
            imp = 4 if count >= 50 else 3
            raw_rows.append(
                {
                    "identity_kind": "aggregate",
                    "source_name": "cninfo_disclosure",
                    "title": f"A股财报披露高峰：{count}家公司预约披露",
                    "event_time": event_time,
                    "region": "CN",
                    "category": "earnings",
                    "event_subtype": "aggregate",
                    "importance": imp,
                    "metrics": {"count": count},
                }
            )
    return raw_rows
