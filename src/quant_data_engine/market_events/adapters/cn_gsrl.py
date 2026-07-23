"""Eastmoney stock calendar (company dynamics) adapter."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List

import pandas as pd
import requests

from quant_data_engine.market_events.filters import MarketEventsFilterConfig
from quant_data_engine.market_events.identity import normalize_symbol, parse_shanghai_datetime

logger = logging.getLogger(__name__)

_EM_URL = "https://datacenter-web.eastmoney.com/api/data/v1/get"
_HTTP_TIMEOUT = 20
_EM_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Referer": "https://data.eastmoney.com/gsrl/gsdt.html",
}


def _is_weekend(yyyymmdd: str) -> bool:
    try:
        dt = datetime.strptime(yyyymmdd, "%Y%m%d")
    except ValueError:
        return False
    return dt.weekday() >= 5


def _fetch_gsrl_day(date: str) -> pd.DataFrame:
    """Fetch one Eastmoney company-dynamics day; null result → empty DataFrame."""
    ymd = f"{date[:4]}-{date[4:6]}-{date[6:8]}"
    resp = requests.get(
        _EM_URL,
        params={
            "sortColumns": "SECURITY_CODE",
            "sortTypes": "1",
            "pageSize": "5000",
            "pageNumber": "1",
            "columns": "SECURITY_CODE,SECUCODE,SECURITY_NAME_ABBR,EVENT_TYPE,EVENT_CONTENT,TRADE_DATE",
            "source": "WEB",
            "client": "WEB",
            "reportName": "RPT_ORGOP_ALL",
            "filter": f"(TRADE_DATE='{ymd}')",
        },
        headers=_EM_HEADERS,
        timeout=_HTTP_TIMEOUT,
    )
    resp.raise_for_status()
    payload = resp.json() or {}
    result = payload.get("result")
    rows = (result or {}).get("data") if isinstance(result, dict) else None
    if not rows:
        return pd.DataFrame(columns=["代码", "简称", "事件类型", "具体事项", "交易日"])

    df = pd.DataFrame(rows)
    df = df.rename(
        columns={
            "SECURITY_CODE": "代码",
            "SECURITY_NAME_ABBR": "简称",
            "EVENT_TYPE": "事件类型",
            "EVENT_CONTENT": "具体事项",
            "TRADE_DATE": "交易日",
        }
    )
    keep = ["代码", "简称", "事件类型", "具体事项", "交易日"]
    out = df[keep].copy()
    out["交易日"] = pd.to_datetime(out["交易日"], errors="coerce").dt.date
    return out


def fetch_cn_gsrl(cfg: MarketEventsFilterConfig, dates: List[str]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    important = cfg.gsrl_important_types or set()
    empty_days = 0
    for d in dates:
        if _is_weekend(d):
            continue
        try:
            df = _fetch_gsrl_day(d)
        except Exception as e:
            logger.warning("stock_gsrl_gsdt_em failed for %s: %s", d, e)
            continue
        if df is None or df.empty:
            empty_days += 1
            continue
        for _, row in df.iterrows():
            event_type = str(row.get("事件类型") or "")
            if important and not any(t in event_type for t in important):
                continue
            code = normalize_symbol(str(row.get("代码") or ""))
            if not cfg.symbol_allowed(code):
                continue
            name = str(row.get("简称") or "")
            content = str(row.get("具体事项") or "")
            trade_date = row.get("交易日")
            date_s = d
            if trade_date is not None and hasattr(trade_date, "strftime"):
                date_s = trade_date.strftime("%Y%m%d")
            try:
                event_time = parse_shanghai_datetime("", default_date=date_s)
            except ValueError:
                continue
            subtype = "shareholder_meeting"
            if "分红" in event_type:
                subtype = "dividend"
            elif "业绩" in event_type:
                subtype = "other"
            out.append(
                {
                    "identity_kind": "corporate",
                    "source_name": "em_gsrl",
                    "title": f"{name}({code}) {event_type}: {content}".strip(),
                    "event_time": event_time,
                    "region": "CN",
                    "category": "corporate",
                    "event_subtype": subtype,
                    "symbol": code,
                    "symbol_name": name,
                    "importance": 3,
                }
            )
    if empty_days:
        logger.info("cn_gsrl: %s trading days had no company events", empty_days)
    return out
