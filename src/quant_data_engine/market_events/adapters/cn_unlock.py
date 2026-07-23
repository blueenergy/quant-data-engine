"""Eastmoney restricted release (unlock) adapter."""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List
from zoneinfo import ZoneInfo

import pandas as pd

from quant_data_engine.market_events.filters import MarketEventsFilterConfig
from quant_data_engine.market_events.identity import normalize_symbol, parse_shanghai_datetime
from quant_data_engine.market_events.normalizer import score_unlock_importance

logger = logging.getLogger(__name__)


def fetch_cn_unlock(cfg: MarketEventsFilterConfig, window_start: datetime, window_end: datetime) -> List[Dict[str, Any]]:
    try:
        import akshare as ak
    except ImportError:
        logger.warning("akshare not installed; cn_unlock skipped")
        return []

    start_s = window_start.astimezone(ZoneInfo("Asia/Shanghai")).strftime("%Y%m%d")
    end_s = window_end.astimezone(ZoneInfo("Asia/Shanghai")).strftime("%Y%m%d")
    try:
        df = ak.stock_restricted_release_detail_em(start_date=start_s, end_date=end_s)
    except Exception as e:
        logger.warning("stock_restricted_release_detail_em failed: %s", e)
        return []
    if df is None or df.empty:
        return []

    by_date: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for _, row in df.iterrows():
        date_val = row.get("解禁时间")
        if date_val is None or (isinstance(date_val, float) and pd.isna(date_val)):
            continue
        date_s = str(date_val)
        if hasattr(date_val, "strftime"):
            date_s = date_val.strftime("%Y-%m-%d")
        try:
            event_time = parse_shanghai_datetime(date_s)
        except ValueError:
            continue
        code = normalize_symbol(str(row.get("股票代码") or ""))
        name = str(row.get("股票简称") or "")
        mktcap = row.get("实际解禁市值")
        try:
            mktcap_f = float(mktcap) if mktcap is not None and not pd.isna(mktcap) else None
        except (TypeError, ValueError):
            mktcap_f = None
        if mktcap_f is not None and mktcap_f < cfg.unlock_min_mktcap_yuan:
            continue
        imp = score_unlock_importance(mktcap_f, cfg)
        by_date[date_s].append(
            {
                "identity_kind": "corporate",
                "source_name": "em_restricted",
                "title": f"{name}({code}) 限售解禁",
                "event_time": event_time,
                "region": "CN",
                "category": "corporate",
                "event_subtype": "restricted_release",
                "symbol": code,
                "symbol_name": name,
                "importance": imp,
                "metrics": {"release_mktcap": mktcap_f},
            }
        )

    out: List[Dict[str, Any]] = []
    for _, rows in by_date.items():
        rows.sort(key=lambda r: (r.get("metrics") or {}).get("release_mktcap") or 0, reverse=True)
        out.extend(rows[: cfg.unlock_top_n_per_day])
    return out
