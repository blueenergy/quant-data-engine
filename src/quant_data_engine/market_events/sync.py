"""Orchestrate market event sync cycle."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

from pymongo import MongoClient

from quant_data_engine.market_events.adapters import (
    fetch_cn_gsrl,
    fetch_cn_report,
    fetch_cn_unlock,
    fetch_macro_baidu,
    fetch_macro_ws,
    fetch_seed_events,
)
from quant_data_engine.market_events.adapters.macro import date_range_yyyymmdd
from quant_data_engine.market_events.filters import MarketEventsFilterConfig
from quant_data_engine.market_events.normalizer import apply_status_rules, build_document, merge_documents
from quant_data_engine.market_events.repository import cleanup_old_events, get_collection, upsert_events
from quant_data_engine.market_events.related_news import attach_related_news

logger = logging.getLogger(__name__)


def run_sync_cycle(
    client: MongoClient,
    db_name: str,
    *,
    cfg: Optional[MarketEventsFilterConfig] = None,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    cfg = cfg or MarketEventsFilterConfig.from_yaml()
    now = now or datetime.now(timezone.utc)
    sh_now = now.astimezone(ZoneInfo("Asia/Shanghai"))
    window_start = now
    window_end = now + timedelta(days=cfg.lookahead_days)

    dates = date_range_yyyymmdd(sh_now.replace(hour=0, minute=0, second=0, microsecond=0), cfg.lookahead_days)

    raw_events: List[Dict[str, Any]] = []
    raw_events.extend(fetch_macro_ws(dates))
    raw_events.extend(fetch_macro_baidu(dates))
    raw_events.extend(fetch_cn_report(cfg, window_start, window_end))
    raw_events.extend(fetch_cn_unlock(cfg, window_start, window_end))
    raw_events.extend(fetch_cn_gsrl(cfg, dates))
    raw_events.extend(fetch_seed_events())

    docs: List[Dict[str, Any]] = []
    for raw in raw_events:
        doc = build_document(raw, cfg)
        if doc:
            docs.append(apply_status_rules(doc, now))

    merged = merge_documents(docs)
    col = get_collection(client, db_name)
    upserted = upsert_events(col, merged)
    deleted = cleanup_old_events(col, cfg.retention_days, now)

    intl_doc = None
    try:
        intl_doc = client[db_name]["international_news_cache"].find_one({"_id": "latest"})
    except Exception:
        pass
    domestic_fetcher = None
    try:
        from quant_data_engine.market_events.domestic_news_pool import fetch_domestic_news_grouped

        domestic_fetcher = fetch_domestic_news_grouped
    except Exception:
        domestic_fetcher = None
    related_updated = attach_related_news(
        col,
        international_news_doc=intl_doc,
        domestic_news_fetcher=domestic_fetcher,
    )

    logger.info(
        "[MarketEventsSync] raw=%s merged=%s upserted=%s deleted_old=%s related_updated=%s",
        len(raw_events),
        len(merged),
        upserted,
        deleted,
        related_updated,
    )
    return {
        "raw_count": len(raw_events),
        "merged_count": len(merged),
        "upserted": upserted,
        "deleted_old": deleted,
        "related_updated": related_updated,
    }
