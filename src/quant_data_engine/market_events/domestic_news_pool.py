"""Optional domestic news pool for related_news attachment (Tushare major_news)."""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


def fetch_domestic_news_grouped(days: int = 3) -> Dict[str, List[Dict[str, Any]]]:
    """
    Best-effort fetch of recent domestic headlines for keyword matching.
    Returns empty dict when Tushare is unavailable.
    """
    token = os.getenv("TUSHARE_TOKEN")
    if not token:
        return {}
    try:
        import tushare as ts
    except ImportError:
        logger.debug("tushare not installed; domestic news pool skipped")
        return {}

    try:
        ts.set_token(token)
        pro = ts.pro_api()
        end = datetime.now()
        start = end - timedelta(days=max(1, days))
        df = pro.major_news(
            start_date=start.strftime("%Y-%m-%d %H:%M:%S"),
            end_date=end.strftime("%Y-%m-%d %H:%M:%S"),
            fields="title,content,pub_time,src",
        )
    except Exception as exc:
        logger.warning("domestic major_news fetch failed: %s", exc)
        return {}

    if df is None or getattr(df, "empty", True):
        return {}

    items: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        title = str(row.get("title") or "").strip()
        if not title:
            continue
        items.append(
            {
                "title": title,
                "url": "",
                "datetime": str(row.get("pub_time") or ""),
                "source": str(row.get("src") or "tushare"),
                "summary": str(row.get("content") or "").strip()[:220],
            }
        )
    return {"domestic": items[:200]}
