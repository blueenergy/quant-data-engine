"""Attach related headlines to upcoming market events."""

from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

from pymongo.collection import Collection

logger = logging.getLogger(__name__)


def _keywords_from_title(title: str) -> List[str]:
    title = title or ""
    parts = re.split(r"[\s,，、:：/()（）]+", title)
    out = []
    for p in parts:
        p = p.strip()
        if len(p) >= 2:
            out.append(p.lower())
    return out[:6]


def _match_score(title: str, keywords: List[str]) -> int:
    t = (title or "").lower()
    return sum(1 for k in keywords if k and k in t)


def _clean_snippet(text: str, limit: int = 180) -> str:
    s = re.sub(r"<[^>]+>", " ", text or "")
    s = re.sub(r"\s+", " ", s).strip()
    if len(s) <= limit:
        return s
    return s[: limit - 1].rstrip() + "…"


def _news_item(
    *,
    title: str,
    url: str = "",
    published: str = "",
    source: str = "",
    summary: str = "",
) -> Dict[str, str]:
    return {
        "title": (title or "").strip(),
        "url": url or "",
        "published": published or "",
        "source": source or "",
        "summary": _clean_snippet(summary),
    }


def attach_related_news(
    col: Collection,
    *,
    domestic_news_fetcher=None,
    international_news_doc: Dict[str, Any] | None = None,
    horizon_hours: int = 48,
    limit_per_event: int = 5,
) -> int:
    """Populate related_news for scheduled events within horizon_hours."""
    now = datetime.now(timezone.utc)
    horizon = now + timedelta(hours=horizon_hours)
    cursor = col.find(
        {
            "status": "scheduled",
            "event_time": {"$gte": now, "$lte": horizon},
            "importance": {"$gte": 3},
        }
    )

    pool: List[Dict[str, str]] = []
    if international_news_doc:
        for items in (international_news_doc.get("categories") or {}).values():
            for item in items or []:
                pool.append(
                    _news_item(
                        title=item.get("title_zh") or item.get("title") or "",
                        url=item.get("url") or "",
                        published=item.get("published") or "",
                        source=item.get("source") or "intl",
                        summary=item.get("summary") or item.get("summary_zh") or "",
                    )
                )

    if domestic_news_fetcher:
        try:
            grouped = domestic_news_fetcher(days=3)
            for items in grouped.values():
                for item in items or []:
                    pool.append(
                        _news_item(
                            title=item.get("title") or "",
                            url=item.get("url") or "",
                            published=item.get("datetime") or item.get("pub_time") or "",
                            source=item.get("source") or item.get("src") or "domestic",
                            summary=item.get("summary") or item.get("content") or "",
                        )
                    )
        except Exception as exc:
            logger.warning("domestic news for related_news failed: %s", exc)

    updated = 0
    for event in cursor:
        keywords = _keywords_from_title(event.get("title") or "")
        if event.get("metric_code"):
            keywords.append(str(event["metric_code"]).lower())
        if event.get("symbol"):
            keywords.append(str(event["symbol"]).split(".")[0].lower())
        scored = []
        for item in pool:
            if not item.get("title"):
                continue
            score = _match_score(item.get("title", ""), keywords)
            if item.get("summary"):
                score += _match_score(item["summary"], keywords)
            if score > 0:
                scored.append((score, item))
        scored.sort(key=lambda x: x[0], reverse=True)
        related = [it for _, it in scored[:limit_per_event]]
        if related != (event.get("related_news") or []):
            col.update_one({"identity_key": event["identity_key"]}, {"$set": {"related_news": related}})
            updated += 1
    return updated
