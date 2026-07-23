"""Attach related headlines to upcoming and recently happened market events."""

from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

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


def _match_score(text: str, keywords: List[str]) -> int:
    t = (text or "").lower()
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


def _build_news_pool(
    *,
    international_news_doc: Optional[Dict[str, Any]],
    domestic_news_fetcher,
) -> List[Dict[str, str]]:
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
    return pool


def _event_keywords(event: Dict[str, Any]) -> List[str]:
    keywords = _keywords_from_title(event.get("title") or "")
    if event.get("metric_code"):
        keywords.append(str(event["metric_code"]).lower())
    if event.get("symbol"):
        keywords.append(str(event["symbol"]).split(".")[0].lower())
    return keywords


def _rank_news(pool: List[Dict[str, str]], keywords: List[str], limit: int) -> List[Dict[str, str]]:
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
    return [it for _, it in scored[:limit]]


def attach_related_news(
    col: Collection,
    *,
    domestic_news_fetcher=None,
    international_news_doc: Dict[str, Any] | None = None,
    horizon_hours: int = 48,
    lookback_hours: int = 72,
    limit_per_event: int = 5,
) -> int:
    """
    Populate:
    - related_news for upcoming scheduled events (pre-event context)
    - outcome_news for recently happened events (post-event coverage)
    Does not overwrite the other field.
    """
    now = datetime.now(timezone.utc)
    future_horizon = now + timedelta(hours=horizon_hours)
    past_cutoff = now - timedelta(hours=lookback_hours)
    pool = _build_news_pool(
        international_news_doc=international_news_doc,
        domestic_news_fetcher=domestic_news_fetcher,
    )
    if not pool:
        return 0

    updated = 0

    upcoming = col.find(
        {
            "status": {"$in": ["scheduled", "happened"]},
            "event_time": {"$gte": now, "$lte": future_horizon},
            "importance": {"$gte": 3},
        }
    )
    for event in upcoming:
        related = _rank_news(pool, _event_keywords(event), limit_per_event)
        if related != (event.get("related_news") or []):
            col.update_one({"identity_key": event["identity_key"]}, {"$set": {"related_news": related}})
            updated += 1

    # Post-event: keep pre-event related_news, add separate outcome_news.
    past = col.find(
        {
            "event_time": {"$gte": past_cutoff, "$lt": now},
            "importance": {"$gte": 3},
        }
    )
    for event in past:
        outcome = _rank_news(pool, _event_keywords(event), limit_per_event)
        if outcome != (event.get("outcome_news") or []):
            col.update_one(
                {"identity_key": event["identity_key"]},
                {"$set": {"outcome_news": outcome, "status": "happened"}},
            )
            updated += 1
    return updated
