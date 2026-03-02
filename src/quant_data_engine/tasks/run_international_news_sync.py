"""
国际新闻同步任务 — 抓取 BBC/CNBC RSS 并缓存到 MongoDB

此任务在 overseas 模式（有代理机器）上运行，将国际新闻预取并存入
international_news_cache 集合，使运行在国内云服务器的 quantAnalyzer
可以直接从 DB 读取，无需直接访问境外 RSS 源。

集合结构:
  {
    "_id": ObjectId,
    "fetched_at": datetime (UTC),
    "categories": {
      "地缘政治": [{"title": ..., "source": ..., "published": ...}],
      "大宗商品": [...],
      ...
    },
    "total": int
  }

TTL: 文档在 fetched_at + 48h 后自动过期。

Usage:
    # Single fetch
    python -m quant_data_engine.tasks.run_international_news_sync

    # Loop mode (runs every 30 minutes)
    python -m quant_data_engine.tasks.run_international_news_sync --loop --interval 1800
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
import re
from datetime import datetime, timezone, timedelta
from typing import Dict, List

from pymongo import MongoClient

from quant_data_engine import get_settings

logger = logging.getLogger(__name__)

# ── RSS Feeds (same as InternationalNewsService in quantAnalyzer) ──────────
RSS_FEEDS = [
    {"url": "https://feeds.bbci.co.uk/news/world/rss.xml",    "name": "BBC World"},
    {"url": "https://feeds.bbci.co.uk/news/business/rss.xml", "name": "BBC Business"},
    {"url": "https://www.cnbc.com/id/100003114/device/rss/rss.html", "name": "CNBC"},
    {"url": "https://finance.yahoo.com/rss/topstories",        "name": "Yahoo Finance"},
]

CATEGORIES = {
    "地缘政治": [
        "war", "strike", "attack", "invasion", "sanction", "military",
        "missile", "conflict", "tension", "iran", "israel", "ukraine",
        "russia", "taiwan", "china", "tariff", "trade war", "nato",
    ],
    "大宗商品": [
        "oil", "crude", "opec", "gold", "silver", "copper", "iron ore",
        "natural gas", "lng", "commodity", "supply", "wti", "brent",
    ],
    "央行/利率": [
        "fed", "federal reserve", "rate hike", "rate cut", "interest rate",
        "inflation", "cpi", "fomc", "powell", "ecb", "boe", "boj",
        "monetary policy", "quantitative",
    ],
    "科技/AI": [
        "nvidia", "ai", "artificial intelligence", "chip", "semiconductor",
        "apple", "microsoft", "google", "meta", "amazon", "tesla",
    ],
    "全球经济": [
        "recession", "gdp", "unemployment", "job", "economy", "growth",
        "imf", "world bank", "dollar", "yuan", "rmb", "debt", "default",
    ],
}

COLLECTION_NAME = "international_news_cache"
TTL_HOURS = 48


def fetch_and_classify(max_age_hours: int = 36) -> Dict[str, List[Dict]]:
    """Fetch RSS headlines and classify into categories."""
    try:
        import feedparser
    except ImportError:
        logger.error("feedparser not installed. Run: pip install feedparser")
        return {}

    cutoff = datetime.now(timezone.utc) - timedelta(hours=max_age_hours)
    all_items = []

    for feed in RSS_FEEDS:
        try:
            parsed = feedparser.parse(feed["url"])
            for entry in parsed.entries[:30]:
                title = getattr(entry, "title", "").strip()
                summary = getattr(entry, "summary", "").strip()

                pub_time = None
                if hasattr(entry, "published_parsed") and entry.published_parsed:
                    import calendar
                    pub_time = datetime.fromtimestamp(
                        calendar.timegm(entry.published_parsed), tz=timezone.utc
                    )
                elif hasattr(entry, "updated_parsed") and entry.updated_parsed:
                    import calendar
                    pub_time = datetime.fromtimestamp(
                        calendar.timegm(entry.updated_parsed), tz=timezone.utc
                    )

                if pub_time and pub_time < cutoff:
                    continue
                if title:
                    all_items.append({
                        "title": title,
                        "summary": summary[:200] if summary else "",
                        "source": feed["name"],
                        "published": pub_time.strftime("%Y-%m-%d %H:%M") if pub_time else "",
                        "published_dt": pub_time or datetime.now(timezone.utc),
                    })

            logger.info(f"[IntlNewsSync] {feed['name']}: fetched {len(parsed.entries)} entries")
            time.sleep(0.5)
        except Exception as e:
            logger.warning(f"[IntlNewsSync] Feed {feed['name']} failed: {e}")

    # Deduplicate
    seen: set = set()
    unique = []
    for item in all_items:
        key = re.sub(r"[^\w\s]", "", item["title"][:40].lower())
        if key not in seen:
            seen.add(key)
            unique.append(item)
    unique.sort(key=lambda x: x["published_dt"], reverse=True)

    # Classify
    result: Dict[str, List] = {cat: [] for cat in CATEGORIES}
    result["其他重要"] = []
    classified: set = set()

    for item in unique:
        text = (item["title"] + " " + item["summary"]).lower()
        matched = False
        for cat, keywords in CATEGORIES.items():
            if any(kw in text for kw in keywords):
                if item["title"] not in classified and len(result[cat]) < 4:
                    result[cat].append({
                        "title": item["title"],
                        "source": item["source"],
                        "published": item["published"],
                    })
                    classified.add(item["title"])
                    matched = True
                    break
        if not matched and item["title"] not in classified and len(result["其他重要"]) < 3:
            result["其他重要"].append({
                "title": item["title"],
                "source": item["source"],
                "published": item["published"],
            })
            classified.add(item["title"])

    total = sum(len(v) for v in result.values())
    result = {k: v for k, v in result.items() if v}
    logger.info(f"[IntlNewsSync] {total} headlines across {len(result)} categories")
    return result


def save_to_db(client: MongoClient, db_name: str, categories: Dict) -> None:
    """Save classified news snapshot to MongoDB.

    Uses replace_one + upsert on fixed _id='latest' so there is always
    exactly ONE document in the collection — the most recent snapshot.
    No TTL index needed; reading code checks fetched_at age instead.
    """
    db = client[db_name]
    col = db[COLLECTION_NAME]

    total = sum(len(v) for v in categories.values())
    doc = {
        "_id": "latest",
        "fetched_at": datetime.now(timezone.utc),
        "categories": categories,
        "total": total,
    }
    col.replace_one({"_id": "latest"}, doc, upsert=True)
    logger.info(f"[IntlNewsSync] Updated 'latest' snapshot: {total} headlines")


def run_once() -> bool:
    """Single fetch-and-save cycle. Returns True on success."""
    settings = get_settings()
    client = MongoClient(settings.mongo_uri)
    try:
        categories = fetch_and_classify(max_age_hours=36)
        if not categories:
            logger.warning("[IntlNewsSync] No categories fetched — skipping DB write")
            return False
        save_to_db(client, settings.mongo_db, categories)
        return True
    finally:
        client.close()


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    parser = argparse.ArgumentParser(description="International news cache sync")
    parser.add_argument("--loop", action="store_true", help="Run in loop mode")
    parser.add_argument("--interval", type=int, default=1800,
                        help="Loop interval in seconds (default: 1800 = 30min)")
    args = parser.parse_args()

    if args.loop:
        logger.info(f"[IntlNewsSync] Starting loop every {args.interval}s")
        while True:
            try:
                run_once()
            except Exception as e:
                logger.error(f"[IntlNewsSync] Cycle failed: {e}", exc_info=True)
            time.sleep(args.interval)
    else:
        success = run_once()
        sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
