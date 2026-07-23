"""Tests for related news attachment."""

from datetime import datetime, timedelta, timezone

from quant_data_engine.market_events.related_news import attach_related_news


class _FakeCol:
    def __init__(self, docs):
        self.docs = docs
        self.updates = []

    def find(self, query):
        out = []
        for d in self.docs:
            et = d.get("event_time")
            if d.get("status") != "scheduled":
                continue
            if et < query["event_time"]["$gte"] or et > query["event_time"]["$lte"]:
                continue
            if d.get("importance", 0) < query["importance"]["$gte"]:
                continue
            out.append(d)
        return out

    def update_one(self, filt, update):
        self.updates.append((filt, update))
        for d in self.docs:
            if d.get("identity_key") == filt.get("identity_key"):
                d.update(update.get("$set") or {})


def test_attach_related_news_matches_title_keywords():
    event_time = datetime.now(timezone.utc) + timedelta(hours=2)
    col = _FakeCol(
        [
            {
                "identity_key": "k1",
                "title": "美国6月CPI年率",
                "metric_code": "CPI_YOY",
                "event_time": event_time,
                "importance": 4,
                "status": "scheduled",
                "related_news": [],
            }
        ]
    )
    intl = {
        "categories": {
            "央行/利率": [
                {
                    "title": "美国CPI_YOY数据超预期",
                    "summary": "核心通胀仍然偏黏，市场关注联储路径。",
                    "url": "http://x",
                    "published": "2026-07-10",
                }
            ]
        }
    }
    n = attach_related_news(col, international_news_doc=intl, horizon_hours=48)
    assert n == 1
    assert col.updates
    related = col.updates[0][1]["$set"]["related_news"]
    assert related[0]["summary"].startswith("核心通胀")


def test_attach_related_news_uses_domestic_fetcher():
    event_time = datetime.now(timezone.utc) + timedelta(hours=3)
    col = _FakeCol(
        [
            {
                "identity_key": "k2",
                "title": "LPR报价公布",
                "metric_code": "LPR",
                "event_time": event_time,
                "importance": 4,
                "status": "scheduled",
                "related_news": [],
            }
        ]
    )

    def domestic_fetcher(days=3):
        return {
            "货币政策": [
                {
                    "title": "央行公布最新LPR利率",
                    "content": "1年期与5年期报价保持不变。",
                    "url": "http://d",
                    "datetime": "2026-07-20",
                }
            ]
        }

    n = attach_related_news(col, domestic_news_fetcher=domestic_fetcher, horizon_hours=48)
    assert n == 1
    related = col.updates[0][1]["$set"]["related_news"]
    assert "LPR" in related[0]["title"]
    assert "报价保持不变" in related[0]["summary"]
