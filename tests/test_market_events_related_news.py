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
            et_q = query.get("event_time") or {}
            gte = et_q.get("$gte")
            lte = et_q.get("$lte")
            lt = et_q.get("$lt")
            if gte and et < gte:
                continue
            if lte and et > lte:
                continue
            if lt and not (et < lt):
                continue
            if d.get("importance", 0) < query.get("importance", {}).get("$gte", 0):
                continue
            status_q = query.get("status")
            if isinstance(status_q, dict) and "$in" in status_q:
                if d.get("status") not in status_q["$in"]:
                    continue
            elif status_q and d.get("status") != status_q:
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


def test_attach_outcome_news_for_past_events():
    past = datetime.now(timezone.utc) - timedelta(hours=6)
    col = _FakeCol(
        [
            {
                "identity_key": "amd",
                "title": "AMD Advancing AI大会",
                "event_time": past,
                "importance": 4,
                "status": "scheduled",
                "related_news": [{"title": "会前预告：演讲中可能披露新芯片"}],
                "outcome_news": [],
            }
        ]
    )
    intl = {
        "categories": {
            "科技": [
                {
                    "title": "AMD大会公布新一代AI芯片路线",
                    "summary": "会上给出出货节奏与合作伙伴进展。",
                    "url": "http://amd",
                }
            ]
        }
    }
    n = attach_related_news(col, international_news_doc=intl, lookback_hours=72)
    assert n == 1
    set_doc = col.updates[0][1]["$set"]
    assert "outcome_news" in set_doc
    assert "AMD" in set_doc["outcome_news"][0]["title"]
    # Pre-event related_news must remain untouched by this update.
    assert "related_news" not in set_doc
    assert col.docs[0]["related_news"][0]["title"].startswith("会前预告")
