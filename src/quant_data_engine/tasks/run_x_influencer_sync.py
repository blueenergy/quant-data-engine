"""
同步 X（Twitter）大 V 推文 → quant_data.x_influencer_posts，并入队 finance.analysis_tasks
（type=x_influencer_post_analysis，由 quantAnalyzer 消费后写入 quant_analyzer.x_influencer_post_analysis）。

依赖官方 X API v2。需环境变量 TWITTER_BEARER_TOKEN（App Bearer Token）。

Usage:
    python -m quant_data_engine.tasks.run_x_influencer_sync
    python -m quant_data_engine.tasks.run_x_influencer_sync --loop --interval 180

环境变量:
    TWITTER_BEARER_TOKEN   必填
    X_INFLUENCER_USERNAMES 逗号分隔用户名，默认 elonmusk,realDonaldTrump,chamath
    X_INFLUENCER_MAX_RESULTS 单次拉取条数，默认 10（最大依套餐而定）
    X_INFLUENCER_BOOTSTRAP_MAX 首次无 since_id 时最多入队分析条数，默认 5

Docker 内由 start.sh 拉起 --loop 时，需额外设置 ENABLE_X_INFLUENCER_SYNC=1（默认关闭，避免无 credits 时反复失败）。
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
from pymongo import MongoClient

from quant_data_engine import get_settings

logger = logging.getLogger(__name__)

TWITTER_API = "https://api.twitter.com/2"
COL_POSTS = "x_influencer_posts"
COL_STATE = "x_influencer_sync_state"
COL_USER_CACHE = "x_influencer_user_cache"
TASK_TYPE = "x_influencer_post_analysis"


def _usernames_from_env() -> List[str]:
    raw = os.getenv("X_INFLUENCER_USERNAMES", "elonmusk,realDonaldTrump,chamath").strip()
    parts = [p.strip().lstrip("@") for p in raw.split(",") if p.strip()]
    return parts or ["elonmusk", "realDonaldTrump", "chamath"]


def _int_env(name: str, default: int) -> int:
    v = os.getenv(name, "").strip()
    if not v:
        return default
    try:
        return int(v)
    except ValueError:
        return default


def _format_twitter_api_error(path: str, status: int, detail: Any) -> str:
    """Turn X API error JSON into a log-friendly string with ops hints."""
    base = f"X API {path} HTTP {status}: {detail}"
    if status == 402:
        return (
            base
            + " — X 已按「额度/Credits」计费：当前开发者账号无可用 credits（CreditsDepleted）。"
            "请在 developer.x.com 对应 Project/App 的 Billing / Usage 中充值或升级套餐后再试。"
        )
    if status in (401, 403):
        return base + " — 请检查 Bearer Token 是否有效、App 权限是否包含所需端点。"
    return base


def _twitter_get(
    session: requests.Session,
    bearer: str,
    path: str,
    params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    url = f"{TWITTER_API}{path}"
    headers = {"Authorization": f"Bearer {bearer}"}
    r = session.get(url, headers=headers, params=params or {}, timeout=45)
    if r.status_code == 429:
        logger.warning("X API rate limited (429), backing off")
        time.sleep(60)
        r = session.get(url, headers=headers, params=params or {}, timeout=45)
    if r.status_code != 200:
        try:
            detail = r.json()
        except Exception:
            detail = r.text[:500]
        raise RuntimeError(_format_twitter_api_error(path, r.status_code, detail))
    return r.json()


def resolve_user_id(
    session: requests.Session,
    bearer: str,
    data_db: Any,
    username: str,
) -> Optional[str]:
    cache = data_db[COL_USER_CACHE]
    hit = cache.find_one({"username": username.lower()})
    if hit and hit.get("user_id"):
        return str(hit["user_id"])

    data = _twitter_get(session, bearer, f"/users/by/username/{username}", {"user.fields": "id,name,username"})
    row = (data or {}).get("data") or {}
    uid = row.get("id")
    if not uid:
        logger.error("X user lookup returned no id for username=%s", username)
        return None
    uid_s = str(uid)
    now = datetime.now(timezone.utc)
    cache.update_one(
        {"username": username.lower()},
        {
            "$set": {
                "user_id": uid_s,
                "name": row.get("name"),
                "username": username.lower(),
                "updated_at": now,
            }
        },
        upsert=True,
    )
    return uid_s


def fetch_timeline(
    session: requests.Session,
    bearer: str,
    user_id: str,
    since_id: Optional[str],
    max_results: int,
) -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {
        "max_results": min(max(5, max_results), 100),
        "tweet.fields": "created_at,lang,conversation_id",
        "exclude": "replies,retweets",
    }
    if since_id:
        params["since_id"] = since_id
    data = _twitter_get(session, bearer, f"/users/{user_id}/tweets", params)
    return list((data or {}).get("data") or [])


def build_post_doc(username: str, author_id: str, tweet: Dict[str, Any]) -> Dict[str, Any]:
    tid = str(tweet.get("id") or "")
    text = tweet.get("text") or ""
    created = tweet.get("created_at")
    lang = tweet.get("lang")
    conv = tweet.get("conversation_id")
    permalink = f"https://x.com/{username}/status/{tid}"
    now = datetime.now(timezone.utc)
    return {
        "tweet_id": tid,
        "author_id": author_id,
        "author_username": username.lower(),
        "text": text,
        "created_at": created,
        "lang": lang,
        "conversation_id": conv,
        "permalink": permalink,
        "ingested_at": now,
        "source": "twitter_api_v2",
    }


def enqueue_analysis(
    finance_db: Any,
    post_snapshot: Dict[str, Any],
    tweet_id: str,
) -> bool:
    col = finance_db["analysis_tasks"]
    exists = col.find_one(
        {
            "type": TASK_TYPE,
            "task_type": TASK_TYPE,
            "params.tweet_id": tweet_id,
            "status": {"$in": ["pending", "processing", "completed"]},
        }
    )
    if exists:
        return False
    now = datetime.now()
    doc: Dict[str, Any] = {
        "type": TASK_TYPE,
        "task_type": TASK_TYPE,
        "status": "pending",
        "priority": int(os.getenv("X_INFLUENCER_TASK_PRIORITY", "5")),
        "user_id": "system",
        "created_by_user_id": "system",
        "created_by_username": None,
        "source": "x_influencer_sync",
        "params": {
            "tweet_id": tweet_id,
            "post": post_snapshot,
        },
        "created_at": now,
        "updated_at": now,
    }
    ins = col.insert_one(doc)
    logger.info("enqueued %s task _id=%s tweet_id=%s", TASK_TYPE, ins.inserted_id, tweet_id)
    return True


def _ensure_indexes(data_db: Any, finance_db: Any) -> None:
    try:
        data_db[COL_POSTS].create_index("tweet_id", unique=True)
    except Exception as e:
        logger.debug("index x_influencer_posts.tweet_id: %s", e)
    try:
        data_db[COL_STATE].create_index("username", unique=True)
    except Exception as e:
        logger.debug("index x_influencer_sync_state.username: %s", e)
    try:
        data_db[COL_USER_CACHE].create_index("username", unique=True)
    except Exception as e:
        logger.debug("index x_influencer_user_cache.username: %s", e)
    # Idempotency: one active/completed task per tweet
    try:
        finance_db["analysis_tasks"].create_index(
            [("task_type", 1), ("params.tweet_id", 1), ("status", 1)],
            name="x_influencer_tweet_status",
            partialFilterExpression={
                "task_type": TASK_TYPE,
                "status": {"$in": ["pending", "processing", "completed"]},
            },
        )
    except Exception as e:
        logger.debug("partial index analysis_tasks x influencer: %s", e)


def run_once(settings) -> Tuple[int, int]:
    bearer = os.getenv("TWITTER_BEARER_TOKEN", "").strip()
    if not bearer:
        logger.warning("TWITTER_BEARER_TOKEN not set; skip X influencer sync")
        return 0, 0

    client = MongoClient(settings.mongo_uri)
    data_db = client[settings.data_db]
    finance_db = client[settings.mongo_db]

    _ensure_indexes(data_db, finance_db)

    usernames = _usernames_from_env()
    max_results = _int_env("X_INFLUENCER_MAX_RESULTS", 10)
    bootstrap_max = _int_env("X_INFLUENCER_BOOTSTRAP_MAX", 5)

    session = requests.Session()
    new_posts = 0
    enqueued = 0

    for username in usernames:
        try:
            uid = resolve_user_id(session, bearer, data_db, username)
            if not uid:
                continue
            state_col = data_db[COL_STATE]
            st = state_col.find_one({"username": username.lower()}) or {}
            since_id = st.get("since_id")

            tweets = fetch_timeline(session, bearer, uid, since_id, max_results)
            if not tweets:
                logger.info("no new tweets user=%s since_id=%s", username, since_id)
                continue

            # API returns newest first; process oldest-first so state since_id advances correctly
            tweets_sorted = list(reversed(tweets))

            posts_col = data_db[COL_POSTS]
            max_id_in_batch: Optional[str] = None
            count_bootstrap = 0

            for tw in tweets_sorted:
                tid = str(tw.get("id") or "")
                if not tid:
                    continue
                if max_id_in_batch is None or int(tid) > int(max_id_in_batch):
                    max_id_in_batch = tid

                existed = posts_col.find_one({"tweet_id": tid}, {"_id": 1})
                doc = build_post_doc(username, uid, tw)
                posts_col.update_one({"tweet_id": tid}, {"$set": doc}, upsert=True)

                if existed:
                    continue
                new_posts += 1

                snapshot = {
                    "tweet_id": doc["tweet_id"],
                    "text": doc["text"],
                    "author_username": doc["author_username"],
                    "author_id": doc["author_id"],
                    "created_at": doc["created_at"],
                    "permalink": doc["permalink"],
                    "lang": doc.get("lang"),
                }

                should_enqueue = True
                if not since_id and count_bootstrap >= bootstrap_max:
                    should_enqueue = False
                if should_enqueue and enqueue_analysis(finance_db, snapshot, tid):
                    enqueued += 1
                if not since_id:
                    count_bootstrap += 1

            if max_id_in_batch:
                state_col.update_one(
                    {"username": username.lower()},
                    {"$set": {"since_id": max_id_in_batch, "updated_at": datetime.now(timezone.utc)}},
                    upsert=True,
                )

            time.sleep(1.2)
        except Exception as e:
            logger.exception("x influencer sync failed for user=%s: %s", username, e)

    client.close()
    logger.info("x_influencer_sync done new_posts=%s enqueued=%s", new_posts, enqueued)
    return new_posts, enqueued


def main(argv: Optional[List[str]] = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    p = argparse.ArgumentParser(description="X 大 V 推文同步（API v2 → quant_data + analysis_tasks）")
    p.add_argument("--loop", action="store_true", help="循环运行")
    p.add_argument("--interval", type=int, default=180, help="循环间隔秒（默认 180）")
    args = p.parse_args(argv)

    settings = get_settings()

    if args.loop:
        while True:
            try:
                run_once(settings)
            except KeyboardInterrupt:
                logger.info("interrupted")
                return 0
            except Exception as e:
                logger.exception("run_once crashed: %s", e)
            time.sleep(max(30, args.interval))
        return 0

    run_once(settings)
    return 0


if __name__ == "__main__":
    sys.exit(main())
