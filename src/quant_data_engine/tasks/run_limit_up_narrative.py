"""组装今日涨停全景数据，入队 ``limit_up_daily_narrative`` 分析任务。

该任务从 ``quant_data.daily_limit_up`` 和 ``quant_data.limit_up_ladder`` 读取今日及昨日的
完整数据（含炸板股），打包成结构化 payload 后向 ``finance.analysis_tasks`` 插入一条
``limit_up_daily_narrative`` 任务，由 quantAnalyzer 异步消费并调用 LLM 生成总结报告。

生成的报告写入 ``quant_analyzer.limit_up_daily_narrative``，通过
``GET /ladder/narrative`` API 暴露给前端。

Usage::

    # 今日（自动获取最新有数据的日期）
    python -m quant_data_engine.tasks.run_limit_up_narrative

    # 指定日期
    python -m quant_data_engine.tasks.run_limit_up_narrative --date 20260507

    # 强制重跑（删除已有 pending/completed 任务后重新入队）
    python -m quant_data_engine.tasks.run_limit_up_narrative --force

    # 仅打印 payload，不入队
    python -m quant_data_engine.tasks.run_limit_up_narrative --dry-run
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from pymongo import MongoClient

from quant_data_engine.config.settings import get_settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ── helpers ──────────────────────────────────────────────────────────────────


def _clean(doc: dict) -> dict:
    """Strip MongoDB-internal fields not needed in the payload."""
    return {k: v for k, v in doc.items() if k not in ("_id", "created_at", "updated_at")}


def _sector_summary(stocks: List[dict]) -> List[dict]:
    """聚合板块计数，返回按 count 降序的列表。"""
    agg: Dict[str, int] = {}
    for s in stocks:
        sect = s.get("primary_sector") or "其他"
        agg[sect] = agg.get(sect, 0) + 1
    return sorted(
        [{"sector": k, "count": v} for k, v in agg.items()],
        key=lambda x: x["count"],
        reverse=True,
    )


def _stock_row(s: dict) -> dict:
    """精简单只股票字段，避免 payload 过大。"""
    return {
        "symbol": s.get("symbol"),
        "name": s.get("name"),
        "streak": s.get("streak", 1),
        "prev_streak": s.get("prev_streak"),
        "status": s.get("status"),          # "limit_up" | "broken"
        "open_count": s.get("open_count", 0),  # 炸板次数
        "seal_amount": s.get("seal_amount"),
        "primary_sector": s.get("primary_sector"),
        "limit_up_time": s.get("limit_up_time"),
        "pct_chg": s.get("pct_chg"),
        "up_stat": s.get("up_stat"),
    }


# ── payload assembly ──────────────────────────────────────────────────────────


def build_payload(
    data_db,
    trade_date: str,
    prev_date: Optional[str] = None,
) -> Dict[str, Any]:
    """组装今日与昨日涨停全景数据为 LLM 可消费的结构化 payload。

    Args:
        data_db: quant_data MongoDB database handle.
        trade_date: 今日业务日期 ``YYYYMMDD``。
        prev_date: 昨日业务日期（可选，自动推断最近有数据的前一天）。

    Returns:
        Structured payload dict.
    """
    daily_col = data_db["daily_limit_up"]
    ladder_col = data_db["limit_up_ladder"]

    # ── Today ──
    today_stocks_raw = list(daily_col.find({"date": trade_date}))
    today_ladder = ladder_col.find_one({"date": trade_date}) or {}

    # ── Prev date: find nearest day with data before trade_date ──
    if not prev_date:
        prev_ladder = ladder_col.find_one(
            {"date": {"$lt": trade_date}},
            sort=[("date", -1)],
        )
        prev_date = (prev_ladder or {}).get("date")

    prev_stocks_raw: List[dict] = []
    prev_ladder_doc: dict = {}
    if prev_date:
        prev_stocks_raw = list(daily_col.find({"date": prev_date}))
        prev_ladder_doc = ladder_col.find_one({"date": prev_date}) or {}

    # ── Partition today's stocks ──
    sealed_stocks = [s for s in today_stocks_raw if s.get("status") == "limit_up"]
    broken_stocks = [s for s in today_stocks_raw if s.get("status") == "broken"]
    all_stocks = today_stocks_raw  # includes both

    # ── Sector summaries ──
    today_sector_summary = _sector_summary(all_stocks)
    prev_sector_summary = _sector_summary(prev_stocks_raw) if prev_stocks_raw else []

    # ── Ladder indicators diff ──
    today_ind = today_ladder.get("indicators", {})
    prev_ind = prev_ladder_doc.get("indicators", {})

    def _diff(key: str) -> Optional[Any]:
        a, b = today_ind.get(key), prev_ind.get(key)
        if a is None or b is None:
            return None
        try:
            return round(float(a) - float(b), 2)
        except (TypeError, ValueError):
            return None

    # ── Build payload ──
    payload: Dict[str, Any] = {
        "trade_date": trade_date,
        "prev_trade_date": prev_date,
        # ── Today snapshot ──
        "today": {
            "total_limit_up": len(all_stocks),
            "sealed_count": len(sealed_stocks),
            "broken_count": len(broken_stocks),
            "broken_rate": (
                round(len(broken_stocks) / len(all_stocks) * 100, 1)
                if all_stocks
                else 0.0
            ),
            "indicators": today_ind,
            "sector_ranking": today_ladder.get("sector_ranking", [])[:10],
            "sector_summary": today_sector_summary[:15],
            # Top-streak stocks (descending streak, up to 20)
            "top_stocks": [
                _stock_row(s)
                for s in sorted(all_stocks, key=lambda x: x.get("streak", 1), reverse=True)[:20]
            ],
            # All broken-plate stocks
            "broken_stocks": [_stock_row(s) for s in broken_stocks],
            # Tier breakdown from ladder
            "tiers": today_ladder.get("tiers", {}),
        },
        # ── Yesterday snapshot (for comparison) ──
        "yesterday": {
            "total_limit_up": len(prev_stocks_raw),
            "sealed_count": len([s for s in prev_stocks_raw if s.get("status") == "limit_up"]),
            "broken_count": len([s for s in prev_stocks_raw if s.get("status") == "broken"]),
            "broken_rate": (
                round(
                    len([s for s in prev_stocks_raw if s.get("status") == "broken"])
                    / len(prev_stocks_raw)
                    * 100,
                    1,
                )
                if prev_stocks_raw
                else 0.0
            ),
            "indicators": prev_ind,
            "sector_ranking": prev_ladder_doc.get("sector_ranking", [])[:10],
            "sector_summary": prev_sector_summary[:15],
            "tiers": prev_ladder_doc.get("tiers", {}),
        },
        # ── Day-over-day diffs ──
        "dod": {
            "total_limit_up": len(all_stocks) - len(prev_stocks_raw),
            "sentiment_score": _diff("sentiment_score"),
            "high_tier_count": _diff("high_tier_count"),
            "promotion_rate_1to2": _diff("promotion_rate_1to2"),
            "max_streak": _diff("max_streak"),
        },
    }
    return payload


# ── enqueue ───────────────────────────────────────────────────────────────────


def enqueue_task(
    finance_db,
    trade_date: str,
    payload: Dict[str, Any],
    *,
    force: bool = False,
) -> Optional[str]:
    """向 finance.analysis_tasks 插入 limit_up_daily_narrative 任务。

    Returns inserted task_id or None if skipped (already exists).
    """
    col = finance_db["analysis_tasks"]

    existing = col.find_one(
        {
            "type": "limit_up_daily_narrative",
            "params.trade_date": trade_date,
            "status": {"$in": ["pending", "processing", "completed"]},
        }
    )

    if existing and not force:
        logger.info(
            "limit_up_daily_narrative already exists for %s (status=%s), skipping. "
            "Use --force to re-enqueue.",
            trade_date,
            existing.get("status"),
        )
        return None

    if existing and force:
        col.delete_many(
            {
                "type": "limit_up_daily_narrative",
                "params.trade_date": trade_date,
            }
        )
        logger.info("Deleted existing tasks for %s (force mode)", trade_date)

    doc = {
        "type": "limit_up_daily_narrative",
        "task_type": "limit_up_daily_narrative",
        "status": "pending",
        "priority": 10,
        "user_id": "system",
        "params": payload,
        "created_at": datetime.now(),
    }
    result = col.insert_one(doc)
    task_id = str(result.inserted_id)
    logger.info("Enqueued limit_up_daily_narrative task %s for %s", task_id, trade_date)
    return task_id


# ── resolve date ──────────────────────────────────────────────────────────────


def resolve_trade_date(data_db, requested: Optional[str]) -> Optional[str]:
    """If no date given, return the most recent date with data in daily_limit_up."""
    if requested:
        return requested
    latest = data_db["daily_limit_up"].find_one(sort=[("date", -1)])
    return (latest or {}).get("date")


# ── CLI ───────────────────────────────────────────────────────────────────────


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Enqueue limit_up_daily_narrative analysis task")
    p.add_argument("--date", type=str, help="Trade date YYYYMMDD (default: latest with data)")
    p.add_argument(
        "--force",
        action="store_true",
        help="Delete existing task for the date and re-enqueue",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print payload JSON, do not write to MongoDB",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    settings = get_settings()

    client = MongoClient(settings.mongo_uri)
    data_db = client[settings.data_db]
    finance_db = client[os.getenv("DB_NAME", "finance")]

    trade_date = resolve_trade_date(data_db, args.date)
    if not trade_date:
        logger.error("No data found in daily_limit_up. Run ladder sync first.")
        sys.exit(1)

    logger.info("Building payload for %s …", trade_date)
    payload = build_payload(data_db, trade_date)

    if args.dry_run:
        print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
        return

    task_id = enqueue_task(finance_db, trade_date, payload, force=args.force)
    if task_id:
        print(f"✅ Enqueued task {task_id} for {trade_date}")
    else:
        print(f"⏭  Skipped — task already exists for {trade_date} (use --force to re-run)")

    client.close()


if __name__ == "__main__":
    main()
