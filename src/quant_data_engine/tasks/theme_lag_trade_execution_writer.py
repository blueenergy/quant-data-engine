"""主题补涨验证后：按固定名义本金（默认每标的 1 万 CNY）折算整手，写入 ``finance.trade_executions`` 纸面买卖腿。

``user_id`` 取自 ``finance.theme_lag_validation_subscriptions``：同一 ``index_store_key`` 下所有 ``enabled`` 非 false 的订阅者各写一套腿，便于各自在看板「交易记录」中查看。

**默认关闭**，见环境变量 ``THEME_LAG_WRITE_SIZED_TRADE_EXECUTIONS``。若无任何订阅者则跳过写入并打日志。

在同一 Mongo 部署**支持多文档事务**（副本集 / sharded）时，对每个订阅者的 ``delete_many`` + ``insert_many`` 在同一 session 事务中提交，避免删成后插入失败导致纸面腿丢失；单机或事务不可用时自动回退为顺序写入并打 warning。
"""
from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

from pymongo import MongoClient
from pymongo.errors import PyMongoError

from quant_data_engine.config.settings import Settings

logger = logging.getLogger(__name__)

STRATEGY = "theme_lag_sized_paper"
LOT = 100
SUBS_COLLECTION = "theme_lag_validation_subscriptions"


def _truthy_env(name: str) -> bool:
    v = os.getenv(name, "").strip().lower()
    return v in ("1", "true", "yes", "on")


def _float_env(name: str, default: float) -> float:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _session_ts_yyyymmdd(yyyymmdd: str, *, hour_utc: int = 12) -> float:
    """锚定/退出业务日 → 稳定可排序的 UTC 时间戳（仅用于纸面记录，非真实撮合时刻）。"""
    s = "".join(c for c in str(yyyymmdd) if c.isdigit())[:8]
    if len(s) != 8:
        return time.time()
    y, m, d = int(s[:4]), int(s[4:6]), int(s[6:8])
    dt = datetime(y, m, d, hour_utc, 0, 0, tzinfo=timezone.utc)
    return dt.timestamp()


def _shares_for_notional(p0: float, notional_cny: float) -> int:
    """A 股整手：向下取整到 100 股；不足一手返回 0。"""
    if p0 <= 0 or notional_cny <= 0:
        return 0
    raw = int(notional_cny / p0)
    return (raw // LOT) * LOT


def _oid_tail(sym: str) -> str:
    return "".join(c if c.isalnum() else "_" for c in sym.strip())[:40]


def _order_uid_slug(uid: str) -> str:
    """嵌入 ``order_id``，避免不同订阅者之间碰撞。"""
    s = "".join(c if c.isalnum() else "_" for c in str(uid))[:48]
    return s or "u"


def _list_subscriber_user_ids(client: MongoClient, finance_db_name: str, index_store_key: str) -> List[str]:
    sk = str(index_store_key).strip()
    if not sk:
        return []
    coll = client[finance_db_name][SUBS_COLLECTION]
    out: List[str] = []
    seen: set[str] = set()
    for doc in coll.find({"index_store_key": sk, "enabled": {"$ne": False}}, projection={"user_id": 1}):
        raw = doc.get("user_id")
        if raw is None:
            continue
        uid = str(raw).strip()
        if not uid or uid in seen:
            continue
        seen.add(uid)
        out.append(uid)
    return out


def _build_paper_execution_docs(
    user_id: str,
    td: str,
    sk: str,
    per: List[Dict[str, Any]],
    notional: float,
    buy_ts_base: float,
) -> Tuple[List[Dict[str, Any]], int, int]:
    filt: Dict[str, Any] = {
        "user_id": user_id,
        "strategy": STRATEGY,
        "theme_lag_anchor": td,
        "theme_lag_index": sk,
    }
    slug = _order_uid_slug(user_id)
    to_insert: List[Dict[str, Any]] = []
    skipped = 0
    pairs = 0

    for row in per:
        if not isinstance(row, dict):
            continue
        sym = str(row.get("symbol") or "").strip()
        if not sym:
            continue
        p0 = row.get("p0")
        try:
            p0f = float(p0) if p0 is not None else 0.0
        except (TypeError, ValueError):
            p0f = 0.0
        if p0f != p0f or p0f <= 0:  # NaN or bad
            skipped += 1
            continue

        shares = _shares_for_notional(p0f, notional)
        if shares < LOT:
            skipped += 1
            continue

        outcome = str(row.get("outcome") or "")
        if outcome == "insufficient_data":
            skipped += 1
            continue

        exd = row.get("exit_trade_date")
        exp = row.get("exit_price")
        if not exd or exp is None:
            skipped += 1
            continue
        try:
            exf = float(exp)
        except (TypeError, ValueError):
            skipped += 1
            continue
        if exf != exf or exf <= 0:
            skipped += 1
            continue
        exs = "".join(c for c in str(exd) if c.isdigit())[:8]
        if len(exs) != 8:
            skipped += 1
            continue

        oid = _oid_tail(sym)
        buy_id = f"tl|{slug}|{td}|{sk}|{oid}|BUY"
        actual_cny = round(shares * p0f, 2)

        buy_doc: Dict[str, Any] = {
            **filt,
            "symbol": sym,
            "side": "BUY",
            "filled_size": shares,
            "filled_price": round(p0f, 6),
            "status": "filled",
            "mode": "paper",
            "order_id": buy_id,
            "timestamp": buy_ts_base,
            "planned_notional_cny": notional,
            "actual_notional_cny": actual_cny,
            "outcome": outcome,
        }
        buy_doc.setdefault("date", td)
        to_insert.append(buy_doc)

        sell_ts = _session_ts_yyyymmdd(exs, hour_utc=13) + 1.0
        if sell_ts <= buy_ts_base:
            sell_ts = buy_ts_base + 2.0
        sell_id = f"tl|{slug}|{td}|{sk}|{oid}|SELL"
        sell_doc: Dict[str, Any] = {
            **filt,
            "symbol": sym,
            "side": "SELL",
            "filled_size": shares,
            "filled_price": round(exf, 6),
            "status": "filled",
            "mode": "paper",
            "order_id": sell_id,
            "timestamp": sell_ts,
            "planned_notional_cny": notional,
            "actual_notional_cny": round(shares * exf, 2),
            "outcome": outcome,
        }
        sell_doc.setdefault("date", exs)
        to_insert.append(sell_doc)
        pairs += 1

    return to_insert, pairs, skipped


def _apply_paper_trade_plans(
    client: MongoClient,
    exec_coll: Any,
    plans: List[Tuple[Dict[str, Any], List[Dict[str, Any]]]],
) -> Tuple[int, int]:
    """对每个 ``(filt, to_insert)`` 执行删旧再插新。优先在同一 ``start_session`` 事务中完成；失败则顺序执行。"""
    if not plans:
        return 0, 0

    try:
        with client.start_session() as session:
            with session.start_transaction():
                total_removed = 0
                total_inserted = 0
                for filt, to_insert in plans:
                    dr = exec_coll.delete_many(filt, session=session)
                    total_removed += int(dr.deleted_count)
                    if to_insert:
                        exec_coll.insert_many(to_insert, session=session)
                        total_inserted += len(to_insert)
        return total_removed, total_inserted
    except PyMongoError as e:
        logger.warning(
            "theme_lag paper trades: multi-document transaction not applied (%s); using sequential delete/insert",
            e,
        )
    except Exception as e:
        logger.warning(
            "theme_lag paper trades: transaction path failed (%s); using sequential delete/insert",
            e,
        )

    total_removed = 0
    total_inserted = 0
    for filt, to_insert in plans:
        dr = exec_coll.delete_many(filt)
        total_removed += int(dr.deleted_count)
        if to_insert:
            exec_coll.insert_many(to_insert)
            total_inserted += len(to_insert)
    return total_removed, total_inserted


def write_sized_paper_trades(settings: Settings, validation_doc: Dict[str, Any]) -> Dict[str, Any]:
    """若开启环境变量，则对每个订阅该指数的用户：先删同锚定同指数同策略的旧纸面腿，再写入 BUY / SELL。"""
    if not _truthy_env("THEME_LAG_WRITE_SIZED_TRADE_EXECUTIONS"):
        return {"enabled": False, "reason": "THEME_LAG_WRITE_SIZED_TRADE_EXECUTIONS not set"}

    td = str(validation_doc.get("anchor_trade_date") or "").strip()
    sk = str(validation_doc.get("index_store_key") or "").strip()
    if len(td) != 8 or not sk:
        return {"enabled": True, "error": "missing anchor_trade_date or index_store_key"}

    notional = _float_env("THEME_LAG_NOTIONAL_CNY_PER_SYMBOL", 10_000.0)
    if notional <= 0:
        return {"enabled": True, "error": "THEME_LAG_NOTIONAL_CNY_PER_SYMBOL must be positive"}

    per: List[Dict[str, Any]] = validation_doc.get("per_symbol") or []
    if not isinstance(per, list):
        return {"enabled": True, "error": "per_symbol not a list"}

    client = MongoClient(settings.mongo_uri)
    try:
        user_ids = _list_subscriber_user_ids(client, settings.mongo_db, sk)
        if not user_ids:
            logger.info(
                "theme_lag sized paper trades skipped: no enabled rows in finance.%s for index_store_key=%s",
                SUBS_COLLECTION,
                sk,
            )
            return {
                "enabled": True,
                "removed": 0,
                "inserted": 0,
                "closed_pairs": 0,
                "skipped_symbol_rows": 0,
                "notional_cny_per_symbol": notional,
                "user_ids": [],
                "reason": "no_subscribers_for_index",
            }

        exec_coll = client[settings.mongo_db]["trade_executions"]
        buy_ts_base = _session_ts_yyyymmdd(td)
        pairs_per_user = 0
        skipped_once = 0
        plans: List[Tuple[Dict[str, Any], List[Dict[str, Any]]]] = []

        for i, uid in enumerate(user_ids):
            filt = {
                "user_id": uid,
                "strategy": STRATEGY,
                "theme_lag_anchor": td,
                "theme_lag_index": sk,
            }
            to_insert, pairs, skipped = _build_paper_execution_docs(uid, td, sk, per, notional, buy_ts_base)
            if i == 0:
                skipped_once = skipped
                pairs_per_user = pairs
            plans.append((filt, to_insert))

        total_removed, total_inserted = _apply_paper_trade_plans(client, exec_coll, plans)

        logger.info(
            "theme_lag sized paper trades: anchor=%s index=%s removed=%s inserted=%s pairs_per_user=%s skipped_rows=%s notional=%s subscriber_count=%s user_ids=%s",
            td,
            sk,
            total_removed,
            total_inserted,
            pairs_per_user,
            skipped_once,
            notional,
            len(user_ids),
            user_ids,
        )

        return {
            "enabled": True,
            "removed": total_removed,
            "inserted": total_inserted,
            "closed_pairs": pairs_per_user,
            "skipped_symbol_rows": skipped_once,
            "notional_cny_per_symbol": notional,
            "user_ids": user_ids,
            "subscriber_count": len(user_ids),
        }
    finally:
        client.close()


__all__ = [
    "SUBS_COLLECTION",
    "STRATEGY",
    "_apply_paper_trade_plans",
    "write_sized_paper_trades",
]
