"""Sector-leader / laggard weak-open-reclaim watchdog (alert-only).

Modes
-----
leader   09:26:30  find stocks sealed at the 09:25 auction, group by L3/L2,
                   keep the highest yesterday streak as the sector leader
trigger  09:32:10  for watchlist + holdings + yesterday's first-board names in
                   the leader's sector, apply the pre-registered laggard
                   thresholds and the completed 09:31 bar
track    09:36/09:45/10:00/10:30  follow first-seal time, turnover,
                   seal_amount / day_amount and seal/broken state changes
summary  10:31     write the day's aggregate event

It writes ``quant_data.sector_follower_watch_events`` and (optionally) sends a
DingTalk message.  It never writes ``trade_signals`` or order intents.

Run inside the engine image with the QMT mailbox mounted::

    python3 -m quant_data_engine.tasks.run_sector_follower_watch --mode leader
"""
from __future__ import annotations

import argparse
import base64
from datetime import datetime
import hashlib
import hmac
import json
import logging
import os
import sys
import time
import urllib.parse
import urllib.request
from typing import Any, Iterable, Mapping, Sequence

from pymongo import MongoClient

from quant_data_engine.auction.core import build_features
from quant_data_engine.auction.universe import load_first_board_pool, normalize_symbol
from quant_data_engine.bridges.bigqmt.file_rpc_client import BigQmtFileRpcClient
from quant_data_engine.bridges.miniqmt.universe import SymbolUniverse
from quant_data_engine.config.settings import get_settings
from quant_data_engine.tasks.run_auction_live_capture import (
    DB,
    FEAT,
    load_bridge_config,
    load_prev_close,
)
from quant_data_engine.tasks.run_auction_backfill import TICK_FIELDS, _tick_rows
from quant_data_engine.tasks.sector_follower import (
    MIN_LIFT_PCT,
    MIN_OI5,
    MIN_RECLAIM_PCT,
    auction_sealed,
    evaluate_laggard,
    ladder_context,
    leader_has_tracked,
    limit_price,
    num,
    seal_metrics,
    sector_key,
    sector_key_l2,
    tracked_in_sector,
)

logger = logging.getLogger(__name__)

EVENTS = "sector_follower_watch_events"
REALTIME_DB = "quant_realtime"
FEATURE_VERSION = "sector_follower_v1"
MODES = ("leader", "trigger", "track", "summary")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sector leader/laggard weak-open-reclaim watchdog")
    parser.add_argument("--mode", required=True, choices=MODES)
    parser.add_argument("--date", default="", help="trade date YYYYMMDD; default today")
    parser.add_argument("--config", default="/app/scripts/bigqmt_rpc_bridge.watchlist_minute_kline.json")
    parser.add_argument("--mailbox", default="/app/bqbox/sector-follower-watch")
    parser.add_argument("--account-id", default="")
    parser.add_argument("--notify", action="store_true", default=False, help="send DingTalk alerts")
    parser.add_argument("--test-alert", action="store_true", help="send one labeled test DingTalk and exit")
    parser.add_argument("--dry-run", action="store_true", help="print events, do not write or notify")
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args(argv)


def _today(value: str) -> str:
    return str(value or datetime.now().strftime("%Y%m%d")).replace("-", "")


def _load_members(db: Any, symbols: Iterable[str]) -> dict[str, dict[str, Any]]:
    wanted = sorted(set(symbols))
    out: dict[str, dict[str, Any]] = {}
    for doc in db.sw_index_member.find(
        {"con_code": {"$in": wanted}, "src": "SW2021", "is_new": "Y"},
        {
            "_id": 0, "con_code": 1, "name": 1,
            "l1_code": 1, "l1_name": 1, "l2_code": 1, "l2_name": 1,
            "l3_code": 1, "l3_name": 1, "out_date": 1,
        },
    ):
        if doc.get("out_date"):
            continue
        code = str(doc.get("con_code"))
        out[code] = doc
    return out


def _load_names(db: Any, symbols: Iterable[str]) -> dict[str, str]:
    wanted = sorted(set(symbols))
    out: dict[str, str] = {}
    for doc in db.stock_info.find(
        {"symbol": {"$in": wanted}}, {"_id": 0, "symbol": 1, "name": 1}
    ):
        if doc.get("name"):
            out[str(doc["symbol"])] = str(doc["name"])
    return out


def _load_features(db: Any, trade_date: str) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for doc in db[FEAT].find({"trade_date": trade_date}, {"_id": 0}):
        out[str(doc.get("symbol"))] = doc
    return out


def _tracked_symbols(db: Any, settings: Any, config: Any) -> set[str]:
    """Watchlist + portfolio symbols (no first-board pool)."""
    symbols: set[str] = set()
    try:
        universe = SymbolUniverse(
            finance_db=db.client[settings.mongo_db],
            trading_db=db.client[settings.trading_db],
            data_db=db.client[settings.data_db],
            config=config.universe,
        )
        symbols.update(universe.resolve())
    except Exception as exc:
        logger.warning("SymbolUniverse resolve failed: %s", exc)
    return {normalize_symbol(s) for s in symbols if normalize_symbol(s)}


def _candidate_symbols(db: Any, settings: Any, config: Any, trade_date: str) -> set[str]:
    symbols = _tracked_symbols(db, settings, config)
    symbols.update(load_first_board_pool(db, trade_date))
    return {normalize_symbol(s) for s in symbols if normalize_symbol(s)}


def _load_latest_sector_strength(realtime_db: Any, code: str, level: str,
                                 now_text: str) -> dict[str, Any] | None:
    if not code:
        return None
    return realtime_db.realtime_sector_strength.find_one(
        {
            "sector_code": code,
            "level": level,
            "ma_period": 5,
            "snapshot_time": {"$lte": now_text},
        },
        {"_id": 0},
        sort=[("snapshot_time", -1)],
    )


def _limit_row(db: Any, trade_date: str, symbol: str) -> dict[str, Any] | None:
    return db.daily_limit_up.find_one(
        {"date": trade_date, "symbol": symbol},
        {
            "_id": 0, "symbol": 1, "name": 1, "status": 1, "limit_up_status": 1,
            "limit_up_time": 1, "last_seal_time": 1, "seal_amount": 1,
            "turnover_ratio": 1, "open_count": 1, "prev_streak": 1, "streak": 1,
            "primary_sector": 1, "limit_reason": 1,
        },
    )


def _price_row(db: Any, trade_date: str, symbol: str) -> dict[str, Any] | None:
    return db.volume_price.find_one(
        {"trade_date": trade_date, "symbol": symbol},
        {"_id": 0, "symbol": 1, "name": 1, "open": 1, "high": 1, "low": 1,
         "close": 1, "pct_chg": 1, "amount": 1, "volume": 1,
         "realtime_snapshot_time": 1},
    )


def _previous_ladder_context(db: Any, trade_date: str, sector_names: Sequence[Any]) -> dict[str, Any]:
    """Yesterday's final ladder + sector_flow (PIT for the 09:27 leader)."""
    ladder = db.limit_up_ladder.find_one(
        {"date": {"$lt": trade_date}}, {"_id": 0}, sort=[("date", -1)]
    )
    if not ladder:
        return {}
    flows = list(db.sector_flow.find({"date": ladder.get("date")}, {"_id": 0}))
    ctx = ladder_context(ladder, flows, sector_names)
    return {
        "ladder_prev_date": ctx["ladder_date"],
        "ladder_prev_indicators": ctx["ladder_indicators"],
        "ladder_prev_sector_flow": ctx["sector_flow"],
        "ladder_prev_sector_ranking": ctx["sector_ranking"],
    }


def _today_ladder_context(db: Any, trade_date: str, sector_names: Sequence[Any]) -> dict[str, Any]:
    """Today's ladder as an explicitly partial intraday snapshot."""
    ladder = db.limit_up_ladder.find_one({"date": trade_date}, {"_id": 0})
    flows = list(db.sector_flow.find({"date": trade_date}, {"_id": 0})) if ladder else []
    ctx = ladder_context(ladder, flows, sector_names)
    return {
        "ladder_today_partial": bool(ladder),
        "ladder_today_indicators": ctx["ladder_indicators"],
        "ladder_today_sector_flow": ctx["sector_flow"],
        "ladder_today_sector_ranking": ctx["sector_ranking"],
    }


def _handle_one_minute(rpc: BigQmtFileRpcClient, symbols: Sequence[str], trade_date: str,
                       timeout: float = 60.0) -> dict[str, dict[str, Any]]:
    """Return {symbol: 09:31 1m bar}; retry once because 09:32:10 can race QMT."""
    out: dict[str, dict[str, Any]] = {}
    fields = ["stime", "open", "high", "low", "close", "volume", "amount"]
    for attempt in range(2):
        data = rpc.get_market_data_ex(
            field_list=fields,
            stock_list=list(symbols),
            period="1m",
            count=241,
            start_time=trade_date,
            end_time=trade_date,
            dividend_type="none",
            fill_data=False,
            timeout_seconds=timeout,
        )
        for symbol in symbols:
            payload = (data or {}).get(symbol) or {}
            records = payload.get("records") if isinstance(payload, Mapping) else None
            if not isinstance(records, Mapping):
                continue
            stimes = records.get("stime") or []
            for index, stime in enumerate(stimes):
                digits = "".join(ch for ch in str(stime) if ch.isdigit())
                if len(digits) >= 12 and digits[8:12] == "0931":
                    out[symbol] = {
                        key: (records.get(key)[index] if index < len(records.get(key) or []) else None)
                        for key in fields
                    }
                    break
        if len(out) == len(symbols):
            break
        time.sleep(3.0)
    return out


def _supplement_feature(rpc: BigQmtFileRpcClient, db: Any, symbol: str, trade_date: str,
                        prev_close: float | None) -> dict[str, Any] | None:
    try:
        data = rpc.get_market_data_ex(
            field_list=TICK_FIELDS,
            stock_list=[symbol],
            period="tick",
            count=100000,
            start_time=trade_date,
            end_time=trade_date,
            dividend_type="none",
            fill_data=False,
            timeout_seconds=45,
        )
    except Exception as exc:
        logger.warning("supplement tick read failed %s: %s", symbol, exc)
        return None
    records = ((data or {}).get(symbol) or {}).get("records") or {}
    rows = _tick_rows(records, symbol, trade_date)
    if not rows:
        return None
    return build_features(rows, symbol, trade_date, prev_close, None, source="qmt_tick_supplement")


def _write_event(db: Any, doc: Mapping[str, Any], *, key: Mapping[str, Any],
                 dry_run: bool) -> None:
    payload = dict(doc)
    payload.setdefault("created_at", datetime.now())
    payload["updated_at"] = datetime.now()
    if dry_run:
        print(json.dumps(payload, ensure_ascii=False, default=str), flush=True)
        return
    db[EVENTS].update_one(dict(key), {"$set": payload}, upsert=True)


def _send_dingtalk(title: str, content: str) -> bool:
    url = os.getenv("DINGTALK_WEBHOOK_URL", "").strip()
    secret = os.getenv("DINGTALK_WEBHOOK_SECRET", "").strip()
    if not url:
        logger.warning("DINGTALK_WEBHOOK_URL not set; alert not sent")
        return False
    ts = str(int(time.time() * 1000))
    if secret:
        digest = hmac.new(
            secret.encode("utf-8"),
            ("%s\n%s" % (ts, secret)).encode("utf-8"),
            hashlib.sha256,
        ).digest()
        sign = urllib.parse.quote_plus(base64.b64encode(digest).decode("ascii"))
        url = "%s&timestamp=%s&sign=%s" % (url, ts, sign)
    payload = json.dumps(
        {"msgtype": "text", "text": {"content": "[SectorFollower] %s\n%s" % (title, content)}},
        ensure_ascii=False,
    ).encode("utf-8")
    request = urllib.request.Request(
        url, data=payload, headers={"Content-Type": "application/json"}
    )
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            body = response.read().decode("utf-8", errors="replace")
        ok = '"errcode":0' in body
        if not ok:
            logger.warning("DingTalk rejected: %s", body[:300])
        return ok
    except Exception as exc:
        logger.warning("DingTalk send failed: %s", exc)
        return False


def _leader_events(
    db: Any,
    settings: Any,
    config: Any,
    rpc: BigQmtFileRpcClient,
    trade_date: str,
    dry_run: bool,
    tracked_symbols: set[str] | None = None,
) -> list[dict[str, Any]]:
    features = _load_features(db, trade_date)
    names = _load_names(db, features.keys())
    for symbol, feature in features.items():
        if not feature.get("name"):
            feature["name"] = names.get(symbol)
    members = _load_members(db, features.keys())
    tracked_members = _load_members(db, tracked_symbols or set())
    limit_rows = {
        str(doc.get("symbol")): doc
        for doc in db.daily_limit_up.find(
            {"date": trade_date},
            {"_id": 0, "symbol": 1, "name": 1, "limit_up_time": 1,
             "limit_up_status": 1, "status": 1, "streak": 1, "prev_streak": 1,
             "primary_sector": 1, "limit_reason": 1},
        )
    }
    # Supplement leaders outside the live universe.  At 09:26 the live task has
    # only watchlists/portfolios + yesterday's first-board pool; high-streak
    # names (e.g. 永臻股份) may be missing.
    supplement = set()
    for symbol, row in limit_rows.items():
        if symbol in features:
            continue
        limit_time = num(row.get("limit_up_time"))
        if limit_time is not None and limit_time <= 92600 and row.get("status") in ("limit_up", "broken"):
            supplement.add(symbol)
    if supplement:
        prev_close = load_prev_close(db, sorted(supplement), trade_date)
        for symbol in sorted(supplement):
            feature = _supplement_feature(rpc, db, symbol, trade_date, prev_close.get(symbol))
            if feature:
                feature["name"] = (limit_rows.get(symbol) or {}).get("name")
                features[symbol] = feature
        members = _load_members(db, features.keys())

    grouped: dict[str, list[dict[str, Any]]] = {}
    for symbol, feature in features.items():
        if not auction_sealed(feature):
            continue
        code, name, level = sector_key_l2(members.get(symbol))
        if not code:
            continue
        member = members.get(symbol) or {}
        row = limit_rows.get(symbol) or {}
        streak = num(row.get("streak"))
        if streak is None:
            prev_streak = num(row.get("prev_streak"))
            streak = (prev_streak + 1) if prev_streak is not None else 0.0
        item = {
            "symbol": symbol,
            "name": feature.get("name") or row.get("name"),
            "streak": streak,
            "sector_code": code,
            "sector_name": name,
            "sector_level": level,
            "primary_sector": row.get("primary_sector"),
            "limit_reason": row.get("limit_reason"),
            "l3_code": member.get("l3_code"),
            "l3_name": member.get("l3_name"),
            "price_0925": feature.get("price_0925"),
            "prev_close": feature.get("prev_close"),
            "limit_price": limit_price(feature.get("prev_close"), symbol, str(feature.get("name") or row.get("name") or "")),
            "gap_pct": feature.get("gap_pct"),
            "auction_volume": feature.get("auction_volume"),
            "auction_amount": feature.get("auction_amount"),
        }
        grouped.setdefault(code, []).append(item)

    events: list[dict[str, Any]] = []
    for code, items in grouped.items():
        items.sort(key=lambda x: (-(x.get("streak") or 0), x.get("symbol") or ""))
        leader = items[0]
        event = {
            "trade_date": trade_date,
            "event_type": "leader",
            "sector_code": code,
            "sector_name": leader.get("sector_name"),
            "sector_level": leader.get("sector_level"),
            "leader_symbol": leader["symbol"],
            "leader_name": leader.get("name"),
            "leader_streak": leader.get("streak"),
            "leader_l3_code": leader.get("l3_code"),
            "leader_l3_name": leader.get("l3_name"),
            "leader_primary_sector": leader.get("primary_sector"),
            "leader_limit_reason": leader.get("limit_reason"),
            "leader_price_0925": leader.get("price_0925"),
            "leader_limit_price": leader.get("limit_price"),
            "leader_gap_pct": leader.get("gap_pct"),
            "leader_auction_volume": leader.get("auction_volume"),
            "leader_auction_amount": leader.get("auction_amount"),
            "tracked": leader_has_tracked(
                tracked_members,
                tracked_symbols or set(),
                code,
                str(leader.get("sector_level") or "L2"),
            ),
            "tracked_symbols": tracked_in_sector(
                tracked_members,
                tracked_symbols or set(),
                code,
                str(leader.get("sector_level") or "L2"),
            ),
            "feature_version": FEATURE_VERSION,
        }
        event.update(_previous_ladder_context(
            db, trade_date,
            [leader.get("primary_sector"), leader.get("limit_reason")],
        ))
        _write_event(
            db, event,
            key={"trade_date": trade_date, "event_type": "leader", "sector_code": code},
            dry_run=dry_run,
        )
        events.append(event)
    return events


def _trigger_events(
    db: Any,
    settings: Any,
    config: Any,
    rpc: BigQmtFileRpcClient,
    trade_date: str,
    dry_run: bool,
    notify: bool,
) -> list[dict[str, Any]]:
    leaders = list(db[EVENTS].find(
        {"trade_date": trade_date, "event_type": "leader"}, {"_id": 0}
    ))
    if not leaders:
        logger.info("no leader events for %s; trigger skipped", trade_date)
        return []
    features = _load_features(db, trade_date)
    names = _load_names(db, features.keys())
    for symbol, feature in features.items():
        if not feature.get("name"):
            feature["name"] = names.get(symbol)
    candidates = _candidate_symbols(db, settings, config, trade_date)
    members = _load_members(db, set(features) | candidates)
    leader_by_sector = {str(e.get("sector_code")): e for e in leaders}

    # Auction-stage filters first; only these symbols need the 09:31 bar.
    pending: list[tuple[dict[str, Any], dict[str, Any]]] = []
    for symbol in sorted(candidates):
        feature = features.get(symbol)
        if not feature:
            continue
        if auction_sealed(feature):
            continue
        member = members.get(symbol) or {}
        code, _name, level = sector_key_l2(member)
        leader = leader_by_sector.get(code)
        if not leader:
            # Fallback for a leader event that was grouped by L3.
            l3_code = member.get("l3_code")
            if l3_code and str(l3_code) in leader_by_sector:
                code, level = str(l3_code), "L3"
                leader = leader_by_sector[code]
        if not leader:
            continue
        if leader.get("leader_symbol") == symbol:
            continue
        gap = num(feature.get("gap_pct"))
        p920 = num(feature.get("price_0920"))
        p925 = num(feature.get("price_0925"))
        pre_lift = (
            (p925 / p920 - 1.0) * 100.0
            if p920 is not None and p925 is not None and p920 > 0
            else None
        )
        pre_reclaim = num(feature.get("reclaim_from_low_pct"))
        oi_values = [
            v for v in (num(feature.get("oi5_0920_0925")), num(feature.get("oi5_match_0925")))
            if v is not None
        ]
        pre_oi5 = max(oi_values) if oi_values else None
        auction_ok = bool(
            gap is not None and gap < 0
            and pre_lift is not None and pre_lift >= MIN_LIFT_PCT
            and pre_reclaim is not None and pre_reclaim >= MIN_RECLAIM_PCT
            and pre_oi5 is not None and pre_oi5 >= MIN_OI5
        )
        # Record every candidate in a leader's sector, triggered or not.  This
        # is the pre-registered control group for the daily evaluation.
        candidate_event = {
            "trade_date": trade_date,
            "event_type": "candidate",
            "symbol": symbol,
            "name": feature.get("name"),
            "sector_code": leader.get("sector_code"),
            "sector_name": leader.get("sector_name"),
            "sector_level": leader.get("sector_level"),
            "laggard_l3_code": member.get("l3_code"),
            "laggard_l3_name": member.get("l3_name"),
            "leader_symbol": leader.get("leader_symbol"),
            "leader_name": leader.get("leader_name"),
            "leader_streak": leader.get("leader_streak"),
            "gap_pct": gap,
            "price_0920": p920,
            "price_0925": p925,
            "lift_pct": pre_lift,
            "reclaim_from_low_pct": pre_reclaim,
            "oi5": pre_oi5,
            "auction_volume": num(feature.get("auction_volume")),
            "auction_ok": auction_ok,
            "triggered": False,
            "feature_version": FEATURE_VERSION,
        }
        _write_event(
            db, candidate_event,
            key={"trade_date": trade_date, "event_type": "candidate", "symbol": symbol},
            dry_run=dry_run,
        )
        if not auction_ok:
            continue
        pending.append((feature, leader))

    if not pending:
        logger.info("trigger: no auction-stage candidates")
        return []
    bars = _handle_one_minute(rpc, [f["symbol"] for f, _ in pending], trade_date)
    events: list[dict[str, Any]] = []
    now_text = datetime.now().strftime("%Y%m%d%H%M")
    for feature, leader in pending:
        symbol = str(feature.get("symbol"))
        bar = bars.get(symbol)
        if not bar:
            logger.warning("trigger: no 09:31 bar for %s", symbol)
            continue
        passed, metrics = evaluate_laggard(feature, bar)
        _write_event(
            db,
            {
                "trade_date": trade_date,
                "event_type": "candidate",
                "symbol": symbol,
                "triggered": passed,
                **metrics,
            },
            key={"trade_date": trade_date, "event_type": "candidate", "symbol": symbol},
            dry_run=dry_run,
        )
        if not passed:
            continue
        strength = _load_latest_sector_strength(
            db.client[REALTIME_DB], str(leader.get("sector_code")),
            str(leader.get("sector_level") or "L2"), now_text,
        )
        member = members.get(symbol) or {}
        l3_strength = _load_latest_sector_strength(
            db.client[REALTIME_DB], str(member.get("l3_code") or ""), "L3", now_text,
        ) if member.get("l3_code") else None
        event = {
            "trade_date": trade_date,
            "event_type": "trigger",
            "symbol": symbol,
            "name": feature.get("name"),
            "sector_code": leader.get("sector_code"),
            "sector_name": leader.get("sector_name"),
            "sector_level": leader.get("sector_level"),
            "laggard_l3_code": member.get("l3_code"),
            "laggard_l3_name": member.get("l3_name"),
            "leader_symbol": leader.get("leader_symbol"),
            "leader_name": leader.get("leader_name"),
            "leader_streak": leader.get("leader_streak"),
            "leader_l3_code": leader.get("leader_l3_code"),
            "leader_l3_name": leader.get("leader_l3_name"),
            "leader_primary_sector": leader.get("leader_primary_sector"),
            "leader_limit_reason": leader.get("leader_limit_reason"),
            "leader_price_0925": leader.get("leader_price_0925"),
            "leader_limit_price": leader.get("leader_limit_price"),
            "sector_above_ma5_ratio": (strength or {}).get("above_ma5_ratio"),
            "sector_median_pct_chg": (strength or {}).get("median_pct_chg"),
            "laggard_l3_above_ma5_ratio": (l3_strength or {}).get("above_ma5_ratio"),
            "laggard_l3_median_pct_chg": (l3_strength or {}).get("median_pct_chg"),
            "feature_version": FEATURE_VERSION,
            **metrics,
        }
        for key in ("ladder_prev_date", "ladder_prev_indicators",
                    "ladder_prev_sector_flow", "ladder_prev_sector_ranking"):
            event[key] = leader.get(key)
        event.update(_today_ladder_context(
            db, trade_date,
            [leader.get("leader_primary_sector"), leader.get("leader_limit_reason")],
        ))
        _write_event(
            db, event,
            key={"trade_date": trade_date, "event_type": "trigger", "symbol": symbol},
            dry_run=dry_run,
        )
        events.append(event)
        if notify and not dry_run:
            title = "板块后排弱开反包 %s %s" % (leader.get("sector_name") or "", symbol)
            content = (
                "%s %s | 龙头 %s(%s板) 09:25竞价封板 | "
                "gap %.2f%% lift %.2f%% reclaim %.2f%% OI5 %.2f | "
                "09:31 收 %.2f (高 %.2f, 量 %.0f) | L3 中位 %s%% 广度 %s"
                % (
                    symbol, feature.get("name") or "",
                    leader.get("leader_symbol"), leader.get("leader_streak"),
                    metrics["gap_pct"] or 0.0, metrics["lift_pct"] or 0.0,
                    metrics["reclaim_from_low_pct"] or 0.0, metrics["oi5"] or 0.0,
                    metrics["first_minute_close"] or 0.0, metrics["first_minute_high"] or 0.0,
                    metrics["first_minute_volume"] or 0.0,
                    (strength or {}).get("median_pct_chg"),
                    (strength or {}).get("above_ma5_ratio"),
                )
            )
            _send_dingtalk(title, content)
    return events


def _track_events(db: Any, trade_date: str, dry_run: bool, notify: bool) -> list[dict[str, Any]]:
    triggers = list(db[EVENTS].find(
        {"trade_date": trade_date, "event_type": "trigger"}, {"_id": 0}
    ))
    if not triggers:
        logger.info("track: no trigger events for %s", trade_date)
        return []
    now_text = datetime.now().strftime("%Y%m%d%H%M")
    events: list[dict[str, Any]] = []
    for trigger in triggers:
        symbol = str(trigger.get("symbol"))
        limit_row = _limit_row(db, trade_date, symbol)
        price_row = _price_row(db, trade_date, symbol)
        metrics = seal_metrics(limit_row, price_row)
        strength = _load_latest_sector_strength(
            db.client[REALTIME_DB], str(trigger.get("sector_code")),
            str(trigger.get("sector_level") or "L2"), now_text,
        )
        l3_strength = _load_latest_sector_strength(
            db.client[REALTIME_DB], str(trigger.get("laggard_l3_code") or ""), "L3", now_text,
        ) if trigger.get("laggard_l3_code") else None
        previous = db[EVENTS].find_one(
            {"trade_date": trade_date, "event_type": {"$in": ["sealed", "broken", "resealed", "not_sealed"]},
             "symbol": symbol},
            {"_id": 0, "state": 1},
            sort=[("updated_at", -1)],
        )
        previous_state = (previous or {}).get("state")
        state = metrics["state"]
        event_type = state if state in ("sealed", "broken", "not_sealed") else "unknown"
        if state == "sealed" and previous_state == "broken":
            event_type = "resealed"
        event = {
            "trade_date": trade_date,
            "event_type": event_type,
            "symbol": symbol,
            "name": trigger.get("name"),
            "state": state,
            "previous_state": previous_state,
            "sector_code": trigger.get("sector_code"),
            "sector_name": trigger.get("sector_name"),
            "sector_level": trigger.get("sector_level"),
            "leader_symbol": trigger.get("leader_symbol"),
            "leader_name": trigger.get("leader_name"),
            "leader_primary_sector": trigger.get("leader_primary_sector"),
            "leader_limit_reason": trigger.get("leader_limit_reason"),
            "sector_above_ma5_ratio": (strength or {}).get("above_ma5_ratio"),
            "sector_median_pct_chg": (strength or {}).get("median_pct_chg"),
            "laggard_l3_code": trigger.get("laggard_l3_code"),
            "laggard_l3_above_ma5_ratio": (l3_strength or {}).get("above_ma5_ratio"),
            "laggard_l3_median_pct_chg": (l3_strength or {}).get("median_pct_chg"),
            "feature_version": FEATURE_VERSION,
            **metrics,
        }
        for key in ("ladder_prev_date", "ladder_prev_indicators",
                    "ladder_prev_sector_flow", "ladder_prev_sector_ranking"):
            event[key] = trigger.get(key)
        event.update(_today_ladder_context(
            db, trade_date,
            [trigger.get("leader_primary_sector"), trigger.get("leader_limit_reason")],
        ))
        _write_event(
            db, event,
            key={"trade_date": trade_date, "event_type": event_type, "symbol": symbol},
            dry_run=dry_run,
        )
        events.append(event)
        if notify and not dry_run and state != previous_state and event_type in ("sealed", "broken", "resealed"):
            title = "板块后排 %s %s" % (event_type, symbol)
            content = (
                "%s %s | 龙头 %s | 状态 %s -> %s | 首封 %s 换手 %s%% 封单/成交 %s%% | L3 中位 %s%%"
                % (
                    symbol, trigger.get("name") or "", trigger.get("leader_symbol"),
                    previous_state or "triggered", state,
                    metrics.get("limit_up_time") or metrics.get("last_seal_time"),
                    round(metrics["turnover_ratio"] * 100.0, 2) if metrics.get("turnover_ratio") is not None else None,
                    round(metrics["seal_ratio"] * 100.0, 1) if metrics.get("seal_ratio") is not None else None,
                    (strength or {}).get("median_pct_chg"),
                )
            )
            _send_dingtalk(title, content)
    return events


def _summary_event(db: Any, trade_date: str, dry_run: bool, notify: bool) -> dict[str, Any]:
    triggers = list(db[EVENTS].find(
        {"trade_date": trade_date, "event_type": "trigger"}, {"_id": 0}
    ))
    states: dict[str, str] = {}
    for doc in db[EVENTS].find(
        {"trade_date": trade_date, "event_type": {"$in": ["sealed", "broken", "resealed", "not_sealed"]}},
        {"_id": 0, "symbol": 1, "state": 1, "updated_at": 1},
    ).sort("updated_at", 1):
        states[str(doc.get("symbol"))] = str(doc.get("state"))
    summary = {
        "trade_date": trade_date,
        "event_type": "summary",
        "triggers": len(triggers),
        "trigger_symbols": [t.get("symbol") for t in triggers],
        "states": states,
        "sealed": sum(1 for s in states.values() if s == "sealed"),
        "broken": sum(1 for s in states.values() if s == "broken"),
        "not_sealed": sum(1 for s in states.values() if s == "not_sealed"),
        "feature_version": FEATURE_VERSION,
        "updated_at": datetime.now(),
    }
    _write_event(
        db, summary,
        key={"trade_date": trade_date, "event_type": "summary"},
        dry_run=dry_run,
    )
    if notify and not dry_run and triggers:
        _send_dingtalk(
            "板块后排 watchdog 日汇总",
            "trigger %d | sealed %d broken %d not_sealed %d | %s"
            % (summary["triggers"], summary["sealed"], summary["broken"],
               summary["not_sealed"], ",".join(summary["trigger_symbols"])),
        )
    return summary


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s [sector_follower] %(levelname)s %(message)s",
    )
    if args.test_alert:
        ok = _send_dingtalk("watchdog 测试", "wrapper 通知链路测试（非实盘信号）")
        logger.info("test_alert sent=%s", ok)
        return 0 if ok else 1
    trade_date = _today(args.date)
    settings = get_settings()
    client = MongoClient(settings.mongo_uri)
    try:
        db = client[DB]
        config = load_bridge_config(args.config)
        account_id = args.account_id or str(config.quote_account_id or "")
        rpc = BigQmtFileRpcClient(mailbox=args.mailbox, account_id=account_id)
        try:
            if args.mode == "leader":
                tracked = _tracked_symbols(db, settings, config)
                events = _leader_events(
                    db, settings, config, rpc, trade_date, args.dry_run,
                    tracked_symbols=tracked,
                )
                tracked_events = [e for e in events if e.get("tracked")]
                if args.notify and not args.dry_run and tracked_events:
                    lines = [
                        "%s %s %s板 | %s | 关注 %s"
                        % (event.get("leader_symbol"), event.get("leader_name"),
                           event.get("leader_streak"), event.get("sector_name"),
                           ",".join(event.get("tracked_symbols") or []))
                        for event in sorted(
                            tracked_events, key=lambda e: -(e.get("leader_streak") or 0)
                        )
                    ]
                    _send_dingtalk(
                        "板块龙头竞价封板 %d 个（watchlist/持仓所在板块）" % len(lines),
                        "\n".join(lines),
                    )
                logger.info(
                    "leader events=%d tracked=%d pushed=%d",
                    len(events), len(tracked_events),
                    len(tracked_events) if args.notify and not args.dry_run else 0,
                )
            elif args.mode == "trigger":
                events = _trigger_events(db, settings, config, rpc, trade_date,
                                         args.dry_run, args.notify)
            elif args.mode == "track":
                events = _track_events(db, trade_date, args.dry_run, args.notify)
            else:
                events = [_summary_event(db, trade_date, args.dry_run, args.notify)]
            logger.info("mode=%s events=%d dry_run=%s", args.mode, len(events), args.dry_run)
            return 0
        finally:
            rpc.close()
    finally:
        client.close()


if __name__ == "__main__":
    raise SystemExit(main())
