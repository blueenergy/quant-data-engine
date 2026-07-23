"""Normalize raw adapter rows into Mongo-ready market event documents."""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from quant_data_engine.market_events.filters import MarketEventsFilterConfig
from quant_data_engine.market_events.identity import (
    build_identity_key,
    event_date_utc8,
    extract_ref_period,
    normalize_region,
    normalize_symbol,
    resolve_metric_code,
)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def infer_macro_category(title: str) -> str:
    t = title or ""
    if any(k in t for k in ("财报", "业绩", "earnings", "Q1", "Q2", "Q3", "Q4")):
        return "earnings"
    if any(k in t for k in ("央行", "联储", "FOMC", "ECB", "BOJ", "利率", "Powell", "鲍威尔")):
        return "central_bank"
    if any(k in t for k in ("财政", "减税", "专项债")):
        return "fiscal"
    if any(k in t for k in ("证监会", "监管", "SEC")):
        return "regulator"
    return "macro_data"


def score_unlock_importance(mktcap: Optional[float], cfg: MarketEventsFilterConfig) -> int:
    if mktcap is None:
        return 3
    if mktcap >= 10_000_000_000:
        return 4
    if mktcap >= cfg.unlock_min_mktcap_yuan:
        return 3
    return 2


def score_report_importance(symbol: str, cfg: MarketEventsFilterConfig) -> int:
    return 4 if cfg.symbol_allowed(symbol) else 3


def maybe_boost_us_earnings(title: str, importance: int, cfg: MarketEventsFilterConfig) -> int:
    upper = (title or "").upper()
    for sym in cfg.us_earnings_boost_symbols:
        if sym in upper:
            return max(importance, 4)
    return importance


def build_document(raw: Dict[str, Any], cfg: MarketEventsFilterConfig) -> Optional[Dict[str, Any]]:
    title = str(raw.get("title") or "").strip()
    if not title:
        return None
    event_time: datetime = raw["event_time"]
    region = normalize_region(str(raw.get("region") or "GLOBAL"))
    category = str(raw.get("category") or infer_macro_category(title))
    event_subtype = raw.get("event_subtype")
    symbol = normalize_symbol(raw.get("symbol") or "") or None
    symbol_name = raw.get("symbol_name")
    metric_code = raw.get("metric_code") or resolve_metric_code(title)
    ref_period = raw.get("ref_period") or extract_ref_period(title, event_time)
    event_date = event_date_utc8(event_time)

    kind = str(raw.get("identity_kind") or "macro")
    seed_id = raw.get("seed_id")
    identity_key = build_identity_key(
        kind=kind,
        region=region,
        metric_code=metric_code,
        ref_period=ref_period,
        symbol=symbol,
        event_subtype=event_subtype,
        event_date=event_date,
        title=title,
        seed_id=seed_id,
    )

    importance = raw.get("importance")
    if importance is None:
        importance = 3
    importance = int(importance)
    if category == "earnings" and kind == "macro":
        importance = maybe_boost_us_earnings(title, importance, cfg)
    if importance < cfg.min_importance:
        return None

    source_name = str(raw.get("source_name") or "unknown")
    source_entry = {
        "name": source_name,
        "url": raw.get("url") or "",
        "raw_title": title,
        "seen_at": _utc_now(),
    }
    metrics = dict(raw.get("metrics") or {})
    now = _utc_now()
    return {
        "identity_key": identity_key,
        "title": title,
        "sources": [source_entry],
        "event_time": event_time,
        "region": region,
        "category": category,
        "event_subtype": event_subtype,
        "symbol": symbol,
        "symbol_name": symbol_name,
        "metric_code": metric_code,
        "ref_period": ref_period,
        "importance": importance,
        "status": str(raw.get("status") or "scheduled"),
        "metrics": metrics,
        "impact_tags": list(raw.get("impact_tags") or []),
        "llm_brief": None,
        "related_news": [],
        "fetched_at": now,
        "updated_at": now,
    }


def merge_metrics(
    existing: Optional[Dict[str, Any]],
    incoming: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """Field-wise merge: prefer non-null incoming, else keep existing."""
    out = dict(existing or {})
    for key, value in (incoming or {}).items():
        if value is None or value == "":
            continue
        out[key] = value
    return out


def merge_documents(docs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Merge in-memory by identity_key (combine sources[], keep latest mutable fields)."""
    # Prefer wallstreetcn over baidu when both present for scalar fields.
    source_rank = {"wallstreetcn": 0, "baidu": 1, "cninfo_disclosure": 0, "em_restricted": 0, "em_gsrl": 0, "seed": 0}
    merged: Dict[str, Dict[str, Any]] = {}
    for doc in docs:
        key = doc["identity_key"]
        if key not in merged:
            merged[key] = deepcopy(doc)
            continue
        existing = merged[key]
        seen_names = {s.get("name") for s in existing.get("sources") or []}
        for src in doc.get("sources") or []:
            if src.get("name") not in seen_names:
                existing.setdefault("sources", []).append(src)
                seen_names.add(src.get("name"))

        existing_rank = min(
            (source_rank.get(s.get("name"), 9) for s in existing.get("sources") or []),
            default=9,
        )
        incoming_rank = min(
            (source_rank.get(s.get("name"), 9) for s in doc.get("sources") or []),
            default=9,
        )
        prefer_incoming = incoming_rank <= existing_rank

        for field in ("event_time", "title", "importance", "status", "metric_code", "ref_period"):
            if doc.get(field) is None:
                continue
            if prefer_incoming or existing.get(field) is None:
                existing[field] = doc[field]
        existing["metrics"] = merge_metrics(existing.get("metrics"), doc.get("metrics"))
        existing["updated_at"] = _utc_now()
    return list(merged.values())


def apply_status_rules(doc: Dict[str, Any], now: Optional[datetime] = None) -> Dict[str, Any]:
    now = now or _utc_now()
    et: datetime = doc["event_time"]
    if et.tzinfo is None:
        et = et.replace(tzinfo=timezone.utc)
    metrics = doc.get("metrics") or {}
    actual = metrics.get("actual")
    if doc.get("status") == "cancelled":
        return doc
    if et < now and actual not in (None, ""):
        doc["status"] = "happened"
    elif et < now and doc.get("category") in {"corporate", "earnings", "conference"}:
        doc["status"] = "happened"
    else:
        doc.setdefault("status", "scheduled")
    return doc
