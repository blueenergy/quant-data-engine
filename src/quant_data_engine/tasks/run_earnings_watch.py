#!/usr/bin/env python3
"""Watchlist / holdings earnings-report watcher.

Two modes:

  watch (default)
      Detect today's formal report disclosures for the union of the user's
      watchlist and live positions, fetch the statements when they are missing
      from ``quant_data`` (the global financial sync is pinned to one period and
      does not roll forward), then enqueue ``earnings_report_analysis`` tasks.

  digest
      Send one DingTalk digest for completed reports that have not been
      notified yet.

Usage:
    python -m quant_data_engine.tasks.run_earnings_watch --mode watch
    python -m quant_data_engine.tasks.run_earnings_watch --mode digest
    python -m quant_data_engine.tasks.run_earnings_watch --dry-run --date 20261030
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import logging
import os
import sys
import time
import urllib.parse
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Set

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

import requests
from pymongo import MongoClient

from quant_data_engine import get_settings
from quant_data_engine.adapters.stock_adapter import StockAdapter
from quant_data_engine.adapters.tushare_adapter import TushareAdapter
from quant_data_engine.pipelines.financial_pipeline import FinancialPipeline
from quant_data_engine.repositories.financial_repository import FinancialRepository
from quant_data_engine.utils.ingestion_job import record_ingestion_run

logger = logging.getLogger(__name__)

TASK_TYPE = "earnings_report_analysis"
REPORT_COLLECTION = "earnings_watch_reports"
HTTP_TIMEOUT = float(os.getenv("EARNINGS_WATCH_HTTP_TIMEOUT", "10"))

# ETF / fund / convertible-bond prefixes that never publish a financial report.
_NON_EQUITY_PREFIXES = ("15", "16", "50", "51", "56", "58")

_REPORT_KIND = {
    "0331": "一季报",
    "0630": "半年报",
    "0930": "三季报",
    "1231": "年报",
}


# ---------------------------------------------------------------------------
# Symbol helpers
# ---------------------------------------------------------------------------


def normalize_symbol(raw: Any) -> str:
    """Normalize ``600048`` / ``600048.sh`` / ``600048.SH`` to ``600048.SH``."""
    s = str(raw or "").strip().upper()
    if not s:
        return ""
    if "." in s:
        code, suffix = s.rsplit(".", 1)
        if code.isdigit() and len(code) == 6:
            return f"{code}.{suffix}"
        return s
    if len(s) == 6 and s.isdigit():
        if s[0] in ("6", "9"):
            return f"{s}.SH"
        if s[0] in ("0", "3"):
            return f"{s}.SZ"
        if s[0] in ("4", "8"):
            return f"{s}.BJ"
    return s


def is_equity(ts_code: str) -> bool:
    code = ts_code.split(".")[0]
    if len(code) != 6 or not code.isdigit():
        return False
    return not code.startswith(_NON_EQUITY_PREFIXES)


def report_kind(end_date: str) -> str:
    return _REPORT_KIND.get(str(end_date or "")[4:8], "定期报告")


# ---------------------------------------------------------------------------
# Watch scope
# ---------------------------------------------------------------------------


def load_watch_symbols(settings: Any) -> Dict[str, Set[str]]:
    """Return ``{ts_code: {"watchlist", "position"}}``.

    Position scope is the most recent completed live snapshot, and only
    positions that still hold shares (``shares > 0``).
    """
    client = MongoClient(settings.mongo_uri)
    finance = client[settings.mongo_db]
    trading = client[settings.trading_db]

    out: Dict[str, Set[str]] = {}

    def _add(raw: Any, source: str) -> None:
        code = normalize_symbol(raw)
        if code and is_equity(code):
            out.setdefault(code, set()).add(source)

    for doc in finance["user_watchlists"].find({}, {"symbols": 1}):
        for symbol in doc.get("symbols") or []:
            _add(symbol, "watchlist")

    latest = trading["portfolio_live_snapshots"].find_one(
        {"status": "completed"}, sort=[("_id", -1)]
    )
    if latest:
        for pos in latest.get("positions") or []:
            if (pos.get("shares") or 0) > 0:
                _add(pos.get("symbol") or pos.get("ts_code"), "position")

    return out


def load_stock_names(settings: Any, symbols: Iterable[str]) -> Dict[str, Dict[str, str]]:
    client = MongoClient(settings.mongo_uri)
    data_db = client[settings.data_db]
    codes = list(symbols)
    if not codes:
        return {}
    docs = data_db["stock_info"].find(
        {"ts_code": {"$in": codes}},
        {"ts_code": 1, "name": 1, "industry": 1, "sw_l1": 1},
    )
    return {
        d["ts_code"]: {
            "name": d.get("name") or "",
            "industry": d.get("industry") or "",
            "sw_l1": d.get("sw_l1") or "",
        }
        for d in docs
    }


# ---------------------------------------------------------------------------
# Disclosure detection
# ---------------------------------------------------------------------------


def fetch_disclosed_from_tushare(settings: Any, as_of: str) -> Dict[str, str]:
    """Today's formal-report disclosures keyed by ``ts_code`` -> ``end_date``."""
    adapter = TushareAdapter(
        token=settings.tushare_token,
        rate_limit_per_sec=settings.rate_limit_per_sec,
    )
    if not adapter.enabled:
        return {}
    try:
        df = adapter.fetch_disclosure_date(actual_date=as_of)
    except Exception as exc:
        logger.warning("disclosure_date(actual_date=%s) failed: %s", as_of, exc)
        return {}
    if df is None or df.empty:
        return {}

    out: Dict[str, str] = {}
    for row in df.to_dict("records"):
        code = normalize_symbol(row.get("ts_code"))
        end_date = str(row.get("end_date") or "").strip()
        if code and len(end_date) == 8:
            out[code] = end_date
    return out


def fetch_disclosed_from_db(settings: Any, as_of: str) -> Dict[str, str]:
    client = MongoClient(settings.mongo_uri)
    data_db = client[settings.data_db]
    out: Dict[str, str] = {}
    for doc in data_db["earnings_disclosure"].find({"actual_date": as_of}):
        code = normalize_symbol(doc.get("ts_code"))
        end_date = str(doc.get("end_date") or "").strip()
        if code and len(end_date) == 8:
            out[code] = end_date
    return out


def detect_disclosures(settings: Any, as_of: str) -> Dict[str, str]:
    """Merge Tushare's authoritative list with the local disclosure table.

    Tushare is primary when reachable; the local table covers runs where the
    token is missing or the API call fails.
    """
    from_api = fetch_disclosed_from_tushare(settings, as_of)
    from_db = fetch_disclosed_from_db(settings, as_of)
    merged = dict(from_db)
    merged.update(from_api)
    if from_api:
        logger.info(
            "disclosure source: tushare=%s db=%s merged=%s",
            len(from_api), len(from_db), len(merged),
        )
    return merged


# ---------------------------------------------------------------------------
# Statement fetch
# ---------------------------------------------------------------------------


def statement_ready(data_db: Any, ts_code: str, end_date: str) -> bool:
    return (
        data_db["financial_income"].find_one(
            {"ts_code": ts_code, "end_date": end_date, "report_type": 1},
            {"_id": 1},
        )
        is not None
    )


def fetch_statement(settings: Any, data_db: Any, ts_code: str, end_date: str) -> bool:
    """Fetch all four statement tables for one stock/period. Returns True when ready."""
    adapter = StockAdapter(
        token=settings.tushare_token,
        rate_limit_per_sec=settings.rate_limit_per_sec,
    )
    repo = FinancialRepository(settings.mongo_uri, settings.data_db)
    pipeline = FinancialPipeline(adapter=adapter, repository=repo)
    try:
        result = pipeline.run(
            [ts_code],
            period=end_date,
            data_types=["income", "balance", "cashflow", "financial_indicator"],
        )
        logger.info("statement fetch %s %s: %s", ts_code, end_date, result)
    except Exception as exc:
        logger.error("statement fetch failed %s %s: %s", ts_code, end_date, exc)
        return False
    return statement_ready(data_db, ts_code, end_date)


# ---------------------------------------------------------------------------
# Task enqueue
# ---------------------------------------------------------------------------


def idempotency_key(ts_code: str, end_date: str) -> str:
    return f"earnings_watch:{ts_code}:{end_date}"


def enqueue_task(
    finance_db: Any,
    *,
    ts_code: str,
    end_date: str,
    ann_date: str,
    stock: Dict[str, str],
    sources: Set[str],
    force: bool = False,
) -> Optional[str]:
    """Insert a pending analysis task; returns task id or None when skipped."""
    key = idempotency_key(ts_code, end_date)
    if not force:
        existing = finance_db["analysis_tasks"].find_one(
            {
                "idempotency_key": key,
                "status": {"$in": ["pending", "processing", "completed"]},
            },
            {"_id": 1},
        )
        if existing:
            logger.info("skip existing task %s (%s)", ts_code, existing.get("_id"))
            return None

    now = datetime.now()
    doc = {
        "type": TASK_TYPE,
        "task_type": TASK_TYPE,
        "status": "pending",
        "priority": 10,
        "user_id": "system",
        "created_by_user_id": "system",
        "source": "earnings_watch",
        "idempotency_key": key,
        "symbol": ts_code,
        "stock_name": stock.get("name") or "",
        "params": {
            "ts_code": ts_code,
            "end_date": end_date,
            "ann_date": ann_date,
            "report_kind": report_kind(end_date),
            "stock_name": stock.get("name") or "",
            "industry": stock.get("industry") or "",
            "sw_l1": stock.get("sw_l1") or "",
            "sources": sorted(sources),
            "source": "earnings_watch",
        },
        "created_at": now,
        "updated_at": now,
    }
    result = finance_db["analysis_tasks"].insert_one(doc)
    logger.info("enqueued %s %s %s -> %s", ts_code, end_date, report_kind(end_date), result.inserted_id)
    return str(result.inserted_id)


# ---------------------------------------------------------------------------
# Watch mode
# ---------------------------------------------------------------------------


def run_watch(
    settings: Any,
    *,
    as_of: str,
    symbols: Optional[List[str]] = None,
    dry_run: bool = False,
    force: bool = False,
    no_fetch: bool = False,
) -> Dict[str, Any]:
    client = MongoClient(settings.mongo_uri)
    data_db = client[settings.data_db]
    finance_db = client[settings.mongo_db]

    scope = load_watch_symbols(settings)
    if symbols:
        wanted = {normalize_symbol(s) for s in symbols if normalize_symbol(s)}
        scope = {k: v for k, v in scope.items() if k in wanted}
        for code in wanted - set(scope):
            scope[code] = {"manual"}
    if not scope:
        logger.info("watch scope is empty")
        return {"scope": 0, "events": 0, "fetched": 0, "enqueued": 0, "skipped": 0}

    names = load_stock_names(settings, scope)
    disclosed = detect_disclosures(settings, as_of)
    events = {code: end_date for code, end_date in disclosed.items() if code in scope}

    logger.info(
        "watch: scope=%s disclosed=%s events=%s date=%s",
        len(scope), len(disclosed), len(events), as_of,
    )

    stats = {"scope": len(scope), "events": len(events), "fetched": 0, "enqueued": 0, "skipped": 0}
    for code, end_date in sorted(events.items()):
        stock = names.get(code, {})
        if not statement_ready(data_db, code, end_date):
            if no_fetch or dry_run:
                logger.info("[dry-run] would fetch %s %s", code, end_date)
                stats["skipped"] += 1
                continue
            if not fetch_statement(settings, data_db, code, end_date):
                stats["skipped"] += 1
                continue
            stats["fetched"] += 1

        if dry_run:
            logger.info("[dry-run] would enqueue %s %s %s", code, end_date, report_kind(end_date))
            stats["enqueued"] += 1
            continue

        task_id = enqueue_task(
            finance_db,
            ts_code=code,
            end_date=end_date,
            ann_date=as_of,
            stock=stock,
            sources=scope[code],
            force=force,
        )
        if task_id:
            stats["enqueued"] += 1
        else:
            stats["skipped"] += 1

    return stats


# ---------------------------------------------------------------------------
# Digest mode
# ---------------------------------------------------------------------------


def _dingtalk_signed_url(webhook: str, secret: str) -> str:
    if not secret:
        return webhook
    timestamp = str(round(time.time() * 1000))
    digest = hmac.new(
        secret.encode("utf-8"), f"{timestamp}\n{secret}".encode("utf-8"), hashlib.sha256
    ).digest()
    sign = urllib.parse.quote_plus(base64.b64encode(digest))
    return f"{webhook}&timestamp={timestamp}&sign={sign}"


def send_dingtalk(title: str, markdown: str) -> str:
    webhook = (
        os.getenv("EARNINGS_WATCH_WEBHOOK_URL", "").strip()
        or os.getenv("BRIDGE_HEARTBEAT_WEBHOOK_URL", "").strip()
    )
    secret = (
        os.getenv("EARNINGS_WATCH_WEBHOOK_SECRET", "").strip()
        or os.getenv("BRIDGE_HEARTBEAT_WEBHOOK_SECRET", "").strip()
    )
    if not webhook:
        return "disabled"
    payload = {"msgtype": "markdown", "markdown": {"title": title, "text": markdown}}
    try:
        resp = requests.post(_dingtalk_signed_url(webhook, secret), json=payload, timeout=HTTP_TIMEOUT)
        body = resp.json()
        errcode = body.get("errcode")
        if errcode:
            return f"error:{errcode} {body.get('errmsg', '')}".strip()
        return "sent"
    except Exception as exc:  # pragma: no cover - network dependent
        return f"error:{exc}"


def _fmt_amount(value: Any) -> str:
    try:
        v = float(value)
    except (TypeError, ValueError):
        return "—"
    if abs(v) >= 1e8:
        return f"{v / 1e8:.2f}亿"
    if abs(v) >= 1e4:
        return f"{v / 1e4:.1f}万"
    return f"{v:.0f}"


def _fmt_pct(value: Any) -> str:
    try:
        return f"{float(value):+.1f}%"
    except (TypeError, ValueError):
        return "—"


def build_digest(reports: List[Dict[str, Any]], as_of: str) -> str:
    lines = [f"## 财报雷达 {as_of}", ""]
    for r in reports:
        num = r.get("key_numbers") or {}
        head = (
            f"### {r.get('name') or r.get('ts_code')} "
            f"({r.get('ts_code')}) {r.get('report_kind') or ''}"
        )
        lines.append(head)
        lines.append(
            f"- 结论：**{r.get('beat_miss') or '—'}** ｜ 评分 {r.get('score', '—')}/10"
        )
        lines.append(
            f"- 营收 {_fmt_amount(num.get('revenue'))} ({_fmt_pct(num.get('revenue_yoy'))})"
            f" ｜ 归母 {_fmt_amount(num.get('net_profit'))} ({_fmt_pct(num.get('net_profit_yoy'))})"
            f" ｜ 扣非 {_fmt_pct(num.get('deduct_net_profit_yoy'))}"
        )
        if r.get("verdict"):
            lines.append(f"- {r['verdict']}")
        consensus = r.get("consensus") or {}
        progress = consensus.get("ytd_vs_annual_consensus_pct")
        delta = consensus.get("progress_delta_pp")
        if progress is not None:
            extra = ""
            if delta is not None:
                extra = f"，去年同期 {progress - delta:.2f}%（{delta:+.2f}pp）"
            lines.append(f"- 年度预期进度 {progress:.1f}%{extra}")
        risk = r.get("risk_flags") or []
        if risk:
            lines.append(f"- 风险：{'；'.join(str(x) for x in risk[:2])}")
        lines.append("")
    return "\n".join(lines).strip()


def run_digest(
    settings: Any,
    *,
    as_of: str,
    limit: int = 20,
    dry_run: bool = False,
) -> Dict[str, Any]:
    client = MongoClient(settings.mongo_uri)
    finance_db = client[settings.mongo_db]
    cutoff = (datetime.now() - timedelta(days=3)).strftime("%Y%m%d")

    reports = list(
        finance_db[REPORT_COLLECTION]
        .find({"notified": {"$ne": True}, "trade_date": {"$gte": cutoff}})
        .sort("created_at", 1)
        .limit(limit)
    )
    if not reports:
        logger.info("digest: nothing to send")
        return {"reports": 0, "status": "empty"}

    text = build_digest(reports, as_of)
    if dry_run:
        print(text)
        return {"reports": len(reports), "status": "dry-run"}

    status = send_dingtalk(f"财报雷达 {as_of}", text)
    logger.info("digest: reports=%s status=%s", len(reports), status)
    if status == "sent":
        finance_db[REPORT_COLLECTION].update_many(
            {"_id": {"$in": [r["_id"] for r in reports]}},
            {"$set": {"notified": True, "notified_at": datetime.now()}},
        )
    return {"reports": len(reports), "status": status}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Watchlist/holdings earnings report watcher")
    p.add_argument("--mode", choices=["watch", "digest"], default="watch")
    p.add_argument("--date", help="Disclosure date YYYYMMDD (default: today)")
    p.add_argument("--symbols", nargs="+", help="Override watch scope (manual run)")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--force", action="store_true", help="Re-enqueue even when a task exists")
    p.add_argument("--no-fetch", action="store_true", help="Never fetch statements")
    p.add_argument("--digest-limit", type=int, default=20)
    return p.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    settings = get_settings()
    as_of = args.date or datetime.now().strftime("%Y%m%d")

    try:
        if args.mode == "digest":
            result = run_digest(settings, as_of=as_of, limit=args.digest_limit, dry_run=args.dry_run)
        else:
            result = run_watch(
                settings,
                as_of=as_of,
                symbols=args.symbols,
                dry_run=args.dry_run,
                force=args.force,
                no_fetch=args.no_fetch,
            )
    except Exception as exc:
        record_ingestion_run(settings, "earnings_watch", as_of, status="failed", error=str(exc))
        raise

    logger.info("earnings watch done: %s", result)
    if not args.dry_run:
        record_ingestion_run(
            settings,
            "earnings_watch",
            as_of,
            records_changed=int(result.get("enqueued") or result.get("reports") or 0),
        )


if __name__ == "__main__":
    main()
