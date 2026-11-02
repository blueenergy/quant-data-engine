#!/usr/bin/env python3
"""Sync full-market top10_floatholders / top10_holders for one report period.

The regular ``institutional_deep`` task only walks a small stale-stock batch and
also pulls share-float / block-trade / daily data. This task is the lightweight
full-market holder backfill used by the institutional-holdings board.

Usage:
  python -m quant_data_engine.tasks.run_holder_snapshot_sync --period 20260930
  python -m quant_data_engine.tasks.run_holder_snapshot_sync --period 20260930 --kind both
  python -m quant_data_engine.tasks.run_holder_snapshot_sync --period 20260930 --limit 300 --offset 0
  python -m quant_data_engine.tasks.run_holder_snapshot_sync --period 20260930 --skip-existing
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime
from typing import Any, Dict, List, Sequence

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from pymongo import MongoClient

from quant_data_engine import get_settings
from quant_data_engine.adapters.stock_adapter import StockAdapter
from quant_data_engine.repositories.institutional_repository import InstitutionalRepository
from quant_data_engine.utils.ingestion_job import record_ingestion_run

logger = logging.getLogger(__name__)

JOB = "holder_snapshot_sync"
KIND_CHOICES = ("top10_floatholders", "top10_holders", "both")


def _normalize_ts_code(value: Any) -> str:
    s = str(value or "").strip().upper()
    if not s:
        return ""
    if "." in s:
        left, right = s.rsplit(".", 1)
        return f"{left.zfill(6)}.{right}"
    digits = "".join(ch for ch in s if ch.isdigit())
    if len(digits) != 6:
        return s
    if digits.startswith("6"):
        return f"{digits}.SH"
    if digits.startswith(("0", "3")):
        return f"{digits}.SZ"
    if digits.startswith(("4", "8", "9")):
        return f"{digits}.BJ"
    return digits


def _is_listed_a_share(doc: Dict[str, Any]) -> bool:
    code = _normalize_ts_code(doc.get("ts_code") or doc.get("symbol") or doc.get("code"))
    if not code or "." not in code:
        return False
    if not code.endswith((".SH", ".SZ", ".BJ")):
        return False
    status = str(doc.get("list_status") or doc.get("status") or "").upper()
    if status and status not in {"L", "LISTED", "上市"}:
        return False
    return True


def _listed_codes(
    data_db: Any,
    *,
    start_after: str = "",
    offset: int = 0,
    limit: int = 0,
) -> List[str]:
    docs = data_db["stock_info"].find(
        {},
        {"_id": 0, "ts_code": 1, "symbol": 1, "code": 1, "list_status": 1, "status": 1},
        sort=[("ts_code", 1), ("symbol", 1)],
    )
    start = _normalize_ts_code(start_after)
    codes: List[str] = []
    seen = set()
    for doc in docs:
        if not _is_listed_a_share(doc):
            continue
        code = _normalize_ts_code(doc.get("ts_code") or doc.get("symbol") or doc.get("code"))
        if start and code <= start:
            continue
        if code in seen:
            continue
        seen.add(code)
        codes.append(code)
    if offset and offset > 0:
        codes = codes[offset:]
    if limit and limit > 0:
        codes = codes[:limit]
    return codes


def _existing_codes(coll: Any, period: str, kind: str) -> set[str]:
    try:
        return {
            str(value)
            for value in coll.distinct("ts_code", {"end_date": period, "kind": kind})
            if value
        }
    except Exception:
        return set()


def run_sync(
    *,
    adapter: StockAdapter,
    repo: InstitutionalRepository,
    codes: Sequence[str],
    period: str,
    kind: str = "top10_floatholders",
    skip_existing: bool = False,
    max_errors: int = 100,
    progress_every: int = 100,
) -> Dict[str, Any]:
    """Fetch and upsert one period for a list of A-share codes."""
    existing_float = (
        _existing_codes(repo.floatholders, period, "top10_floatholders")
        if skip_existing
        else set()
    )
    existing_holders = (
        _existing_codes(repo.holders, period, "top10_holders")
        if skip_existing
        else set()
    )
    stats: Dict[str, Any] = {
        "codes": len(codes),
        "stocks_ok": 0,
        "stocks_empty": 0,
        "stocks_skipped": 0,
        "rows": 0,
        "upserts": 0,
        "errors": [],
    }

    for index, code in enumerate(codes, 1):
        try:
            did_work = False
            if kind in ("top10_floatholders", "both"):
                if not (skip_existing and code in existing_float):
                    df = adapter.fetch_top10_floatholders(ts_code=code, period=period)
                    rows = 0 if df is None else len(df)
                    stats["rows"] += rows
                    if rows:
                        stats["upserts"] += repo.upsert_top10_floatholders(code, period, df)
                        did_work = True
                    else:
                        stats["stocks_empty"] += 1
            if kind in ("top10_holders", "both"):
                if not (skip_existing and code in existing_holders):
                    df = adapter.fetch_top10_holders(ts_code=code, period=period)
                    rows = 0 if df is None else len(df)
                    stats["rows"] += rows
                    if rows:
                        stats["upserts"] += repo.upsert_top10_holders(code, period, df)
                        did_work = True
                    else:
                        stats["stocks_empty"] += 1
            if did_work:
                stats["stocks_ok"] += 1
            elif skip_existing and (
                code in existing_float or code in existing_holders
            ):
                stats["stocks_skipped"] += 1
        except Exception as exc:  # keep going; a single stock must not kill the batch
            stats["errors"].append(f"{code}: {exc}")
            logger.warning("holder sync failed code=%s: %s", code, exc)
            if max_errors and len(stats["errors"]) >= max_errors:
                logger.error("holder sync reached max_errors=%s; stopping", max_errors)
                break
        if progress_every and index % progress_every == 0:
            logger.info(
                "holder sync progress %s/%s ok=%s empty=%s skipped=%s rows=%s errors=%s",
                index,
                len(codes),
                stats["stocks_ok"],
                stats["stocks_empty"],
                stats["stocks_skipped"],
                stats["rows"],
                len(stats["errors"]),
            )
    return stats


def main() -> None:
    p = argparse.ArgumentParser(description="Backfill full-market top10 holders for one period.")
    p.add_argument("--period", required=True, help="报告期 YYYYMMDD，例如 20260930")
    p.add_argument("--kind", choices=KIND_CHOICES, default="top10_floatholders")
    p.add_argument("--start-after", default="", help="从该 ts_code 之后开始，便于分批")
    p.add_argument("--offset", type=int, default=0)
    p.add_argument("--limit", type=int, default=0, help="0 = 全市场")
    p.add_argument("--skip-existing", action="store_true", help="跳过该期已有数据的股票")
    p.add_argument("--max-errors", type=int, default=100)
    p.add_argument(
        "--rate-limit",
        type=float,
        default=0.0,
        help="每秒请求数；0 = min(settings.rate_limit_per_sec, 8)，避免 Tushare 500/min 限频",
    )
    p.add_argument("--no-record-job", action="store_true")
    args = p.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    period = "".join(ch for ch in args.period if ch.isdigit())[:8]
    if len(period) != 8:
        raise SystemExit("--period must be YYYYMMDD")

    settings = get_settings()
    if not settings.tushare_token:
        logger.error("TUSHARE_TOKEN 未配置，退出")
        sys.exit(1)

    client = MongoClient(settings.mongo_uri)
    data_db = client[settings.data_db]
    codes = _listed_codes(
        data_db,
        start_after=args.start_after,
        offset=args.offset,
        limit=args.limit,
    )
    logger.info(
        "holder sync period=%s kind=%s codes=%s offset=%s limit=%s",
        period,
        args.kind,
        len(codes),
        args.offset,
        args.limit,
    )

    rate_limit = (
        args.rate_limit
        if args.rate_limit and args.rate_limit > 0
        else min(float(settings.rate_limit_per_sec or 8.0), 8.0)
    )
    adapter = StockAdapter(
        settings.tushare_token,
        rate_limit_per_sec=rate_limit,
    )
    repo = InstitutionalRepository(settings.mongo_uri, settings.data_db)
    stats = run_sync(
        adapter=adapter,
        repo=repo,
        codes=codes,
        period=period,
        kind=args.kind,
        skip_existing=args.skip_existing,
        max_errors=args.max_errors,
    )
    logger.info(
        "holder sync done: %s",
        {key: value for key, value in stats.items() if key != "errors"},
    )

    status = "complete"
    if stats["errors"]:
        status = "partial" if stats["stocks_ok"] else "failed"
    if not args.no_record_job:
        record_ingestion_run(
            settings,
            JOB,
            datetime.now().strftime("%Y%m%d"),
            status=status,
            error="; ".join(stats["errors"][:10]) or None,
            records_changed=stats["upserts"],
            period=period,
            kind=args.kind,
            codes=stats["codes"],
            stocks_ok=stats["stocks_ok"],
            stocks_empty=stats["stocks_empty"],
            stocks_skipped=stats["stocks_skipped"],
            rows=stats["rows"],
            errors=len(stats["errors"]),
        )
    if status == "failed":
        raise SystemExit(2)


if __name__ == "__main__":  # pragma: no cover
    main()
