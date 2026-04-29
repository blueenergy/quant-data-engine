"""
主题补涨候选验证：按 ``THEME_LAG_CANDIDATE_VALIDATION_SPEC`` 从 ``theme_lag_candidates`` + ``volume_price`` 计算止盈/止损/期满结果，
写入 ``quant_data.theme_lag_candidate_validation``。

用法::

  python -m quant_data_engine.tasks.run_theme_lag_candidate_validation --trade-date 20250425 --index-store-key csi1000
  python -m quant_data_engine.tasks.run_theme_lag_candidate_validation --trade-date 20250425 --index-store-key csi1000 --dry-run
  python -m quant_data_engine.tasks.run_theme_lag_candidate_validation ... --skip-empty   # §3.1 策略 B
  python -m quant_data_engine.tasks.run_theme_lag_candidate_validation ... --no-pipeline-check

环境变量::

  THEME_LAG_VALIDATE_MAX_TRADING_DAYS=10   # §4.1 默认 10 个开市日名额
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from pymongo import MongoClient

from quant_data_engine.adapters.stock_adapter import StockAdapter
from quant_data_engine.config.settings import Settings, get_settings
from quant_data_engine.repositories.dc_concept_repository import _norm_yyyymmdd
from quant_data_engine.repositories.pipeline_tasks_repository import DATA_COLLECTION_DOMAIN
from quant_data_engine.repositories.theme_lag_candidate_validation_repository import (
    ThemeLagCandidateValidationRepository,
)
from quant_data_engine.tasks.run_theme_lag_candidates import get_open_days
from quant_data_engine.tasks.theme_lag_validation_logic import (
    SymbolValidationResult,
    aggregate_mvp,
    trading_slots_after_anchor,
    validate_one_symbol_stream,
)
from quant_data_engine.utils.ingestion_job import record_ingestion_run

logger = logging.getLogger(__name__)


def _int_env(name: str, default: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _pipeline_task_complete(finance_db: Any, task_type: str, date_yyyymmdd: str) -> bool:
    doc = finance_db["pipeline_tasks"].find_one(
        {"domain": DATA_COLLECTION_DOMAIN, "task_type": task_type, "date": date_yyyymmdd},
        projection={"status": 1},
    )
    return bool(doc and str(doc.get("status", "")).lower() == "complete")


def _has_theme_lag_candidate_rows(data_db: Any, td: str, sk: str) -> bool:
    """候选表已有行时，可认为主题补涨任务已产出（弥补 pipeline_tasks 漏写或镜像失败）。"""
    return bool(
        data_db["theme_lag_candidates"].find_one(
            {"trade_date": td, "index_store_key": sk},
            projection={"_id": 1},
        )
    )


def _has_volume_price_for_date(vp_coll: Any, td: str) -> bool:
    """当日 volume_price 已有任意文档时，可认为日线已落库（弥补 stock_daily 的 pipeline_tasks 缺失）。"""
    return bool(vp_coll.find_one({"trade_date": td}, projection={"_id": 1}))


def _load_close_map(
    vp_coll: Any, symbols: List[str], dates: List[str]
) -> Dict[Tuple[str, str], float]:
    """(symbol, trade_date) -> close"""
    out: Dict[Tuple[str, str], float] = {}
    if not symbols or not dates:
        return out
    batch = 500
    for d in dates:
        d = _norm_yyyymmdd(d)
        for i in range(0, len(symbols), batch):
            chunk = symbols[i : i + batch]
            for doc in vp_coll.find({"trade_date": d, "symbol": {"$in": chunk}}, projection={"symbol": 1, "close": 1}):
                sym = str(doc.get("symbol", "")).strip()
                c = doc.get("close")
                if not sym or c is None:
                    continue
                try:
                    out[(sym, d)] = float(c)
                except (TypeError, ValueError):
                    continue
    return out


def run_validation(
    settings: Settings,
    anchor_trade_date: str,
    index_store_key: str,
    *,
    skip_empty: bool = False,
    no_pipeline_check: bool = False,
    dry_run: bool = False,
) -> Dict[str, Any]:
    td = _norm_yyyymmdd(anchor_trade_date)
    sk = str(index_store_key).strip()
    max_slots = max(1, _int_env("THEME_LAG_VALIDATE_MAX_TRADING_DAYS", 10))

    client = MongoClient(settings.mongo_uri)
    data_db = client[settings.data_db]
    finance_db = client[settings.mongo_db]
    vp_coll = data_db["volume_price"]

    if not no_pipeline_check:
        tl_pt = _pipeline_task_complete(finance_db, "theme_lag_candidates", td)
        tl_rows = _has_theme_lag_candidate_rows(data_db, td, sk)
        if not tl_pt and not tl_rows:
            logger.info("pipeline not ready: theme_lag_candidates date=%s (no pipeline complete and no rows)", td)
            return {"ok": False, "reason": "not_ready", "detail": "theme_lag_candidates"}
        if not tl_pt and tl_rows:
            logger.info(
                "readiness fallback: theme_lag_candidates has rows for date=%s index=%s but pipeline_tasks not complete",
                td,
                sk,
            )

        sd_pt = _pipeline_task_complete(finance_db, "stock_daily", td)
        sd_rows = _has_volume_price_for_date(vp_coll, td)
        if not sd_pt and not sd_rows:
            logger.info("pipeline not ready: stock_daily date=%s (no pipeline complete and no volume_price rows)", td)
            return {"ok": False, "reason": "not_ready", "detail": "stock_daily"}
        if not sd_pt and sd_rows:
            logger.info(
                "readiness fallback: volume_price has rows for trade_date=%s but stock_daily pipeline_tasks not complete",
                td,
            )

    repo = ThemeLagCandidateValidationRepository(settings.mongo_uri, settings.data_db)
    symbols = repo.list_distinct_symbols(td, sk)

    now_utc = datetime.now(timezone.utc)

    if not symbols:
        if skip_empty:
            logger.info("skip-empty: no candidates for anchor=%s index=%s", td, sk)
            return {"ok": True, "reason": "skip_empty", "symbols_count": 0}
        doc: Dict[str, Any] = {
            "anchor_trade_date": td,
            "index_store_key": sk,
            "schema_version": 1,
            "eval_completed_at": now_utc,
            "last_close_trade_date_included": None,
            "run_status": "empty_universe",
            "symbols_count": 0,
            "max_trading_day_slots": max_slots,
            "per_symbol": [],
            "aggregate": {
                "denominator": 0,
                "take_profit_count": 0,
                "stop_loss_count": 0,
                "expire_count": 0,
                "insufficient_data_count": 0,
                "hit_rate_take_profit": None,
                "median_return": None,
            },
        }
        if dry_run:
            return {"ok": True, "dry_run": True, "document": doc}
        repo.replace_validation_doc(doc)
        return {"ok": True, "reason": "empty_universe", "symbols_count": 0}

    sa = StockAdapter(settings.tushare_token or "", rate_limit_per_sec=settings.rate_limit_per_sec)
    if not sa.enabled:
        return {"ok": False, "reason": "no_calendar", "detail": "TUSHARE_TOKEN missing; cannot fetch trade_cal"}
    end_cal = (datetime.strptime(td, "%Y%m%d") + timedelta(days=120)).strftime("%Y%m%d")
    open_days = get_open_days(sa, td, end_cal)
    slots = trading_slots_after_anchor(open_days, td, max_slots)
    if len(slots) < max_slots:
        logger.warning(
            "trade_cal returned only %s open days after anchor (need %s); will use shorter slot list",
            len(slots),
            max_slots,
        )

    dates_needed: Set[str] = {td, *slots}
    close_map = _load_close_map(vp_coll, symbols, sorted(dates_needed))

    results: List[SymbolValidationResult] = []
    for sym in symbols:
        p0 = close_map.get((sym, td))
        if p0 is None or p0 <= 0:
            results.append(
                SymbolValidationResult(
                    symbol=sym,
                    p0=float("nan"),
                    outcome="insufficient_data",
                    exit_trade_date=None,
                    exit_price=None,
                    return_pct=None,
                    eod_float_path=[],
                )
            )
            continue

        def _get_close(d: str) -> Optional[float]:
            return close_map.get((sym, _norm_yyyymmdd(d)))

        results.append(validate_one_symbol_stream(sym, p0, slots, _get_close))

    exit_dates = [r.exit_trade_date for r in results if r.exit_trade_date]
    if exit_dates:
        last_close_td = max(exit_dates)
    else:
        last_close_td = slots[-1] if slots else None

    agg = aggregate_mvp(results)
    per_symbol_out: List[Dict[str, Any]] = []
    for r in results:
        p0_out = r.p0 if r.p0 == r.p0 and r.p0 > 0 else None  # NaN / 无效 -> None，避免 BSON NaN
        row: Dict[str, Any] = {
            "symbol": r.symbol,
            "p0": p0_out,
            "outcome": r.outcome,
            "exit_trade_date": r.exit_trade_date,
            "exit_price": r.exit_price,
            "return_pct": r.return_pct,
        }
        if r.eod_float_path:
            row["eod_float_path"] = r.eod_float_path
        per_symbol_out.append(row)

    doc = {
        "anchor_trade_date": td,
        "index_store_key": sk,
        "schema_version": 1,
        "eval_completed_at": now_utc,
        "last_close_trade_date_included": last_close_td,
        "run_status": "computed",
        "symbols_count": len(symbols),
        "max_trading_day_slots": max_slots,
        "trading_slots": slots,
        "aggregate": agg,
        "per_symbol": per_symbol_out,
    }

    if dry_run:
        return {"ok": True, "dry_run": True, "document": json.loads(json.dumps(doc, default=str))}

    repo.replace_validation_doc(doc)

    try:
        record_ingestion_run(
            settings,
            "theme_lag_candidate_validation",
            td,
            status="complete",
            records_changed=len(symbols),
            index_store_key=sk,
        )
    except Exception as e:
        logger.warning("record_ingestion_run failed (validation ok): %s", e)

    return {"ok": True, "symbols_count": len(symbols), "aggregate": agg}


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser(description="主题补涨候选验证（写入 quant_data.theme_lag_candidate_validation）")
    p.add_argument("--trade-date", default="", help="锚定业务日 YYYYMMDD（= theme_lag_candidates.trade_date）")
    p.add_argument("--index-store-key", default="csi1000", help="index_store_key，与候选表一致")
    p.add_argument("--skip-empty", action="store_true", help="§3.1 策略 B：无标的时不写库")
    p.add_argument("--no-pipeline-check", action="store_true", help="不检查 finance.pipeline_tasks 就绪")
    p.add_argument("--dry-run", action="store_true", help="只打印 JSON，不写库")
    args = p.parse_args()
    settings = get_settings()
    if not args.trade_date:
        logger.error("--trade-date 必填")
        sys.exit(2)
    out = run_validation(
        settings,
        args.trade_date,
        args.index_store_key,
        skip_empty=args.skip_empty,
        no_pipeline_check=args.no_pipeline_check,
        dry_run=args.dry_run,
    )
    if not out.get("ok"):
        code = 3 if out.get("reason") == "not_ready" else (4 if out.get("reason") == "no_calendar" else 1)
        logger.error("exit: %s", out)
        sys.exit(code)
    if args.dry_run:
        print(json.dumps(out.get("document", out), ensure_ascii=False, indent=2, default=str))
    else:
        logger.info("done: %s", out)
    sys.exit(0)


if __name__ == "__main__":
    main()
