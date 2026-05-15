"""Realtime daily K sync and intraday market spectrum generation.

This task ingests Tushare ``rt_k`` daily-shaped snapshots, upserts the current
trading day's ``volume_price`` rows, and writes a time series for the existing
frontend "minute / daily realtime" spectrum slot.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence

import pandas as pd
from pymongo import MongoClient

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from quant_data_engine.adapters.stock_adapter import StockAdapter
from quant_data_engine.config.settings import get_settings
from quant_data_engine.repositories.stock_repository import StockRepository
from quant_data_engine.tasks.realtime_sector_strength import calculate_realtime_sector_strength
from quant_data_engine.tasks.sentiment_analyzer import calculate_realtime_daily_snapshot_sentiment
from quant_data_engine.utils.ingestion_job import record_ingestion_run


CN_TZ = timezone(timedelta(hours=8))
TASK_TYPE = "a_daily_realtime_sync"
SOURCE_TAG = "tushare_rt_k"
CORE_FIELDS = ("open", "high", "low", "close", "pre_close", "pct_chg", "volume")
OPPORTUNITY_INPUTS = [
    "intraday_breadth_inflection",
    "index_false_breakout_filter",
    "sector_relative_breadth",
    "limit_up_continuation_environment",
    "volume_confirmed_ma_reclaim",
    "weak_to_strong_reversal",
    "style_rotation_monitor",
]


logger = logging.getLogger("realtime_daily_k")


def _in_cn_stock_session(now_cn: datetime) -> bool:
    """A-share continuous call window for realtime daily K snapshots."""
    hhmm = now_cn.hour * 100 + now_cn.minute
    return 930 <= hhmm <= 1500


def _num(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _int_num(value: Any) -> int:
    n = _num(value)
    return int(n) if n is not None else 0


def _pick(row: Dict[str, Any], *names: str) -> Any:
    for name in names:
        if name in row and row.get(name) not in (None, ""):
            return row.get(name)
    return None


def _normalize_rt_k_rows(
    df: pd.DataFrame,
    trade_date: str,
    snapshot_time: str,
    source_mode: str,
) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    for raw in df.to_dict("records"):
        ts_code = str(_pick(raw, "ts_code", "symbol", "code") or "").strip()
        if not ts_code:
            continue
        close = _num(_pick(raw, "close", "price", "last_price"))
        pre_close = _num(_pick(raw, "pre_close", "preclose", "preClose", "prev_close"))
        open_ = _num(_pick(raw, "open", "open_price"))
        high = _num(_pick(raw, "high", "high_price"))
        low = _num(_pick(raw, "low", "low_price"))
        change = _num(_pick(raw, "change", "chg"))
        pct_chg = _num(_pick(raw, "pct_chg", "pct_change", "change_pct", "pctChg"))
        if change is None and close is not None and pre_close is not None:
            change = close - pre_close
        if pct_chg is None and change is not None and pre_close:
            pct_chg = change / pre_close * 100.0
        amount_yuan = _num(_pick(raw, "amount", "amt"))

        records.append(
            {
                "symbol": ts_code,
                "ts_code": ts_code,
                "trade_date": str(_pick(raw, "trade_date", "date") or trade_date).replace("-", "")[:8],
                "open": open_,
                "high": high,
                "low": low,
                "close": close,
                "pre_close": pre_close,
                "change": change,
                "pct_chg": pct_chg,
                "volume": _int_num(_pick(raw, "volume", "vol")),
                # Tushare daily amount is stored in thousand-yuan units in volume_price.
                "amount": amount_yuan / 1000.0 if amount_yuan is not None else None,
                "realtime_amount_yuan": amount_yuan,
                "realtime_num": _int_num(_pick(raw, "num")),
                "realtime_ask_price1": _num(_pick(raw, "ask_price1")),
                "realtime_ask_volume1": _int_num(_pick(raw, "ask_volume1")),
                "realtime_bid_price1": _num(_pick(raw, "bid_price1")),
                "realtime_bid_volume1": _int_num(_pick(raw, "bid_volume1")),
                "realtime_trade_time": _pick(raw, "trade_time"),
                "source": SOURCE_TAG,
                "source_mode": source_mode,
                "realtime_snapshot_time": snapshot_time,
            }
        )
    return [r for r in records if r.get("close") is not None]


def _field_coverage(records: Sequence[Dict[str, Any]]) -> float:
    if not records:
        return 0.0
    ok = 0
    for record in records:
        if all(record.get(field) not in (None, "") for field in CORE_FIELDS):
            ok += 1
    return ok / len(records)


def _listed_symbols(adapter: StockAdapter) -> List[str]:
    try:
        basic = adapter.fetch_stock_basic(list_status="L")
        if basic is not None and not basic.empty:
            return [
                str(ts_code).strip()
                for ts_code in basic["ts_code"].tolist()
                if str(ts_code).strip()
            ]
    except Exception as exc:
        logger.warning("stock_basic listed symbol fetch failed: %s", exc)
    return []


def _chunks(items: Sequence[str], size: int) -> Sequence[Sequence[str]]:
    step = max(int(size or 1), 1)
    return [items[i : i + step] for i in range(0, len(items), step)]


def _fetch_rt_k_for_patterns(adapter: StockAdapter, patterns: str, trade_date: str, fields: Optional[str]) -> pd.DataFrame:
    """Fetch rt_k with Tushare wildcard patterns.

    Tushare documents full-market retrieval as e.g.:
    ``3*.SZ,6*.SH,0*.SZ,9*.BJ``. Try the combined expression first; if a
    deployment rejects it, fall back to each pattern individually.
    """
    expr = ",".join([p.strip() for p in str(patterns or "").split(",") if p.strip()])
    if not expr:
        return pd.DataFrame()

    try:
        df = adapter.fetch_rt_k(ts_code=expr, trade_date=trade_date, fields=fields)
        if df is not None and not df.empty:
            return df
    except Exception as exc:
        logger.warning("rt_k wildcard expression failed (%s): %s", expr, exc)

    frames: List[pd.DataFrame] = []
    for pattern in [p.strip() for p in expr.split(",") if p.strip()]:
        try:
            df = adapter.fetch_rt_k(ts_code=pattern, trade_date=trade_date, fields=fields)
            if df is not None and not df.empty:
                frames.append(df)
        except Exception as exc:
            logger.warning("rt_k wildcard pattern failed (%s): %s", pattern, exc)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _fetch_rt_k_for_symbols(
    adapter: StockAdapter,
    symbols: Sequence[str],
    trade_date: str,
    batch_size: int,
    fields: Optional[str],
) -> pd.DataFrame:
    frames: List[pd.DataFrame] = []
    for chunk in _chunks(list(symbols), batch_size):
        ts_codes = ",".join(chunk)
        try:
            df = adapter.fetch_rt_k(ts_code=ts_codes, trade_date=trade_date, fields=fields)
            if df is not None and not df.empty:
                frames.append(df)
            continue
        except Exception as batch_exc:
            logger.warning(
                "rt_k batch failed (size=%d, first=%s): %s; falling back to single-symbol calls",
                len(chunk),
                chunk[0] if chunk else "",
                batch_exc,
            )

        for symbol in chunk:
            try:
                df = adapter.fetch_rt_k(ts_code=symbol, trade_date=trade_date, fields=fields)
                if df is not None and not df.empty:
                    frames.append(df)
            except Exception as single_exc:
                logger.debug("rt_k single-symbol failed %s: %s", symbol, single_exc)

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _post_realtime_trigger(
    *,
    name: str,
    url: Optional[str],
    key: Optional[str],
    timeout_sec: float,
    trade_date: str,
    snapshot_time: str,
    status: str,
) -> None:
    """Notify quantFinance after a realtime daily K snapshot."""
    if not url:
        return
    payload = json.dumps(
        {
            "trade_date": trade_date,
            "snapshot_time": snapshot_time,
            "source": "auto",
            "sync_status": status,
        }
    ).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    if key:
        headers["X-Hermes-Quant-Key"] = key
    request = urllib.request.Request(url, data=payload, headers=headers, method="POST")
    timeout = float(timeout_sec or 5.0)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            logger.info("%s trigger status=%s", name, response.status)
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        logger.warning("%s trigger failed: %s", name, exc)


def _trigger_intraday_t0_signals(settings: Any, trade_date: str, snapshot_time: str, status: str) -> None:
    """Notify quantFinance to generate T+0 signals after a realtime daily K snapshot."""
    _post_realtime_trigger(
        name="intraday T+0",
        url=getattr(settings, "intraday_t0_trigger_url", None),
        key=getattr(settings, "intraday_t0_trigger_key", None),
        timeout_sec=float(getattr(settings, "intraday_t0_trigger_timeout_sec", 5.0) or 5.0),
        trade_date=trade_date,
        snapshot_time=snapshot_time,
        status=status,
    )


def _trigger_market_insights(settings: Any, trade_date: str, snapshot_time: str, status: str) -> None:
    """Notify quantFinance to generate market insight events after realtime data materializes."""
    _post_realtime_trigger(
        name="market insight",
        url=getattr(settings, "market_insight_trigger_url", None),
        key=getattr(settings, "market_insight_trigger_key", None),
        timeout_sec=float(getattr(settings, "market_insight_trigger_timeout_sec", 5.0) or 5.0),
        trade_date=trade_date,
        snapshot_time=snapshot_time,
        status=status,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync Tushare rt_k realtime daily K snapshots")
    parser.add_argument("--date", default=None, help="Trade date YYYYMMDD, default: today CN")
    parser.add_argument("--snapshot-time", default=None, help="Snapshot time YYYYMMDDHHMM, default: now CN")
    parser.add_argument("--ma-period", type=int, default=5, help="Spectrum MA period")
    parser.add_argument("--min-stocks", type=int, default=100, help="Minimum stocks for spectrum")
    parser.add_argument("--expected-symbols", type=int, default=None, help="Override expected listed stock count")
    parser.add_argument("--batch-size", type=int, default=None, help="rt_k ts_code batch size")
    parser.add_argument(
        "--ts-code-patterns",
        default=None,
        help="Tushare rt_k wildcard expression, default from REALTIME_DAILY_K_TS_CODE_PATTERNS",
    )
    parser.add_argument("--fields", default=None, help="Optional Tushare rt_k fields list")
    parser.add_argument("--dry-run", action="store_true", help="Fetch and validate without writing")
    parser.add_argument("--force", action="store_true", help="Run even when ENABLE_REALTIME_DAILY_K is false")
    parser.add_argument(
        "--ignore-session",
        action="store_true",
        help="Fetch outside the regular A-share session (useful for post-close testing)",
    )
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [realtime_daily_k] %(message)s")
    args = parse_args()
    settings = get_settings()
    now_cn = datetime.now(CN_TZ)
    trade_date = (args.date or now_cn.strftime("%Y%m%d")).replace("-", "")[:8]
    snapshot_time = (args.snapshot_time or now_cn.strftime("%Y%m%d%H%M")).replace("-", "")[:12]

    allow_outside_session = settings.realtime_daily_k_allow_outside_session or args.ignore_session
    if not allow_outside_session and not _in_cn_stock_session(now_cn):
        logger.info(
            "outside A-share realtime daily K session (now=%s); set REALTIME_DAILY_K_ALLOW_OUTSIDE_SESSION=1 "
            "or pass --ignore-session for post-close testing",
            now_cn.strftime("%Y-%m-%d %H:%M:%S%z"),
        )
        return

    if not settings.enable_realtime_daily_k and not args.force:
        record_ingestion_run(
            settings,
            TASK_TYPE,
            trade_date,
            status="disabled",
            mode="disabled",
            reason="ENABLE_REALTIME_DAILY_K is false",
            opportunity_inputs=OPPORTUNITY_INPUTS,
        )
        logger.info("Realtime daily K disabled; set ENABLE_REALTIME_DAILY_K=1 or pass --force")
        return

    if not settings.tushare_token:
        record_ingestion_run(settings, TASK_TYPE, trade_date, status="failed", error="TUSHARE_TOKEN missing")
        raise SystemExit("TUSHARE_TOKEN missing")

    adapter = StockAdapter(settings.tushare_token, rate_limit_per_sec=settings.rate_limit_per_sec)
    listed_symbols = _listed_symbols(adapter)
    expected_symbols = args.expected_symbols or len(listed_symbols)
    if expected_symbols <= 0:
        record_ingestion_run(
            settings,
            TASK_TYPE,
            trade_date,
            status="failed",
            error="Failed to resolve expected listed stock count for rt_k",
            expected_symbols=expected_symbols,
            synced_symbols=0,
            coverage_ratio=0.0,
            source_health="unavailable",
            opportunity_inputs=OPPORTUNITY_INPUTS,
        )
        logger.error("Failed to resolve expected listed stock count for rt_k")
        return

    patterns = args.ts_code_patterns or settings.realtime_daily_k_ts_code_patterns
    fields = args.fields or settings.realtime_daily_k_fields
    try:
        df = _fetch_rt_k_for_patterns(adapter, patterns=patterns, trade_date=trade_date, fields=fields)
        if (df is None or df.empty) and listed_symbols:
            logger.warning("rt_k wildcard returned no rows; falling back to listed symbol batches")
            df = _fetch_rt_k_for_symbols(
                adapter,
                listed_symbols,
                trade_date=trade_date,
                batch_size=args.batch_size or settings.realtime_daily_k_batch_size,
                fields=fields,
            )
    except Exception as exc:
        record_ingestion_run(settings, TASK_TYPE, trade_date, status="failed", error=str(exc))
        raise

    if df is None or df.empty:
        record_ingestion_run(
            settings,
            TASK_TYPE,
            trade_date,
            status="failed",
            error="Tushare rt_k returned no rows",
            expected_symbols=expected_symbols,
            synced_symbols=0,
            coverage_ratio=0.0,
            source_health="unavailable",
            opportunity_inputs=OPPORTUNITY_INPUTS,
        )
        logger.error("Tushare rt_k returned no rows")
        return

    records = _normalize_rt_k_rows(
        df,
        trade_date=trade_date,
        snapshot_time=snapshot_time,
        source_mode=settings.realtime_daily_k_source_mode,
    )
    if not records:
        record_ingestion_run(
            settings,
            TASK_TYPE,
            trade_date,
            status="failed",
            error="Tushare rt_k rows could not be normalized into valid daily K records",
            expected_symbols=expected_symbols,
            synced_symbols=0,
            coverage_ratio=0.0,
            source_health="unavailable",
            opportunity_inputs=OPPORTUNITY_INPUTS,
        )
        logger.error("No valid realtime daily K records after normalization")
        return

    synced_symbols = len({r["symbol"] for r in records})
    coverage_ratio = synced_symbols / expected_symbols if expected_symbols > 0 else 1.0
    field_coverage = _field_coverage(records)
    logger.info(
        "snapshot=%s rows=%d symbols=%d expected=%d coverage=%.2f%% field_coverage=%.2f%%",
        snapshot_time,
        len(records),
        synced_symbols,
        expected_symbols,
        coverage_ratio * 100,
        field_coverage * 100,
    )

    if args.dry_run:
        return

    repo = StockRepository(settings.mongo_uri, settings.data_db, realtime_db_name=settings.realtime_db)
    changed = repo.upsert_daily(records)
    latest_changed = repo.upsert_realtime_latest(records)

    client = MongoClient(settings.mongo_uri)
    try:
        sentiment = calculate_realtime_daily_snapshot_sentiment(
            client=client,
            db_name=settings.data_db,
            source_collection_name="volume_price",
            output_collection_name=settings.realtime_daily_k_collection,
            snapshot_time=snapshot_time,
            ma_period=args.ma_period,
            min_stocks=args.min_stocks,
            expected_symbols=expected_symbols or synced_symbols,
            output_db_name=settings.realtime_db,
            source_mode=settings.realtime_daily_k_source_mode,
            verbose=True,
        )
    finally:
        client.close()

    client = MongoClient(settings.mongo_uri)
    try:
        sector_strength = calculate_realtime_sector_strength(
            client=client,
            data_db_name=settings.data_db,
            realtime_db_name=settings.realtime_db,
            snapshot_time=snapshot_time,
            ma_period=args.ma_period,
            data_source=settings.realtime_daily_k_source_mode,
            verbose=True,
        )
    finally:
        client.close()

    quality_passed = (
        coverage_ratio >= settings.realtime_daily_k_min_coverage
        and field_coverage >= settings.realtime_daily_k_min_field_coverage
        and bool(sentiment.get("success"))
    )
    status = "complete" if quality_passed else "partial"
    source_health = "healthy" if quality_passed else "degraded"
    record_ingestion_run(
        settings,
        TASK_TYPE,
        trade_date,
        status=status,
        records_changed=changed,
        realtime_latest_changed=latest_changed,
        expected_symbols=expected_symbols,
        synced_symbols=synced_symbols,
        coverage_ratio=coverage_ratio,
        field_coverage=field_coverage,
        last_realtime_at=snapshot_time,
        source_health=source_health,
        quality_passed=quality_passed,
        fallback_decision="skip_eod_allowed" if quality_passed else "fallback_eod_required",
        source_mode=settings.realtime_daily_k_source_mode,
        spectrum_db=settings.realtime_db,
        spectrum_collection=settings.realtime_daily_k_collection,
        spectrum_written=len(sentiment.get("results") or []),
        sector_strength_db=settings.realtime_db,
        sector_strength_collection="realtime_sector_strength",
        sector_strength_written=sector_strength.get("written", 0),
        opportunity_inputs=OPPORTUNITY_INPUTS,
        error=None if sentiment.get("success") else str(sentiment.get("error") or "sentiment failed")[:2000],
    )
    if sentiment.get("success"):
        client = MongoClient(settings.mongo_uri)
        try:
            client[settings.realtime_db][settings.realtime_daily_k_collection].update_one(
                {
                    "trade_date": snapshot_time,
                    "ma_period": args.ma_period,
                    "granularity": "daily_realtime",
                },
                {
                    "$set": {
                        "quality_passed": quality_passed,
                        "field_coverage": field_coverage,
                        "fallback_decision": "skip_eod_allowed"
                        if quality_passed
                        else "fallback_eod_required",
                    }
                },
            )
        finally:
            client.close()

    _trigger_intraday_t0_signals(settings, trade_date, snapshot_time, status)
    _trigger_market_insights(settings, trade_date, snapshot_time, status)


if __name__ == "__main__":
    main()
