"""Shared post-processing after a realtime daily-K snapshot is materialized.

Used by Tushare ``rt_k`` sync and the miniQMT full-market bridge so quality
gates, spectrum, sector strength, job_status, and downstream triggers stay aligned.
"""
from __future__ import annotations

from dataclasses import dataclass
import json
import logging
import urllib.error
import urllib.request
from typing import Any, Callable, Dict, Optional, Sequence

from pymongo import MongoClient

from quant_data_engine.tasks.realtime_sector_strength import (
    _member_map,
    calculate_realtime_sector_strength,
)
from quant_data_engine.tasks.sentiment_analyzer import (
    _get_trading_days_lookback,
    calculate_realtime_daily_snapshot_sentiment,
)
from quant_data_engine.utils.ingestion_job import record_ingestion_run


logger = logging.getLogger(__name__)

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


def field_coverage(records: Sequence[Dict[str, Any]]) -> float:
    if not records:
        return 0.0
    ok = 0
    for record in records:
        if all(record.get(field) not in (None, "") for field in CORE_FIELDS):
            ok += 1
    return ok / len(records)


def post_realtime_trigger(
    *,
    name: str,
    url: Optional[str],
    key: Optional[str],
    timeout_sec: float,
    trade_date: str,
    snapshot_time: str,
    status: str,
    urlopen: Callable[..., Any] = urllib.request.urlopen,
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
        with urlopen(request, timeout=timeout) as response:
            logger.info("%s trigger status=%s", name, getattr(response, "status", None))
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        logger.warning("%s trigger failed: %s", name, exc)


def trigger_intraday_t0_signals(settings: Any, trade_date: str, snapshot_time: str, status: str) -> None:
    post_realtime_trigger(
        name="intraday T+0",
        url=getattr(settings, "intraday_t0_trigger_url", None),
        key=getattr(settings, "intraday_t0_trigger_key", None),
        timeout_sec=float(getattr(settings, "intraday_t0_trigger_timeout_sec", 5.0) or 5.0),
        trade_date=trade_date,
        snapshot_time=snapshot_time,
        status=status,
    )


def trigger_market_insights(settings: Any, trade_date: str, snapshot_time: str, status: str) -> None:
    post_realtime_trigger(
        name="market insight",
        url=getattr(settings, "market_insight_trigger_url", None),
        key=getattr(settings, "market_insight_trigger_key", None),
        timeout_sec=float(getattr(settings, "market_insight_trigger_timeout_sec", 5.0) or 5.0),
        trade_date=trade_date,
        snapshot_time=snapshot_time,
        status=status,
    )


@dataclass(frozen=True)
class RealtimeDailyKPostResult:
    daily_changed: int
    latest_changed: int
    synced_symbols: int
    expected_symbols: int
    coverage_ratio: float
    field_coverage: float
    quality_passed: bool
    status: str
    source_health: str
    sentiment: Dict[str, Any]
    sector_strength: Dict[str, Any]


def _resolve_ma_periods(ma_period: int, ma_periods: Optional[Sequence[int]]) -> List[int]:
    if ma_periods:
        out: List[int] = []
        for value in ma_periods:
            period = int(value)
            if period < 2:
                raise ValueError(f"ma_period must be >= 2, got {period}")
            if period not in out:
                out.append(period)
        if not out:
            raise ValueError("ma_periods produced an empty list")
        return out
    return [int(ma_period)]


def _prefetch_market_window(client, data_db_name, trade_date, periods, today_records):
    """Fetch the recent daily closes once for every MA period and consumer.

    Before this existed, every (consumer, period) pair re-read the same 30-35
    day window from ``volume_price``: two periods × (spectrum + sector) meant
    four full-window queries per snapshot, ~60 万 documents each round.

    Returns ``(today_frame, sum_prev_by_period, limit_up, limit_down)`` or
    ``None`` when today's records are empty. ``today_frame`` has one row per
    symbol for ``trade_date`` plus a ``ma{p}`` column per period;
    ``sum_prev_by_period[p][symbol]`` is the sum of the last ``p-1`` closes
    strictly before today (the sector above-MA rule).
    """
    import pandas as pd

    periods = sorted({int(p) for p in periods})
    max_period = max(periods) if periods else 5
    volume_price = client[data_db_name]["volume_price"]

    window_start = _get_trading_days_lookback(volume_price, trade_date, max_period + 5)
    hist_docs = list(
        volume_price.find(
            {
                "trade_date": {"$gte": window_start, "$lt": trade_date},
                "close": {"$exists": True, "$ne": None},
            },
            {"_id": 0, "symbol": 1, "trade_date": 1, "close": 1},
        )
    )
    today = pd.DataFrame(
        [
            {
                "symbol": record.get("symbol"),
                "close": record.get("close"),
                "pct_chg": record.get("pct_chg"),
            }
            for record in today_records
            if record.get("symbol")
        ]
    )
    if today.empty:
        return None
    today["trade_date"] = trade_date

    hist = pd.DataFrame(hist_docs)
    frames = [today[["symbol", "trade_date", "close"]]]
    if not hist.empty:
        frames.insert(0, hist[["symbol", "trade_date", "close"]])
    df = pd.concat(frames, ignore_index=True)
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = (
        df.dropna(subset=["symbol", "trade_date", "close"])
        .sort_values(["symbol", "trade_date"])
        .reset_index(drop=True)
    )
    for period in periods:
        df[f"ma{period}"] = (
            df.groupby("symbol")["close"]
            .rolling(period)
            .mean()
            .reset_index(level=0, drop=True)
        )

    today_frame = df[df["trade_date"] == trade_date].merge(
        today[["symbol", "pct_chg"]], on="symbol", how="left"
    )
    hist_only = df[df["trade_date"] < trade_date]
    sum_prev_by_period = {}
    for period in periods:
        need = period - 1
        if need <= 0 or hist_only.empty:
            sum_prev_by_period[period] = {symbol: 0.0 for symbol in today_frame["symbol"]}
            continue
        sums = hist_only.groupby("symbol")["close"].apply(
            lambda closes, n=need: float(closes.tail(n).sum()) if len(closes) >= n else None
        )
        sum_prev_by_period[period] = sums.to_dict()

    db = client[data_db_name]
    limit_up = {
        doc["symbol"]
        for doc in db["daily_limit_up"].find(
            {"date": trade_date, "status": "limit_up"}, {"symbol": 1}
        )
    }
    limit_down = {
        doc["symbol"]
        for doc in db["daily_limit_down"].find(
            {"date": trade_date, "status": "limit_down"}, {"symbol": 1}
        )
    }
    return today_frame, sum_prev_by_period, limit_up, limit_down


def post_realtime_daily_k_snapshot(
    *,
    settings: Any,
    records: Sequence[Dict[str, Any]],
    trade_date: str,
    snapshot_time: str,
    expected_symbols: int,
    source_mode: str,
    task_type: str = "a_daily_realtime_sync",
    repo: Any = None,
    already_upserted: bool = False,
    daily_changed: int = 0,
    latest_changed: int = 0,
    run_derivatives: bool = True,
    run_triggers: bool = True,
    ma_period: int = 5,
    ma_periods: Optional[Sequence[int]] = None,
    min_stocks: int = 100,
    universe_type: Optional[str] = None,
    mongo_client_factory: Callable[[str], Any] = MongoClient,
    sentiment_func: Callable[..., Dict[str, Any]] = calculate_realtime_daily_snapshot_sentiment,
    sector_func: Callable[..., Dict[str, Any]] = calculate_realtime_sector_strength,
    record_func: Callable[..., Any] = record_ingestion_run,
    trigger_t0_func: Callable[..., None] = trigger_intraday_t0_signals,
    trigger_insight_func: Callable[..., None] = trigger_market_insights,
) -> RealtimeDailyKPostResult:
    """Upsert snapshot rows (unless already written), then run full-market follow-up."""
    periods = _resolve_ma_periods(ma_period, ma_periods)
    primary_period = periods[0]

    if not already_upserted:
        if repo is None:
            raise ValueError("repo is required when already_upserted is false")
        daily_changed = repo.upsert_daily(records) if records else 0
        latest_changed = repo.upsert_realtime_latest(records) if records else 0

    synced_symbols = len({str(record.get("symbol")) for record in records if record.get("symbol")})
    coverage_ratio = synced_symbols / expected_symbols if expected_symbols > 0 else 1.0
    fields_ok = field_coverage(list(records))
    sentiment: Dict[str, Any] = {"success": False, "results": [], "error": "derivatives disabled"}
    sector_strength: Dict[str, Any] = {"success": False, "written": 0, "results": []}
    spectrum_written = 0
    sector_written = 0
    derivative_errors: List[str] = []

    if run_derivatives:
        # One connection and one history fetch for every period and consumer.
        # The previous shape rebuilt a Mongo client twice per period and let
        # each (consumer, period) pair re-read the same 30-35 day window.
        client = mongo_client_factory(settings.mongo_uri)
        try:
            # The prefetch is an optimization only: if it fails (unexpected
            # collection shape, a driver without aggregate, a test double), the
            # per-caller queries below still produce the same snapshot.
            try:
                window = (
                    _prefetch_market_window(
                        client, settings.data_db, trade_date, periods, records
                    )
                    if records
                    else None
                )
            except Exception as exc:  # noqa: BLE001 - degrade, never block
                logger.warning(
                    "market window prefetch failed; falling back to per-period queries: %s",
                    exc,
                )
                window = None
            symbols = [r.get("symbol") for r in records if r.get("symbol")]
            try:
                member_by_symbol = (
                    _member_map(client[settings.data_db], symbols) if symbols else {}
                )
            except Exception as exc:  # noqa: BLE001 - same degradation as above
                logger.warning("sector member map prefetch failed: %s", exc)
                member_by_symbol = {}
            for index, period in enumerate(periods):
                today_frame = window[0] if window else None
                one_sentiment = sentiment_func(
                    client=client,
                    db_name=settings.data_db,
                    source_collection_name="volume_price",
                    output_collection_name=settings.realtime_daily_k_collection,
                    snapshot_time=snapshot_time,
                    ma_period=period,
                    min_stocks=min_stocks,
                    expected_symbols=expected_symbols or synced_symbols,
                    output_db_name=settings.realtime_db,
                    source_mode=source_mode,
                    verbose=True,
                    today_frame=today_frame,
                    limit_up_symbols=window[2] if window else None,
                    limit_down_symbols=window[3] if window else None,
                    ensure_indexes=index == 0,
                )

                one_sector = sector_func(
                    client=client,
                    data_db_name=settings.data_db,
                    realtime_db_name=settings.realtime_db,
                    snapshot_time=snapshot_time,
                    ma_period=period,
                    data_source=source_mode,
                    verbose=True,
                    latest_rows=records or None,
                    sum_prev_by_symbol=(window[1].get(period) if window else None),
                    member_by_symbol=member_by_symbol or None,
                    ensure_indexes=index == 0,
                )

                if period == primary_period:
                    sentiment = one_sentiment
                    sector_strength = one_sector
                if one_sentiment.get("success"):
                    spectrum_written += len(one_sentiment.get("results") or [])
                else:
                    derivative_errors.append(
                        f"MA{period} sentiment: {one_sentiment.get('error') or 'failed'}"
                    )
                if one_sector.get("success"):
                    sector_written += int(one_sector.get("written") or 0)
                else:
                    derivative_errors.append(
                        f"MA{period} sector: {one_sector.get('error') or 'failed'}"
                    )
        finally:
            close = getattr(client, "close", None)
            if callable(close):
                close()

    quality_passed = (
        coverage_ratio >= float(getattr(settings, "realtime_daily_k_min_coverage", 0.98) or 0.98)
        and fields_ok >= float(getattr(settings, "realtime_daily_k_min_field_coverage", 0.98) or 0.98)
        and bool(sentiment.get("success"))
    )
    status = "complete" if quality_passed else "partial"
    source_health = "healthy" if quality_passed else "degraded"
    extra: Dict[str, Any] = dict(
        realtime_latest_changed=latest_changed,
        expected_symbols=expected_symbols,
        synced_symbols=synced_symbols,
        coverage_ratio=coverage_ratio,
        field_coverage=fields_ok,
        last_realtime_at=snapshot_time,
        source_health=source_health,
        quality_passed=quality_passed,
        fallback_decision="skip_eod_allowed" if quality_passed else "fallback_eod_required",
        source_mode=source_mode,
        spectrum_db=settings.realtime_db,
        spectrum_collection=settings.realtime_daily_k_collection,
        spectrum_written=spectrum_written,
        sector_strength_db=settings.realtime_db,
        sector_strength_collection="realtime_sector_strength",
        sector_strength_written=sector_written,
        opportunity_inputs=OPPORTUNITY_INPUTS,
        full_market_derivatives_enabled=bool(run_derivatives),
        ma_periods=periods,
    )
    if universe_type:
        extra["universe_type"] = universe_type
    error = None
    if not sentiment.get("success"):
        error = str(sentiment.get("error") or "sentiment failed")[:2000]
    elif derivative_errors:
        error = "; ".join(derivative_errors)[:2000]
    record_func(
        settings,
        task_type,
        trade_date,
        status=status,
        records_changed=daily_changed,
        error=error,
        **extra,
    )

    if run_derivatives and sentiment.get("success"):
        client = mongo_client_factory(settings.mongo_uri)
        try:
            for period in periods:
                client[settings.realtime_db][settings.realtime_daily_k_collection].update_one(
                    {
                        "trade_date": snapshot_time,
                        "ma_period": period,
                        "granularity": "daily_realtime",
                    },
                    {
                        "$set": {
                            "quality_passed": quality_passed,
                            "field_coverage": fields_ok,
                            "fallback_decision": extra["fallback_decision"],
                        }
                    },
                )
        finally:
            close = getattr(client, "close", None)
            if callable(close):
                close()

    if run_triggers:
        trigger_t0_func(settings, trade_date, snapshot_time, status)
        trigger_insight_func(settings, trade_date, snapshot_time, status)

    return RealtimeDailyKPostResult(
        daily_changed=daily_changed,
        latest_changed=latest_changed,
        synced_symbols=synced_symbols,
        expected_symbols=expected_symbols,
        coverage_ratio=coverage_ratio,
        field_coverage=fields_ok,
        quality_passed=quality_passed,
        status=status,
        source_health=source_health,
        sentiment=sentiment,
        sector_strength=sector_strength,
    )
