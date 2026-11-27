"""Intraday volume alerts pushed to DingTalk.

The reading is the **projected** full-day ratio — today's cumulative turnover
divided by the fraction of the day that is normally done at this clock time,
then divided by the 20-day mean. Measured over 53 sessions, at 14:00 that
projection agreed with the closing ratio 52/53 times at the 1.2× threshold,
with no false positives; before 13:30 it misses too many back-loaded days.

Two stages per day, each sent at most once:

* ``first``   — from 14:00, the first time the projected ratio reaches 1.2×
* ``confirm`` — from 14:50, a final reading (sent only if still above)

State lives in ``turnover_alerts`` keyed by (trade_date, stage), so a container
restart cannot send twice.
"""
from __future__ import annotations

import logging
from datetime import datetime, time as dtime
from typing import Any, Dict, List, Optional, Sequence

logger = logging.getLogger(__name__)

ALERT_THRESHOLD = 1.2
FIRST_ALERT_FROM = dtime(14, 0)
CONFIRM_ALERT_FROM = dtime(14, 50)
SESSION_END = dtime(15, 10)

#: Same clock grid as the API's intraday profile — both must agree.
PROFILE_GRID: List[str] = [
    f"{hour:02d}{minute:02d}"
    for hour, start, end in ((9, 30, 59), (10, 0, 59), (11, 0, 30), (13, 0, 59), (14, 0, 59))
    for minute in range(start, end + 1, 2)
] + ["1500"]

BOARD_LABELS = {
    "sh": "沪市主板",
    "sz": "深市主板",
    "cx": "创业板",
    "star": "科创板",
    "bse": "北交所",
}


def _percentile(values: Sequence[float], q: float) -> Optional[float]:
    if not values:
        return None
    idx = (len(values) - 1) * q
    lower = int(idx)
    upper = min(lower + 1, len(values) - 1)
    if lower == upper:
        return values[lower]
    return values[lower] + (values[upper] - values[lower]) * (idx - lower)


def build_profile(day_series: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    """Distribution of the cumulative fraction of the day at each clock time."""
    profile: List[Dict[str, Any]] = []
    for bucket in PROFILE_GRID:
        fractions = []
        for rows in day_series.values():
            if not rows:
                continue
            end_of_day = rows[-1].get("total")
            if not end_of_day or end_of_day <= 0:
                continue
            cumulative = None
            for row in rows:
                if row["t"] <= bucket:
                    cumulative = row.get("total")
                else:
                    break
            if cumulative is None:
                continue
            fractions.append(cumulative / end_of_day)
        if len(fractions) < 3:
            continue
        fractions.sort()
        profile.append(
            {
                "t": bucket,
                "p10": _percentile(fractions, 0.10),
                "p50": _percentile(fractions, 0.50),
                "p90": _percentile(fractions, 0.90),
                "n": len(fractions),
            }
        )
    return profile


def fraction_at(profile: Sequence[Dict[str, Any]], clock: str) -> Optional[Dict[str, Any]]:
    found = None
    for entry in profile:
        if entry["t"] <= clock:
            found = entry
        else:
            break
    return found


def load_profile(repo: Any, trade_date: str, days: int = 60) -> List[Dict[str, Any]]:
    history = sorted(d for d in repo.intraday_coll.distinct("trade_date") if d < trade_date)[-days:]
    day_series: Dict[str, List[Dict[str, Any]]] = {}
    for doc in repo.intraday_coll.find(
        {"trade_date": {"$in": history}},
        {"_id": 0, "trade_date": 1, "snapshot_time": 1, "total": 1},
    ).sort("snapshot_time", 1):
        day_series.setdefault(doc["trade_date"], []).append(
            {"t": str(doc["snapshot_time"])[-4:], "total": doc.get("total")}
        )
    # A day that stopped early would report a tiny end-of-day total and inflate every fraction.
    day_series = {d: rows for d, rows in day_series.items() if rows and rows[-1]["t"] >= "1430"}
    return build_profile(day_series)


def trailing_mean(repo: Any, trade_date: str, window: int = 20) -> Optional[float]:
    rows = list(
        repo.daily_coll.find({"trade_date": {"$lt": trade_date}}, {"_id": 0, "stock_total": 1})
        .sort("trade_date", -1)
        .limit(window)
    )
    if len(rows) < window:
        return None
    return sum(row["stock_total"] for row in rows) / window


def history_line(repo: Any, threshold: float, index_code: str = "000852.SH") -> str:
    """One-line unconditional statistics for the same volume bucket."""
    turnover = list(
        repo.daily_coll.find({}, {"_id": 0, "trade_date": 1, "stock_total": 1}).sort("trade_date", 1)
    )
    prices = list(
        repo.data_db["index_prices"].find(
            {"ts_code": index_code}, {"_id": 0, "trade_date": 1, "close": 1}
        ).sort("trade_date", 1)
    )
    if not turnover or not prices:
        return ""
    price_map = {row["trade_date"]: row["close"] for row in prices}
    values = [row["stock_total"] for row in turnover]
    dates = [row["trade_date"] for row in turnover]
    event_returns: List[float] = []
    for i in range(20, len(values) - 20):
        mean = sum(values[i - 20 : i]) / 20
        if not mean:
            continue
        close_now, close_later = price_map.get(dates[i]), price_map.get(dates[i + 20])
        if not close_now or not close_later:
            continue
        if values[i] / mean > threshold:
            event_returns.append(close_later / close_now - 1)
    if not event_returns:
        return ""
    mean_return = sum(event_returns) / len(event_returns)
    wins = sum(1 for x in event_returns if x > 0) / len(event_returns)
    return (
        f"中证1000 同档历史 {len(event_returns)} 次，之后 20 日平均 "
        f"{mean_return * 100:+.2f}%，胜率 {wins * 100:.0f}%"
    )


def build_message(doc: Dict[str, Any], ratio: float, ma20: float, clock: str, stage: str, history: str) -> str:
    title = "量能放量" if stage == "first" else "量能放量（收盘前确认）"
    cumulative = doc.get("total")
    lines = [
        f"【{title}】{doc['trade_date'][:4]}-{doc['trade_date'][4:6]}-{doc['trade_date'][6:]} {clock[:2]}:{clock[2:]}",
        f"全市场 {ratio:.2f}×（累计 {cumulative:,.0f} 亿 / 20 日均线 {ma20:,.0f} 亿）",
    ]
    boards = []
    for key, label in BOARD_LABELS.items():
        value = doc.get(key)
        if value:
            boards.append(f"{label} {value:,.0f} 亿")
    if boards:
        lines.append("分板块：" + " · ".join(boards))
    if history:
        lines.append(history)
    lines.append("提醒：深回撤档样本集中在 2012/2015-2016/2018，是历史均值不是概率承诺")
    return "\n".join(lines)


def maybe_send(repo: Any, doc: Dict[str, Any], *, now: Optional[datetime] = None, dry_run: bool = False) -> Dict[str, Any]:
    """Evaluate the alert for one snapshot; send at most one message per stage per day."""
    now = now or datetime.now()
    clock_time = now.time()
    if clock_time < FIRST_ALERT_FROM or clock_time > SESSION_END:
        return {"status": "outside_alert_window"}
    stage = "confirm" if clock_time >= CONFIRM_ALERT_FROM else "first"

    trade_date = str(doc.get("trade_date") or "")
    if not trade_date:
        return {"status": "no_trade_date"}
    if repo.alert_exists(trade_date, stage):
        return {"status": "already_sent", "stage": stage}

    ma20 = trailing_mean(repo, trade_date)
    if not ma20:
        return {"status": "no_baseline"}
    clock = now.strftime("%H%M")
    profile = load_profile(repo, trade_date)
    entry = fraction_at(profile, clock)
    fraction = (entry or {}).get("p50")
    cumulative = doc.get("total")
    if not fraction or not cumulative:
        return {"status": "no_profile"}

    ratio = cumulative / (ma20 * fraction)
    if ratio < ALERT_THRESHOLD:
        if stage == "confirm":
            repo.record_alert(trade_date, stage, ratio, sent=False)
        return {"status": "below_threshold", "ratio": round(ratio, 3)}

    history = history_line(repo, ALERT_THRESHOLD)
    message = build_message(doc, ratio, ma20, clock, stage, history)
    if dry_run:
        return {"status": "dry_run", "stage": stage, "ratio": round(ratio, 3), "message": message}

    from .notify import send_dingtalk

    error = send_dingtalk(message)
    repo.record_alert(trade_date, stage, ratio, sent=error is None, error=error)
    if error:
        logger.error("turnover alert delivery failed: %s", error)
        return {"status": "send_failed", "error": error, "ratio": round(ratio, 3)}
    logger.info("turnover alert sent stage=%s ratio=%.2f", stage, ratio)
    return {"status": "sent", "stage": stage, "ratio": round(ratio, 3)}
