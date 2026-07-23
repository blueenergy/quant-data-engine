"""Macro calendar adapters: Wall Street CN + Baidu.

Uses direct HTTP clients instead of akshare wrappers:
- Wall Street CN: empty-day responses have no columns; akshare KeyErrors on
  ``public_date``.
- Baidu: bare requests often get HTTP 403; browser-like headers + circuit
  breaker keep the sync usable when one day fails.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

import pandas as pd
import requests

from quant_data_engine.market_events.identity import normalize_region, parse_shanghai_datetime
from quant_data_engine.market_events.normalizer import infer_macro_category

logger = logging.getLogger(__name__)

_WS_URL = "https://api-one-wscn.awtmt.com/apiv1/finance/macrodatas"
_BAIDU_URL = "https://finance.pae.baidu.com/api/financecalendar"
_BAIDU_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Referer": "https://gushitong.baidu.com/calendar",
    "Origin": "https://gushitong.baidu.com",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
}
_HTTP_TIMEOUT = 20
_BAIDU_PAUSE_SEC = 0.35


def _safe_float(val: Any) -> Optional[float]:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _row_to_raw(row: pd.Series, *, source_name: str) -> Dict[str, Any]:
    title = str(row.get("事件") or row.get("title") or "").strip()
    region = normalize_region(str(row.get("地区") or row.get("region") or ""))
    time_str = str(row.get("时间") or row.get("datetime") or "")
    event_time = parse_shanghai_datetime(time_str)
    importance = row.get("重要性")
    try:
        imp = int(importance) if importance is not None and not pd.isna(importance) else 3
    except (TypeError, ValueError):
        imp = 3
    category = infer_macro_category(title)
    previous = _safe_float(row.get("前值"))
    revised = _safe_float(row.get("修正"))
    if revised is not None:
        previous = revised
    return {
        "identity_kind": "macro",
        "source_name": source_name,
        "title": title,
        "event_time": event_time,
        "region": region,
        "category": category,
        "importance": imp,
        "metrics": {
            "forecast": _safe_float(row.get("预期")),
            "previous": previous,
            "actual": _safe_float(row.get("今值") if "今值" in row.index else row.get("公布")),
            "revised": revised,
        },
        "url": str(row.get("链接") or row.get("url") or ""),
    }


def _yyyymmdd_to_day_bounds(date: str) -> tuple[int, int]:
    start = datetime.strptime(date, "%Y%m%d")
    end = start + timedelta(days=1)
    # Wall Street CN API expects local (naive) epoch seconds matching China wall clock.
    return int(start.timestamp()), int(end.timestamp())


def _fetch_ws_day(date: str, session: requests.Session) -> pd.DataFrame:
    """Fetch one day from Wall Street CN; empty calendar days return empty DF."""
    start_ts, end_ts = _yyyymmdd_to_day_bounds(date)
    resp = session.get(_WS_URL, params={"start": start_ts, "end": end_ts}, timeout=_HTTP_TIMEOUT)
    resp.raise_for_status()
    payload = resp.json() or {}
    items = ((payload.get("data") or {}).get("items")) or []
    if not items:
        return pd.DataFrame(
            columns=["时间", "地区", "事件", "重要性", "今值", "预期", "前值", "修正", "链接"]
        )

    rows: List[Dict[str, Any]] = []
    for item in items:
        public_date = item.get("public_date")
        if public_date in (None, ""):
            continue
        try:
            ts = float(public_date)
            local = datetime.fromtimestamp(ts, tz=timezone.utc).astimezone(ZoneInfo("Asia/Shanghai"))
            time_str = local.strftime("%Y-%m-%d %H:%M:%S")
        except (TypeError, ValueError, OSError):
            continue
        rows.append(
            {
                "时间": time_str,
                "地区": item.get("country") or "",
                "事件": item.get("title") or item.get("event") or "",
                "重要性": item.get("importance"),
                "今值": item.get("actual"),
                "预期": item.get("forecast"),
                "前值": item.get("previous"),
                "修正": item.get("revised"),
                "链接": item.get("uri") or "",
            }
        )
    return pd.DataFrame(rows)


def _fetch_baidu_day(date: str, session: requests.Session) -> pd.DataFrame:
    ymd = f"{date[:4]}-{date[4:6]}-{date[6:8]}"
    resp = session.get(
        _BAIDU_URL,
        params={
            "start_date": ymd,
            "end_date": ymd,
            "market": "",
            "cate": "economic_data",
            "finClientType": "pc",
        },
        headers=_BAIDU_HEADERS,
        timeout=_HTTP_TIMEOUT,
    )
    if resp.status_code == 403:
        raise PermissionError(f"baidu calendar HTTP 403 for {date}")
    resp.raise_for_status()
    payload = resp.json() or {}
    # Soft ban: HTTP 200 with ResultCode=403 and empty Result.
    result_code = str(payload.get("ResultCode") or "")
    if result_code in {"403", "100013", "100002"}:
        raise PermissionError(f"baidu calendar ResultCode={result_code} for {date}")
    result = payload.get("Result") or []
    rows: List[Dict[str, Any]] = []
    for day in result:
        for item in day.get("list") or []:
            rows.append(
                {
                    "日期": item.get("date") or date,
                    "时间": item.get("time") or "09:00",
                    "地区": item.get("region") or item.get("country") or "",
                    "事件": item.get("title") or item.get("event") or "",
                    "公布": item.get("pubVal") or item.get("actual") or item.get("publish"),
                    "预期": item.get("indicateVal") or item.get("predict") or item.get("forecast"),
                    "前值": item.get("formerVal") or item.get("previous"),
                    "重要性": item.get("star") or item.get("importance"),
                }
            )
    return pd.DataFrame(rows)


def fetch_macro_ws(dates: List[str]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    session = requests.Session()
    empty_days = 0
    for d in dates:
        try:
            df = _fetch_ws_day(d, session)
            if df is None or df.empty:
                empty_days += 1
                continue
            for _, row in df.iterrows():
                try:
                    out.append(_row_to_raw(row, source_name="wallstreetcn"))
                except Exception as e:
                    logger.debug("macro_ws row skip %s: %s", d, e)
        except Exception as e:
            logger.warning("macro_info_ws failed for %s: %s", d, e)
    if empty_days:
        logger.info("macro_info_ws: %s/%s days had no events", empty_days, len(dates))
    return out


def fetch_macro_baidu(dates: List[str]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    session = requests.Session()
    blocked = False
    for i, d in enumerate(dates):
        if blocked:
            break
        try:
            if i:
                time.sleep(_BAIDU_PAUSE_SEC)
            df = _fetch_baidu_day(d, session)
            if df is None or df.empty:
                continue
            for _, row in df.iterrows():
                try:
                    date_part = str(row.get("日期") or d)
                    time_part = str(row.get("时间") or "09:00")
                    if len(date_part) == 8 and date_part.isdigit():
                        date_part = f"{date_part[:4]}-{date_part[4:6]}-{date_part[6:8]}"
                    date_part = date_part[:10]
                    if len(time_part) == 5:
                        dt_str = f"{date_part} {time_part}:00"
                    else:
                        dt_str = f"{date_part} {time_part}"
                    raw = {
                        "事件": row.get("事件"),
                        "地区": row.get("地区"),
                        "时间": dt_str,
                        "重要性": row.get("重要性"),
                        "预期": row.get("预期"),
                        "前值": row.get("前值"),
                        "今值": row.get("公布"),
                        "链接": "",
                    }
                    out.append(_row_to_raw(pd.Series(raw), source_name="baidu"))
                except Exception as e:
                    logger.debug("macro_baidu row skip %s: %s", d, e)
        except PermissionError as e:
            logger.warning("%s — skipping remaining baidu days this cycle", e)
            blocked = True
        except Exception as e:
            logger.warning("news_economic_baidu failed for %s: %s", d, e)
    return out


def date_range_yyyymmdd(start: datetime, days: int) -> List[str]:
    out = []
    for i in range(days + 1):
        d = start + timedelta(days=i)
        out.append(d.strftime("%Y%m%d"))
    return out
