"""Real-time quotes for the Asian indices, from East Money (push2).

Why this exists
---------------
Yahoo serves the Asian indices on a **delayed** feed.  Measured on 2026-11-27
at 09:49 CST, ``^HSI`` / ``HSTECH.HK`` / ``^HSCE`` / ``^KS11`` / ``^TWII`` /
``^N225`` all carried a ``regularMarketTime`` 902-1207 seconds behind the wall
clock, while the US indices were only 1-6 seconds behind.  During HK hours
the market card therefore showed a quarter-hour-old price, and the K-line's
unclosed bar did too.

East Money's ``push2`` endpoint serves the same indices in real time — its
``f124`` timestamp is the current moment — and answers all of them in **one**
request, so the extra cost per sync cycle is a single 20 ms call.

Field semantics, verified 2026-11-27 against our own daily bars (600114,
600519 and the six indices): ``f2`` latest, ``f15`` **high**, ``f16`` **low**,
``f17`` **open**, ``f18`` **previous close**.

Note this differs from ``tasks/run_watchlist_intraday_eastmoney.py``, which
reads ``f15`` as the previous close and ``f18`` as the high — the opposite of
what the endpoint actually serves.  That task is left alone here; the two
never meet, but the discrepancy is worth knowing about.

Only the *live* quote comes from East Money.  Daily bars keep coming from
Yahoo (their closes agree with tushare on 493 of 493 sessions), because
East Money's daily history is a different, undocumented endpoint.
"""
from __future__ import annotations

import logging
import time
from typing import Any, Dict, Iterable, Optional

import requests

logger = logging.getLogger("quant_data_engine.eastmoney_quote")

# Our symbol -> East Money secid.
#
#   100.*  global indices (Hang Seng, HSCEI, Taiwan Weighted, KOSPI, Nikkei)
#   124.*  Hang Seng TECH (only served on this market id)
#   1.*   Shanghai / Shenzhen
#   116.* HK-listed ETFs
EASTMONEY_SECIDS: Dict[str, str] = {
    "^HSI": "100.HSI",
    "HSTECH.HK": "124.HSTECH",
    "^HSCE": "100.HSCEI",
    "^KS11": "100.KS11",
    "^TWII": "100.TWII",
    "^N225": "100.N225",
    "000001.SS": "1.000001",
    "2823.HK": "116.02823",
}

EASTMONEY_URL = "https://push2.eastmoney.com/api/qt/ulist.np/get"
EASTMONEY_FIELDS = "f2,f3,f4,f5,f12,f13,f14,f15,f16,f17,f18,f124"
EASTMONEY_TIMEOUT = 12
EASTMONEY_MAX_RETRIES = 3
EASTMONEY_RETRY_BACKOFF = 1.5

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    "Referer": "https://quote.eastmoney.com/",
}


def _num(value: Any) -> Optional[float]:
    try:
        num = float(value)
    except (TypeError, ValueError):
        return None
    return None if num != num else num  # NaN


def parse_eastmoney_rows(diff: Iterable[dict]) -> Dict[str, Dict[str, Any]]:
    """Turn a ``push2`` ``diff`` list into ``{our_symbol: quote}``.

    Pure, so the field mapping can be checked against recorded responses
    without touching the network (see ``tests/test_eastmoney_quote.py``).
    """
    by_secid = {
        f"{row.get('f13')}.{row.get('f12')}": row
        for row in diff
        if row.get("f12") is not None and row.get("f13") is not None
    }

    out: Dict[str, Dict[str, Any]] = {}
    for symbol, secid in EASTMONEY_SECIDS.items():
        row = by_secid.get(secid)
        if not row:
            continue
        price = _num(row.get("f2"))
        if price is None:
            continue
        out[symbol] = {
            "price": price,             # f2  最新价
            "prev_close": _num(row.get("f18")),  # f18 昨收
            "open": _num(row.get("f17")),        # f17 今开
            "high": _num(row.get("f15")),        # f15 最高
            "low": _num(row.get("f16")),         # f16 最低
            "volume": _num(row.get("f5")),       # f5  成交量
            "time": _num(row.get("f124")),       # f124 行情时间戳
            "name": row.get("f14"),
        }
    return out


def fetch_eastmoney_quotes(
    symbols: Iterable[str],
    max_retries: int = EASTMONEY_MAX_RETRIES,
) -> Dict[str, Dict[str, Any]]:
    """Return ``{symbol: quote}`` for the symbols East Money can serve.

    Returns an empty dict when the request fails; the caller then keeps
    Yahoo's values, which are correct but delayed.
    """
    wanted = [s for s in symbols if s in EASTMONEY_SECIDS]
    if not wanted:
        return {}

    secids = ",".join(EASTMONEY_SECIDS[s] for s in wanted)
    session = requests.Session()
    session.headers.update(_HEADERS)

    diff: list = []
    for attempt in range(max_retries):
        try:
            resp = session.get(
                EASTMONEY_URL,
                params={
                    "fltt": 2,
                    "invt": 2,
                    "secids": secids,
                    "fields": EASTMONEY_FIELDS,
                },
                timeout=EASTMONEY_TIMEOUT,
            )
            resp.raise_for_status()
            diff = ((resp.json() or {}).get("data") or {}).get("diff") or []
            if diff:
                break
            logger.warning(
                "[EastMoney] empty result for %d symbol(s), attempt %d",
                len(wanted), attempt + 1,
            )
        except Exception as exc:
            logger.warning(
                "[EastMoney] request failed (attempt %d/%d): %s",
                attempt + 1, max_retries, exc,
            )
        if attempt < max_retries - 1:
            time.sleep(EASTMONEY_RETRY_BACKOFF * (attempt + 1))

    if not diff:
        logger.warning("[EastMoney] giving up; keeping Yahoo values")
        return {}

    parsed = parse_eastmoney_rows(diff)
    out = {s: q for s, q in parsed.items() if s in wanted}

    logger.info(
        "[EastMoney] %d/%d symbol(s) real-time: %s",
        len(out), len(wanted), ", ".join(sorted(out)),
    )
    return out
