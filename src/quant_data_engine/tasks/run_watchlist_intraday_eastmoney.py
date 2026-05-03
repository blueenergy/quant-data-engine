"""东方财富快照轮询：交易日 9:30–收盘，将全站用户自选股的当日 OHLCV 写入 ``quant_data.volume_price``。

数据为快照级、非官方推送；夜间 ``run_stock_daily``（Tushare）会按同一 (symbol, trade_date) 覆盖，属预期行为。

Usage:
    python -m quant_data_engine.tasks.run_watchlist_intraday_eastmoney --once
    python -m quant_data_engine.tasks.run_watchlist_intraday_eastmoney --interval 60
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import datetime, time as dtime
from typing import Any, Dict, List, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

import requests
from pymongo import MongoClient
from pymongo.collection import Collection

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from quant_data_engine.config.settings import get_settings
from quant_data_engine.repositories.stock_repository import StockRepository

logger = logging.getLogger(__name__)

CN_TZ = ZoneInfo("Asia/Shanghai")
ULIST_URL = "https://push2.eastmoney.com/api/qt/ulist.np/get"
SOURCE_TAG = "eastmoney_snapshot"

# 东财 ulist 单次 secids 不宜过长，避免 URL 过大
DEFAULT_BATCH = 80


def _parse_ts_code(raw: str) -> Optional[str]:
    s = raw.strip().upper()
    if not s:
        return None
    for suf in (".SH", ".SZ", ".BJ"):
        if s.endswith(suf):
            return s
    return None


def ts_code_to_secid(ts_code: str) -> Optional[str]:
    """Tushare 风格 ts_code -> 东财 secid（push2）。"""
    ts = _parse_ts_code(ts_code)
    if not ts:
        return None
    code, suf = ts.split(".", 1)
    if len(code) != 6 or not code.isdigit():
        return None
    if suf == "SH":
        return f"1.{code}"
    if suf == "SZ":
        return f"0.{code}"
    if suf == "BJ":
        return f"0.{code}"
    return None


def normalize_watchlist_symbol_to_ts_code(raw: str) -> Optional[str]:
    """自选里常见裸六位代码 -> Tushare 风格 ts_code（东财 ulist 与 volume_price 写入用）。"""
    s = raw.strip().upper()
    if not s:
        return None
    parsed = _parse_ts_code(s)
    if parsed:
        return parsed
    if len(s) != 6 or not s.isdigit():
        return None
    if s.startswith("6"):
        return f"{s}.SH"
    if s.startswith(("0", "3")):
        return f"{s}.SZ"
    if s.startswith("5"):
        return f"{s}.SH"
    if s.startswith(("1", "2")):
        return f"{s}.SZ"
    if s.startswith(("8", "4")):
        return f"{s}.BJ"
    return f"{s}.SZ"


def load_union_watchlist_symbols(watch_coll: Collection) -> List[str]:
    """``finance.user_watchlists`` 中所有用户 symbols 去重并排序。"""
    out: set[str] = set()
    for doc in watch_coll.find({}, {"symbols": 1}):
        for sym in doc.get("symbols") or []:
            if isinstance(sym, str) and sym.strip():
                norm = _parse_ts_code(sym) or normalize_watchlist_symbol_to_ts_code(sym)
                if norm:
                    out.add(norm)
    return sorted(out)


def in_cn_stock_session(now: datetime) -> bool:
    """沪深交易日时段：周一–周五 9:30–11:30、13:00–15:00（上海时区）。"""
    if now.tzinfo is None:
        now = now.replace(tzinfo=CN_TZ)
    else:
        now = now.astimezone(CN_TZ)
    if now.weekday() >= 5:
        return False
    t = now.time()
    morning = dtime(9, 30) <= t <= dtime(11, 30)
    afternoon = dtime(13, 0) <= t <= dtime(15, 0)
    return morning or afternoon


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer": "https://quote.eastmoney.com/",
        }
    )
    return s


def fetch_eastmoney_ulist(
    session: requests.Session, pairs: Sequence[Tuple[str, str]]
) -> Dict[str, Dict[str, Any]]:
    """pairs: (ts_code, secid)。返回 ts_code -> 行情字段。"""
    if not pairs:
        return {}
    secids = ",".join(sec for _, sec in pairs)
    params = {
        "fltt": 2,
        "invt": 2,
        "secids": secids,
        "fields": "f2,f3,f4,f5,f6,f12,f13,f15,f16,f17,f18",
        "ut": "7eea3edcaed734bea9cbfc24409ed989",
    }
    r = session.get(ULIST_URL, params=params, timeout=30, allow_redirects=True)
    r.raise_for_status()
    payload = r.json()
    if payload.get("rc") != 0:
        logger.warning("eastmoney ulist rc=%s msg=%s", payload.get("rc"), payload.get("msg"))
        return {}
    diff = (payload.get("data") or {}).get("diff") or []
    by_secid: Dict[str, Dict[str, Any]] = {}
    for row in diff:
        f12 = row.get("f12")
        f13 = row.get("f13")
        if f12 is None or f13 is None:
            continue
        sid = f"{f13}.{f12}"
        by_secid[sid] = row
    out: Dict[str, Dict[str, Any]] = {}
    for ts_code, secid in pairs:
        row = by_secid.get(secid)
        if row:
            out[ts_code] = row
    return out


def _row_to_volume_price_record(ts_code: str, trade_date: str, row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """东财 ulist 单行 -> StockRepository.upsert_daily 记录。成交额与 Tushare 对齐为千元。

    push2 ``ulist`` 的 f15–f18：f15 昨收、f16 最低；**f17 为今开、f18 为最高**（此前将二者对调写入会导致库中开盘/最高反了）。
    """
    try:
        close = float(row["f2"])
        pre_close = float(row["f15"])
        low = float(row["f16"])
        open_ = float(row["f17"])
        high = float(row["f18"])
        vol = int(float(row["f5"])) if row.get("f5") is not None else 0
        amt_yuan = float(row["f6"]) if row.get("f6") is not None else 0.0
        amount_k = amt_yuan / 1000.0
        change = float(row["f4"]) if row.get("f4") is not None else (close - pre_close)
        pct = float(row["f3"]) if row.get("f3") is not None else (
            (change / pre_close * 100.0) if pre_close else 0.0
        )
    except (KeyError, TypeError, ValueError) as e:
        logger.debug("skip row %s: %s", ts_code, e)
        return None
    if close <= 0 and vol == 0:
        return None
    return {
        "symbol": ts_code,
        "ts_code": ts_code,
        "trade_date": trade_date,
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
        "pre_close": pre_close,
        "change": change,
        "pct_chg": pct,
        "volume": vol,
        "amount": amount_k,
        "source": SOURCE_TAG,
    }


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Watchlist intraday East Money -> volume_price")
    p.add_argument("--interval", type=int, default=60, help="Polling interval seconds when not in session (idle sleep)")
    p.add_argument(
        "--active-interval",
        type=int,
        default=None,
        help="Interval seconds during trading session (default: same as --interval)",
    )
    p.add_argument("--once", action="store_true", help="Run one refresh and exit (ignores session window)")
    p.add_argument("--batch", type=int, default=DEFAULT_BATCH, help="Max secids per ulist request")
    return p.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [watchlist_em] %(message)s")
    args = parse_args()
    settings = get_settings()
    active_interval = args.active_interval if args.active_interval is not None else args.interval

    repo = StockRepository(settings.mongo_uri, settings.data_db)
    mongo_client = MongoClient(settings.mongo_uri)
    watch_coll = mongo_client[settings.mongo_db]["user_watchlists"]
    session = _session()

    logger.info(
        "finance_db=%s data_db=%s batch=%s interval=%ss active=%ss",
        settings.mongo_db,
        settings.data_db,
        args.batch,
        args.interval,
        active_interval,
    )

    while True:
        now_cn = datetime.now(CN_TZ)
        if not args.once and not in_cn_stock_session(now_cn):
            logger.info("outside CN session, sleep %ss", args.interval)
            time.sleep(args.interval)
            continue

        symbols = load_union_watchlist_symbols(watch_coll)
        pairs: List[Tuple[str, str]] = []
        for ts in symbols:
            sid = ts_code_to_secid(ts)
            if sid:
                pairs.append((ts, sid))

        trade_date = now_cn.strftime("%Y%m%d")
        if not pairs:
            logger.info("no watchlist A-share symbols, sleep %ss", args.interval)
            time.sleep(args.interval if not args.once else 0)
            if args.once:
                break
            continue

        n_ok = 0
        for i in range(0, len(pairs), args.batch):
            chunk = pairs[i : i + args.batch]
            try:
                rows = fetch_eastmoney_ulist(session, chunk)
            except Exception as e:
                logger.exception("ulist batch failed: %s", e)
                time.sleep(2)
                continue
            records: List[Dict[str, Any]] = []
            for ts_code, _ in chunk:
                row = rows.get(ts_code)
                if not row:
                    logger.debug("missing quote %s", ts_code)
                    continue
                rec = _row_to_volume_price_record(ts_code, trade_date, row)
                if rec:
                    records.append(rec)
            if records:
                try:
                    n_ok += repo.upsert_daily(records)
                except Exception as e:
                    logger.exception("upsert_daily failed: %s", e)
            time.sleep(max(0.15, 1.0 / max(settings.rate_limit_per_sec, 0.5)))

        logger.info("symbols=%d batches=%d upsert_ops=%d", len(pairs), (len(pairs) + args.batch - 1) // args.batch, n_ok)

        if args.once:
            break
        time.sleep(active_interval)


if __name__ == "__main__":
    main()
