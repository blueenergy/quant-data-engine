"""ETF etf_share_size (Tushare) ingestion: fetch with 5000-row chunking -> upsert."""
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timedelta
import logging
import math
from decimal import Decimal, InvalidOperation
from numbers import Integral
from typing import Any, Dict, List, Optional

import pandas as pd
from bson.decimal128 import Decimal128

logger = logging.getLogger(__name__)

# Tushare 单次限量
_MAX_ROWS = 5000
_EXCHANGES_FULL_MARKET = ("SSE", "SZSE", "BSE")


def _merge_share_size_frames(frames: List[pd.DataFrame]) -> pd.DataFrame:
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    if "ts_code" in out.columns and "trade_date" in out.columns:
        return out.drop_duplicates(subset=["ts_code", "trade_date"], keep="last")
    return out


def _fetch_etf_share_size_full_market_one_day(adapter: Any, trade_date: str) -> pd.DataFrame:
    """单日全市场：先 trade_date；若触顶 5000 则按交易所拆分再合并（与 Tushare 文档示例一致）。"""
    df = adapter.fetch_etf_share_size(trade_date=trade_date)
    if df is None:
        return pd.DataFrame()
    if len(df) < _MAX_ROWS:
        return df
    logger.warning(
        "etf_share_size trade_date=%s returned %s rows (>=%s); refetching by exchange (SSE/SZSE/BSE)",
        trade_date,
        len(df),
        _MAX_ROWS,
    )
    parts: List[pd.DataFrame] = []
    for ex in _EXCHANGES_FULL_MARKET:
        try:
            chunk = adapter.fetch_etf_share_size(trade_date=trade_date, exchange=ex)
        except Exception as e:
            logger.warning("etf_share_size trade_date=%s exchange=%s failed: %s", trade_date, ex, e)
            continue
        if chunk is not None and not chunk.empty:
            parts.append(chunk)
    merged = _merge_share_size_frames(parts)
    if merged.empty:
        return df
    if len(merged) > len(df):
        return merged
    if len(merged) == len(df):
        return merged
    logger.warning(
        "etf_share_size trade_date=%s exchange-merge rows=%s < single-call %s; "
        "keeping single-call (may be truncated or partial exchange failure)",
        trade_date,
        len(merged),
        len(df),
    )
    return df


def _yyyymmdd_range(start: str, end: str) -> List[str]:
    """含首尾的自然日 YYYYMMDD 列表。"""
    ds = datetime.strptime(start, "%Y%m%d")
    de = datetime.strptime(end, "%Y%m%d")
    out: List[str] = []
    d = ds
    while d <= de:
        out.append(d.strftime("%Y%m%d"))
        d += timedelta(days=1)
    return out


def _mid_date_str(start: str, end: str) -> str:
    ds = datetime.strptime(start, "%Y%m%d")
    de = datetime.strptime(end, "%Y%m%d")
    if de <= ds:
        return start
    mid = ds + (de - ds) // 2
    return mid.strftime("%Y%m%d")


def _next_day(s: str) -> str:
    d = datetime.strptime(s, "%Y%m%d") + timedelta(days=1)
    return d.strftime("%Y%m%d")


def fetch_etf_share_size_chunked(adapter, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
    """按代码与日期区间拉取；若命中 5000 条上限则按日期二分递归，合并去重。"""
    frames: List[pd.DataFrame] = []

    def rec(s: str, e: str) -> None:
        if s > e:
            return
        df = adapter.fetch_etf_share_size(ts_code=ts_code, start_date=s, end_date=e)
        if df is None or df.empty:
            return
        if len(df) < _MAX_ROWS or s >= e:
            frames.append(df)
            return
        sm = _mid_date_str(s, e)
        if sm >= e:
            frames.append(df)
            return
        rec(s, sm)
        sn = _next_day(sm)
        if sn <= e:
            rec(sn, e)

    rec(start_date, end_date)
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    if "ts_code" in out.columns and "trade_date" in out.columns:
        return out.drop_duplicates(subset=["ts_code", "trade_date"], keep="last")
    return out


def _is_missing(v: Any) -> bool:
    if v is None:
        return True
    try:
        return bool(pd.isna(v))
    except Exception:
        return False


def _norm_trade_date(v: Any) -> Optional[str]:
    """统一为 YYYYMMDD；避免 float 日期的 int 截断、Timestamp 格式问题。"""
    if _is_missing(v):
        return None
    if isinstance(v, pd.Timestamp):
        return v.strftime("%Y%m%d")
    if hasattr(v, "strftime") and callable(getattr(v, "strftime")):
        try:
            return v.strftime("%Y%m%d")  # type: ignore[no-any-return]
        except Exception:
            pass
    if isinstance(v, str):
        s = v.strip().replace("-", "")[:8]
        return s if len(s) == 8 and s.isdigit() else None
    if isinstance(v, bool):
        return None
    if isinstance(v, Integral):
        s = str(int(v))
        return s.zfill(8)[-8:] if len(s) >= 8 else s.zfill(8)
    if isinstance(v, float):
        if math.isnan(v) or math.isinf(v):
            return None
        s = str(int(round(v)))
        return s.zfill(8)[-8:] if len(s) >= 8 else s.zfill(8)
    if hasattr(v, "item"):
        try:
            return _norm_trade_date(v.item())
        except Exception:
            return None
    return None


def _metric_to_mongo(v: Any) -> Any:
    """
    total_share / total_size / nav / close 用 Decimal128 落库，减轻 float64 大数精度损失；
    与网页/JSON 长数字更一致。失败时退回 float。
    """
    if _is_missing(v):
        return None
    if isinstance(v, Decimal128):
        return v
    if isinstance(v, Decimal):
        try:
            return Decimal128(v)
        except Exception:
            return None
    if isinstance(v, str):
        s = v.strip().replace(",", "")
        if not s or s.lower() == "nan":
            return None
        try:
            return Decimal128(Decimal(s))
        except (InvalidOperation, ValueError):
            return None
    if isinstance(v, bool):
        return None
    if isinstance(v, Integral):
        try:
            return Decimal128(Decimal(int(v)))
        except Exception:
            return None
    try:
        x = float(v.item()) if hasattr(v, "item") else float(v)
    except (TypeError, ValueError):
        return None
    if math.isnan(x) or math.isinf(x):
        return None
    try:
        return Decimal128(Decimal(format(x, ".15g")))
    except (InvalidOperation, ValueError):
        return float(x)


def records_from_share_size_df(df: pd.DataFrame, fallback_ts_code: str) -> List[Dict[str, Any]]:
    metric_cols = {"total_share", "total_size", "nav", "close"}
    records: List[Dict[str, Any]] = []
    skipped_td = 0
    for _, row in df.iterrows():
        raw = row.to_dict()
        td = _norm_trade_date(raw.get("trade_date"))
        if not td:
            skipped_td += 1
            continue
        ts_c = raw.get("ts_code") or fallback_ts_code
        if not ts_c:
            continue
        ts_c = str(ts_c).strip()
        doc: Dict[str, Any] = {"ts_code": ts_c, "trade_date": td, "source": "tushare"}
        for k, v in raw.items():
            if k in ("ts_code", "trade_date"):
                continue
            if _is_missing(v):
                continue
            if k in metric_cols:
                mv = _metric_to_mongo(v)
                if mv is not None:
                    doc[k] = mv
            elif k == "etf_name":
                doc[k] = str(v)
            elif k == "exchange":
                doc[k] = str(v)
            else:
                if hasattr(v, "item"):
                    try:
                        doc[k] = v.item()
                    except Exception:
                        doc[k] = v
                else:
                    doc[k] = v
        records.append(doc)
    if skipped_td and not records:
        logger.warning(
            "etf_share_size: skipped %s rows (trade_date normalize failed); columns=%s sample=%s",
            skipped_td,
            list(df.columns),
            df.head(1).to_dict("records"),
        )
    return records


@dataclass
class EtfShareSizeResult:
    ts_code: str
    inserted: int
    issues: List[str]


class EtfShareSizePipeline:
    def __init__(self, adapter, repository):
        self.adapter = adapter
        self.repository = repository

    def run(
        self,
        codes: Optional[List[str]],
        start_date: str,
        end_date: str,
    ) -> Dict[str, Any]:
        """
        - ``codes is None``：不传 ``ts_code``，按 **trade_date** 逐日拉全市场；若单日返回行数触顶 5000，
          自动按 ``exchange``（SSE/SZSE/BSE）拆分再合并，减轻静默截断。
        - ``codes`` 非空：按代码 + 日期区间拉取（兼容 ``--codes`` / ``--core`` 等）。
        """
        if codes is None:
            return self._run_by_trade_dates(start_date, end_date)
        results: List[EtfShareSizeResult] = []
        for i, code in enumerate(codes, 1):
            if i % 50 == 0 or i == 1 or i == len(codes):
                print(f"  [{i}/{len(codes)}] etf_share_size {code}...")
            try:
                df = fetch_etf_share_size_chunked(self.adapter, code, start_date, end_date)
            except Exception as ex:
                results.append(EtfShareSizeResult(code, 0, [str(ex)]))
                continue
            if df is None or df.empty:
                results.append(EtfShareSizeResult(code, 0, ["no_data"]))
                continue
            recs = records_from_share_size_df(df, code)
            if not recs:
                results.append(EtfShareSizeResult(code, 0, ["no_records"]))
                continue
            n = self.repository.upsert_etf_share_size(recs)
            results.append(EtfShareSizeResult(code, n, []))
        return {"mode": "by_ts_code", "run_codes": codes, "results": [r.__dict__ for r in results]}

    def _run_by_trade_dates(self, start_date: str, end_date: str) -> Dict[str, Any]:
        """不传 ts_code，按 trade_date 拉全市场（单日一行多 ETF）。"""
        days = _yyyymmdd_range(start_date, end_date)
        day_results: List[Dict[str, Any]] = []
        for j, td in enumerate(days, 1):
            if j == 1 or j == len(days) or j % 10 == 0:
                print(f"  [{j}/{len(days)}] etf_share_size trade_date={td} (all ts_code)...")
            try:
                df = _fetch_etf_share_size_full_market_one_day(self.adapter, td)
            except Exception as ex:
                day_results.append(
                    {"trade_date": td, "inserted": 0, "rowcount": 0, "issues": [str(ex)]}
                )
                continue
            if df is None or df.empty:
                day_results.append(
                    {"trade_date": td, "inserted": 0, "rowcount": 0, "issues": []}
                )
                continue
            recs = records_from_share_size_df(df, "")
            if not recs:
                day_results.append(
                    {
                        "trade_date": td,
                        "inserted": 0,
                        "rowcount": int(len(df)),
                        "issues": ["no_records_after_normalize"],
                    }
                )
                continue
            n = self.repository.upsert_etf_share_size(recs)
            day_results.append(
                {"trade_date": td, "inserted": n, "rowcount": int(len(df)), "issues": []}
            )
        return {
            "mode": "by_trade_date",
            "start_date": start_date,
            "end_date": end_date,
            "days": len(days),
            "results": day_results,
        }


__all__ = [
    "EtfShareSizePipeline",
    "EtfShareSizeResult",
    "fetch_etf_share_size_chunked",
    "records_from_share_size_df",
]
