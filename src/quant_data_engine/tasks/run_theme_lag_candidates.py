"""
主题补涨候选：主线（连板天梯 ``sector_ranking``）对齐东财概念，与指数成分求交，
按 ``volume_price.pct_chg`` **严格小于**可配置上限筛选，每桶最多 N 只。

依赖 ``quant_data``：``limit_up_ladder``、``dc_concept_index``、``dc_concept_member``、
``index_constituents``、``volume_price``、``daily_limit_up``。

环境变量（可被 CLI 覆盖）::

  THEME_CANDIDATE_MAX_PCT_CHG=3   # 候选当日涨幅须 < 该值（百分点，与 volume_price 一致）
  THEME_MAINLINE_TOP_K=5          # 资金主线桶数量上限
  THEME_PER_BUCKET_MAX=3          # 每桶最多推荐股数

用法::

  python -m quant_data_engine.tasks.run_theme_lag_candidates --trade-date 20250425
  python -m quant_data_engine.tasks.run_theme_lag_candidates --index-store-key csi1000 --dry-run
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Tuple

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from pymongo import MongoClient

from quant_data_engine.adapters.stock_adapter import StockAdapter
from quant_data_engine.config.settings import Settings, get_settings
from quant_data_engine.repositories.dc_concept_repository import _norm_yyyymmdd
from quant_data_engine.repositories.theme_lag_repository import ThemeLagRepository
from quant_data_engine.utils.ingestion_job import record_ingestion_run

logger = logging.getLogger(__name__)


def _float_env(name: str, default: float) -> float:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _int_env(name: str, default: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def get_open_days(sa: StockAdapter, start: str, end: str) -> List[str]:
    df = sa.fetch_trade_cal(exchange="SSE", start_date=start, end_date=end)
    if df is None or df.empty:
        return []
    o = df[df["is_open"] == 1]["cal_date"].astype(str).str.replace("-", "")
    return sorted(o.tolist())


def resolve_trade_date(sa: StockAdapter, explicit: Optional[str]) -> str:
    if explicit:
        s = "".join(c for c in str(explicit) if c.isdigit())[:8]
        if len(s) != 8:
            raise ValueError(f"trade_date must be YYYYMMDD, got {explicit!r}")
        return s
    end = datetime.now().strftime("%Y%m%d")
    start = (datetime.now() - timedelta(days=45)).strftime("%Y%m%d")
    days = get_open_days(sa, start, end)
    if not days:
        raise RuntimeError("trade_cal returned no open days")
    return days[-1]


def _load_dc_index_by_name(
    index_coll: Any, trade_date: str
) -> Tuple[str, Dict[str, Dict[str, Any]]]:
    """返回实际使用的 ``dc_concept_index.trade_date`` 与 ``name.strip() -> 行``。"""
    td = _norm_yyyymmdd(trade_date)
    rows = list(index_coll.find({"trade_date": td}, projection={"_id": 0}))
    eff = td
    if not rows:
        doc = index_coll.find_one(sort=[("trade_date", -1)], projection={"trade_date": 1})
        if not doc:
            return td, {}
        eff = _norm_yyyymmdd(doc.get("trade_date", ""))
        rows = list(index_coll.find({"trade_date": eff}, projection={"_id": 0}))
        logger.warning("dc_concept_index 无 %s，回退使用 trade_date=%s", td, eff)
    name_map: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        nm = str(r.get("name", "")).strip()
        if nm:
            name_map[nm] = r
    return eff, name_map


def _match_concept_row(sector: str, name_map: Dict[str, Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    s = (sector or "").strip()
    if not s:
        return None
    if s in name_map:
        return name_map[s]
    partial: List[Dict[str, Any]] = []
    for name, row in name_map.items():
        if s in name or name in s:
            partial.append(row)
    if len(partial) == 1:
        return partial[0]
    return None


def _concept_index_pct(row: Dict[str, Any]) -> Optional[float]:
    for k in ("pct_chg", "pct_change", "change"):
        v = row.get(k)
        if v is None:
            continue
        try:
            return float(v)
        except (TypeError, ValueError):
            continue
    return None


def _member_codes_latest(mem_coll: Any, concept_ts_code: str, trade_date: str) -> Tuple[Optional[str], List[str]]:
    td = _norm_yyyymmdd(trade_date)
    agg = list(
        mem_coll.aggregate(
            [
                {"$match": {"concept_ts_code": concept_ts_code, "as_of_date": {"$lte": td}}},
                {"$group": {"_id": None, "mx": {"$max": "$as_of_date"}}},
            ]
        )
    )
    if not agg or not agg[0].get("mx"):
        return None, []
    mx = str(agg[0]["mx"])
    codes = mem_coll.distinct("con_code", {"concept_ts_code": concept_ts_code, "as_of_date": mx})
    out = [str(c).strip() for c in codes if c and str(c).strip()]
    return mx, out


def _constituent_symbols(ic_coll: Any, store_key: str, trade_date: str) -> Tuple[Optional[str], Optional[str], Set[str]]:
    """返回 ``(weight_trade_date, update_date, symbols)``。"""
    td = _norm_yyyymmdd(trade_date)
    doc = ic_coll.find_one(
        {"index_code": store_key, "weight_trade_date": {"$lte": td}},
        sort=[("weight_trade_date", -1), ("update_date", -1)],
    )
    if not doc:
        return None, None, set()
    wt = str(doc.get("weight_trade_date", "")).replace("-", "")[:8]
    ud = str(doc.get("update_date", "")).replace("-", "")[:8]
    syms = ic_coll.distinct("symbol", {"index_code": store_key, "weight_trade_date": wt, "update_date": ud})
    return wt, ud, {str(s).strip() for s in syms if s and str(s).strip()}


def _limit_up_symbols(dlu_coll: Any, trade_date: str) -> Set[str]:
    td = _norm_yyyymmdd(trade_date)
    cur = dlu_coll.find(
        {"date": td, "status": "limit_up"},
        projection={"symbol": 1, "_id": 0},
    )
    return {str(d["symbol"]).strip() for d in cur if d.get("symbol")}


def _volume_price_map(vp_coll: Any, trade_date: str, symbols: Set[str]) -> Dict[str, Dict[str, Any]]:
    td = _norm_yyyymmdd(trade_date)
    if not symbols:
        return {}
    syms = list(symbols)
    out: Dict[str, Dict[str, Any]] = {}
    # Mongo $in 长度限制，分批
    batch = 500
    for i in range(0, len(syms), batch):
        chunk = syms[i : i + batch]
        for d in vp_coll.find({"trade_date": td, "symbol": {"$in": chunk}}, projection={"_id": 0}):
            sym = str(d.get("symbol", "")).strip()
            if sym:
                out[sym] = d
    return out


def _normalize_to_index_symbol(con: str, index_set: Set[str]) -> Optional[str]:
    c = str(con).strip()
    if not c:
        return None
    if c in index_set:
        return c
    base = c.split(".", 1)[0] if "." in c else c
    if not base.isdigit() or len(base) != 6:
        return None
    first = base[0]
    if first == "6":
        cand = f"{base}.SH"
    elif first in ("0", "3"):
        cand = f"{base}.SZ"
    elif first in ("4", "8", "9"):
        cand = f"{base}.BJ"
    else:
        cand = f"{base}.SH"
    if cand in index_set:
        return cand
    for suf in (".SH", ".SZ", ".BJ"):
        t = f"{base}{suf}"
        if t in index_set:
            return t
    return None


def run(
    settings: Settings,
    trade_date: str,
    index_store_key: str,
    mainline_top_k: int,
    per_bucket_max: int,
    max_candidate_pct: float,
    dry_run: bool,
) -> int:
    td = _norm_yyyymmdd(trade_date)
    client = MongoClient(settings.mongo_uri)
    db = client[settings.data_db]

    ladder = db["limit_up_ladder"].find_one({"date": td})
    if not ladder:
        raise RuntimeError(f"limit_up_ladder 无 date={td}，请先跑连板天梯同步")

    sector_ranking = ladder.get("sector_ranking") or []
    if not sector_ranking:
        raise RuntimeError(f"limit_up_ladder date={td} 无 sector_ranking")

    dc_index_coll = db["dc_concept_index"]
    dc_mem_coll = db["dc_concept_member"]
    ic_coll = db["index_constituents"]
    vp_coll = db["volume_price"]
    dlu_coll = db["daily_limit_up"]

    dc_eff_date, name_map = _load_dc_index_by_name(dc_index_coll, td)
    if not name_map:
        raise RuntimeError("dc_concept_index 无可用数据，请先跑 run_dc_concept_sync --sync index")

    wt, ud, index_syms = _constituent_symbols(ic_coll, index_store_key, td)
    if not index_syms:
        raise RuntimeError(
            f"index_constituents 无 index_code={index_store_key!r} 且 weight_trade_date<={td} 的成分，请先同步指数成分"
        )

    limit_up_syms = _limit_up_symbols(dlu_coll, td)

    ranking_slice = sector_ranking[: max(1, mainline_top_k)]
    out_docs: List[Dict[str, Any]] = []

    for bucket_rank, item in enumerate(ranking_slice, start=1):
        sector = str(item.get("sector", "")).strip()
        if not sector:
            continue
        row = _match_concept_row(sector, name_map)
        if not row:
            logger.info("跳过主线（无法对齐东财概念）: sector=%r", sector)
            continue
        concept_ts = str(row.get("ts_code", "")).strip()
        concept_name = str(row.get("name", "")).strip() or sector
        if not concept_ts:
            continue
        mx, member_codes = _member_codes_latest(dc_mem_coll, concept_ts, td)
        if not member_codes:
            logger.info("主线无成分: %s %s", concept_ts, concept_name)
            continue
        members_set: Set[str] = set()
        for m in member_codes:
            n = _normalize_to_index_symbol(m, index_syms)
            if n:
                members_set.add(n)
        pool = members_set & index_syms
        pool -= limit_up_syms
        if not pool:
            logger.info("交集为空: bucket=%s", concept_name)
            continue

        vp_map = _volume_price_map(vp_coll, td, pool)
        candidates: List[Tuple[float, float, str, str]] = []
        for sym in pool:
            vp = vp_map.get(sym)
            if not vp:
                continue
            p = vp.get("pct_chg")
            if p is None:
                continue
            try:
                pf = float(p)
            except (TypeError, ValueError):
                continue
            if pf >= max_candidate_pct:
                continue
            amt = vp.get("amount")
            try:
                af = float(amt) if amt is not None else 0.0
            except (TypeError, ValueError):
                af = 0.0
            nm = str(vp.get("name", "") or "").strip()
            if not nm:
                ic_doc = ic_coll.find_one(
                    {"index_code": index_store_key, "symbol": sym},
                    sort=[("update_date", -1)],
                    projection={"name": 1},
                )
                nm = str(ic_doc.get("name", "") if ic_doc else "") or sym
            candidates.append((pf, -af, sym, nm))

        candidates.sort(key=lambda x: (x[0], x[1]))
        top = candidates[: max(1, per_bucket_max)]
        c_pct = _concept_index_pct(row)

        for pf, _neg_amt, sym, nm in top:
            out_docs.append(
                {
                    "trade_date": td,
                    "index_store_key": index_store_key,
                    "bucket_id": concept_ts,
                    "bucket_name": concept_name,
                    "bucket_rank": bucket_rank,
                    "matched_sector": sector,
                    "concept_index_trade_date": dc_eff_date,
                    "concept_index_pct": c_pct,
                    "member_as_of": mx,
                    "constituent_weight_trade_date": wt,
                    "constituent_update_date": ud,
                    "symbol": sym,
                    "name": nm,
                    "pct_chg": pf,
                    "max_candidate_pct": max_candidate_pct,
                }
            )

    logger.info(
        "theme_lag_candidates trade_date=%s index=%s rows=%s (mainline_k=%s per_bucket=%s pct_lt=%s)",
        td,
        index_store_key,
        len(out_docs),
        mainline_top_k,
        per_bucket_max,
        max_candidate_pct,
    )
    if dry_run:
        return len(out_docs)
    repo = ThemeLagRepository(settings.mongo_uri, settings.data_db)
    n = repo.replace_for_trade_date(td, index_store_key, out_docs)
    record_ingestion_run(
        settings,
        "theme_lag_candidates",
        td,
        status="complete",
        records_changed=n,
        index_store_key=index_store_key,
    )
    return n


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser(
        description="主题补涨候选：天梯主线 ∩ 东财概念 ∩ 指数成分，pct_chg < 阈值",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
涨幅阈值（默认 3）:
  使用 volume_price.pct_chg，与 Tushare 一致为「百分点」；候选须满足 pct_chg < 阈值（严格小于）。

默认规模:
  主线 Top-K 默认 5（环境变量 THEME_MAINLINE_TOP_K），每桶最多 3 只（THEME_PER_BUCKET_MAX）。
        """,
    )
    p.add_argument("--trade-date", default="", help="业务日 YYYYMMDD；默认最近 A 股开市日")
    p.add_argument("--index-store-key", default="csi1000", help="index_constituents.index_code，默认中证1000")
    p.add_argument(
        "--mainline-top-k",
        type=int,
        default=0,
        help="主线桶数量；0 表示用环境变量 THEME_MAINLINE_TOP_K（默认 5）",
    )
    p.add_argument(
        "--per-bucket-max",
        type=int,
        default=0,
        help="每桶最多股票数；0 表示用环境变量 THEME_PER_BUCKET_MAX（默认 3）",
    )
    p.add_argument(
        "--max-candidate-pct",
        type=float,
        default=None,
        help="候选 pct_chg 须小于该值（百分点）；默认环境变量 THEME_CANDIDATE_MAX_PCT_CHG 或 3",
    )
    p.add_argument("--dry-run", action="store_true", help="不落库、不写 job_status")
    args = p.parse_args()

    settings = get_settings()
    stock_ad = StockAdapter(settings.tushare_token or "", rate_limit_per_sec=settings.rate_limit_per_sec)
    if not stock_ad.enabled:
        raise SystemExit("StockAdapter 不可用（Tushare）")

    td = resolve_trade_date(stock_ad, args.trade_date.strip() or None)
    mk = args.mainline_top_k if args.mainline_top_k > 0 else _int_env("THEME_MAINLINE_TOP_K", 5)
    pb = args.per_bucket_max if args.per_bucket_max > 0 else _int_env("THEME_PER_BUCKET_MAX", 3)
    mxp = (
        float(args.max_candidate_pct)
        if args.max_candidate_pct is not None
        else _float_env("THEME_CANDIDATE_MAX_PCT_CHG", 3.0)
    )

    n = run(settings, td, args.index_store_key.strip() or "csi1000", mk, pb, mxp, args.dry_run)
    logger.info("done rows=%s dry_run=%s", n, args.dry_run)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.exception("%s", e)
        try:
            settings = get_settings()
            sa = StockAdapter(settings.tushare_token or "", rate_limit_per_sec=settings.rate_limit_per_sec)
            td = resolve_trade_date(sa, None) if sa.enabled else datetime.now().strftime("%Y%m%d")
            if not isinstance(e, SystemExit):
                record_ingestion_run(
                    settings,
                    "theme_lag_candidates",
                    td,
                    status="error",
                    error=str(e)[:2000],
                )
        except Exception:
            pass
        raise SystemExit(1) from e
