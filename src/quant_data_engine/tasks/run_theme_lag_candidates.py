"""
主题补涨候选：主线（连板天梯 ``sector_ranking`` 的 ``sector``，即当日涨停列表里的 ``limit_reason``：
**推荐**与 ``run_ladder_sync --source eastmoney`` 一致——此时为东财涨停池字段 ``hybk``（行业板块标签），
与 Tushare ``dc_index`` 的「行业板块」口径一致；若天梯用 ``--source tushare``，则 ``limit_reason`` 来自
``limit_list_d`` 的 ``industry``，与东财行业名可能不一致，宜改用东财源或另行对齐。
主线名称对齐东财 ``dc_index``（**优先行业板块、其次概念板块**）与指数成分求交，
按 ``volume_price.pct_chg`` 落在 **(min, max)** 开区间内筛选（默认 **大于 0 且小于 3** 个百分点），每桶最多 N 只。

东财概念板块与成分 **仅通过 Tushare** ``dc_index`` / ``dc_member`` 在线拉取（**不落** ``dc_concept_index`` / ``dc_concept_member``）；
无精确 ``trade_date`` 截面或接口无数据时 **直接抛错退出**。

依赖 ``quant_data``：``limit_up_ladder``、``index_constituents``、``volume_price``、``daily_limit_up``。
另需 ``TUSHARE_TOKEN``。

环境变量（可被 CLI 覆盖）::

  THEME_CANDIDATE_MIN_PCT_CHG=0   # 下界：候选须 pct_chg > 该值（默认 0 即严格为正涨幅）
  THEME_CANDIDATE_MAX_PCT_CHG=3   # 上界：候选须 pct_chg < 该值（百分点，与 volume_price 一致）
  THEME_MAINLINE_TOP_K=5          # 资金主线桶数量上限
  THEME_PER_BUCKET_MAX=3          # 每桶最多推荐股数
  THEME_LAG_DEBUG_NAMES=          # 设为 1/true/yes/on：额外打印带 [THEME_LAG_DEBUG_NAMES] 的采样/提示（仍为 INFO 级，默认关闭）

用法::

  python -m quant_data_engine.tasks.run_theme_lag_candidates --trade-date 20250425
  python -m quant_data_engine.tasks.run_theme_lag_candidates --index-store-key csi1000 --dry-run
  python -m quant_data_engine.tasks.run_theme_lag_candidates --skip-narrative-task   # 不入队叙事任务
  python -m quant_data_engine.tasks.run_theme_lag_candidates --force-narrative-task  # 删旧任务后重入队
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from pymongo import MongoClient
import pandas as pd

from quant_data_engine.adapters.dc_concept_adapter import DcConceptAdapter
from quant_data_engine.adapters.stock_adapter import StockAdapter
from quant_data_engine.config.settings import Settings, get_settings
from quant_data_engine.repositories.dc_concept_repository import _norm_yyyymmdd
from quant_data_engine.repositories.theme_lag_repository import ThemeLagRepository
from quant_data_engine.utils.ingestion_job import record_ingestion_run

logger = logging.getLogger(__name__)

THEME_LAG_NARRATIVE_TASK_TYPE = "theme_lag_narrative"


def _serialize_ranking_item(item: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k in ("sector", "count", "score", "avg_streak"):
        if k not in item:
            continue
        v = item[k]
        if isinstance(v, float):
            out[k] = round(v, 4)
        else:
            out[k] = v
    return out


def enqueue_theme_lag_narrative_task(
    settings: Settings,
    narrative_params: Dict[str, Any],
    *,
    force: bool = False,
) -> None:
    """向 ``finance.analysis_tasks`` 插入叙事任务；默认幂等跳过 pending/processing/completed。"""
    td = narrative_params.get("trade_date")
    sk = narrative_params.get("index_store_key")
    if not td or not sk:
        logger.warning("enqueue_theme_lag_narrative_task: missing trade_date or index_store_key")
        return

    finance = MongoClient(settings.mongo_uri)[settings.mongo_db]
    col = finance["analysis_tasks"]
    q_base: Dict[str, Any] = {
        "type": THEME_LAG_NARRATIVE_TASK_TYPE,
        "task_type": THEME_LAG_NARRATIVE_TASK_TYPE,
        "params.trade_date": td,
        "params.index_store_key": sk,
    }
    if force:
        r = col.delete_many(q_base)
        if r.deleted_count:
            logger.info("force: 已删除既有 theme_lag_narrative 任务 %s 条", r.deleted_count)
    else:
        ex = col.find_one({**q_base, "status": {"$in": ["pending", "processing", "completed"]}})
        if ex:
            logger.info(
                "theme_lag_narrative 任务已存在(status=%s)，跳过入队 _id=%s",
                ex.get("status"),
                ex.get("_id"),
            )
            return

    doc: Dict[str, Any] = {
        "type": THEME_LAG_NARRATIVE_TASK_TYPE,
        "task_type": THEME_LAG_NARRATIVE_TASK_TYPE,
        "status": "pending",
        "user_id": "system",
        "created_by_user_id": "system",
        "priority": int(os.getenv("THEME_LAG_NARRATIVE_PRIORITY", "0")),
        "created_at": datetime.now(),
        "params": narrative_params,
    }
    ins = col.insert_one(doc)
    logger.info("已入队 theme_lag_narrative 任务 _id=%s", ins.inserted_id)


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


def _truthy_env(name: str) -> bool:
    v = os.getenv(name, "").strip().lower()
    return v in ("1", "true", "yes", "on")


def _sample_sorted_dc_names(name_map: Dict[str, Dict[str, Any]], max_names: int = 20) -> str:
    """按名排序后取前若干，用于调试采样（过长则截断）。"""
    keys = sorted(name_map.keys())
    if not keys:
        return ""
    take = min(max_names, len(keys))
    chunk = keys[:take]
    s = "、".join(chunk)
    if len(keys) > take:
        s += f" …(共{len(keys)}个)"
    if len(s) > 480:
        s = s[:477] + "…"
    return s


def _debug_nomatch_hints(
    sector: str,
    industry_map: Dict[str, Dict[str, Any]],
    concept_map: Dict[str, Dict[str, Any]],
    max_each: int = 5,
) -> str:
    """与 sector 存在互为子串关系的板块名（各侧最多 max_each 条），便于核对写法差异。"""
    s = (sector or "").strip()
    if len(s) < 2:
        return "submatch_industry=— submatch_concept=—"
    ind_h: List[str] = []
    cpt_h: List[str] = []
    for n in sorted(industry_map.keys()):
        if s in n or n in s:
            ind_h.append(n)
            if len(ind_h) >= max_each:
                break
    for n in sorted(concept_map.keys()):
        if s in n or n in s:
            cpt_h.append(n)
            if len(cpt_h) >= max_each:
                break
    return (
        f"submatch_industry={ind_h or '—'} submatch_concept={cpt_h or '—'}"
    )


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


def _prepare_dc_index_df(df: pd.DataFrame, target_td: str) -> Tuple[pd.DataFrame, str]:
    """自 Tushare ``dc_index`` 返回的 DataFrame 中截取 ``target_td`` 截面；缺日或无列则抛错。"""
    if df is None or df.empty:
        raise RuntimeError("dc_index 返回空，退出")

    def norm_cell(x: object) -> str:
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return ""
        s = str(x).strip().replace("-", "")[:8]
        digits = "".join(c for c in s if c.isdigit())
        return digits[:8] if len(digits) >= 8 else digits

    if "trade_date" in df.columns:
        tds = df["trade_date"].map(norm_cell)
        df = df.copy()
        df["_td"] = tds
        sub = df[df["_td"] == target_td]
        if not sub.empty:
            return sub.drop(columns=["_td"], errors="ignore"), target_td
        raise RuntimeError(
            f"dc_index 中无 trade_date={target_td} 的截面，退出（不采用其它日期）"
        )

    raise RuntimeError("dc_index 响应缺少 trade_date 列，无法按业务日对齐，退出")


def _load_dc_index_name_map(
    adapter: DcConceptAdapter, anchor_td: str, idx_type: str
) -> Tuple[str, Dict[str, Dict[str, Any]]]:
    """在线拉 Tushare ``dc_index``（指定 ``idx_type``），构建 ``name -> 行`` 映射。"""
    if not adapter.enabled:
        raise RuntimeError("DcConceptAdapter 不可用（请配置 TUSHARE_TOKEN）")
    td = _norm_yyyymmdd(anchor_td)
    df = adapter.fetch_dc_index(trade_date=td, idx_type=idx_type)
    sub, write_td = _prepare_dc_index_df(df, td)
    if sub is None or sub.empty:
        raise RuntimeError("过滤后 dc_index 为空")
    name_map: Dict[str, Dict[str, Any]] = {}
    for r in sub.to_dict(orient="records"):
        nm = str(r.get("name", "")).strip()
        if nm:
            name_map[nm] = r
    return write_td, name_map


def _member_codes_from_tushare(
    adapter: DcConceptAdapter, concept_ts_code: str, anchor_td: str
) -> Tuple[str, List[str]]:
    """在线拉 ``dc_member``；无数据或无成分列则抛错。"""
    td = _norm_yyyymmdd(anchor_td)
    mdf = adapter.fetch_dc_member_for_concept(concept_ts_code, trade_date=td)
    if mdf is None or mdf.empty:
        raise RuntimeError(
            f"Tushare dc_member 无数据: ts_code={concept_ts_code!r} trade_date={td!r}"
        )
    col = "con_code" if "con_code" in mdf.columns else ("ts_code" if "ts_code" in mdf.columns else None)
    if not col:
        raise RuntimeError(
            f"dc_member 返回无 con_code/ts_code 列: ts_code={concept_ts_code!r} trade_date={td}"
        )
    out: List[str] = []
    for _, row in mdf.iterrows():
        v = row.get(col)
        if v is None or (isinstance(v, float) and pd.isna(v)):
            continue
        s = str(v).strip()
        if s:
            out.append(s)
    if not out:
        raise RuntimeError(
            f"dc_member 有行但未解析出成分代码: ts_code={concept_ts_code!r} trade_date={td}"
        )
    return td, sorted(set(out))


def _match_in_name_map(sector: str, name_map: Dict[str, Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """在单层 ``name_map`` 内匹配：精确、唯一子串；或多条均为东财「同级后缀 Ⅱ/Ⅲ…」时取 Ⅱ 层（确定性）。"""
    s = (sector or "").strip()
    if not s or not name_map:
        return None
    if s in name_map:
        return name_map[s]
    partial: List[Tuple[str, Dict[str, Any]]] = []
    for name, row in name_map.items():
        if s in name or name in s:
            partial.append((name, row))
    if not partial:
        return None
    if len(partial) == 1:
        return partial[0][1]

    names = [n for n, _ in partial]
    roman_marks = ("Ⅱ", "Ⅲ", "Ⅳ", "Ⅰ")

    def strip_roman_suffix(n: str) -> Optional[str]:
        for mk in roman_marks:
            if n.endswith(mk):
                return n[: -len(mk)]
        return None

    bases = {strip_roman_suffix(n) for n in names}
    bases.discard(None)
    if len(bases) == 1 and all(strip_roman_suffix(n) is not None for n in names):
        # 仅 航空装备Ⅱ/Ⅲ、家电零部件Ⅱ/Ⅲ 这类同级分叉：固定取 Ⅱ（与申万/简写主线对齐时较常用）
        def roman_order(n: str) -> int:
            for i, mk in enumerate(roman_marks):
                if n.endswith(mk):
                    return i
            return 99

        partial.sort(key=lambda nr: (roman_order(nr[0]), len(nr[0]), nr[0]))
        return partial[0][1]

    prefix_hits = [(n, r) for n, r in partial if n.startswith(s)]
    if len(prefix_hits) == 1:
        return prefix_hits[0][1]

    return None


def _match_concept_row(
    sector: str,
    industry_name_map: Dict[str, Dict[str, Any]],
    concept_name_map: Dict[str, Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    """将 ``sector``（= 涨停池 ``limit_reason``；东财源下为 ``hybk``）对齐 ``dc_index``：先行业板块、后概念板块。"""
    row = _match_in_name_map(sector, industry_name_map)
    if row:
        return row
    return _match_in_name_map(sector, concept_name_map)


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
    min_candidate_pct: float,
    max_candidate_pct: float,
    dry_run: bool,
    *,
    skip_narrative_task: bool = False,
    force_narrative_task: bool = False,
) -> int:
    td = _norm_yyyymmdd(trade_date)
    if max_candidate_pct <= min_candidate_pct:
        raise ValueError(
            f"max_candidate_pct ({max_candidate_pct}) 必须大于 min_candidate_pct ({min_candidate_pct})"
        )
    client = MongoClient(settings.mongo_uri)
    db = client[settings.data_db]

    ladder = db["limit_up_ladder"].find_one({"date": td})
    if not ladder:
        raise RuntimeError(f"limit_up_ladder 无 date={td}，请先跑连板天梯同步")

    sector_ranking = ladder.get("sector_ranking") or []
    if not sector_ranking:
        raise RuntimeError(f"limit_up_ladder date={td} 无 sector_ranking")

    dc_adapter = DcConceptAdapter(
        settings.tushare_token or "", rate_limit_per_sec=settings.rate_limit_per_sec
    )
    if not dc_adapter.enabled:
        raise RuntimeError("DcConceptAdapter 不可用（请配置 TUSHARE_TOKEN）")
    industry_map: Dict[str, Dict[str, Any]] = {}
    dc_eff_date_ind = ""
    try:
        dc_eff_date_ind, industry_map = _load_dc_index_name_map(dc_adapter, td, "行业板块")
    except RuntimeError as e:
        logger.warning("东财行业板块 dc_index 不可用，将仅用概念板块对齐: %s", e)

    dc_eff_date_cpt, concept_map = _load_dc_index_name_map(dc_adapter, td, "概念板块")
    dc_eff_date = dc_eff_date_ind or dc_eff_date_cpt
    if not industry_map and not concept_map:
        raise RuntimeError("行业板块与概念板块 dc_index 均无可用 name 行，退出")

    if _truthy_env("THEME_LAG_DEBUG_NAMES"):
        logger.info(
            "[THEME_LAG_DEBUG_NAMES] trade_date=%s |industry|=%s |concept|=%s sample_industry_names=%s",
            td,
            len(industry_map),
            len(concept_map),
            _sample_sorted_dc_names(industry_map, 20),
        )

    ic_coll = db["index_constituents"]
    vp_coll = db["volume_price"]
    dlu_coll = db["daily_limit_up"]

    wt, ud, index_syms = _constituent_symbols(ic_coll, index_store_key, td)
    if not index_syms:
        raise RuntimeError(
            f"index_constituents 无 index_code={index_store_key!r} 且 weight_trade_date<={td} 的成分，请先同步指数成分"
        )

    limit_up_syms = _limit_up_symbols(dlu_coll, td)

    ranking_slice = sector_ranking[: max(1, mainline_top_k)]
    out_docs: List[Dict[str, Any]] = []
    bucket_summaries: List[Dict[str, Any]] = []

    for bucket_rank, item in enumerate(ranking_slice, start=1):
        sector = str(item.get("sector", "")).strip()
        if not sector:
            continue
        row = _match_concept_row(sector, industry_map, concept_map)
        if not row:
            if _truthy_env("THEME_LAG_DEBUG_NAMES"):
                logger.info(
                    "跳过主线（无法对齐东财行业/概念板块）: sector=%r [%s]",
                    sector,
                    _debug_nomatch_hints(sector, industry_map, concept_map),
                )
            else:
                logger.info("跳过主线（无法对齐东财行业/概念板块）: sector=%r", sector)
            continue
        concept_ts = str(row.get("ts_code", "")).strip()
        concept_name = str(row.get("name", "")).strip() or sector
        if not concept_ts:
            continue
        mx, member_codes = _member_codes_from_tushare(dc_adapter, concept_ts, td)
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
        pool_scored: List[Tuple[float, float, str, str]] = []
        excluded_over: List[Dict[str, Any]] = []
        excluded_low: List[Dict[str, Any]] = []
        for sym in pool:
            vp = vp_map.get(sym)
            if not vp or vp.get("pct_chg") is None:
                continue
            try:
                pf = float(vp["pct_chg"])
            except (TypeError, ValueError):
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
            pool_scored.append((pf, -af, sym, nm))
            if pf >= max_candidate_pct:
                excluded_over.append({"symbol": sym, "name": nm, "pct_chg": round(pf, 4)})
            elif pf <= min_candidate_pct:
                excluded_low.append({"symbol": sym, "name": nm, "pct_chg": round(pf, 4)})

        excluded_over.sort(key=lambda d: -d["pct_chg"])
        excluded_over = excluded_over[:5]
        excluded_low.sort(key=lambda d: d["pct_chg"])
        excluded_low = excluded_low[:5]

        lag = [t for t in pool_scored if min_candidate_pct < t[0] < max_candidate_pct]
        lag.sort(key=lambda x: (x[0], x[1]))
        top = lag[: max(1, per_bucket_max)]
        runners = lag[max(1, per_bucket_max) : max(1, per_bucket_max) + 3]
        c_pct = _concept_index_pct(row)

        bucket_summaries.append(
            {
                "bucket_rank": bucket_rank,
                "ladder_item": _serialize_ranking_item(item),
                "matched_sector": sector,
                "concept_ts_code": concept_ts,
                "concept_name": concept_name,
                "concept_index_pct": c_pct,
                "member_as_of": mx,
                "pool_size_after_excluding_limit_up": len(pool),
                "symbols_with_volume_price_row": len(pool_scored),
                "candidates_matching_pct_range": len(lag),
                "pct_range_open_interval": {
                    "min_gt": min_candidate_pct,
                    "max_lt": max_candidate_pct,
                },
                "excluded_due_to_pct_gte_cap_sample": excluded_over,
                "excluded_due_to_pct_lte_min_sample": excluded_low,
                "picked": [{"symbol": x[2], "name": x[3], "pct_chg": round(x[0], 4)} for x in top],
                "runner_up_in_pct_range_not_picked": [
                    {"symbol": x[2], "name": x[3], "pct_chg": round(x[0], 4)} for x in runners
                ],
            }
        )

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
                    "min_candidate_pct": min_candidate_pct,
                    "max_candidate_pct": max_candidate_pct,
                }
            )

    logger.info(
        "theme_lag_candidates trade_date=%s index=%s rows=%s (mainline_k=%s per_bucket=%s pct_in=(%s,%s))",
        td,
        index_store_key,
        len(out_docs),
        mainline_top_k,
        per_bucket_max,
        min_candidate_pct,
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

    if not skip_narrative_task:
        narrative_params: Dict[str, Any] = {
            "trade_date": td,
            "index_store_key": index_store_key,
            "rules": {
                "min_candidate_pct": min_candidate_pct,
                "max_candidate_pct": max_candidate_pct,
                "mainline_top_k": mainline_top_k,
                "per_bucket_max": per_bucket_max,
                "sort": "Among pct in (min, max): pct_chg ascending, then amount descending (tie-breaker)",
            },
            "data_snapshot": {
                "dc_concept_index_trade_date": dc_eff_date,
                "constituent_weight_trade_date": wt,
                "constituent_update_date": ud,
            },
            "sector_ranking_top": [_serialize_ranking_item(x) for x in ranking_slice],
            "buckets": bucket_summaries,
        }
        enqueue_theme_lag_narrative_task(
            settings,
            narrative_params,
            force=force_narrative_task,
        )

    return n


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser(
        description="主题补涨候选：天梯主线 ∩ 东财概念 ∩ 指数成分，涨幅须在 (min,max) 内（默认 0<pct<3）",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
涨幅区间（百分点，与 volume_price.pct_chg 一致）:
  默认 min=0（即严格 pct_chg>0）、max=3（严格 pct_chg<3）：只选当日上涨且涨幅较小的成分股，排除下跌或平盘。
  环境变量 THEME_CANDIDATE_MIN_PCT_CHG / THEME_CANDIDATE_MAX_PCT_CHG；须满足 max > min。

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
        "--min-candidate-pct",
        type=float,
        default=None,
        help="候选 pct_chg 须大于该值（百分点）；默认环境变量 THEME_CANDIDATE_MIN_PCT_CHG 或 0（即严格为正）",
    )
    p.add_argument(
        "--max-candidate-pct",
        type=float,
        default=None,
        help="候选 pct_chg 须小于该值（百分点）；默认环境变量 THEME_CANDIDATE_MAX_PCT_CHG 或 3",
    )
    p.add_argument("--dry-run", action="store_true", help="不落库、不写 job_status、不入队叙事任务")
    p.add_argument(
        "--skip-narrative-task",
        action="store_true",
        help="落库候选后不入队 finance.analysis_tasks（theme_lag_narrative）",
    )
    p.add_argument(
        "--force-narrative-task",
        action="store_true",
        help="删除同日同指数既有 theme_lag_narrative 任务后重新入队（慎用）",
    )
    args = p.parse_args()

    settings = get_settings()
    stock_ad = StockAdapter(settings.tushare_token or "", rate_limit_per_sec=settings.rate_limit_per_sec)
    if not stock_ad.enabled:
        raise SystemExit("StockAdapter 不可用（Tushare）")

    td = resolve_trade_date(stock_ad, args.trade_date.strip() or None)
    mk = args.mainline_top_k if args.mainline_top_k > 0 else _int_env("THEME_MAINLINE_TOP_K", 5)
    pb = args.per_bucket_max if args.per_bucket_max > 0 else _int_env("THEME_PER_BUCKET_MAX", 3)
    mnp = (
        float(args.min_candidate_pct)
        if args.min_candidate_pct is not None
        else _float_env("THEME_CANDIDATE_MIN_PCT_CHG", 0.0)
    )
    mxp = (
        float(args.max_candidate_pct)
        if args.max_candidate_pct is not None
        else _float_env("THEME_CANDIDATE_MAX_PCT_CHG", 3.0)
    )

    n = run(
        settings,
        td,
        args.index_store_key.strip() or "csi1000",
        mk,
        pb,
        mnp,
        mxp,
        args.dry_run,
        skip_narrative_task=args.skip_narrative_task,
        force_narrative_task=args.force_narrative_task,
    )
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
