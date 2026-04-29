"""
东方财富概念板块落库：Tushare ``dc_index``（日更截面）+ ``dc_member``（成分，建议周更）。

用法::

  python -m quant_data_engine.tasks.run_dc_concept_sync --sync index
  python -m quant_data_engine.tasks.run_dc_concept_sync --sync members --trade-date 20250425
  python -m quant_data_engine.tasks.run_dc_concept_sync --sync all --max-concepts 50

环境变量：与工程一致 ``MONGO_URI`` / ``DATA_DB_NAME`` / ``TUSHARE_TOKEN`` / ``RATE_LIMIT_PER_SEC``。
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timedelta
from typing import List, Optional, Tuple

import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from quant_data_engine.adapters.dc_concept_adapter import DcConceptAdapter
from quant_data_engine.adapters.stock_adapter import StockAdapter
from quant_data_engine.config.settings import get_settings
from quant_data_engine.repositories.dc_concept_repository import DcConceptRepository
from quant_data_engine.utils.ingestion_job import record_ingestion_run

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def _norm_yyyymmdd(d: str) -> str:
    s = "".join(c for c in str(d) if c.isdigit())[:8]
    if len(s) != 8:
        raise ValueError(f"trade_date must be YYYYMMDD, got {d!r}")
    return s


def get_open_days(sa: StockAdapter, start: str, end: str) -> List[str]:
    df = sa.fetch_trade_cal(exchange="SSE", start_date=start, end_date=end)
    if df is None or df.empty:
        return []
    o = df[df["is_open"] == 1]["cal_date"].astype(str).str.replace("-", "")
    return sorted(o.tolist())


def resolve_trade_date(sa: StockAdapter, explicit: Optional[str]) -> str:
    if explicit:
        return _norm_yyyymmdd(explicit)
    end = datetime.now().strftime("%Y%m%d")
    start = (datetime.now() - timedelta(days=45)).strftime("%Y%m%d")
    days = get_open_days(sa, start, end)
    if not days:
        raise RuntimeError("无法从交易日历解析最近开市日（trade_cal 为空）")
    return days[-1]


def _prepare_index_df(df: pd.DataFrame, target_td: str) -> Tuple[pd.DataFrame, str]:
    """返回待写入的 DataFrame 与最终采用的 ``trade_date``（YYYYMMDD）。"""
    if df is None or df.empty:
        return df, target_td

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
        latest = tds.max()
        if latest:
            logger.warning(
                "dc_index 中无 trade_date=%s 的行，改用数据源中的最近日期 %s",
                target_td,
                latest,
            )
            sub2 = df[df["_td"] == latest].drop(columns=["_td"], errors="ignore")
            return sub2, latest
        return df.drop(columns=["_td"], errors="ignore"), target_td

    return df, target_td


def _concept_ts_codes_from_index_df(sub: pd.DataFrame) -> List[str]:
    if sub is None or sub.empty or "ts_code" not in sub.columns:
        return []
    s = sub["ts_code"].dropna().astype(str).str.strip()
    return sorted({x for x in s.tolist() if x})


def run_index(
    adapter: DcConceptAdapter,
    repo: DcConceptRepository,
    stock_ad: StockAdapter,
    trade_date_explicit: Optional[str],
    dry_run: bool,
) -> Tuple[str, List[str], int]:
    target = resolve_trade_date(stock_ad, trade_date_explicit)
    df = adapter.fetch_dc_index(src="dc")
    if df is None or df.empty:
        raise RuntimeError("dc_index 返回空，请检查 Tushare 权限/积分")

    sub, write_td = _prepare_index_df(df, target)
    if sub is None or sub.empty:
        raise RuntimeError("过滤后 dc_index 为空")

    concept_codes = _concept_ts_codes_from_index_df(sub)
    recs = sub.to_dict(orient="records")
    sync_time = datetime.now().isoformat()
    logger.info("dc_index rows=%s trade_date=%s", len(recs), write_td)
    if dry_run:
        return write_td, concept_codes, 0
    n = repo.upsert_index_rows(recs, trade_date=write_td, sync_time_iso=sync_time)
    repo.set_meta("index_last_trade_date", write_td)
    logger.info("dc_concept_index upsert count=%s", n)
    return write_td, concept_codes, n


def run_members(
    adapter: DcConceptAdapter,
    repo: DcConceptRepository,
    stock_ad: StockAdapter,
    trade_date_for_index: Optional[str],
    as_of_explicit: Optional[str],
    dry_run: bool,
    max_concepts: Optional[int],
    concept_codes_override: Optional[List[str]] = None,
) -> Tuple[str, int]:
    """返回 ``(as_of_date, upsert 计数合计)``。"""
    if concept_codes_override is not None:
        codes = list(concept_codes_override)
        td_list = trade_date_for_index or ""
    else:
        td_list = trade_date_for_index or repo.latest_index_trade_date()
        if not td_list:
            raise RuntimeError("无 dc_concept_index 数据：请先执行 --sync index")
        td_list = _norm_yyyymmdd(str(td_list))
        codes = repo.list_concept_ts_codes_for_trade_date(td_list)
    if not codes:
        raise RuntimeError(
            f"dc_concept_index 在 trade_date={td_list or trade_date_for_index} 下无 ts_code；"
            "请先执行 --sync index"
        )

    if max_concepts is not None and max_concepts > 0:
        codes = codes[: int(max_concepts)]

    if as_of_explicit:
        as_of = _norm_yyyymmdd(as_of_explicit)
    else:
        as_of = resolve_trade_date(stock_ad, None)

    logger.info(
        "dc_member concepts=%s as_of_date=%s (index_trade_date_hint=%s)",
        len(codes),
        as_of,
        td_list or trade_date_for_index or "",
    )
    total = 0
    for i, ccode in enumerate(codes):
        mdf = adapter.fetch_dc_member_for_concept(ccode, trade_date="")
        if mdf is None or mdf.empty:
            logger.debug("dc_member empty for %s", ccode)
            continue
        rows = mdf.to_dict(orient="records")
        if dry_run:
            total += len(rows)
            continue
        n = repo.upsert_member_rows(ccode, as_of, rows)
        total += n
        if (i + 1) % 50 == 0:
            logger.info("... members progress %s/%s", i + 1, len(codes))

    if dry_run:
        logger.info("[dry-run] would upsert approximately %s member rows (rough)", total)
        return as_of, 0
    repo.set_meta("members_last_as_of", as_of)
    logger.info("dc_concept_member upsert total field=%s (modified+upserted rows)", total)
    return as_of, total


def main() -> None:
    p = argparse.ArgumentParser(description="东方财富概念 dc_index / dc_member 落 quant_data")
    p.add_argument("--sync", choices=("index", "members", "all"), default="all")
    p.add_argument("--trade-date", default="", help="index 业务日 YYYYMMDD；默认最近 A 股开市日")
    p.add_argument(
        "--as-of-date",
        default="",
        help="members 版本戳 as_of_date YYYYMMDD；默认最近开市日（可与 index 列表 trade_date 不同）",
    )
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--max-concepts", type=int, default=0, help="仅拉取前 N 个概念成分（0 表示不限制）")
    args = p.parse_args()

    settings = get_settings()
    if not settings.tushare_token:
        raise SystemExit("TUSHARE_TOKEN 未设置")

    stock_ad = StockAdapter(settings.tushare_token, rate_limit_per_sec=settings.rate_limit_per_sec)
    if not stock_ad.enabled:
        raise SystemExit("StockAdapter 未启用（tushare 不可用）")

    adapter = DcConceptAdapter(settings.tushare_token, rate_limit_per_sec=settings.rate_limit_per_sec)
    if not adapter.enabled:
        raise SystemExit("DcConceptAdapter 未启用")

    repo = DcConceptRepository(settings.mongo_uri, settings.data_db)
    explicit_td = args.trade_date.strip() or None
    max_c = args.max_concepts if args.max_concepts and args.max_concepts > 0 else None
    as_of_e = args.as_of_date.strip() or None

    index_write_td: Optional[str] = None
    index_concept_codes: Optional[List[str]] = None
    n_index = 0
    n_members = 0
    if args.sync in ("index", "all"):
        index_write_td, index_concept_codes, n_index = run_index(adapter, repo, stock_ad, explicit_td, args.dry_run)

    if args.sync in ("members", "all"):
        td_for_list = index_write_td if index_write_td else explicit_td
        override = index_concept_codes if index_concept_codes is not None else None
        _, n_members = run_members(
            adapter,
            repo,
            stock_ad,
            td_for_list,
            as_of_e,
            args.dry_run,
            max_c,
            concept_codes_override=override,
        )

    date_for_job = index_write_td or explicit_td or repo.latest_index_trade_date() or resolve_trade_date(
        stock_ad, None
    )
    if not args.dry_run:
        record_ingestion_run(
            settings,
            "dc_concept_sync",
            _norm_yyyymmdd(str(date_for_job)),
            status="complete",
            records_changed=int(n_index) + int(n_members),
            sync_mode=args.sync,
        )
    logger.info("done sync=%s dry_run=%s", args.sync, args.dry_run)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.exception("dc_concept_sync failed: %s", e)
        try:
            settings = get_settings()
            sa = StockAdapter(settings.tushare_token or "", rate_limit_per_sec=settings.rate_limit_per_sec)
            td = resolve_trade_date(sa, None) if sa.enabled else datetime.now().strftime("%Y%m%d")
            record_ingestion_run(
                settings,
                "dc_concept_sync",
                td,
                status="error",
                error=str(e)[:2000],
            )
        except Exception:
            pass
        raise SystemExit(1) from e
