"""单标的/股票列表：机构深度（top10、解禁 6M、大宗半年）同步并写入 quant_data。

用法::

  python -m quant_data_engine.tasks.run_institutional_deep_sync --code 000001.SZ
  python -m quant_data_engine.tasks.run_institutional_deep_sync --codes 600519.SH,000001.SZ --no-mongo
  python -m quant_data_engine.tasks.run_institutional_deep_sync --code 000001.SZ --brief
  python -m quant_data_engine.tasks.run_institutional_deep_sync --code 300667.SZ --no-mongo --print-holders
  python -m quant_data_engine.tasks.run_institutional_deep_sync --code 000001.SZ --no-mongo --no-analysis
  python -m quant_data_engine.tasks.run_institutional_deep_sync --code 300667.SZ --no-mongo --debug-intl
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, timedelta
from typing import Any, Iterable

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from pymongo import MongoClient

from quant_data_engine.config.settings import get_settings
from quant_data_engine.adapters.stock_adapter import StockAdapter
from quant_data_engine.pipelines.institutional_pipeline import InstitutionalPipeline
from quant_data_engine.repositories.institutional_repository import InstitutionalRepository
from quant_data_engine.repositories.sw_industry_repository import SwIndustryRepository
from quant_data_engine.utils.ingestion_job import record_ingestion_run


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="机构持仓/解禁/大宗 深度分析数据同步到 Mongo (quant_data)"
    )
    p.add_argument(
        "--code", type=str, default="", help="单只 ts_code，如 000001.SZ"
    )
    p.add_argument(
        "--codes",
        type=str,
        default="",
        help="逗号分隔的 ts_code 列表，与 --code/--from-stock-info 三选一",
    )
    p.add_argument(
        "--from-stock-info",
        action="store_true",
        help="从 quant_data.stock_info 批量读取上市 A 股 ts_code",
    )
    p.add_argument(
        "--limit",
        type=int,
        default=0,
        help="批量模式最多处理多少只；0 表示不限制（慎用）",
    )
    p.add_argument(
        "--offset",
        type=int,
        default=0,
        help="批量模式跳过前 N 只（按 ts_code 排序后）",
    )
    p.add_argument(
        "--start-after",
        type=str,
        default="",
        help="批量模式仅处理 ts_code 大于该值的股票，便于分片续跑",
    )
    p.add_argument(
        "--stale-days",
        type=int,
        default=0,
        help="批量模式只处理 institutional_deep_snapshot 缺失或 as_of 早于 N 天前的股票；0 表示不过滤",
    )
    p.add_argument(
        "--no-mongo", action="store_true", help="只跑内存分析不落库"
    )
    p.add_argument(
        "--n-quarters", type=int, default=4, help="向过去拉取 N 个完整季度 end_date 截面"
    )
    p.add_argument(
        "--block-lookback", type=int, default=183, help="大宗/日线回溯自然日"
    )
    p.add_argument("--json", action="store_true", help="将每条结果打印为 JSON 一行")
    p.add_argument(
        "--brief",
        action="store_true",
        help="只打印 comp/opp/risk/tags 四列（与旧版一致）",
    )
    p.add_argument(
        "--print-holders",
        action="store_true",
        help="打印各期十大流通股东与两期 diff（含 norm 归一标签），用于人工核对",
    )
    p.add_argument(
        "--no-analysis",
        action="store_true",
        help="不生成/不打印 plan 分析结论（仅拉数+打分，适合调试拉数）",
    )
    p.add_argument(
        "--debug-intl",
        action="store_true",
        help="打印 new_intl 实体聚合明细，并在引擎内打 WARNING 日志（含 norms_seen/holder_names）",
    )
    return p.parse_args()


def _fmt_num(v: object, nd: int = 3) -> str:
    if v is None:
        return "n/a"
    try:
        f = float(v)  # type: ignore[arg-type]
        if f != f:  # NaN
            return "n/a"
        return f"{f:.{nd}f}"
    except (TypeError, ValueError):
        return str(v)


def _print_human_line(ts_code: str, r: dict, brief: bool) -> None:
    snap = r.get("snapshot") or {}
    if brief:
        print(
            f"{ts_code}\tcomp={snap.get('score_composite', '')}\t"
            f"opp={snap.get('score_opportunity', '')}\t"
            f"risk={snap.get('score_risk', '')}\t"
            f"tags={snap.get('tags', '')}"
        )
        return
    det = snap.get("detail") or {}
    fw = r.get("float_window") or {}
    ddiff = r.get("diff_float") or {}
    print(
        f"{ts_code}\t"
        f"comp={_fmt_num(snap.get('score_composite'), 3)}\t"
        f"opp={_fmt_num(snap.get('score_opportunity'), 3)}\t"
        f"risk={_fmt_num(snap.get('score_risk'), 3)}\t"
        f"intl_new={snap.get('new_intl_count', 0)}\t"
        f"intl_raw={det.get('new_intl_raw_count', '')}\t"
        f"r20={_fmt_num(snap.get('ret_20d'), 2)}\t"
        f"r60={_fmt_num(snap.get('ret_60d'), 2)}\t"
        f"float_tier={snap.get('float_risk_tier', '') or fw.get('risk_tier', 'n/a')}\t"
        f"float_rows6m={fw.get('rows_in_window', 0)}\t"
        f"block_dis={_fmt_num(snap.get('block_avg_discount_pct'), 2)}\t"
        f"new10_flo={det.get('new_holders_float', 0)}\t"
        f"exited10_flo={len(ddiff.get('exited_holders') or [])}\t"
        f"tags={snap.get('tags', '')}"
    )


def _print_holders_section(r: dict) -> None:
    hd = r.get("holder_debug")
    if not hd:
        return
    comp = hd.get("compare") or {}
    p_prev, p_curr = comp.get("prev"), comp.get("curr")
    print("\n======== 十大流通股东（Tushare top10_floatholders 原始行 + 本库 norm）========")
    for p in r.get("periods") or []:
        rows = (hd.get("by_period") or {}).get(p) or []
        print(f"\n--- 报告期 end_date = {p}  （{len(rows)} 户）---")
        for i, row in enumerate(rows, 1):
            nm = str(row.get("holder_name", ""))[:78]
            print(
                f"  {i:2d}  {nm}  |  amount={row.get('hold_amount')}  "
                f"ratio={row.get('hold_ratio')}  |  norm={row.get('norm')}"
            )
    ddiff = r.get("diff_float") or {}
    print(
        f"\n--- 两期对比（流通 float）: {p_prev}  ->  {p_curr}  ---"
    )
    print("  新进 (槽位):", len(ddiff.get("new_holders") or []))
    for x in ddiff.get("new_holders") or []:
        print("    +", x)
    print("  退出:", len(ddiff.get("exited_holders") or []))
    for x in ddiff.get("exited_holders") or []:
        print("    -", x)
    print("  new_intl (规则匹配为外资/通道，用于 intl_new 分):", len(ddiff.get("new_intl") or []))
    for x in ddiff.get("new_intl") or []:
        print("    *", x)
    print("  增持:", ddiff.get("increased") or [])
    print("  减持:", ddiff.get("decreased") or [])
    print("==================================================================\n")


def _print_intl_entity_debug(r: dict) -> None:
    summ = r.get("intl_entity_summary")
    det = (r.get("snapshot") or {}).get("detail") or {}
    if summ is None and not det.get("new_intl_by_entity"):
        return
    print("\n======== new_intl 实体聚合（机会分/intl_new 不含 HKSC 桶）========")
    if summ is not None:
        print(json.dumps(summ, ensure_ascii=False, indent=2))
    else:
        print(json.dumps({"from_detail": det.get("new_intl_by_entity")}, ensure_ascii=False, indent=2))
    print("==============================================\n")


def _print_analysis_section(r: dict) -> None:
    a = r.get("analysis")
    if not a:
        return
    print("\n======== 与计划书对齐的分析(数据时效、交叉验证、未覆盖项)========")
    print("【Tushare vs 终端】", a.get("tushare_vs_terminal_note", ""))
    print("【一段式摘要】", a.get("narrative_zh", ""))
    pl = a.get("plan_coverage") or {}
    if pl:
        print("【计划条款覆盖】")
        for k, v in pl.items():
            print(f"  - {k}: {v}")
    for label, key in (("机会侧信号", "positive_signals"), ("风险侧信号", "risk_signals")):
        items = a.get(key) or []
        if not items:
            print(f"【{label}】(无自动命中)")
            continue
        print(f"【{label}】")
        for it in items:
            print("  * ", it.get("title", ""))
            print("    ", it.get("detail", ""))
    acts = a.get("recommended_actions") or []
    if acts:
        print("【建议动作(非投资建议)】")
        for s in acts:
            print("  -", s)
    cav = a.get("caveats") or []
    if cav:
        print("【口径/免责提醒】")
        for s in cav:
            print("  -", s)
    print("==============================================\n")


def _normalize_ts_code(value: Any) -> str:
    s = str(value or "").strip().upper()
    if not s:
        return ""
    if "." in s:
        left, right = s.rsplit(".", 1)
        return f"{left.zfill(6)}.{right}"
    digits = "".join(ch for ch in s if ch.isdigit())
    if len(digits) != 6:
        return s
    if digits.startswith("6"):
        return f"{digits}.SH"
    if digits.startswith(("0", "3")):
        return f"{digits}.SZ"
    if digits.startswith(("4", "8")):
        return f"{digits}.BJ"
    return digits


def _is_listed_a_share(doc: dict[str, Any]) -> bool:
    code = _normalize_ts_code(doc.get("ts_code") or doc.get("symbol") or doc.get("code"))
    if not code or "." not in code:
        return False
    if not code.endswith((".SH", ".SZ", ".BJ")):
        return False
    status = str(doc.get("list_status") or doc.get("status") or "").upper()
    if status and status not in {"L", "LISTED", "上市"}:
        return False
    return True


def _stock_info_codes(settings, args: argparse.Namespace, today: date) -> list[str]:
    client = MongoClient(settings.mongo_uri)
    db = client[settings.data_db]
    stock_info = db["stock_info"]
    snapshot = db["institutional_deep_snapshot"]
    docs: Iterable[dict[str, Any]] = stock_info.find(
        {},
        {"_id": 0, "ts_code": 1, "symbol": 1, "code": 1, "list_status": 1, "status": 1},
        sort=[("ts_code", 1), ("symbol", 1)],
    )
    codes: list[str] = []
    seen = set()
    for doc in docs:
        if not _is_listed_a_share(doc):
            continue
        code = _normalize_ts_code(doc.get("ts_code") or doc.get("symbol") or doc.get("code"))
        if args.start_after and code <= _normalize_ts_code(args.start_after):
            continue
        if code in seen:
            continue
        seen.add(code)
        codes.append(code)

    if args.stale_days and args.stale_days > 0:
        threshold = (today - timedelta(days=args.stale_days)).strftime("%Y%m%d")
        stale: list[str] = []
        for code in codes:
            latest = snapshot.find_one(
                {"ts_code": code},
                {"_id": 0, "as_of": 1},
                sort=[("as_of", -1), ("update_time", -1)],
            )
            if not latest or str(latest.get("as_of") or "") < threshold:
                stale.append(code)
        codes = stale

    if args.offset and args.offset > 0:
        codes = codes[args.offset:]
    if args.limit and args.limit > 0:
        codes = codes[:args.limit]
    return codes


def _batch_hit_limit(args: argparse.Namespace, codes: list[str]) -> bool:
    return bool(args.from_stock_info and args.limit and args.limit > 0 and len(codes) >= args.limit)


def _final_run_status(args: argparse.Namespace, codes: list[str], n_err: int) -> tuple[str, Optional[str]]:
    if n_err:
        return "partial", f"errors={n_err}"
    if _batch_hit_limit(args, codes):
        return "partial", "selected reached --limit; stale stocks may remain"
    return "complete", None


def main() -> None:
    args = parse_args()
    settings = get_settings()
    codes: list[str] = []
    if args.code:
        codes.append(args.code.strip())
    if args.codes:
        codes.extend(
            c.strip() for c in args.codes.split(",") if c.strip()
        )
    today = date.today()
    if args.from_stock_info:
        codes.extend(_stock_info_codes(settings, args, today))
    codes = list(dict.fromkeys(_normalize_ts_code(c) for c in codes if c.strip()))
    if not codes:
        if args.from_stock_info:
            print("当前批次无待处理股票")
            if not args.no_mongo:
                record_ingestion_run(
                    settings,
                    "institutional_deep",
                    today.strftime("%Y%m%d"),
                    status="complete",
                    records_changed=0,
                )
            return
        print("请指定 --code / --codes / --from-stock-info，或当前批次无待处理股票", file=sys.stderr)
        raise SystemExit(1)

    adapter = StockAdapter(
        settings.tushare_token, rate_limit_per_sec=settings.rate_limit_per_sec
    )
    repo: InstitutionalRepository | None
    if args.no_mongo:
        repo = None
        sw_repo: SwIndustryRepository | None = None
    else:
        repo = InstitutionalRepository(
            settings.mongo_uri, settings.data_db
        )
        sw_repo = SwIndustryRepository(
            settings.mongo_uri, settings.data_db
        )
    pl = InstitutionalPipeline(
        adapter=adapter, repository=repo, sw_industry_repository=sw_repo
    )

    print(f"selected_codes={len(codes)} first={codes[0]} last={codes[-1]}")
    if _batch_hit_limit(args, codes):
        print("本批已达到 --limit，仍可能有剩余 stale 股票；完成后将记录为 partial。")
    n_ok, n_err = 0, 0
    for c in codes:
        try:
            r = pl.run_one(
                c,
                as_of=today,
                n_quarters=args.n_quarters,
                persist=not args.no_mongo and repo is not None,
                block_lookback_days=args.block_lookback,
                include_holder_debug=bool(args.print_holders or args.json),
                include_analysis=not args.no_analysis,
                debug_intl=bool(args.debug_intl),
            )
            n_ok += 1 if r.get("ok") else 0
            n_err += 0 if r.get("ok") else 1
            if args.json:
                print(json.dumps(r, ensure_ascii=False, default=str))
            else:
                _print_human_line(c, r, args.brief)
                if args.debug_intl and r.get("ok"):
                    _print_intl_entity_debug(r)
                if args.print_holders and r.get("ok"):
                    _print_holders_section(r)
                if not args.brief and not args.no_analysis and r.get("ok"):
                    _print_analysis_section(r)
        except Exception as e:  # pragma: no cover
            n_err += 1
            print(f"{c}\tERROR\t{e}", file=sys.stderr)
    if not args.no_mongo and repo is not None:
        status, error = _final_run_status(args, codes, n_err)
        record_ingestion_run(
            settings,
            "institutional_deep",
            today.strftime("%Y%m%d"),
            status=status,
            records_changed=n_ok,
            error=error,
        )


if __name__ == "__main__":
    main()
