#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Tushare ETF 相关接口：全市场（不传 ts_code）与「区间一次拉取」语义实网验证。

官方文档要点（摘录，以 https://tushare.pro 为准）：

- **fund_daily**（doc_id=127）：``ts_code`` / ``trade_date`` / ``start_date`` / ``end_date`` 均为可选；
  文档示例为单代码 + 区间；单次最大 **5000** 行，并提示可按代码与日期循环。
- **etf_share_size**（doc_id=408）：全市场示例为 ``trade_date`` + ``exchange``（如 SSE），
  而非「仅 ``start_date``/``end_date``、无 ``ts_code``」；单次最大 **5000** 行。

实验矩阵（本脚本）：

- **R1**：不传 ``ts_code``，仅 ``trade_date=TD`` → 是否返回当日多行全市场数据。
- **R2**：不传 ``ts_code``，仅 ``start_date`` + ``end_date`` → 区间单次调用行数与去重键数；
  若行数等于接口上限且仍少于 R3，可能存在**静默截断**。
  实网校验示例：**fund_daily** 会返回「ts_code 和 trade_date 至少填写一个」，即**不能**仅靠区间拉全市场日线；
  **fund_adj** / **fund_share** 在短区间内即可能顶满 **2000** 行而与 R3 不一致。
- **R3**：对 ``[S,E]`` 内每个**自然日**分别做 R1，合并后按 ``(ts_code, trade_date)`` 去重计数，
  作为「按日拉全市场」的参照总量（与当前 ``EtfDailyPipeline`` / ``EtfShareSizePipeline`` 的按日策略一致）。
- **R4**（仅 ``etf_share_size``）：``trade_date=TD`` 下分别 ``exchange=SSE/SZSE/BSE`` 拉取后合并去重，
  与 R1（不传 exchange）对比，判断是否需要分交易所才能凑全。

**如何解读 R2 vs R3**：

- 若 R3 的去重条数 **大于** R2 的去重条数（或 R2 行数已顶满上限而 R3 更大）：区间单次调用**不能**替代按日循环，
  或需 offset 分页（若接口支持）；当前流水线按 ``trade_date`` 逐日是合理防御。
- 若两者接近或相等（在抽样区间内）：可再扩大区间复测；仍建议在变更入库策略前用更长区间与分页验证。

环境：``TUSHARE_TOKEN``；可选加载仓库旁 ``quantFinance/.env``。脚本会把 ``quant-data-engine/src`` 插入 ``sys.path``，
并通过 ``importlib`` 直接加载 ``tushare_adapter.py``，**无需**安装 ``pytdx`` / ``baostock``。
注意：``tushare.set_token`` 可能在用户主目录写入 ``tk.csv``；若临时把 ``HOME`` 指到无用户 site-packages 的目录，
可能导致 ``import pandas`` 失败，请使用带依赖的 Python 环境并保持可写的 ``HOME``（或接受写入主目录）。

示例::

    cd quant-data-engine
    python tools/verify_tushare_etf_bulk_semantics.py \\
        --trade-date 20250410 --start 20250408 --end 20250410 --json
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import os
import pathlib
import sys
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Tuple, Type

import pandas as pd
from dotenv import load_dotenv

_env = pathlib.Path(__file__).resolve().parent.parent.parent / "quantFinance" / ".env"
if _env.exists():
    load_dotenv(_env, override=True)
else:
    load_dotenv(override=True)

_ROOT = pathlib.Path(__file__).resolve().parent.parent
_SRC = _ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))


def _load_tushare_adapter_class() -> Type[Any]:
    """Load ``TushareAdapter`` without importing ``quant_data_engine`` package (avoids pytdx/baostock)."""
    path = _SRC / "quant_data_engine" / "adapters" / "tushare_adapter.py"
    spec = importlib.util.spec_from_file_location("qde_tushare_adapter_standalone", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load spec for {path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.TushareAdapter  # type: ignore[no-any-return]


TushareAdapter = _load_tushare_adapter_class()

# 与 Tushare 文档 / 本仓库 pipeline 注释一致的单次行上限（用于 R2 截断提示与 --strict）
_ROW_LIMIT: Dict[str, int] = {
    "fund_daily": 5000,
    "fund_adj": 2000,
    "fund_share": 2000,
    "etf_share_size": 5000,
}

_EXCHANGES_ETF_SHARE = ("SSE", "SZSE", "BSE")


def _calendar_range(start: str, end: str) -> List[str]:
    ds = datetime.strptime(start, "%Y%m%d")
    de = datetime.strptime(end, "%Y%m%d")
    out: List[str] = []
    d = ds
    while d <= de:
        out.append(d.strftime("%Y%m%d"))
        d += timedelta(days=1)
    return out


def _norm_trade_date_series(s: pd.Series) -> pd.Series:
    """Normalize to YYYYMMDD strings for dedupe keys."""
    out: List[str] = []
    for v in s:
        if v is None or (isinstance(v, float) and pd.isna(v)):
            out.append("")
            continue
        if isinstance(v, pd.Timestamp):
            out.append(v.strftime("%Y%m%d"))
            continue
        if hasattr(v, "strftime"):
            try:
                out.append(v.strftime("%Y%m%d"))  # type: ignore[arg-type]
                continue
            except Exception:
                pass
        if isinstance(v, str):
            t = v.strip().replace("-", "")[:8]
            out.append(t if len(t) == 8 and t.isdigit() else "")
            continue
        try:
            iv = int(float(v))
            st = str(iv).zfill(8)
            out.append(st[-8:] if len(st) >= 8 else st.zfill(8))
        except Exception:
            out.append("")
    return pd.Series(out, index=s.index)


def _summary_frame(df: Optional[pd.DataFrame]) -> Tuple[int, int, Optional[str], Optional[str], Optional[str]]:
    """Returns rowcount, unique (ts_code, trade_date), min_td, max_td, error."""
    if df is None:
        return 0, 0, None, None, "null_dataframe"
    if df.empty:
        return 0, 0, None, None, None
    if "trade_date" not in df.columns:
        return len(df), len(df), None, None, "missing_trade_date_column"
    td = _norm_trade_date_series(df["trade_date"])
    valid = td != ""
    if "ts_code" in df.columns:
        sub = df.loc[valid].copy()
        sub["_td"] = td.loc[valid].values
        sub["_ts"] = sub["ts_code"].astype(str)
        nuniq = int(sub.drop_duplicates(subset=["_ts", "_td"], keep="last").shape[0])
    else:
        nuniq = int(valid.sum())
    tds = sorted(td[valid].unique().tolist())
    mn, mx = (tds[0], tds[-1]) if tds else (None, None)
    return len(df), nuniq, mn, mx, None


def _call_safe(
    fn: Callable[..., Any],
    **kw: Any,
) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    try:
        df = fn(**kw)
        return df, None
    except Exception as e:  # pragma: no cover - network
        return None, str(e)


def _one_result(
    api: str,
    mode: str,
    params: Dict[str, Any],
    df: Optional[pd.DataFrame],
    err: Optional[str],
) -> Dict[str, Any]:
    rc, uq, mn, mx, col_err = _summary_frame(df)
    if err:
        col_err = err
    return {
        "api": api,
        "mode": mode,
        "params": params,
        "rowcount": rc,
        "unique_pairs": uq,
        "min_trade_date": mn,
        "max_trade_date": mx,
        "error_message": col_err,
    }


def run_r1_r2_r3(
    adapter: TushareAdapter,
    api: str,
    trade_date: str,
    start: str,
    end: str,
) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    if api == "fund_daily":
        fetch = adapter.fetch_etf_daily
    elif api == "fund_adj":
        fetch = adapter.fetch_fund_adj
    elif api == "fund_share":
        fetch = adapter.fetch_fund_share
    elif api == "etf_share_size":
        fetch = adapter.fetch_etf_share_size
    else:
        raise ValueError(api)

    p1: Dict[str, Any] = {"trade_date": trade_date}
    df1, e1 = _call_safe(fetch, trade_date=trade_date)
    r1 = _one_result(api, "R1_trade_date_only", p1, df1, e1)

    p2: Dict[str, Any] = {"start_date": start, "end_date": end}
    df2, e2 = _call_safe(fetch, start_date=start, end_date=end)
    r2 = _one_result(api, "R2_range_only_no_ts_code", p2, df2, e2)

    parts: List[pd.DataFrame] = []
    err3: Optional[str] = None
    for d in _calendar_range(start, end):
        dfi, ei = _call_safe(fetch, trade_date=d)
        if ei:
            err3 = ei
            break
        if dfi is not None and not dfi.empty:
            parts.append(dfi)
    if err3:
        r3 = _one_result(api, "R3_loop_trade_dates_merged", {"start": start, "end": end}, None, err3)
    elif not parts:
        r3 = _one_result(api, "R3_loop_trade_dates_merged", {"start": start, "end": end}, pd.DataFrame(), None)
    else:
        merged = pd.concat(parts, ignore_index=True)
        if "ts_code" in merged.columns and "trade_date" in merged.columns:
            td = _norm_trade_date_series(merged["trade_date"])
            merged = merged.assign(_td=td)
            merged = merged[merged["_td"] != ""].drop_duplicates(subset=["ts_code", "_td"], keep="last")
            merged = merged.drop(columns=["_td"], errors="ignore")
        r3 = _one_result(api, "R3_loop_trade_dates_merged", {"start": start, "end": end}, merged, None)

    return r1, r2, r3


def run_r4_etf_share_by_exchange(
    adapter: TushareAdapter,
    trade_date: str,
) -> Dict[str, Any]:
    parts: List[pd.DataFrame] = []
    errs: List[str] = []
    for ex in _EXCHANGES_ETF_SHARE:
        df, err = _call_safe(
            adapter.fetch_etf_share_size,
            trade_date=trade_date,
            exchange=ex,
        )
        if err:
            errs.append(f"{ex}:{err}")
        elif df is not None and not df.empty:
            parts.append(df)
    params = {"trade_date": trade_date, "exchanges": list(_EXCHANGES_ETF_SHARE)}
    if errs and not parts:
        return _one_result("etf_share_size", "R4_by_exchange_merged", params, None, ";".join(errs))
    if not parts:
        return _one_result("etf_share_size", "R4_by_exchange_merged", params, pd.DataFrame(), None)
    merged = pd.concat(parts, ignore_index=True)
    if "ts_code" in merged.columns and "trade_date" in merged.columns:
        td = _norm_trade_date_series(merged["trade_date"])
        merged = merged.assign(_td=td)
        merged = merged[merged["_td"] != ""].drop_duplicates(subset=["ts_code", "_td", "exchange"], keep="last")
        merged = merged.drop(columns=["_td"], errors="ignore")
    return _one_result("etf_share_size", "R4_by_exchange_merged", params, merged, None)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--trade-date", required=True, help="参考交易日 YYYYMMDD（R1 / R4）")
    p.add_argument("--start", required=True, help="区间起点 YYYYMMDD（R2 / R3）")
    p.add_argument("--end", required=True, help="区间终点 YYYYMMDD（含，R2 / R3）")
    p.add_argument("--token", default=None, help="覆盖环境变量 TUSHARE_TOKEN")
    p.add_argument("--json", action="store_true", help="只打印 JSON 摘要到 stdout")
    p.add_argument(
        "--strict-under-limit",
        action="store_true",
        help="若某接口 R2 行数等于该接口单次上限且 R3.unique_pairs > R2.rowcount，则以退出码 2 结束",
    )
    p.add_argument("--rate-limit", type=float, default=4.0, help="TushareAdapter 每秒请求上限")
    return p.parse_args()


def main() -> int:
    args = parse_args()

    token = args.token or os.environ.get("TUSHARE_TOKEN")
    if not token:
        msg = "缺少 TUSHARE_TOKEN（环境变量或 --token）"
        if args.json:
            print(json.dumps({"ok": False, "error": msg}, ensure_ascii=False))
        else:
            print(msg, file=sys.stderr)
        return 1

    adapter = TushareAdapter(token=token, rate_limit_per_sec=args.rate_limit)
    if not adapter.enabled:
        msg = "TushareAdapter 未启用（检查是否安装 tushare 且 token 非空）"
        if args.json:
            print(json.dumps({"ok": False, "error": msg}, ensure_ascii=False))
        else:
            print(msg, file=sys.stderr)
        return 1

    apis = ("fund_daily", "fund_adj", "fund_share", "etf_share_size")
    rows: List[Dict[str, Any]] = []
    strict_violations: List[str] = []

    for api in apis:
        r1, r2, r3 = run_r1_r2_r3(adapter, api, args.trade_date, args.start, args.end)
        for r in (r1, r2, r3):
            r["row_limit"] = _ROW_LIMIT.get(api)
            rows.append(r)
        lim = _ROW_LIMIT[api]
        if (
            args.strict_under_limit
            and r2.get("error_message") is None
            and r3.get("error_message") is None
            and int(r2.get("rowcount") or 0) == lim
            and int(r3.get("unique_pairs") or 0) > int(r2.get("rowcount") or 0)
        ):
            strict_violations.append(
                f"{api}: R2 rowcount=={lim} but R3 unique_pairs={r3.get('unique_pairs')} > R2 rowcount (truncation risk)"
            )

    r4 = run_r4_etf_share_by_exchange(adapter, args.trade_date)
    r4["row_limit"] = _ROW_LIMIT["etf_share_size"]
    rows.append(r4)

    payload: Dict[str, Any] = {
        "ok": True,
        "trade_date": args.trade_date,
        "range": {"start": args.start, "end": args.end},
        "results": rows,
        "strict_violations": strict_violations,
    }

    if args.json:
        print(json.dumps(payload, indent=2, ensure_ascii=False, default=str))
    else:
        print("Tushare ETF bulk semantics check")
        print(f"  trade_date={args.trade_date}  range={args.start}..{args.end}")
        for r in rows:
            lim = r.get("row_limit")
            err = r.get("error_message") or ""
            err_s = f" err={err!r}" if err else ""
            print(
                f"  [{r['api']}] {r['mode']}: rows={r['rowcount']} unique_pairs={r['unique_pairs']}"
                f" min_td={r['min_trade_date']} max_td={r['max_trade_date']} limit={lim}{err_s}"
            )
            if r.get("params"):
                print(f"       params={r['params']}")
        sys.stdout.flush()
        if strict_violations:
            print("Strict violations (--strict-under-limit):", file=sys.stderr)
            for v in strict_violations:
                print(f"  {v}", file=sys.stderr)
        # R2 vs R3 简要结论（按 api 分组）
        by_api: Dict[str, List[Dict[str, Any]]] = {}
        for r in rows:
            by_api.setdefault(str(r["api"]), []).append(r)
        r1e = next((x for x in rows if x["api"] == "etf_share_size" and x["mode"] == "R1_trade_date_only"), None)
        r4e = next((x for x in rows if x["mode"] == "R4_by_exchange_merged"), None)
        if (
            r1e
            and r4e
            and not r1e.get("error_message")
            and not r4e.get("error_message")
        ):
            u1, u4 = int(r1e.get("unique_pairs") or 0), int(r4e.get("unique_pairs") or 0)
            if u1 == u4 and u1 > 0:
                print(
                    f"  >> etf_share_size: R1（仅 trade_date）与 R4（SSE+SZSE+BSE 合并）unique_pairs 均为 {u1}，"
                    "与官方按交易所拆分拉全市场的用法一致（当日样本）"
                )
            elif u1 != u4:
                print(
                    f"  >> etf_share_size: R1 unique={u1} 与 R4 unique={u4} 不一致，需核对是否缺 exchange 或数据延迟"
                )

        for api, lst in by_api.items():
            r2 = next((x for x in lst if x["mode"] == "R2_range_only_no_ts_code"), None)
            r3 = next((x for x in lst if x["mode"] == "R3_loop_trade_dates_merged"), None)
            if not r2 or not r3 or r2.get("error_message") or r3.get("error_message"):
                continue
            u2, u3 = int(r2.get("unique_pairs") or 0), int(r3.get("unique_pairs") or 0)
            n2 = int(r2.get("rowcount") or 0)
            lim = _ROW_LIMIT.get(api, 0)
            em = (r2.get("error_message") or "") if r2 else ""
            if em and ("trade_date" in em or "ts_code" in em):
                print(
                    f"  >> {api}: R2 接口拒绝或参数校验失败（全市场不能仅用 start/end 或需 trade_date）；"
                    f"应使用 R1/R3 按 trade_date 拉取。detail={em[:120]}"
                )
            elif u3 > u2 or (lim and n2 >= lim and u3 > n2):
                print(f"  >> {api}: R3 unique_pairs ({u3}) > R2 unique ({u2}); 区间单次不足以覆盖按日合并（或 R2 已触顶 {lim}）")
            elif u2 == u3 and u2 > 0:
                print(f"  >> {api}: R2/R3 unique_pairs 一致 ({u2})，在区间 [{args.start},{args.end}] 内区间调用与按日合并一致（仅作抽样，扩大区间再验）")

    if args.strict_under_limit and strict_violations:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
