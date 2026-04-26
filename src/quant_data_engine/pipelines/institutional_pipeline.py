"""单标的：拉取 4 期 top10、解禁窗、大宗窗 + 打分；可选落库到 quant_data 库。"""
from __future__ import annotations

import logging
from dataclasses import asdict
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

import pandas as pd

from quant_data_engine.adapters.stock_adapter import StockAdapter
from quant_data_engine.institutional import (
    block_trade_window_stats,
    build_institutional_snapshot,
    diff_top10_periods,
    last_n_completed_quarter_ends,
    share_float_next_window,
)
from quant_data_engine.institutional.name_normalize import normalize_institution_key
from quant_data_engine.institutional.analytical_report import build_structured_analysis
from quant_data_engine.repositories.institutional_repository import InstitutionalRepository
from quant_data_engine.repositories.sw_industry_repository import SwIndustryRepository
from quant_data_engine.institutional.sw_excess import compute_shenwan_l1_excess_60d

_log = logging.getLogger(__name__)


def _as_df(x: Any) -> pd.DataFrame:
    if x is None:
        return pd.DataFrame()
    if isinstance(x, pd.DataFrame):
        return x
    return pd.DataFrame()


class InstitutionalPipeline:
    def __init__(
        self,
        adapter: StockAdapter,
        repository: Optional[InstitutionalRepository] = None,
        sw_industry_repository: Optional[SwIndustryRepository] = None,
    ):
        self.adapter = adapter
        self.repository = repository
        self.sw_industry_repository = sw_industry_repository

    def _fetch_top10_both(
        self, ts_code: str, period: str
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        fh = _as_df(
            self.adapter.fetch_top10_floatholders(ts_code=ts_code, period=period)
        )
        th = _as_df(self.adapter.fetch_top10_holders(ts_code=ts_code, period=period))
        return fh, th

    def run_one(
        self,
        ts_code: str,
        as_of: Optional[date] = None,
        n_quarters: int = 4,
        persist: bool = True,
        block_lookback_days: int = 183,
        include_holder_debug: bool = False,
        include_analysis: bool = True,
    ) -> Dict[str, Any]:
        """
        拉取、可选持久化、返回带 ``snapshot``、``analysis``（与计划书交叉验证）等键。
        """
        a = as_of or date.today()
        periods = last_n_completed_quarter_ends(n_quarters, a)
        if len(periods) < 2:
            return {"ok": False, "error": "quarter_ends", "ts_code": ts_code}

        float_by_p: Dict[str, pd.DataFrame] = {}
        hold_by_p: Dict[str, pd.DataFrame] = {}
        for p in periods:
            fdf, hdf = self._fetch_top10_both(ts_code, p)
            float_by_p[p] = fdf
            hold_by_p[p] = hdf
            if self.repository and persist and not fdf.empty:
                self.repository.upsert_top10_floatholders(ts_code, p, fdf)
            if self.repository and persist and not hdf.empty:
                self.repository.upsert_top10_holders(ts_code, p, hdf)

        p_prev, p_curr = periods[1], periods[0]
        d_float = diff_top10_periods(
            float_by_p[p_prev], float_by_p[p_curr], p_prev, p_curr
        )

        holder_debug: Optional[Dict[str, Any]] = None
        if include_holder_debug:
            holder_debug = {
                "compare": {"prev": p_prev, "curr": p_curr},
                "by_period": {},
            }
            name_col = "holder_name"
            for p, fdf in float_by_p.items():
                rows: list[dict[str, Any]] = []
                if fdf is not None and not fdf.empty and name_col in fdf.columns:
                    for _, r in fdf.iterrows():
                        nm = str(r.get(name_col, "") or "")
                        ratio = r.get("hold_ratio", r.get("hold_float_ratio", ""))
                        rows.append(
                            {
                                "holder_name": nm,
                                "hold_amount": r.get("hold_amount"),
                                "hold_ratio": ratio,
                                "norm": normalize_institution_key(nm),
                            }
                        )
                elif fdf is not None and not fdf.empty and "name" in fdf.columns:
                    for _, r in fdf.iterrows():
                        nm = str(r.get("name", "") or "")
                        ratio = r.get("hold_ratio", r.get("hold_float_ratio", ""))
                        rows.append(
                            {
                                "holder_name": nm,
                                "hold_amount": r.get("hold_amount"),
                                "hold_ratio": ratio,
                                "norm": normalize_institution_key(nm),
                            }
                        )
                holder_debug["by_period"][p] = rows

        sf = _as_df(
            self.adapter.fetch_share_float(ts_code=ts_code, start_date="", end_date="")
        )
        if self.repository and persist and not sf.empty:
            self.repository.replace_share_float(ts_code, sf)

        float_res = share_float_next_window(sf, a, months=6)

        start_bt = (a - timedelta(days=block_lookback_days)).strftime("%Y%m%d")
        end_bt = a.strftime("%Y%m%d")
        # 日线需略长于 block 与 60 交易日申万比较（约 120+ 自然日更稳）
        _look = max(block_lookback_days + 5, 200)
        start_d = (a - timedelta(days=_look)).strftime("%Y%m%d")
        blk = _as_df(
            self.adapter.fetch_block_trade(
                ts_code=ts_code, start_date=start_bt, end_date=end_bt
            )
        )
        daily = _as_df(
            self.adapter.fetch_daily(
                ts_code=ts_code, start_date=start_d, end_date=end_bt
            )
        )
        if self.repository and persist and not blk.empty:
            self.repository.replace_block_trade(ts_code, start_bt, end_bt, blk)

        bstats = block_trade_window_stats(blk, daily, a, lookback_days=block_lookback_days)
        snap = build_institutional_snapshot(
            ts_code, a, float_res, bstats, d_float, daily=daily
        )
        snap_doc = snap.to_mongo()
        if self.sw_industry_repository is not None:
            try:
                ex = compute_shenwan_l1_excess_60d(
                    ts_code, a, daily, self.sw_industry_repository
                )
                if ex:
                    det = dict(snap_doc.get("detail") or {})
                    det["shenwan_l1_60d"] = ex
                    snap_doc["detail"] = det
            except Exception as e:  # pragma: no cover
                _log.debug("shenwan excess: %s", e)
        # 未来 6 月窗内原始行(截断)供分析里「解禁方 vs 流通十大」字符串重叠
        sfw_rows: List[Dict[str, Any]] = []
        if float_res.rows:
            sfw_rows = list(float_res.rows)[:300]
        out: Dict[str, Any] = {
            "ok": True,
            "ts_code": ts_code,
            "as_of": a.isoformat(),
            "periods": periods,
            "diff_float": asdict(d_float),
            "float_window": {
                "risk_tier": float_res.risk_tier,
                "rows_in_window": len(float_res.rows),
            },
            "block_trade": {
                "avg_discount_pct": bstats.avg_discount_pct,
                "trade_days": bstats.trade_days,
                "total_amount": bstats.total_amount,
            },
            "share_float_window_raw_rows": sfw_rows,
            "snapshot": snap_doc,
        }
        if include_holder_debug and holder_debug is not None:
            out["holder_debug"] = holder_debug
        if include_analysis:
            an = build_structured_analysis(out)
            out["analysis"] = an
            sm = dict(out["snapshot"])
            sm["analysis"] = an
            out["snapshot"] = sm
            if self.repository and persist:
                self.repository.upsert_snapshot(sm)
        else:
            if self.repository and persist:
                self.repository.upsert_snapshot(snap_doc)
        return out
