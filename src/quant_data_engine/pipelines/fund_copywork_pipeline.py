"""从 fund_holdings 生成抄作业观察名单与两期 diff。"""
from __future__ import annotations

import logging
from dataclasses import replace
from typing import Any, Dict, List, Optional, Sequence

from quant_data_engine.fund_copywork.diff import canonical_holdings, consensus_snapshots, product_snapshot
from quant_data_engine.fund_copywork.watchlist import (
    build_watchlist,
    load_watchlist_config,
    strategy_aum_from_rows,
)
from quant_data_engine.fund_inventory.builder import active_fund_meta, filter_active_holdings
from quant_data_engine.fund_inventory.aggregate import crop_fund_top10, stock_key
from quant_data_engine.fund_inventory.periods import (
    disclosure_deadline,
    disclosure_kind,
    is_period_mature,
    latest_mature_full_period,
    pick_comparable_pair,
    pick_preview_pairs,
)
from quant_data_engine.pipelines.fund_inventory_pipeline import (
    _MIN_FULL_FUND_RATIO,
    _MIN_FULL_ROW_RATIO,
    _active_coverage,
    _candidates,
    _coverage_quality,
    _latest_disclosures,
    _provisional_reporting_cohort,
)

logger = logging.getLogger(__name__)


_PROJECTION = {
    "_id": 0,
    "ts_code": 1,
    "symbol": 1,
    "mkv": 1,
    "amount": 1,
    "end_date": 1,
    "ann_date": 1,
    "fund_name": 1,
}


class FundCopyworkPipeline:
    def __init__(
        self,
        holdings_coll,
        fund_basic_coll,
        snapshot_repo=None,
        stock_info_coll=None,
        hk_stock_info_coll=None,
    ):
        self.holdings = holdings_coll
        self.fund_basic = fund_basic_coll
        self.repo = snapshot_repo
        self.stock_info = stock_info_coll
        self.hk_stock_info = hk_stock_info_coll

    def _period_rows(self, period: str, ts_codes: Sequence[str]) -> List[Dict[str, Any]]:
        codes = [c for c in ts_codes if c]
        if not codes:
            return []
        rows: List[Dict[str, Any]] = []
        batch = 2000
        for i in range(0, len(codes), batch):
            chunk = codes[i : i + batch]
            rows.extend(
                list(
                    self.holdings.find(
                        {"end_date": period, "ts_code": {"$in": chunk}},
                        _PROJECTION,
                    )
                    or []
                )
            )
        return _latest_disclosures(rows)

    def _available_periods(self) -> List[str]:
        return [str(p) for p in (self.holdings.distinct("end_date") or []) if p]

    def _stock_names(self, symbols: Sequence[str]) -> Dict[str, str]:
        collections = [c for c in (self.stock_info, self.hk_stock_info) if c is not None]
        if not collections or not symbols:
            return {}
        names: Dict[str, str] = {}
        keys: List[str] = []
        for symbol in symbols:
            if symbol:
                keys.extend(_candidates(symbol))
        keys = list(dict.fromkeys(keys))
        if not keys:
            return {}
        for coll in collections:
            for doc in coll.find(
                {"$or": [{"symbol": {"$in": keys}}, {"ts_code": {"$in": keys}}]},
                {"_id": 0, "symbol": 1, "ts_code": 1, "name": 1},
            ):
                name = doc.get("name")
                if not name:
                    continue
                for field in ("symbol", "ts_code"):
                    raw = str(doc.get(field) or "")
                    if raw:
                        names[stock_key(raw)] = name
                        names[raw] = name
        return names

    def run_default(
        self,
        *,
        pair_mode: str = "previous_full",
        persist: bool = True,
        as_of: Optional[str] = None,
        include_preview: bool = True,
    ) -> Dict[str, Any]:
        """Mature full pair plus newer incomplete/top-10 preview pairs."""
        official = self.run(
            pair_mode=pair_mode,
            quality_mode="auto",
            persist=persist,
            as_of=as_of,
        )
        results = [official]
        if include_preview:
            base = official.get("period_curr") or latest_mature_full_period(as_of)
            for prev, curr in pick_preview_pairs(self._available_periods(), base):
                results.append(
                    self.run(
                        period_prev=prev,
                        period_curr=curr,
                        quality_mode="provisional",
                        persist=persist,
                        as_of=as_of,
                    )
                )
        ok_rows = [row for row in results if row.get("ok")]
        return {
            "ok": bool(ok_rows),
            "n_ok": len(ok_rows),
            "n_pairs": len(results),
            "results": results,
            "period_curr": (ok_rows[-1] if ok_rows else results[0]).get("period_curr"),
            "period_prev": (ok_rows[-1] if ok_rows else results[0]).get("period_prev"),
            "n_written": {
                "products": sum(int((r.get("n_written") or {}).get("products") or 0) for r in ok_rows),
                "stocks": sum(int((r.get("n_written") or {}).get("stocks") or 0) for r in ok_rows),
            },
        }

    def run(
        self,
        *,
        pair_mode: str = "previous_full",
        quality_mode: str = "auto",
        period_prev: Optional[str] = None,
        period_curr: Optional[str] = None,
        persist: bool = True,
        as_of: Optional[str] = None,
    ) -> Dict[str, Any]:
        if quality_mode not in {"auto", "official", "provisional"}:
            return {"ok": False, "error": "invalid_quality_mode"}
        fund_meta = active_fund_meta(list(self.fund_basic.find({}, {"_id": 0}) or []))
        if period_prev and period_curr:
            prev, curr = period_prev, period_curr
            compare_mode = (
                "full_vs_full"
                if disclosure_kind(prev) == "full" and disclosure_kind(curr) == "full"
                else "top10_vs_top10"
            )
        else:
            pair = pick_comparable_pair(self._available_periods(), pair_mode=pair_mode, as_of=as_of)
            if not pair:
                return {"ok": False, "error": "no comparable fund_holdings periods"}
            prev, curr, compare_mode = pair

        active_codes = list(fund_meta.keys())
        logger.info(
            "fund copywork pair %s -> %s mode=%s quality_mode=%s active_codes=%s",
            prev,
            curr,
            compare_mode,
            quality_mode,
            len(active_codes),
        )
        prev_rows = filter_active_holdings(self._period_rows(prev, active_codes), fund_meta)
        curr_rows = filter_active_holdings(self._period_rows(curr, active_codes), fund_meta)
        logger.info(
            "fund copywork rows prev=%s curr=%s",
            len(prev_rows),
            len(curr_rows),
        )
        if not prev_rows or not curr_rows:
            return {
                "ok": False,
                "error": "empty active holdings for pair",
                "period_prev": prev,
                "period_curr": curr,
            }

        coverage_prev = _active_coverage(prev_rows, fund_meta)
        coverage_curr = _active_coverage(curr_rows, fund_meta)
        coverage_quality = _coverage_quality(coverage_prev, coverage_curr)
        decided = _decide_copywork_quality(
            quality_mode=quality_mode,
            period_prev=prev,
            period_curr=curr,
            compare_mode=compare_mode,
            coverage_prev=coverage_prev,
            coverage_curr=coverage_curr,
            as_of=as_of,
        )
        if not decided.get("ok"):
            return {
                **decided,
                "period_prev": prev,
                "period_curr": curr,
                "coverage_prev": coverage_prev,
                "coverage_curr": coverage_curr,
                "quality": coverage_quality,
            }
        provisional = bool(decided["provisional"])
        compare_mode = str(decided["compare_mode"])

        build_prev_rows, build_curr_rows = prev_rows, curr_rows
        if provisional:
            build_prev_rows, build_curr_rows = _provisional_reporting_cohort(
                prev_rows,
                curr_rows,
                fund_meta,
            )
        if compare_mode == "top10_vs_top10":
            build_prev_rows = crop_fund_top10(build_prev_rows)
            build_curr_rows = crop_fund_top10(build_curr_rows)
        if not build_prev_rows or not build_curr_rows:
            return {
                "ok": False,
                "error": "empty active holdings for pair",
                "period_prev": prev,
                "period_curr": curr,
            }

        prev_books = canonical_holdings(build_prev_rows, fund_meta)
        curr_books = canonical_holdings(build_curr_rows, fund_meta)
        aum_map = strategy_aum_from_rows(build_curr_rows, fund_meta)
        # 上期没有持仓的新品/新份额：整本都会标成「新买」，不能进自动 Top 30。
        comparable_aum = {
            key: rec for key, rec in aum_map.items() if prev_books.get(key)
        }
        skipped_new = len(aum_map) - len(comparable_aum)
        if skipped_new:
            logger.info("fund copywork skip %s products with empty prev book", skipped_new)
        watch_cfg = load_watchlist_config()
        if provisional:
            # 前十大市值不是 AUM，20 亿地板会把大多数已披露产品挡在门外。
            watch_cfg = replace(watch_cfg, aum_floor_yi=0.0)
        watchlist = build_watchlist(comparable_aum, watch_cfg)
        products = []
        for row in watchlist:
            key = str(row.get("fund_key") or "")
            snap = product_snapshot(
                key,
                prev_books.get(key) or {},
                curr_books.get(key) or {},
                period_prev=prev,
                period_curr=curr,
                compare_mode=compare_mode,
                meta=row,
                rank=row.get("rank"),
                source=str(row.get("source") or "auto"),
            )
            products.append(snap)

        names = self._stock_names(
            [str(h.get("symbol") or "") for book in curr_books.values() for h in book.values()]
            + [str(h.get("symbol") or "") for book in prev_books.values() for h in book.values()]
        )
        stocks = consensus_snapshots(
            prev_books,
            curr_books,
            period_prev=prev,
            period_curr=curr,
            compare_mode=compare_mode,
            stock_names=names,
        )
        quality_status = "provisional" if provisional else "official"
        comparison_scope = "top10" if provisional else "full"
        deadline = disclosure_deadline(curr)
        if provisional:
            lag_note = (
                f"数据截止报告期 {curr}（{disclosure_kind(curr)}），"
                "披露进行中或覆盖不足，按前十大对齐。"
                f"法定披露截止 {deadline.isoformat() if deadline else '-'}。"
                "不是今日买卖；清仓可能只是掉出前十。"
                "观察名单规模是前十大市值合计，不是完整 AUM。"
            )
            warning = "当前为披露进行中或覆盖不足的前十大预览；未披露基金不会被视为退出。"
        else:
            lag_note = (
                f"数据截止报告期 {curr}（{disclosure_kind(curr)}），"
                f"法定披露截止 {deadline.isoformat() if deadline else '-'}。"
                "不是今日买卖；季报清仓可能只是掉出前十。"
            )
            warning = "已通过报告期成熟度与全持仓覆盖检查。"
        quality = {
            **coverage_quality,
            "status": quality_status,
            "comparison_scope": comparison_scope,
            "baseline_period": prev,
            "as_of": as_of,
            "warning": warning,
        }
        meta = {
            "compare_mode": compare_mode,
            "disclosure_prev": disclosure_kind(prev),
            "disclosure_curr": disclosure_kind(curr),
            "quality_status": quality_status,
            "comparison_scope": comparison_scope,
            "aum_basis": "top10_mkv" if provisional else "full_mkv",
            "n_watchlist": len(products),
            "n_consensus": len(stocks),
            "quality": quality,
            "watchlist": [
                {
                    "fund_key": p.get("fund_key"),
                    "name": p.get("name"),
                    "rank": p.get("rank"),
                    "n_new": p.get("n_new"),
                    "n_increased": p.get("n_increased"),
                    "n_decreased": p.get("n_decreased"),
                    "n_exited": p.get("n_exited"),
                    "aum_curr": p.get("aum_curr"),
                    "source": p.get("source"),
                }
                for p in products
            ],
            "lag_note": lag_note,
        }
        written = {"products": 0, "stocks": 0}
        if persist and self.repo is not None:
            written = self.repo.replace_pair(
                period_prev=prev,
                period_curr=curr,
                universe="active_equity",
                meta=meta,
                products=products,
                stocks=stocks,
            )
        logger.info(
            "fund copywork done watchlist=%s consensus=%s quality=%s written=%s",
            len(products),
            len(stocks),
            quality_status,
            written,
        )
        out = {
            "ok": True,
            "period_prev": prev,
            "period_curr": curr,
            "compare_mode": compare_mode,
            "quality_status": quality_status,
            "comparison_scope": comparison_scope,
            "n_watchlist": len(products),
            "n_consensus": len(stocks),
            "n_written": written,
            "lag_note": lag_note,
            "meta": meta,
        }
        if not persist:
            out["products"] = products
            out["stocks"] = stocks
        return out


def _incomplete_full_coverage(coverage_prev: Dict[str, Any], coverage_curr: Dict[str, Any]) -> bool:
    row_ratio = coverage_curr["canonical_rows"] / max(coverage_prev["canonical_rows"], 1)
    fund_ratio = coverage_curr["strategies"] / max(coverage_prev["strategies"], 1)
    return row_ratio < _MIN_FULL_ROW_RATIO or fund_ratio < _MIN_FULL_FUND_RATIO


def _decide_copywork_quality(
    *,
    quality_mode: str,
    period_prev: str,
    period_curr: str,
    compare_mode: str,
    coverage_prev: Dict[str, Any],
    coverage_curr: Dict[str, Any],
    as_of: Optional[str],
) -> Dict[str, Any]:
    curr_kind = disclosure_kind(period_curr)
    immature = not is_period_mature(period_curr, as_of)
    incomplete = compare_mode == "full_vs_full" and _incomplete_full_coverage(
        coverage_prev, coverage_curr
    )
    wants_preview = curr_kind == "top10" or immature or incomplete
    if quality_mode == "official":
        if wants_preview:
            return {
                "ok": False,
                "error": "incomplete_current_period"
                if incomplete
                else "official_requires_mature_full",
                "period_prev": period_prev,
                "period_curr": period_curr,
            }
        return {"ok": True, "provisional": False, "compare_mode": compare_mode}
    if quality_mode == "provisional" or wants_preview:
        return {"ok": True, "provisional": True, "compare_mode": "top10_vs_top10"}
    return {"ok": True, "provisional": False, "compare_mode": compare_mode}
