"""从 Mongo fund_holdings + fund_basic + 日线 生成公募库存快照。"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Sequence

from quant_data_engine.fund_inventory.builder import (
    active_fund_meta,
    build_universe_snapshots,
    filter_active_holdings,
)
from quant_data_engine.fund_inventory.periods import (
    latest_mature_full_period,
    pick_comparable_pair,
)
from quant_data_engine.fund_inventory.aggregate import stock_key

logger = logging.getLogger(__name__)
_MIN_FULL_ROW_RATIO = 0.60
_MIN_FULL_FUND_RATIO = 0.70


def _docs(cursor) -> List[Dict[str, Any]]:
    return list(cursor or [])


def _latest_disclosures(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Keep the latest amendment for each fund × stock × report period."""
    latest: Dict[tuple, Dict[str, Any]] = {}
    for row in rows:
        key = (
            str(row.get("ts_code") or ""),
            stock_key(row.get("symbol")),
            str(row.get("end_date") or ""),
        )
        current = latest.get(key)
        if current is None or str(row.get("ann_date") or "") >= str(current.get("ann_date") or ""):
            latest[key] = row
    return list(latest.values())


def _active_coverage(
    rows: Sequence[Dict[str, Any]],
    fund_meta: Dict[str, Any],
) -> Dict[str, int]:
    active = filter_active_holdings(rows, fund_meta)
    return {
        "rows": len(active),
        "funds": len({str(row.get("ts_code") or "") for row in active}),
    }


def _candidates(symbol: str) -> List[str]:
    s = str(symbol or "").strip().upper()
    out = [s]
    if "." in s:
        out.append(s.split(".", 1)[0])
    elif len(s) == 6 and s.isdigit():
        out.extend([f"{s}.SH", f"{s}.SZ", f"{s}.BJ"])
    return list(dict.fromkeys(out))


class FundInventoryPipeline:
    def __init__(
        self,
        holdings_coll,
        fund_basic_coll,
        volume_price_coll,
        snapshot_repo,
        adj_factor_coll=None,
        stock_info_coll=None,
    ):
        self.holdings = holdings_coll
        self.fund_basic = fund_basic_coll
        self.volume_price = volume_price_coll
        self.adj_factor = adj_factor_coll
        self.stock_info = stock_info_coll
        self.snapshot_repo = snapshot_repo

    def distinct_periods(self) -> List[str]:
        try:
            values = self.holdings.distinct("end_date")
        except Exception:
            values = []
        periods = sorted({str(v).replace("-", "")[:8] for v in values if v})
        return [p for p in periods if len(p) == 8]

    def _load_period(self, end_date: str) -> List[Dict[str, Any]]:
        return _docs(
            self.holdings.find(
                {"end_date": end_date},
                {
                    "_id": 0,
                    "ts_code": 1,
                    "symbol": 1,
                    "end_date": 1,
                    "ann_date": 1,
                    "mkv": 1,
                    "amount": 1,
                },
            )
        )

    def _stock_info(self, key: str) -> Dict[str, Any]:
        if self.stock_info is None:
            return {}
        cands = _candidates(key)
        doc = self.stock_info.find_one(
            {"$or": [{"symbol": {"$in": cands}}, {"ts_code": {"$in": cands}}]},
            {"_id": 0, "symbol": 1, "ts_code": 1, "name": 1, "industry": 1},
        )
        return doc or {}

    def _bar_on_or_before(self, coll, key: str, yyyymmdd: str) -> Optional[Dict[str, Any]]:
        if coll is None:
            return None
        cands = _candidates(key)
        return coll.find_one(
            {
                "$or": [{"symbol": {"$in": cands}}, {"ts_code": {"$in": cands}}],
                "trade_date": {"$lte": yyyymmdd},
            },
            sort=[("trade_date", -1)],
        )

    def _prices_for(self, key: str, period_prev: str, period_curr: str) -> Dict[str, Any]:
        prev_bar = self._bar_on_or_before(self.volume_price, key, period_prev)
        curr_bar = self._bar_on_or_before(self.volume_price, key, period_curr)
        prev_adj = self._bar_on_or_before(self.adj_factor, key, period_prev)
        curr_adj = self._bar_on_or_before(self.adj_factor, key, period_curr)

        def _close(bar):
            if not bar:
                return None
            for field in ("close", "close_price", "adj_close"):
                try:
                    v = float(bar.get(field))
                except (TypeError, ValueError):
                    continue
                if v > 0:
                    return v
            return None

        def _adj(bar):
            if not bar:
                return None
            try:
                v = float(bar.get("adj_factor"))
            except (TypeError, ValueError):
                return None
            return v if v > 0 else None

        return {
            "price_prev": _close(prev_bar),
            "price_curr": _close(curr_bar),
            "adj_prev": _adj(prev_adj),
            "adj_curr": _adj(curr_adj),
        }

    def run(
        self,
        *,
        pair_mode: str = "previous_full",
        period_prev: Optional[str] = None,
        period_curr: Optional[str] = None,
        persist: bool = True,
        symbols: Optional[Sequence[str]] = None,
        as_of: Optional[str] = None,
    ) -> Dict[str, Any]:
        fund_meta = active_fund_meta(_docs(self.fund_basic.find({}, {"_id": 0})))
        if not fund_meta:
            return {
                "ok": False,
                "error": "no_active_fund_metadata",
                "n_snapshots": 0,
            }
        if period_prev and period_curr:
            prev, curr = str(period_prev), str(period_curr)
            picked = pick_comparable_pair(
                [prev, curr],
                pair_mode="previous_quarter",
                as_of=as_of,
            )
            if not picked or picked[:2] != (prev, curr):
                return {
                    "ok": False,
                    "error": "report_period_not_mature",
                    "period_prev": prev,
                    "period_curr": curr,
                    "n_snapshots": 0,
                }
            compare_mode = picked[2]
        else:
            periods = self.distinct_periods()
            picked = pick_comparable_pair(periods, pair_mode=pair_mode, as_of=as_of)
            if not picked:
                return {
                    "ok": False,
                    "error": "need_two_mature_periods",
                    "periods": periods,
                    "n_snapshots": 0,
                }
            prev, curr, compare_mode = picked
            if pair_mode == "previous_full":
                expected_curr = latest_mature_full_period(as_of)
                if curr != expected_curr:
                    return {
                        "ok": False,
                        "error": "latest_mature_full_period_missing",
                        "expected_period_curr": expected_curr,
                        "period_curr": curr,
                        "periods": periods,
                        "n_snapshots": 0,
                    }

        prev_rows = _latest_disclosures(self._load_period(prev))
        curr_rows = _latest_disclosures(self._load_period(curr))
        if not prev_rows or not curr_rows:
            return {
                "ok": False,
                "error": "missing_period_holdings",
                "period_prev": prev,
                "period_curr": curr,
                "n_snapshots": 0,
            }
        coverage_prev = _active_coverage(prev_rows, fund_meta)
        coverage_curr = _active_coverage(curr_rows, fund_meta)
        if compare_mode == "full_vs_full":
            row_ratio = coverage_curr["rows"] / max(coverage_prev["rows"], 1)
            fund_ratio = coverage_curr["funds"] / max(coverage_prev["funds"], 1)
            if row_ratio < _MIN_FULL_ROW_RATIO or fund_ratio < _MIN_FULL_FUND_RATIO:
                return {
                    "ok": False,
                    "error": "incomplete_current_period",
                    "period_prev": prev,
                    "period_curr": curr,
                    "coverage_prev": coverage_prev,
                    "coverage_curr": coverage_curr,
                    "row_ratio": row_ratio,
                    "fund_ratio": fund_ratio,
                    "n_snapshots": 0,
                }
        allow = {stock_key(s) for s in (symbols or []) if s}
        snapshots = build_universe_snapshots(
            prev_rows,
            curr_rows,
            fund_meta=fund_meta,
            period_prev=prev,
            period_curr=curr,
            compare_mode=compare_mode,
            price_lookup=lambda key: self._prices_for(key, prev, curr),
            stock_info_lookup=self._stock_info,
        )
        if allow:
            snapshots = [s for s in snapshots if s.get("symbol") in allow]
        if not snapshots:
            return {
                "ok": False,
                "error": "no_valid_a_share_snapshots",
                "period_prev": prev,
                "period_curr": curr,
                "coverage_prev": coverage_prev,
                "coverage_curr": coverage_curr,
                "n_snapshots": 0,
            }
        n_write = 0
        if persist and self.snapshot_repo is not None and snapshots:
            n_write = self.snapshot_repo.replace_snapshots(snapshots)
        logger.info(
            "fund inventory snapshots period_prev=%s period_curr=%s mode=%s n=%s wrote=%s",
            prev,
            curr,
            compare_mode,
            len(snapshots),
            n_write,
        )
        return {
            "ok": True,
            "period_prev": prev,
            "period_curr": curr,
            "compare_mode": compare_mode,
            "n_active_funds": len(fund_meta),
            "coverage_prev": coverage_prev,
            "coverage_curr": coverage_curr,
            "n_snapshots": len(snapshots),
            "n_written": n_write,
            "snapshots": snapshots,
        }
