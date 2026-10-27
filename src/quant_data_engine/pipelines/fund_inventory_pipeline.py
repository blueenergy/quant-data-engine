"""从 Mongo fund_holdings + fund_basic + 日线 生成公募库存快照。"""
from __future__ import annotations

from collections import Counter
from datetime import date
import logging
import statistics
from typing import Any, Dict, List, Optional, Sequence

from quant_data_engine.fund_inventory.active_funds import share_class_key
from quant_data_engine.fund_inventory.builder import (
    active_fund_meta,
    build_universe_snapshots,
    filter_active_holdings,
)
from quant_data_engine.fund_inventory.periods import (
    disclosure_kind,
    is_period_mature,
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
) -> Dict[str, Any]:
    active = filter_active_holdings(rows, fund_meta)
    per_code = Counter(str(row.get("ts_code") or "") for row in active)
    per_strategy: Dict[str, int] = {}
    for code, count in per_code.items():
        meta = fund_meta.get(code) or {}
        key = share_class_key(
            name=meta.get("name"),
            ts_code=code,
            management=meta.get("management"),
        )
        per_strategy[key] = max(per_strategy.get(key, 0), count)
    strategy_rows = sorted(per_strategy.values())

    def _percentile(values: Sequence[int], fraction: float) -> float:
        if not values:
            return 0.0
        return float(values[round((len(values) - 1) * fraction)])

    return {
        "rows": len(active),
        "funds": len(per_code),
        "strategies": len(per_strategy),
        "canonical_rows": sum(strategy_rows),
        "median_holdings": (
            round(float(statistics.median(strategy_rows)), 2) if strategy_rows else 0.0
        ),
        "p90_holdings": round(_percentile(strategy_rows, 0.90), 2),
    }


def _coverage_quality(
    previous: Dict[str, Any],
    current: Dict[str, Any],
) -> Dict[str, Any]:
    def _ratio(numerator: float, denominator: float) -> float:
        return round(min(1.0, numerator / max(denominator, 1.0)) * 100.0, 1)

    breadth = _ratio(current["strategies"], previous["strategies"])
    avg_prev = previous["canonical_rows"] / max(previous["strategies"], 1)
    avg_curr = current["canonical_rows"] / max(current["strategies"], 1)
    depth = _ratio(avg_curr, avg_prev)
    return {
        "strategy_coverage_pct": breadth,
        "holding_depth_pct": depth,
        "full_readiness_pct": min(breadth, depth),
        "previous": previous,
        "current": current,
    }


def _strategy_key(code: str, fund_meta: Dict[str, Any]) -> str:
    meta = fund_meta.get(code) or {}
    return share_class_key(
        name=meta.get("name"),
        ts_code=code,
        management=meta.get("management"),
    )


def _provisional_reporting_cohort(
    previous: Sequence[Dict[str, Any]],
    current: Sequence[Dict[str, Any]],
    fund_meta: Dict[str, Any],
) -> tuple:
    """Only compare funds that have disclosed the current provisional period."""
    current_active = filter_active_holdings(current, fund_meta)
    reporting = {
        _strategy_key(str(row.get("ts_code") or ""), fund_meta)
        for row in current_active
    }
    previous_active = filter_active_holdings(previous, fund_meta)
    previous_reporting = [
        row
        for row in previous_active
        if _strategy_key(str(row.get("ts_code") or ""), fund_meta) in reporting
    ]
    return previous_reporting, current_active


def _implied_report_prices(
    rows: Sequence[Dict[str, Any]],
) -> Dict[str, float]:
    """Median report-currency price inferred from disclosed mv / shares."""
    values: Dict[str, List[float]] = {}
    for row in rows:
        key = stock_key(row.get("symbol"))
        if not key.endswith(".HK"):
            continue
        try:
            market_value = float(row.get("mkv"))
            shares = float(row.get("amount"))
        except (TypeError, ValueError):
            continue
        if market_value <= 0 or shares <= 0:
            continue
        values.setdefault(key, []).append(market_value / shares)
    return {
        key: float(statistics.median(prices))
        for key, prices in values.items()
        if prices
    }


def _candidates(symbol: str) -> List[str]:
    s = str(symbol or "").strip().upper()
    out = [s]
    if "." in s:
        code, suffix = s.split(".", 1)
        out.append(code)
        if suffix == "HK" and code.isdigit():
            canonical = code.zfill(5)
            out.extend([canonical, f"{canonical}.HK", f"{int(code)}.HK"])
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
        hk_stock_info_coll=None,
    ):
        self.holdings = holdings_coll
        self.fund_basic = fund_basic_coll
        self.volume_price = volume_price_coll
        self.adj_factor = adj_factor_coll
        self.stock_info = stock_info_coll
        self.hk_stock_info = hk_stock_info_coll
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
        collection = self.hk_stock_info if str(key).endswith(".HK") else self.stock_info
        if collection is None:
            return {}
        cands = _candidates(key)
        doc = collection.find_one(
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
        quality_mode: str = "auto",
        period_prev: Optional[str] = None,
        period_curr: Optional[str] = None,
        persist: bool = True,
        symbols: Optional[Sequence[str]] = None,
        as_of: Optional[str] = None,
    ) -> Dict[str, Any]:
        if quality_mode not in {"auto", "official", "provisional"}:
            return {
                "ok": False,
                "error": "invalid_quality_mode",
                "n_snapshots": 0,
            }
        fund_meta = active_fund_meta(_docs(self.fund_basic.find({}, {"_id": 0})))
        if not fund_meta:
            return {
                "ok": False,
                "error": "no_active_fund_metadata",
                "n_snapshots": 0,
            }
        periods = self.distinct_periods()
        provisional = quality_mode == "provisional"
        use_preview_pair = pair_mode == "previous_full" and quality_mode != "official"

        if use_preview_pair:
            if period_prev and period_curr:
                prev, curr = str(period_prev), str(period_curr)
                if (
                    disclosure_kind(prev) != "full"
                    or disclosure_kind(curr) != "full"
                    or prev >= curr
                ):
                    return {
                        "ok": False,
                        "error": "provisional_requires_two_full_periods",
                        "period_prev": prev,
                        "period_curr": curr,
                        "n_snapshots": 0,
                    }
            else:
                fulls = sorted(p for p in periods if disclosure_kind(p) == "full")
                if len(fulls) < 2:
                    return {
                        "ok": False,
                        "error": "need_two_full_periods",
                        "periods": periods,
                        "n_snapshots": 0,
                    }
                prev, curr = fulls[-2], fulls[-1]

            expected_curr = latest_mature_full_period(as_of)
            if curr < expected_curr:
                return {
                    "ok": False,
                    "error": "latest_mature_full_period_missing",
                    "expected_period_curr": expected_curr,
                    "period_curr": curr,
                    "periods": periods,
                    "n_snapshots": 0,
                }
            if quality_mode == "auto":
                provisional = not is_period_mature(curr, as_of)
            compare_mode = "full_vs_full"
        elif period_prev and period_curr:
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
        coverage_quality = _coverage_quality(coverage_prev, coverage_curr)
        if compare_mode == "full_vs_full" and not provisional:
            row_ratio = coverage_curr["canonical_rows"] / max(
                coverage_prev["canonical_rows"], 1
            )
            fund_ratio = coverage_curr["strategies"] / max(
                coverage_prev["strategies"], 1
            )
            if row_ratio < _MIN_FULL_ROW_RATIO or fund_ratio < _MIN_FULL_FUND_RATIO:
                if quality_mode == "auto" and disclosure_kind(curr) == "full":
                    provisional = True
                else:
                    return {
                        "ok": False,
                        "error": "incomplete_current_period",
                        "period_prev": prev,
                        "period_curr": curr,
                        "coverage_prev": coverage_prev,
                        "coverage_curr": coverage_curr,
                        "quality": coverage_quality,
                        "row_ratio": row_ratio,
                        "fund_ratio": fund_ratio,
                        "n_snapshots": 0,
                    }

        build_prev_rows = prev_rows
        build_curr_rows = curr_rows
        if provisional:
            build_prev_rows, build_curr_rows = _provisional_reporting_cohort(
                prev_rows,
                curr_rows,
                fund_meta,
            )
            compare_mode = "top10_vs_top10"
        quality_status = "provisional" if provisional else "official"
        # previous_quarter pairs crop both sides to the disclosed top-10, so the
        # scope label must follow compare_mode rather than the provisional flag;
        # otherwise a quarterly snapshot is persisted as official "full" holdings.
        quarterly_top10 = compare_mode == "top10_vs_top10"
        comparison_scope = "top10" if (provisional or quarterly_top10) else "full"
        quality = {
            **coverage_quality,
            "status": quality_status,
            "comparison_scope": comparison_scope,
            "baseline_period": prev,
            "as_of": as_of or date.today().strftime("%Y%m%d"),
            "warning": (
                "当前为披露进行中或覆盖不足的前十大预览；未披露基金不会被视为退出。"
                if provisional
                else (
                    "季报口径：两期均按前十大持仓对齐；未披露基金不会被视为退出。"
                    if quarterly_top10
                    else "已通过报告期成熟度与全持仓覆盖检查。"
                )
            ),
        }
        implied_prev = _implied_report_prices(prev_rows)
        implied_curr = _implied_report_prices(curr_rows)

        def _price_lookup(key: str) -> Dict[str, Any]:
            normalized = stock_key(key)
            if normalized.endswith(".HK"):
                return {
                    "price_prev": implied_prev.get(normalized),
                    "price_curr": implied_curr.get(normalized),
                    # The report-implied ratio already includes price and FX.
                    # Corporate actions are not adjusted until hk_daily_adj is available.
                    "adj_prev": 1.0,
                    "adj_curr": 1.0,
                }
            return self._prices_for(normalized, prev, curr)

        allow = {stock_key(s) for s in (symbols or []) if s}
        snapshots = build_universe_snapshots(
            build_prev_rows,
            build_curr_rows,
            fund_meta=fund_meta,
            period_prev=prev,
            period_curr=curr,
            compare_mode=compare_mode,
            price_lookup=_price_lookup,
            stock_info_lookup=self._stock_info,
        )
        for snapshot in snapshots:
            is_hk = str(snapshot.get("ts_code") or "").endswith(".HK")
            snapshot["market"] = "HK" if is_hk else "A"
            snapshot["price_source"] = (
                "fund_report_implied" if is_hk else "market_adjusted"
            )
            snapshot["corporate_action_adjusted"] = not is_hk
            snapshot["price_note"] = (
                "由基金报告 mkv/amount 推导人民币计价价格，已包含汇率影响，"
                "暂未校正拆股等公司行动。"
                if is_hk
                else "使用收盘价与复权因子剔除价格影响。"
            )
            snapshot["quality_status"] = quality_status
            snapshot["comparison_scope"] = comparison_scope
            snapshot["quality"] = quality
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
                "quality": quality,
                "n_snapshots": 0,
            }
        n_write = 0
        if persist and self.snapshot_repo is not None and snapshots:
            n_write = self.snapshot_repo.replace_snapshots(snapshots)
        logger.info(
            "fund inventory snapshots period_prev=%s period_curr=%s mode=%s quality=%s n=%s wrote=%s",
            prev,
            curr,
            compare_mode,
            quality_status,
            len(snapshots),
            n_write,
        )
        return {
            "ok": True,
            "period_prev": prev,
            "period_curr": curr,
            "compare_mode": compare_mode,
            "quality_status": quality_status,
            "comparison_scope": comparison_scope,
            "quality": quality,
            "n_active_funds": len(fund_meta),
            "coverage_prev": coverage_prev,
            "coverage_curr": coverage_curr,
            "n_snapshots": len(snapshots),
            "n_written": n_write,
            "snapshots": snapshots,
        }
