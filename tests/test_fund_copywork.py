"""公募抄作业：观察名单、产品 diff、全市场共识。"""
from __future__ import annotations

import pytest

from quant_data_engine.fund_copywork.actions import (
    classify_action,
    holding_change,
)
from quant_data_engine.fund_copywork.diff import (
    canonical_holdings,
    consensus_snapshots,
    product_snapshot,
)
from quant_data_engine.fund_copywork.watchlist import (
    WatchlistConfig,
    build_watchlist,
    strategy_aum_from_rows,
)

YI = 1e8


def test_classify_new_increase_exit():
    assert classify_action(prev_exists=False, curr_exists=True, change=None) == "new"
    assert classify_action(prev_exists=True, curr_exists=False, change=None) == "exited"
    assert classify_action(prev_exists=True, curr_exists=True, change=0.2) == "increased"
    assert classify_action(prev_exists=True, curr_exists=True, change=-0.2) == "decreased"
    assert classify_action(prev_exists=True, curr_exists=True, change=0.001) == "unchanged"


def test_holding_change_prefers_shares():
    assert holding_change(100, 150) == 0.5
    assert holding_change(None, None, 100, 80) == pytest.approx(-0.2)


def test_watchlist_merges_share_class_and_excludes_small():
    meta = {
        "A.OF": {"name": "明星混合A", "management": "易方达"},
        "C.OF": {"name": "明星混合C", "management": "易方达"},
        "B.OF": {"name": "白酒指数C", "management": "招商"},
        "D.OF": {"name": "小型混合A", "management": "某某"},
    }
    rows = [
        {"ts_code": "A.OF", "symbol": "300502", "mkv": 80 * YI},
        {"ts_code": "C.OF", "symbol": "300502", "mkv": 80 * YI},  # A/C 重复，不可相加
        {"ts_code": "A.OF", "symbol": "600519", "mkv": 40 * YI},
        {"ts_code": "B.OF", "symbol": "000858", "mkv": 400 * YI},
        {"ts_code": "D.OF", "symbol": "000001", "mkv": 5 * YI},
    ]
    aum = strategy_aum_from_rows(rows, meta)
    star_key = [k for k in aum if "明星混合" in k][0]
    assert aum[star_key]["aum"] == 120 * YI
    cfg = WatchlistConfig(top_n=30, aum_floor_yi=20, exclude=["白酒指数C"], pin=["小型混合A"])
    picked = build_watchlist(aum, cfg)
    names = [r["name"] for r in picked]
    assert "白酒指数C" not in names
    assert "明星混合A" in names or "明星混合C" in names
    assert any("小型混合" in (r.get("name") or "") for r in picked)


def test_product_and_consensus_diff():
    meta = {
        "001.OF": {"name": "甲混合A", "management": "易方达"},
        "002.OF": {"name": "乙混合A", "management": "中欧"},
    }
    prev_rows = [
        {"ts_code": "001.OF", "symbol": "300502", "mkv": 10 * YI, "amount": 100},
        {"ts_code": "001.OF", "symbol": "600519", "mkv": 20 * YI, "amount": 50},
        {"ts_code": "002.OF", "symbol": "300502", "mkv": 8 * YI, "amount": 80},
    ]
    curr_rows = [
        {"ts_code": "001.OF", "symbol": "300502", "mkv": 15 * YI, "amount": 150},
        {"ts_code": "001.OF", "symbol": "000858", "mkv": 12 * YI, "amount": 30},
        {"ts_code": "002.OF", "symbol": "300502", "mkv": 4 * YI, "amount": 40},
    ]
    prev_books = canonical_holdings(prev_rows, meta)
    curr_books = canonical_holdings(curr_rows, meta)
    key = next(iter(prev_books))
    snap = product_snapshot(
        key,
        prev_books[key],
        curr_books[key],
        period_prev="20250630",
        period_curr="20251231",
        compare_mode="full_vs_full",
    )
    actions = {row["symbol"]: row["action"] for row in snap["holdings"]}
    assert actions["300502"] == "increased"
    assert actions["600519"] == "exited"
    assert actions["000858"] == "new"
    assert snap["n_new"] == 1
    assert snap["n_exited"] == 1

    cons = consensus_snapshots(
        prev_books,
        curr_books,
        period_prev="20250630",
        period_curr="20251231",
        compare_mode="full_vs_full",
        stock_names={"300502": "新易盛"},
    )
    nvda = next(row for row in cons if row["symbol"] == "300502")
    assert nvda["n_operated"] == 2
    assert nvda["n_increased"] == 1
    assert     nvda["n_decreased"] == 1
    assert nvda["name"] == "新易盛"


class _FakeColl:
    def __init__(self, docs):
        self.docs = list(docs)

    def find(self, query=None, projection=None):
        query = query or {}
        out = []
        for doc in self.docs:
            if not _match(doc, query):
                continue
            item = dict(doc)
            if projection:
                keep = {k for k, v in projection.items() if v and k != "_id"}
                drop = {k for k, v in projection.items() if not v}
                if keep:
                    item = {k: doc.get(k) for k in keep if k in doc}
                for key in drop:
                    item.pop(key, None)
            out.append(item)
        return out

    def distinct(self, field):
        return list({doc.get(field) for doc in self.docs if doc.get(field)})


def _match(doc, query):
    for key, expected in query.items():
        actual = doc.get(key)
        if isinstance(expected, dict) and "$in" in expected:
            if actual not in expected["$in"]:
                return False
        elif actual != expected:
            return False
    return True


def test_pipeline_builds_watchlist_without_persist():
    from quant_data_engine.pipelines.fund_copywork_pipeline import FundCopyworkPipeline

    fund_basic = _FakeColl(
        [
            {
                "ts_code": "001.OF",
                "name": "明星混合A",
                "management": "易方达",
                "fund_type": "混合型",
                "invest_type": "偏股混合型",
            }
        ]
    )
    holdings = _FakeColl(
        [
            {
                "ts_code": "001.OF",
                "symbol": "300502",
                "mkv": 80 * YI,
                "amount": 100,
                "end_date": "20250630",
                "ann_date": "20250801",
                "fund_name": "明星混合A",
            },
            {
                "ts_code": "001.OF",
                "symbol": "300502",
                "mkv": 100 * YI,
                "amount": 150,
                "end_date": "20251231",
                "ann_date": "20260401",
                "fund_name": "明星混合A",
            },
            {
                "ts_code": "001.OF",
                "symbol": "600519",
                "mkv": 40 * YI,
                "amount": 20,
                "end_date": "20251231",
                "ann_date": "20260401",
                "fund_name": "明星混合A",
            },
        ]
    )
    pipeline = FundCopyworkPipeline(
        holdings_coll=holdings,
        fund_basic_coll=fund_basic,
        snapshot_repo=None,
    )
    result = pipeline.run(
        period_prev="20250630",
        period_curr="20251231",
        persist=False,
    )
    assert result["ok"]
    assert result["compare_mode"] == "full_vs_full"
    assert result["quality_status"] == "official"
    assert result["n_watchlist"] == 1
    product = result["products"][0]
    actions = {row["symbol"]: row["action"] for row in product["holdings"]}
    assert actions["300502"] == "increased"
    assert actions["600519"] == "new"


def test_pipeline_skips_new_share_class_from_auto_watchlist():
    from quant_data_engine.pipelines.fund_copywork_pipeline import FundCopyworkPipeline

    fund_basic = _FakeColl(
        [
            {
                "ts_code": "OLD.OF",
                "name": "老牌混合A",
                "management": "易方达",
                "fund_type": "混合型",
                "invest_type": "偏股混合型",
            },
            {
                "ts_code": "NEW.OF",
                "name": "新品混合C",
                "management": "诺安",
                "fund_type": "混合型",
                "invest_type": "偏股混合型",
            },
        ]
    )
    holdings = _FakeColl(
        [
            {
                "ts_code": "OLD.OF",
                "symbol": "300502",
                "mkv": 80 * YI,
                "amount": 100,
                "end_date": "20250630",
                "ann_date": "20250801",
            },
            {
                "ts_code": "OLD.OF",
                "symbol": "300502",
                "mkv": 90 * YI,
                "amount": 110,
                "end_date": "20251231",
                "ann_date": "20260401",
            },
            {
                "ts_code": "NEW.OF",
                "symbol": "300502",
                "mkv": 200 * YI,
                "amount": 200,
                "end_date": "20251231",
                "ann_date": "20260401",
            },
        ]
    )
    result = FundCopyworkPipeline(holdings, fund_basic).run(
        period_prev="20250630",
        period_curr="20251231",
        persist=False,
    )
    names = [row["name"] for row in result["products"]]
    assert any("老牌混合" in (name or "") for name in names)
    assert not any("新品混合" in (name or "") for name in names)
    assert result["quality_status"] == "official"


def _holding(code, symbol, period, mkv, amount=100, ann="20260401"):
    return {
        "ts_code": code,
        "symbol": symbol,
        "mkv": mkv,
        "amount": amount,
        "end_date": period,
        "ann_date": ann,
    }


def test_pipeline_q1_preview_crops_and_drops_undisclosed():
    from quant_data_engine.pipelines.fund_copywork_pipeline import FundCopyworkPipeline

    fund_basic = _FakeColl(
        [
            {
                "ts_code": "A.OF",
                "name": "披露混合A",
                "management": "易方达",
                "fund_type": "混合型",
                "invest_type": "偏股混合型",
            },
            {
                "ts_code": "B.OF",
                "name": "未报混合A",
                "management": "华夏",
                "fund_type": "混合型",
                "invest_type": "偏股混合型",
            },
        ]
    )
    prev_a = [
        _holding("A.OF", f"{i:06d}", "20251231", (20 - i) * YI, ann="20260401")
        for i in range(12)
    ]
    curr_a = [
        _holding("A.OF", f"{i:06d}", "20260331", (18 - i) * YI, ann="20260420")
        for i in range(8)
    ] + [_holding("A.OF", "300502", "20260331", 30 * YI, ann="20260420")]
    silent = [_holding("B.OF", "000001", "20251231", 50 * YI, ann="20260401")]
    result = FundCopyworkPipeline(_FakeColl(prev_a + curr_a + silent), fund_basic).run(
        period_prev="20251231",
        period_curr="20260331",
        persist=False,
        as_of="20260830",
    )
    assert result["ok"]
    assert result["quality_status"] == "provisional"
    assert result["compare_mode"] == "top10_vs_top10"
    assert result["n_watchlist"] == 1
    assert "未报" not in (result["products"][0].get("name") or "")
    symbols = {row["symbol"] for row in result["stocks"]}
    assert "000001" not in symbols
    assert "300502" in {h["symbol"] for h in result["products"][0]["holdings"]}


def test_pipeline_official_refuses_top10_period():
    from quant_data_engine.pipelines.fund_copywork_pipeline import FundCopyworkPipeline

    fund_basic = _FakeColl(
        [
            {
                "ts_code": "A.OF",
                "name": "披露混合A",
                "management": "易方达",
                "fund_type": "混合型",
                "invest_type": "偏股混合型",
            }
        ]
    )
    holdings = _FakeColl(
        [
            _holding("A.OF", "300502", "20251231", 80 * YI),
            _holding("A.OF", "300502", "20260331", 90 * YI, ann="20260420"),
        ]
    )
    result = FundCopyworkPipeline(holdings, fund_basic).run(
        period_prev="20251231",
        period_curr="20260331",
        quality_mode="official",
        persist=False,
        as_of="20260830",
    )
    assert not result["ok"]
    assert result["error"] == "official_requires_mature_full"


def test_pipeline_run_default_includes_preview_pairs():
    from quant_data_engine.pipelines.fund_copywork_pipeline import FundCopyworkPipeline

    fund_basic = _FakeColl(
        [
            {
                "ts_code": "A.OF",
                "name": "披露混合A",
                "management": "易方达",
                "fund_type": "混合型",
                "invest_type": "偏股混合型",
            }
        ]
    )
    holdings = _FakeColl(
        [
            _holding("A.OF", "300502", "20250630", 80 * YI, ann="20250801"),
            _holding("A.OF", "300502", "20251231", 90 * YI),
            _holding("A.OF", "600519", "20251231", 40 * YI),
            _holding("A.OF", "300502", "20260331", 95 * YI, ann="20260420"),
            _holding("A.OF", "300502", "20260630", 100 * YI, ann="20260820"),
        ]
    )
    bundled = FundCopyworkPipeline(holdings, fund_basic).run_default(
        persist=False,
        as_of="20260830",
    )
    assert bundled["ok"]
    assert bundled["n_ok"] == 3
    pairs = {(row["period_prev"], row["period_curr"], row["quality_status"]) for row in bundled["results"]}
    assert ("20250630", "20251231", "official") in pairs
    assert ("20251231", "20260331", "provisional") in pairs
    assert ("20251231", "20260630", "provisional") in pairs
