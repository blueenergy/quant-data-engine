"""主动公募库存：剥股价、四态、披露口径。"""
from __future__ import annotations

import pytest

from quant_data_engine.fund_inventory.active_funds import (
    is_active_equity_fund,
    share_class_key,
)
from quant_data_engine.fund_inventory.aggregate import (
    FundPosition,
    build_stock_snapshot,
    crop_fund_top10,
    net_ex_price_from_mv,
)
from quant_data_engine.fund_inventory.classify import (
    STATE_EXIT,
    STATE_NEW_INVENTORY,
    STATE_STRENGTHEN,
    STATE_TURNOVER,
    classify_inventory_state,
)
from quant_data_engine.fund_inventory.periods import disclosure_kind, pick_comparable_pair

YI = 1e8


def test_disclosure_kind_and_full_pair():
    assert disclosure_kind("20260630") == "full"
    assert disclosure_kind("20260331") == "top10"
    pair = pick_comparable_pair(
        ["20260331", "20251231", "20260630"],
        as_of="20260901",
    )
    assert pair == ("20251231", "20260630", "full_vs_full")
    seq = pick_comparable_pair(
        ["20260331", "20260630"],
        pair_mode="previous_quarter",
        as_of="20260901",
    )
    assert seq == ("20260331", "20260630", "top10_vs_top10")


def test_unmatured_full_period_is_not_selected():
    pair = pick_comparable_pair(
        ["20250630", "20251231", "20260630"],
        as_of="20260829",
    )
    assert pair == ("20250630", "20251231", "full_vs_full")


def test_active_equity_fund_filter():
    assert is_active_equity_fund(
        {"fund_type": "混合型", "invest_type": "偏股混合型", "name": "某某成长混合A"}
    )
    assert is_active_equity_fund(
        {"fund_type": "股票型", "invest_type": "普通股票型", "name": "某某股票"}
    )
    assert not is_active_equity_fund(
        {"fund_type": "股票型", "invest_type": "被动指数型", "name": "沪深300ETF联接"}
    )
    assert not is_active_equity_fund(
        {"fund_type": "混合型", "invest_type": "灵活配置型", "name": "某某中证500指数增强"}
    )
    assert not is_active_equity_fund(
        {"fund_type": "债券型", "invest_type": "长期纯债型", "name": "某某纯债"}
    )
    assert not is_active_equity_fund(
        {"fund_type": "混合型", "invest_type": "偏债混合型", "name": "某某稳健混合"}
    )
    assert not is_active_equity_fund(
        {"fund_type": "股票型", "invest_type": "普通股票型", "name": "某某股票FOF"}
    )


def test_share_class_key_merges_a_c():
    a = share_class_key("易方达蓝筹精选A", "001753.OF", "易方达")
    c = share_class_key("易方达蓝筹精选C", "001754.OF", "易方达")
    paren = share_class_key("易方达蓝筹精选（C）", "001754.OF", "易方达")
    assert a == c == paren


def test_positions_choose_one_share_class_instead_of_double_counting():
    from quant_data_engine.fund_inventory.aggregate import positions_from_rows

    meta = {
        "001753.OF": {"name": "易方达蓝筹精选A", "management": "易方达"},
        "001754.OF": {"name": "易方达蓝筹精选C", "management": "易方达"},
    }
    positions = positions_from_rows(
        [
            {"ts_code": "001753.OF", "mkv": 100 * YI, "amount": 10},
            {"ts_code": "001754.OF", "mkv": 100 * YI, "amount": 10},
        ],
        fund_meta=meta,
    )
    assert len(positions) == 1
    assert next(iter(positions.values())).mkv == 100 * YI


def test_zhongji_price_inflation_not_buy():
    """中际旭创：市值随股价膨胀，剔除后持股数量略降。"""
    stats = net_ex_price_from_mv(1348.65 * YI, 2999.73 * YI, 1.2304)
    inflated = 1348.65 * YI * (1 + 1.2304)
    assert stats["mv_price_effect"] == inflated - 1348.65 * YI
    assert stats["net_ex_price"] == 2999.73 * YI - inflated
    assert stats["net_ex_price"] < 0
    assert abs(stats["net_ex_price"] / YI + 8.34) < 0.2


def test_four_states_from_article_examples():
    xinyisheng = classify_inventory_state(789 * YI, 286 * YI)
    assert xinyisheng["state"] == STATE_STRENGTHEN
    assert abs(xinyisheng["incumbent_delta"] / YI - 503) < 1e-6

    cambricon = classify_inventory_state(762 * YI, 757 * YI)
    assert cambricon["state"] == STATE_NEW_INVENTORY

    dongshan = classify_inventory_state(-50 * YI, 311 * YI)
    assert dongshan["state"] == STATE_TURNOVER
    assert abs(dongshan["incumbent_delta"] / YI + 361) < 1e-6

    hengtong = classify_inventory_state(-280 * YI, 124 * YI)
    assert hengtong["state"] == STATE_EXIT


def test_crop_fund_top10():
    rows = [
        {"ts_code": "A.OF", "symbol": str(i), "mkv": float(i)}
        for i in range(1, 13)
    ]
    cropped = crop_fund_top10(rows)
    assert len(cropped) == 10
    assert {r["symbol"] for r in cropped} == {str(i) for i in range(3, 13)}


def test_build_stock_snapshot_new_vs_incumbent():
    prev = {
        "old": FundPosition("old", "001.OF", mkv=100 * YI),
    }
    curr = {
        "old": FundPosition("old", "001.OF", mkv=150 * YI),
        "new": FundPosition("new", "002.OF", mkv=80 * YI),
    }
    snap = build_stock_snapshot(
        symbol="300502.SZ",
        period_prev="20251231",
        period_curr="20260630",
        compare_mode="full_vs_full",
        prev_positions=prev,
        curr_positions=curr,
        price_prev=10.0,
        price_curr=10.0,
        adj_prev=1.0,
        adj_curr=1.0,
    )
    assert snap["price_adjusted"] is True
    assert snap["price_ret"] == 0.0
    assert snap["n_new"] == 1
    assert snap["new_inflow"] == 80 * YI
    assert snap["net_ex_price"] == 130 * YI
    assert snap["state"] == STATE_STRENGTHEN


def test_build_stock_snapshot_does_not_fake_net_without_adjustment():
    snap = build_stock_snapshot(
        symbol="300502.SZ",
        period_prev="20251231",
        period_curr="20260630",
        compare_mode="full_vs_full",
        prev_positions={"old": FundPosition("old", "001.OF", mkv=100 * YI)},
        curr_positions={"old": FundPosition("old", "001.OF", mkv=200 * YI)},
        price_prev=10.0,
        price_curr=20.0,
    )
    assert snap["price_adjusted"] is False
    assert snap["net_ex_price"] is None
    assert snap["state"] is None


def test_build_universe_snapshots_filters_index_funds():
    from quant_data_engine.fund_inventory.builder import build_universe_snapshots

    fund_meta = {
        "001753.OF": {
            "ts_code": "001753.OF",
            "name": "主动混合A",
            "fund_type": "混合型",
            "invest_type": "偏股混合型",
        },
    }
    prev_rows = [
        {"ts_code": "001753.OF", "symbol": "300502", "mkv": 100 * YI, "amount": 1e6},
        {"ts_code": "510300.SH", "symbol": "300502", "mkv": 999 * YI, "amount": 9e6},
    ]
    curr_rows = [
        {"ts_code": "001753.OF", "symbol": "300502", "mkv": 220 * YI, "amount": 1.1e6},
    ]
    snaps = build_universe_snapshots(
        prev_rows,
        curr_rows,
        fund_meta=fund_meta,
        period_prev="20251231",
        period_curr="20260630",
        compare_mode="full_vs_full",
        price_lookup=lambda _k: {
            "price_prev": 10.0,
            "price_curr": 20.0,
            "adj_prev": 1.0,
            "adj_curr": 1.0,
        },
        stock_info_lookup=lambda _k: {"ts_code": "300502.SZ", "name": "新易盛", "industry": "电子"},
    )
    assert len(snaps) == 1
    snap = snaps[0]
    assert snap["name"] == "新易盛"
    # 100亿 * (1+100%) = 200亿，实际 220亿 → 净增 20亿，且无新进
    assert snap["price_ret"] == 1.0
    assert abs(snap["net_ex_price"] - 20 * YI) < 1e-6
    assert snap["n_new"] == 0
    assert snap["state"] == STATE_STRENGTHEN


def test_build_universe_snapshots_skips_non_a_share_and_missing_adjustment():
    from quant_data_engine.fund_inventory.builder import build_universe_snapshots

    fund_meta = {
        "001753.OF": {
            "ts_code": "001753.OF",
            "name": "主动混合A",
            "fund_type": "混合型",
            "invest_type": "偏股混合型",
        },
    }
    rows = [
        {"ts_code": "001753.OF", "symbol": "300502", "mkv": 100 * YI},
        {"ts_code": "001753.OF", "symbol": "0700.HK", "mkv": 100 * YI},
    ]

    def info(key):
        if key == "300502":
            return {"ts_code": "300502.SZ", "name": "新易盛"}
        return {}

    snapshots = build_universe_snapshots(
        rows,
        rows,
        fund_meta=fund_meta,
        period_prev="20251231",
        period_curr="20260630",
        compare_mode="full_vs_full",
        price_lookup=lambda key: {
            "price_prev": 10.0,
            "price_curr": 10.0,
            "adj_prev": 1.0 if key == "300502" else None,
            "adj_curr": 1.0 if key == "300502" else None,
        },
        stock_info_lookup=info,
    )
    assert [row["symbol"] for row in snapshots] == ["300502"]


class _FakeColl:
    def __init__(self, docs=None):
        self.docs = list(docs or [])

    def distinct(self, field):
        return list({doc.get(field) for doc in self.docs if doc.get(field) is not None})

    def find(self, query=None, projection=None, sort=None):
        rows = [dict(doc) for doc in self.docs if self._matches(doc, query or {})]
        if sort:
            for field, direction in reversed(sort):
                rows.sort(key=lambda row: row.get(field) or "", reverse=direction < 0)
        return rows

    def find_one(self, query=None, projection=None, sort=None):
        rows = self.find(query, projection, sort)
        return rows[0] if rows else None

    def _matches(self, doc, query):
        for key, expected in query.items():
            if key == "$or":
                if not any(self._matches(doc, clause) for clause in expected):
                    return False
                continue
            value = doc.get(key)
            if isinstance(expected, dict):
                if "$in" in expected and value not in expected["$in"]:
                    return False
                if "$lte" in expected and not (str(value) <= str(expected["$lte"])):
                    return False
                continue
            if value != expected:
                return False
        return True


class _FakeRepo:
    def __init__(self):
        self.saved = []

    def replace_snapshots(self, snapshots):
        self.saved = list(snapshots)
        return len(self.saved)


def test_pipeline_writes_snapshots_for_full_pair():
    from quant_data_engine.pipelines.fund_inventory_pipeline import FundInventoryPipeline

    holdings = _FakeColl(
        [
            {"ts_code": "001753.OF", "symbol": "300502.SZ", "end_date": "20251231", "mkv": 100 * YI},
            {"ts_code": "001753.OF", "symbol": "300502.SZ", "end_date": "20260630", "mkv": 120 * YI},
            {"ts_code": "001753.OF", "symbol": "300502.SZ", "end_date": "20260331", "mkv": 110 * YI},
            {
                "ts_code": "001753.OF",
                "symbol": "700.HK",
                "end_date": "20251231",
                "mkv": 50 * YI,
                "amount": 10,
            },
            {
                "ts_code": "001753.OF",
                "symbol": "00700.HK",
                "end_date": "20260630",
                "mkv": 60 * YI,
                "amount": 10,
            },
        ]
    )
    fund_basic = _FakeColl(
        [
            {
                "ts_code": "001753.OF",
                "name": "主动混合A",
                "fund_type": "混合型",
                "invest_type": "偏股混合型",
            }
        ]
    )
    prices = _FakeColl(
        [
            {"symbol": "300502.SZ", "trade_date": "20251231", "close": 10.0},
            {"symbol": "300502.SZ", "trade_date": "20260630", "close": 10.0},
        ]
    )
    repo = _FakeRepo()
    pipe = FundInventoryPipeline(
        holdings_coll=holdings,
        fund_basic_coll=fund_basic,
        volume_price_coll=prices,
        snapshot_repo=repo,
        adj_factor_coll=_FakeColl(
            [
                {"symbol": "300502.SZ", "trade_date": "20251231", "adj_factor": 1.0},
                {"symbol": "300502.SZ", "trade_date": "20260630", "adj_factor": 1.0},
            ]
        ),
        stock_info_coll=_FakeColl(
            [{"symbol": "300502.SZ", "ts_code": "300502.SZ", "name": "新易盛", "industry": "电子"}]
        ),
        hk_stock_info_coll=_FakeColl(
            [{"symbol": "00700.HK", "ts_code": "00700.HK", "name": "腾讯控股"}]
        ),
    )
    preview = pipe.run(as_of="20260829")
    assert preview["ok"] is True
    assert preview["quality_status"] == "provisional"
    assert preview["comparison_scope"] == "top10"
    assert preview["compare_mode"] == "top10_vs_top10"
    assert repo.saved[0]["quality"]["status"] == "provisional"

    result = pipe.run(as_of="20260901")
    assert result["ok"] is True
    assert result["period_prev"] == "20251231"
    assert result["period_curr"] == "20260630"
    assert result["compare_mode"] == "full_vs_full"
    assert result["quality_status"] == "official"
    assert result["n_snapshots"] == 2
    a_share = next(row for row in repo.saved if row["market"] == "A")
    hk_share = next(row for row in repo.saved if row["market"] == "HK")
    assert a_share["state"] == STATE_STRENGTHEN
    assert hk_share["ts_code"] == "00700.HK"
    assert hk_share["price_source"] == "fund_report_implied"
    assert hk_share["corporate_action_adjusted"] is False


def test_pipeline_rejects_incomplete_current_full_period():
    from quant_data_engine.pipelines.fund_inventory_pipeline import FundInventoryPipeline

    prev = [
        {
            "ts_code": "001753.OF",
            "symbol": f"300{i:03d}.SZ",
            "end_date": "20251231",
            "ann_date": "20260301",
            "mkv": 100 * YI,
        }
        for i in range(10)
    ]
    curr = [
        {
            "ts_code": "001753.OF",
            "symbol": "300000.SZ",
            "end_date": "20260630",
            "ann_date": "20260831",
            "mkv": 100 * YI,
        }
    ]
    pipe = FundInventoryPipeline(
        holdings_coll=_FakeColl(prev + curr),
        fund_basic_coll=_FakeColl(
            [
                {
                    "ts_code": "001753.OF",
                    "name": "主动混合A",
                    "fund_type": "混合型",
                    "invest_type": "偏股混合型",
                }
            ]
        ),
        volume_price_coll=_FakeColl([]),
        snapshot_repo=_FakeRepo(),
    )
    result = pipe.run(
        period_prev="20251231",
        period_curr="20260630",
        as_of="20260901",
        quality_mode="official",
    )
    assert result["ok"] is False
    assert result["error"] == "incomplete_current_period"
    assert result["row_ratio"] == 0.1


def test_provisional_cohort_ignores_funds_that_have_not_reported_current_period():
    from quant_data_engine.pipelines.fund_inventory_pipeline import (
        _provisional_reporting_cohort,
    )

    meta = {
        "001.OF": {"name": "已披露基金A", "management": "甲"},
        "002.OF": {"name": "未披露基金A", "management": "乙"},
    }
    previous = [
        {"ts_code": "001.OF", "symbol": "300001.SZ"},
        {"ts_code": "002.OF", "symbol": "300001.SZ"},
    ]
    current = [{"ts_code": "001.OF", "symbol": "300001.SZ"}]

    cohort_previous, cohort_current = _provisional_reporting_cohort(
        previous,
        current,
        meta,
    )
    assert [row["ts_code"] for row in cohort_previous] == ["001.OF"]
    assert [row["ts_code"] for row in cohort_current] == ["001.OF"]


def test_hk_report_price_uses_canonical_code_and_cross_fund_median():
    from quant_data_engine.fund_inventory.aggregate import stock_key
    from quant_data_engine.pipelines.fund_inventory_pipeline import (
        _implied_report_prices,
    )

    prices = _implied_report_prices(
        [
            {"symbol": "700.HK", "mkv": 400.0, "amount": 2.0},
            {"symbol": "00700.HK", "mkv": 600.0, "amount": 3.0},
            {"symbol": "000001.SZ", "mkv": 100.0, "amount": 10.0},
        ]
    )
    assert stock_key("700.HK") == "00700.HK"
    assert prices == {"00700.HK": 200.0}


def test_fund_basic_clean_skips_non_scalar_isna():
    from quant_data_engine.repositories.fund_basic_repository import FundBasicRepository

    row = FundBasicRepository._clean(
        {
            "ts_code": "001753.OF",
            "name": float("nan"),
            "tags": ["主动", "偏股"],
        }
    )
    assert row["ts_code"] == "001753.OF"
    assert row["name"] is None
    assert row["tags"] == ["主动", "偏股"]


def test_fetch_fund_portfolio_paged_rejects_ignored_offset():
    import pandas as pd
    from quant_data_engine.fund_inventory.tushare_pages import fetch_fund_portfolio_paged

    class _Adapter:
        def fetch_fund_portfolio(self, **kwargs):
            return pd.DataFrame({"ts_code": ["001753.OF"], "symbol": ["300502"], "mkv": [1.0]})

    with pytest.raises(RuntimeError, match="offset appears ignored"):
        fetch_fund_portfolio_paged(_Adapter(), period="20260630", page_limit=1, max_pages=5)


def test_fetch_fund_portfolio_paged_continues_after_short_server_page():
    import pandas as pd
    from quant_data_engine.fund_inventory.tushare_pages import fetch_fund_portfolio_paged

    class _Adapter:
        def fetch_fund_portfolio(self, **kwargs):
            pages = {
                0: ["000001", "000002"],
                2: ["000003"],
                3: [],
            }
            symbols = pages[kwargs["offset"]]
            return pd.DataFrame(
                {
                    "ts_code": ["001753.OF"] * len(symbols),
                    "symbol": symbols,
                    "mkv": [1.0] * len(symbols),
                }
            )

    df = fetch_fund_portfolio_paged(
        _Adapter(),
        period="20260630",
        page_limit=5,
        max_pages=5,
    )
    assert df["symbol"].tolist() == ["000001", "000002", "000003"]
