"""Tests for market events identity, normalization, merge, and filters."""

from __future__ import annotations

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import pytest

from quant_data_engine.market_events.filters import MarketEventsFilterConfig
from quant_data_engine.market_events.identity import (
    build_identity_key,
    extract_ref_period,
    normalize_symbol,
    parse_shanghai_datetime,
    resolve_metric_code,
)
from quant_data_engine.market_events.normalizer import (
    build_document,
    merge_documents,
    merge_metrics,
    score_report_importance,
)


def test_parse_shanghai_datetime_to_utc():
    dt = parse_shanghai_datetime("2026-07-23 09:30:00")
    assert dt.tzinfo is not None
    local = dt.astimezone(ZoneInfo("Asia/Shanghai"))
    assert local.hour == 9 and local.minute == 30


def test_resolve_metric_code_cpi_yoy_mom_core():
    assert resolve_metric_code("美国6月CPI年率") == "CPI_YOY"
    assert resolve_metric_code("美国6月CPI同比") == "CPI_YOY"
    assert resolve_metric_code("美国6月CPI环比") == "CPI_MOM"
    assert resolve_metric_code("美国6月核心CPI年率") == "CORE_CPI"
    assert resolve_metric_code("美国非农就业人口") == "NFP"


def test_identity_key_macro_yoy_mom_distinct():
    key_yoy = build_identity_key(
        kind="macro",
        region="US",
        metric_code="CPI_YOY",
        ref_period="2026-06",
        title="美国6月CPI年率",
    )
    key_mom = build_identity_key(
        kind="macro",
        region="US",
        metric_code="CPI_MOM",
        ref_period="2026-06",
        title="美国6月CPI环比",
    )
    key_core = build_identity_key(
        kind="macro",
        region="US",
        metric_code="CORE_CPI",
        ref_period="2026-06",
        title="美国6月核心CPI",
    )
    assert key_yoy != key_mom
    assert key_yoy != key_core


def test_identity_key_macro_semantic_same_code():
    key1 = build_identity_key(
        kind="macro",
        region="US",
        metric_code="CPI_YOY",
        ref_period="2026-06",
        title="美国6月CPI",
    )
    key2 = build_identity_key(
        kind="macro",
        region="US",
        metric_code="CPI_YOY",
        ref_period="2026-06",
        title="美国CPI同比",
    )
    assert key1 == key2


def test_dual_macro_sources_merge_same_identity():
    cfg = MarketEventsFilterConfig(min_importance=1)
    et = parse_shanghai_datetime("2026-07-10 20:30:00")
    raw_ws = {
        "identity_kind": "macro",
        "source_name": "wallstreetcn",
        "title": "美国6月CPI年率",
        "event_time": et,
        "region": "US",
        "category": "macro_data",
        "importance": 4,
        "metrics": {"forecast": 3.1, "previous": 3.0, "actual": None},
    }
    raw_bd = {
        "identity_kind": "macro",
        "source_name": "baidu",
        "title": "美国6月CPI年率",
        "event_time": et,
        "region": "US",
        "category": "macro_data",
        "importance": 4,
        "metrics": {"forecast": 3.1, "previous": 3.0, "actual": None},
    }
    d1 = build_document(raw_ws, cfg)
    d2 = build_document(raw_bd, cfg)
    assert d1 and d2
    assert d1["identity_key"] == d2["identity_key"]
    merged = merge_documents([d1, d2])
    assert len(merged) == 1
    assert len(merged[0]["sources"]) == 2
    names = {s["name"] for s in merged[0]["sources"]}
    assert names == {"wallstreetcn", "baidu"}


def test_merge_metrics_preserves_non_null():
    assert merge_metrics(
        {"forecast": 3.1, "previous": 3.0, "actual": 3.2},
        {"forecast": None, "previous": "", "actual": None},
    ) == {"forecast": 3.1, "previous": 3.0, "actual": 3.2}
    assert merge_metrics(
        {"forecast": 3.1},
        {"previous": 2.9, "actual": 3.0},
    ) == {"forecast": 3.1, "previous": 2.9, "actual": 3.0}


def test_merge_documents_empty_metrics_does_not_wipe():
    cfg = MarketEventsFilterConfig(min_importance=1)
    et = parse_shanghai_datetime("2026-07-10 20:30:00")
    ws = build_document(
        {
            "identity_kind": "macro",
            "source_name": "wallstreetcn",
            "title": "美国6月CPI年率",
            "event_time": et,
            "region": "US",
            "category": "macro_data",
            "importance": 4,
            "metrics": {"forecast": 3.1, "previous": 3.0},
        },
        cfg,
    )
    bd = build_document(
        {
            "identity_kind": "macro",
            "source_name": "baidu",
            "title": "美国6月CPI年率",
            "event_time": et,
            "region": "US",
            "category": "macro_data",
            "importance": 4,
            "metrics": {},
        },
        cfg,
    )
    merged = merge_documents([ws, bd])
    assert merged[0]["metrics"]["forecast"] == 3.1
    assert merged[0]["metrics"]["previous"] == 3.0


def test_empty_whitelist_blocks_individual_allows_aggregate_only():
    cfg = MarketEventsFilterConfig(
        symbol_whitelist=set(),
        individual_require_whitelist=True,
    )
    assert cfg.symbol_allowed("600000.SH") is False
    assert score_report_importance("600000.SH", cfg) == 3

    cfg_open = MarketEventsFilterConfig(
        symbol_whitelist=set(),
        individual_require_whitelist=False,
    )
    assert cfg_open.symbol_allowed("600000.SH") is True

    cfg_wl = MarketEventsFilterConfig(symbol_whitelist={"600000.SH"})
    assert cfg_wl.symbol_allowed("600000") is True
    assert cfg_wl.symbol_allowed("000001.SZ") is False
    assert score_report_importance("600000.SH", cfg_wl) == 4


def test_cn_report_reschedule_same_identity():
    cfg = MarketEventsFilterConfig(min_importance=1)
    t1 = parse_shanghai_datetime("2026-08-15 09:00:00")
    t2 = parse_shanghai_datetime("2026-08-20 09:00:00")
    base = {
        "identity_kind": "cn_report",
        "source_name": "cninfo_disclosure",
        "title": "浦发银行(600000.SH) 2026半年报预约披露",
        "region": "CN",
        "category": "earnings",
        "event_subtype": "report_disclosure",
        "symbol": "600000.SH",
        "symbol_name": "浦发银行",
        "ref_period": "2026Q2",
        "importance": 4,
    }
    d1 = build_document({**base, "event_time": t1}, cfg)
    d2 = build_document({**base, "event_time": t2}, cfg)
    assert d1 and d2
    assert d1["identity_key"] == d2["identity_key"]


def test_normalize_symbol():
    assert normalize_symbol("600000") == "600000.SH"
    assert normalize_symbol("000001.SZ") == "000001.SZ"


def test_extract_ref_period_month():
    et = datetime(2026, 7, 10, tzinfo=timezone.utc)
    assert extract_ref_period("美国6月CPI年率", et) == "2026-06"
