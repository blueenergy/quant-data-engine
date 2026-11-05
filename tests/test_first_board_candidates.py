import mongomock
import pytest

from quant_data_engine.tasks.run_first_board_candidates import (
    DEFAULT_PARAMS,
    normalize_trade_date,
    run_scoring,
    score_candidate,
)


def _feature(symbol: str, gap: float, volume: float, complete: bool = True) -> dict:
    return {
        "symbol": symbol,
        "trade_date": "20261104",
        "gap_pct": gap,
        "auction_volume": volume,
        "auction_amount": 1_000_000.0,
        "auction_amount_ratio_20d": 2.0,
        "price_0915": 10.0,
        "price_0920": 10.1,
        "price_0925": 10.2,
        "price_slope_pct": 2.0,
        "is_auction_complete": complete,
        "feature_version": "auction_v1",
        "source": "qmt_tick_live",
        "partial": False,
    }


def test_normalize_trade_date():
    assert normalize_trade_date("2026-11-04") == "20261104"
    assert normalize_trade_date("20261104") == "20261104"
    with pytest.raises(ValueError):
        normalize_trade_date("2026/11/04")


def test_score_candidate_requires_prev_volume():
    doc = score_candidate(
        symbol="600001.SH",
        trade_date="20261104",
        prev_limit_up_date="20261103",
        feature=_feature("600001.SH", 3.0, 100),
        prev_price=None,
        limit_up={},
        params=DEFAULT_PARAMS,
    )
    assert doc is None


def test_run_scoring_filters_gap_volume_and_prev_day():
    db = mongomock.MongoClient().quant_data
    db.daily_limit_up.insert_many([
        {"date": "20261103", "symbol": "600001.SH", "status": "limit_up", "streak": 1,
         "name": "甲", "seal_amount": 100_000},
        {"date": "20261103", "symbol": "600002.SH", "status": "limit_up", "streak": 1,
         "name": "乙", "seal_amount": 50_000},
        {"date": "20261103", "symbol": "600003.SH", "status": "limit_up", "streak": 2,
         "name": "丙", "seal_amount": 50_000},
        {"date": "20261103", "symbol": "600004.SH", "status": "limit_up", "streak": 1,
         "name": "丁", "seal_amount": 50_000},
        {"date": "20261103", "symbol": "600005.SH", "status": "limit_up", "streak": 1,
         "name": "戊", "seal_amount": 50_000},
        {"date": "20261102", "symbol": "600002.SH", "status": "limit_up", "streak": 1},
    ])
    db.auction_features.insert_many([
        _feature("600001.SH", 3.0, 10_000),
        _feature("600002.SH", 2.0, 10_000),
        _feature("600003.SH", 2.0, 10_000),
        _feature("600004.SH", 7.0, 10_000),
        _feature("600005.SH", 2.0, 100),
    ])
    db.volume_price.insert_many([
        {"symbol": "600001.SH", "trade_date": "20261103", "volume": 200_000, "amount": 2_000_000},
        {"symbol": "600002.SH", "trade_date": "20261103", "volume": 1_000_000, "amount": 2_000_000},
        {"symbol": "600003.SH", "trade_date": "20261103", "volume": 200_000, "amount": 2_000_000},
        {"symbol": "600004.SH", "trade_date": "20261103", "volume": 200_000, "amount": 2_000_000},
        {"symbol": "600005.SH", "trade_date": "20261103", "volume": 1_000_000, "amount": 2_000_000},
    ])

    summary = run_scoring(db, trade_date="20261104", dry_run=False)

    assert summary["prev_limit_up_date"] == "20261103"
    # streak==1 keeps 600001/2/4/5; the 20261102 pool removes 600002 (3 left).
    assert summary["pool_size"] == 3
    assert summary["candidates"] == 1
    candidate = summary["top"][0]
    assert candidate["symbol"] == "600001.SH"
    assert candidate["auction_volume_ratio"] == pytest.approx(0.05)
    assert candidate["prev_volume_lots"] == 200_000
    assert candidate["seal_ratio"] == pytest.approx(0.00005)
    assert candidate["score"] > 0

    stored = db.first_board_candidates.find_one({"trade_date": "20261104", "symbol": "600001.SH"})
    assert stored is not None
    assert stored["model_version"] == "first_board_v0_skeleton"
    run = db.first_board_candidate_runs.find_one({"trade_date": "20261104"})
    assert run is not None
    assert run["candidates"] == 1


def test_run_scoring_excludes_bj_by_default():
    db = mongomock.MongoClient().quant_data
    db.daily_limit_up.insert_one(
        {"date": "20261103", "symbol": "920770.BJ", "status": "limit_up", "streak": 1}
    )
    db.auction_features.insert_one(_feature("920770.BJ", 3.0, 10_000))
    db.volume_price.insert_one(
        {"symbol": "920770.BJ", "trade_date": "20261103", "volume": 200_000, "amount": 2_000_000}
    )
    summary = run_scoring(db, trade_date="20261104", dry_run=True)
    assert summary["pool_size"] == 0
    assert summary["excluded_bj"] == ["920770.BJ"]

    included = run_scoring(db, trade_date="20261104", dry_run=True, include_bj=True)
    assert included["pool_size"] == 1
    assert included["candidates"] == 1


def test_run_scoring_dry_run_writes_nothing():
    db = mongomock.MongoClient().quant_data
    db.daily_limit_up.insert_one(
        {"date": "20261103", "symbol": "600001.SH", "status": "limit_up", "streak": 1}
    )
    db.auction_features.insert_one(_feature("600001.SH", 3.0, 10_000))
    db.volume_price.insert_one(
        {"symbol": "600001.SH", "trade_date": "20261103", "volume": 200_000, "amount": 2_000_000}
    )
    summary = run_scoring(db, trade_date="20261104", dry_run=True)
    assert summary["candidates"] == 1
    assert db.first_board_candidates.count_documents({}) == 0
