from quant_data_engine.tasks.sector_follower import (
    auction_sealed,
    evaluate_laggard,
    first_minute_bullish,
    leader_has_tracked,
    limit_price,
    seal_metrics,
    sector_key_l2,
    tracked_in_sector,
)


def _dongfang_feature(**overrides):
    feature = {
        "symbol": "300118.SZ",
        "name": "东方日升",
        "prev_close": 8.91,
        "price_0915": 8.49,
        "price_0920": 8.35,
        "price_0925": 8.57,
        "gap_pct": -3.8159,
        "reclaim_from_low_pct": 3.253,
        "oi5_0920_0925": 0.0878,
        "oi5_match_0925": 0.7685,
        "auction_volume": 21318.0,
        "auction_amount": 18269920.22,
    }
    feature.update(overrides)
    return feature


def _first_minute(**overrides):
    bar = {
        "open": 8.58,
        "high": 8.85,
        "low": 8.56,
        "close": 8.85,
        "volume": 56476.0,
    }
    bar.update(overrides)
    return bar


def test_limit_price_boards():
    assert limit_price(8.91, "300118.SZ", "东方日升") == 10.69
    assert limit_price(18.85, "603381.SH", "永臻股份") == 20.74
    assert limit_price(10.0, "600000.SH", "ST测试") == 10.50
    assert limit_price(10.0, "830000.BJ", "北交测试") == 13.00


def test_auction_sealed():
    assert auction_sealed({
        "symbol": "603381.SH", "name": "永臻股份", "prev_close": 18.85, "price_0925": 20.74,
    })
    assert not auction_sealed(_dongfang_feature())


def test_dongfang_weak_open_reclaim_triggers():
    passed, metrics = evaluate_laggard(_dongfang_feature(), _first_minute())
    assert passed
    assert round(metrics["lift_pct"], 2) == 2.63
    assert metrics["oi5"] == 0.7685
    assert metrics["first_minute_close_at_high"]


def test_positive_gap_does_not_trigger():
    passed, _ = evaluate_laggard(_dongfang_feature(gap_pct=2.0), _first_minute())
    assert not passed


def test_weak_oi5_does_not_trigger():
    passed, _ = evaluate_laggard(
        _dongfang_feature(oi5_0920_0925=0.1, oi5_match_0925=0.2), _first_minute()
    )
    assert not passed


def test_first_minute_must_close_at_high():
    passed, _ = evaluate_laggard(_dongfang_feature(), _first_minute(close=8.70))
    assert not passed
    assert not first_minute_bullish(_first_minute(close=8.70), 21318.0)


def test_first_minute_volume_must_be_material():
    passed, _ = evaluate_laggard(_dongfang_feature(), _first_minute(volume=1000.0))
    assert not passed


def test_low_gap_reclaim_needs_the_lift():
    passed, _ = evaluate_laggard(
        _dongfang_feature(price_0920=8.52, price_0925=8.57), _first_minute()
    )
    assert not passed


def test_seal_metrics_broken_and_sealed():
    broken = seal_metrics(
        {"status": "broken", "limit_up_status": "open", "seal_amount": 258892558,
         "turnover_ratio": 0.1563, "limit_up_time": 95712},
        {"amount": 1554767.7, "close": 10.36, "pct_chg": 16.27},
    )
    assert broken["state"] == "broken"
    assert round(broken["seal_ratio"], 3) == 0.167
    sealed = seal_metrics(
        {"status": "limit_up", "limit_up_status": "sealed", "seal_amount": 100000000,
         "turnover_ratio": 0.04},
        {"amount": 100000.0},
    )
    assert sealed["state"] == "sealed"
    assert sealed["seal_ratio"] == 1.0


def test_sector_key_l2_prefers_l2():
    member = {
        "l2_code": "801735.SI", "l2_name": "光伏设备",
        "l3_code": "857352.SI", "l3_name": "光伏电池组件",
    }
    code, name, level = sector_key_l2(member)
    assert code == "801735.SI"
    assert name == "光伏设备"
    assert level == "L2"


def test_leader_alert_only_for_tracked_sector():
    members = {
        "300118.SZ": {"l2_code": "801735.SI", "l3_code": "857352.SI"},
        "600000.SH": {"l2_code": "801780.SI", "l3_code": "857111.SI"},
    }
    tracked = {"300118.SZ"}
    assert leader_has_tracked(members, tracked, "801735.SI", "L2")
    assert not leader_has_tracked(members, tracked, "801780.SI", "L2")
    assert tracked_in_sector(members, tracked, "801735.SI", "L2") == ["300118.SZ"]
    assert tracked_in_sector(members, tracked, "801780.SI", "L2") == []
