import mongomock

from quant_data_engine.tasks.realtime_sector_strength import calculate_realtime_sector_strength


def test_calculate_realtime_sector_strength_writes_l1_l2_l3_groups():
    client = mongomock.MongoClient()
    data_db = client["quant_data"]
    realtime_db = client["quant_realtime"]

    data_db["volume_price"].insert_many(
        [
            {"symbol": "000001.SZ", "trade_date": "20260512", "close": 9.8},
            {"symbol": "000001.SZ", "trade_date": "20260513", "close": 10.0},
            {"symbol": "000001.SZ", "trade_date": "20260514", "close": 10.1},
            {"symbol": "000001.SZ", "trade_date": "20260515", "close": 10.5},
            {"symbol": "000002.SZ", "trade_date": "20260512", "close": 19.8},
            {"symbol": "000002.SZ", "trade_date": "20260513", "close": 20.0},
            {"symbol": "000002.SZ", "trade_date": "20260514", "close": 20.1},
            {"symbol": "000002.SZ", "trade_date": "20260515", "close": 19.5},
        ]
    )
    data_db["sw_index_member"].insert_many(
        [
            {
                "con_code": "000001.SZ",
                "src": "SW2021",
                "is_new": "Y",
                "l1_code": "801010.SI",
                "l1_name": "银行",
                "l2_code": "801011.SI",
                "l2_name": "国有大型银行",
                "l3_code": "851011.SI",
                "l3_name": "大型银行",
            },
            {
                "con_code": "000002.SZ",
                "src": "SW2021",
                "is_new": "Y",
                "l1_code": "801010.SI",
                "l1_name": "银行",
                "l2_code": "801012.SI",
                "l2_name": "股份制银行",
                "l3_code": "851012.SI",
                "l3_name": "股份银行",
            },
        ]
    )
    realtime_db["volume_price_realtime_latest"].insert_many(
        [
            {
                "symbol": "000001.SZ",
                "trade_date": "20260515",
                "close": 10.5,
                "pct_chg": 1.5,
                "volume": 1000,
                "realtime_amount_yuan": 10500,
                "realtime_num": 10,
                "realtime_bid_volume1": 600,
                "realtime_ask_volume1": 400,
            },
            {
                "symbol": "000002.SZ",
                "trade_date": "20260515",
                "close": 19.5,
                "pct_chg": -0.5,
                "volume": 2000,
                "amount": 39,
                "realtime_num": 20,
                "realtime_bid_volume1": 300,
                "realtime_ask_volume1": 700,
            },
        ]
    )

    result = calculate_realtime_sector_strength(
        client,
        data_db_name="quant_data",
        realtime_db_name="quant_realtime",
        snapshot_time="202605151030",
        ma_period=3,
        verbose=False,
    )

    assert result["success"] is True
    assert realtime_db["realtime_sector_strength"].count_documents({}) == 5
    l1 = realtime_db["realtime_sector_strength"].find_one({"level": "L1", "sector_code": "801010.SI"})
    assert l1["stock_count"] == 2
    assert l1["positive_ratio"] == 0.5
    assert l1["ma_period"] == 3
    assert l1["above_ma5_count"] == 1
    assert l1["total_amount_yuan"] == 49500


def test_realtime_ma_matches_daily_sector_uses_realtime_close_not_stale_vp_row():
    """Same definition as daily sector spectrum: mean(last N closes including today).

    Intraday ``volume_price`` may already contain a same-day row; realtime must
    substitute today's close from the snapshot, not that row, inside the MA.
    """
    client = mongomock.MongoClient()
    data_db = client["quant_data"]
    realtime_db = client["quant_realtime"]

    data_db["volume_price"].insert_many(
        [
            {"symbol": "000001.SZ", "trade_date": "20260512", "close": 9.8},
            {"symbol": "000001.SZ", "trade_date": "20260513", "close": 10.0},
            {"symbol": "000001.SZ", "trade_date": "20260514", "close": 10.1},
            # Stale / partial same-day bar (must not drive MA vs realtime snapshot)
            {"symbol": "000001.SZ", "trade_date": "20260515", "close": 100.0},
            {"symbol": "000002.SZ", "trade_date": "20260512", "close": 19.8},
            {"symbol": "000002.SZ", "trade_date": "20260513", "close": 20.0},
            {"symbol": "000002.SZ", "trade_date": "20260514", "close": 20.1},
            {"symbol": "000002.SZ", "trade_date": "20260515", "close": 19.5},
        ]
    )
    data_db["sw_index_member"].insert_many(
        [
            {
                "con_code": "000001.SZ",
                "src": "SW2021",
                "is_new": "Y",
                "l1_code": "801010.SI",
                "l1_name": "银行",
                "l2_code": "801011.SI",
                "l2_name": "国有大型银行",
                "l3_code": "851011.SI",
                "l3_name": "大型银行",
            },
            {
                "con_code": "000002.SZ",
                "src": "SW2021",
                "is_new": "Y",
                "l1_code": "801010.SI",
                "l1_name": "银行",
                "l2_code": "801012.SI",
                "l2_name": "股份制银行",
                "l3_code": "851012.SI",
                "l3_name": "股份银行",
            },
        ]
    )
    realtime_db["volume_price_realtime_latest"].insert_many(
        [
            {
                "symbol": "000001.SZ",
                "trade_date": "20260515",
                "close": 10.5,
                "pct_chg": 1.5,
                "volume": 1000,
                "realtime_amount_yuan": 10500,
                "realtime_num": 10,
                "realtime_bid_volume1": 600,
                "realtime_ask_volume1": 400,
            },
            {
                "symbol": "000002.SZ",
                "trade_date": "20260515",
                "close": 19.5,
                "pct_chg": -0.5,
                "volume": 2000,
                "amount": 39,
                "realtime_num": 20,
                "realtime_bid_volume1": 300,
                "realtime_ask_volume1": 700,
            },
        ]
    )

    result = calculate_realtime_sector_strength(
        client,
        data_db_name="quant_data",
        realtime_db_name="quant_realtime",
        snapshot_time="202605151030",
        ma_period=3,
        verbose=False,
    )

    assert result["success"] is True
    l1 = realtime_db["realtime_sector_strength"].find_one({"level": "L1", "sector_code": "801010.SI"})
    # 000001: S=10.0+10.1, P=10.5 -> MA=(20.1+10.5)/3=10.2 -> above
    # 000002: S=20.0+20.1, P=19.5 -> MA=(40.1+19.5)/3≈19.87 -> below
    assert l1["above_ma5_count"] == 1
