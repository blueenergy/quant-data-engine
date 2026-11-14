"""Hot stock sync: per-market sources share one Tushare call and never overwrite."""

from quant_data_engine.tasks.run_hot_stock_sync import SOURCE_CONFIG, sync_hot_stock


class FakeAdapter:
    def __init__(self):
        self.calls = []

    def fetch_ths_hot(self, trade_date=""):
        self.calls.append(("ths_hot", trade_date))
        rows = [
            {"trade_date": trade_date, "data_type": "热股", "ts_code": f"60000{i}.SH",
             "ts_name": f"A股{i}", "rank": i, "pct_change": 1.0, "rank_time": f"{trade_date} 22:30:00"}
            for i in range(1, 31)
        ] + [
            {"trade_date": trade_date, "data_type": "港股", "ts_code": f"0070{i}.HK",
             "ts_name": f"港股{i}", "rank": i, "pct_change": -1.0, "rank_time": f"{trade_date} 22:30:00"}
            for i in range(1, 31)
        ]
        import pandas as pd
        return pd.DataFrame(rows)

    def fetch_dc_hot(self, trade_date="", market="A股市场"):
        self.calls.append(("dc_hot", trade_date, market))
        import pandas as pd
        name = "A股市场榜" if market == "A股市场" else "港股市场榜"
        return pd.DataFrame([
            {"trade_date": trade_date, "data_type": market, "ts_code": f"{i:05d}.HK" if market != "A股市场" else f"6000{i:02d}.SH",
             "ts_name": f"{name}{i}", "rank": i, "pct_change": 0.5,
             "rank_time": f"{trade_date} 22:30:03"}
            for i in range(1, 31)
        ])


class FakeCollection:
    def __init__(self):
        self.docs = {}

    def update_one(self, flt, update, upsert=False):
        key = (flt["source"], flt["trade_date"])
        self.docs[key] = update["$set"]


class FakeDB:
    def __init__(self):
        self.col = FakeCollection()

    def __getitem__(self, name):
        assert name == "hot_stock_raw"
        return self.col


def test_hk_source_config_exists_for_both_providers():
    assert SOURCE_CONFIG["ths_hk"]["data_type"] == "港股"
    assert SOURCE_CONFIG["dc_hk"]["fetch_kwargs"] == {"market": "港股市场"}
    assert SOURCE_CONFIG["dc"]["fetch_kwargs"] == {"market": "A股市场"}


def test_ths_and_ths_hk_share_one_fetch():
    adapter = FakeAdapter()
    db = FakeDB()
    cache = {}

    a = sync_hot_stock(adapter, "ths", db, cache=cache)
    hk = sync_hot_stock(adapter, "ths_hk", db, cache=cache)

    assert a["success"] and hk["success"]
    # One ths_hot call serves both sources.
    assert len([c for c in adapter.calls if c[0] == "ths_hot"]) == 1
    assert a["stock_count"] == 30
    assert hk["stock_count"] == 30


def test_hk_sources_are_stored_under_their_own_keys():
    adapter = FakeAdapter()
    db = FakeDB()

    sync_hot_stock(adapter, "dc_hk", db, cache={})

    stored = db.col.docs
    assert ("dc_hk", next(iter(stored))[1]) in stored
    doc = next(iter(stored.values()))
    assert doc["source"] == "dc_hk"
    assert doc["source_name"] == "东方财富(港股)"
    assert doc["data_type"] == "港股市场"
    assert all(r["ts_code"].endswith(".HK") for r in doc["rows"])


def test_missing_data_type_reports_failure_instead_of_storing_a_share_list():
    adapter = FakeAdapter()
    db = FakeDB()

    # dc_hk asks for 港股市场 but the fake adapter returns A股市场 rows only
    class WrongMarketAdapter(FakeAdapter):
        def fetch_dc_hot(self, trade_date="", market="A股市场"):
            import pandas as pd
            return pd.DataFrame([
                {"trade_date": trade_date, "data_type": "A股市场", "ts_code": f"6000{i:02d}.SH",
                 "ts_name": f"x{i}", "rank": i, "pct_change": 1.0,
                 "rank_time": f"{trade_date} 22:30:03"}
                for i in range(1, 31)
            ])

    result = sync_hot_stock(WrongMarketAdapter(), "dc_hk", db, cache={})

    assert result["success"] is False
    assert "港股市场" in result["error"]
    assert db.col.docs == {}
