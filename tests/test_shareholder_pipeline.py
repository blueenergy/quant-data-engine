import pandas as pd

from quant_data_engine.pipelines.shareholder_pipeline import ShareholderPipeline


class FakeAdapter:
    """Single-page fake: returns rows only for offset 0, empty afterwards."""

    enabled = True

    def __init__(self):
        self.calls = []

    def fetch_stk_holdernumber(self, start_date="", end_date="", offset=None, limit=None):
        self.calls.append(("holdernumber", start_date, end_date, offset))
        if offset:
            return pd.DataFrame()
        return pd.DataFrame({"ts_code": ["300196.SZ"], "end_date": [end_date], "holder_num": [1]})

    def fetch_stk_holdertrade(self, ann_date="", offset=None, limit=None):
        self.calls.append(("holdertrade", ann_date, offset))
        if offset:
            return pd.DataFrame()
        return pd.DataFrame({"ts_code": ["300196.SZ"], "ann_date": [ann_date]})

    def fetch_share_float(self, ann_date="", offset=None, limit=None):
        self.calls.append(("share_float", ann_date, offset))
        return pd.DataFrame()

    def fetch_repurchase(self, ann_date="", offset=None, limit=None):
        self.calls.append(("repurchase", ann_date, offset))
        if offset:
            return pd.DataFrame()
        return pd.DataFrame({"ts_code": ["300196.SZ"], "ann_date": [ann_date], "proc": ["完成"]})

    def fetch_hk_hold(self, trade_date="", offset=None, limit=None):
        self.calls.append(("hk_hold", trade_date, offset))
        if offset:
            return pd.DataFrame()
        return pd.DataFrame({"ts_code": ["300196.SZ"], "trade_date": [trade_date]})


class PagedAdapter(FakeAdapter):
    """Two-page fake for hk_hold to exercise pagination."""

    def fetch_hk_hold(self, trade_date="", offset=None, limit=None):
        self.calls.append(("hk_hold", trade_date, offset))
        off = offset or 0
        if off == 0:
            return pd.DataFrame({"ts_code": [f"{i:06d}.SZ" for i in range(limit)], "trade_date": [trade_date] * limit})
        if off == limit:
            return pd.DataFrame({"ts_code": ["300196.SZ"], "trade_date": [trade_date]})
        return pd.DataFrame()


class FakeRepo:
    def _n(self, df):
        return 0 if df is None or df.empty else len(df)

    upsert_holdernumber = _n
    upsert_holdertrade = _n
    upsert_share_float = _n
    upsert_repurchase = _n
    upsert_hk_hold = _n


def test_run_holdernumber_by_range():
    pipe = ShareholderPipeline(FakeAdapter(), FakeRepo())
    result = pipe.run_holdernumber("20180101", "20260601")
    assert result["records"] == 1
    assert result["status"] == "success"


def test_run_events_aggregates_three_sources_and_skips_empty():
    adapter = FakeAdapter()
    pipe = ShareholderPipeline(adapter, FakeRepo())
    result = pipe.run_events(["20260430"])
    assert result["holdertrade_records"] == 1
    assert result["share_float_records"] == 0   # empty df
    assert result["repurchase_records"] == 1
    assert result["records"] == 2
    assert ("share_float", "20260430", 0) in adapter.calls


def test_run_hk_hold_paginates_until_short_page():
    adapter = PagedAdapter()
    pipe = ShareholderPipeline(adapter, FakeRepo(), page_limit=10)
    result = pipe.run_hk_hold(["20260331"])
    # 10 (page 1, == limit) + 1 (page 2, < limit) = 11 rows; page 3 is empty -> stop
    assert result["records"] == 11
    offsets = [c[2] for c in adapter.calls if c[0] == "hk_hold"]
    # page 2 is short but NOT trusted as terminal; a third (empty) call confirms end
    assert offsets == [0, 10, 11]


def test_disabled_adapter_skips():
    class Disabled(FakeAdapter):
        enabled = False

    pipe = ShareholderPipeline(Disabled(), FakeRepo())
    assert pipe.run_holdernumber("20180101", "20260601")["status"] == "skipped"
    assert pipe.run_events(["20260430"])["status"] == "skipped"
    assert pipe.run_hk_hold(["20260331"])["status"] == "skipped"
