import pandas as pd
import pytest

from quant_data_engine.adapters.tushare_adapter import (
    REPORT_RC_MAX_PAGES,
    TushareAdapter,
)

class DummyProApi:
    def index_daily(self, ts_code, start_date, end_date):
        raise Exception("API error")

def test_adapter_error_handling(monkeypatch):
    # Patch ts and pro_api
    class DummyTs:
        @staticmethod
        def set_token(token):
            pass
        @staticmethod
        def pro_api():
            return DummyProApi()
    monkeypatch.setattr("quant_data_engine.adapters.tushare_adapter.ts", DummyTs)
    adapter = TushareAdapter(token="dummy", max_retries=1)
    adapter._pro = DummyProApi()
    adapter.enabled = True
    with pytest.raises(RuntimeError) as excinfo:
        adapter.fetch_index_daily("000300.SH", "20240101", "20240102")
    assert "failed after retries" in str(excinfo.value)


# --------------------------------------------------------------------------- #
#  report_rc pagination                                                        #
# --------------------------------------------------------------------------- #

class PagingProApi:
    """Serves ``report_rc`` from an ``{offset: rows}`` map and records requests.

    Unknown offsets yield an empty page, which is how the real interface signals
    the end of the result set.
    """

    def __init__(self, pages):
        self._pages = pages
        self.offsets = []

    def query(self, name, **kw):
        assert name == "report_rc"
        offset = kw.get("offset", 0)
        self.offsets.append(offset)
        rows = self._pages.get(offset, [])
        return pd.DataFrame(rows, columns=["ts_code", "quarter"])


def _adapter_with(pro):
    adapter = TushareAdapter(token="dummy", rate_limit_per_sec=10000)
    adapter.enabled = True
    adapter._pro = pro
    return adapter


def _rows(n, prefix):
    return [{"ts_code": f"{prefix}{i:06d}.SZ", "quarter": "2026Q4"} for i in range(n)]


def test_report_rc_pages_past_the_single_call_cap():
    """A capped first page must not be the whole answer.

    2023-10-31 really held 5,589 reports against a 5,000 cap, and the interface
    reported no error — the tail was simply absent.
    """
    pro = PagingProApi({0: _rows(5000, "a"), 5000: _rows(589, "b")})

    df = _adapter_with(pro).fetch_report_rc(report_date="20231031")

    assert len(df) == 5589
    assert pro.offsets == [0, 5000, 5589]


def test_report_rc_short_page_is_not_the_end():
    """Stop only on an empty page — the cap belongs to the server, not to us."""
    pro = PagingProApi({0: _rows(3000, "a"), 3000: _rows(2000, "b")})

    df = _adapter_with(pro).fetch_report_rc(report_date="20240101")

    assert len(df) == 5000
    assert 3000 in pro.offsets


def test_report_rc_returns_a_single_page_unchanged():
    pro = PagingProApi({0: _rows(3, "a")})

    df = _adapter_with(pro).fetch_report_rc(report_date="20240101")

    assert len(df) == 3
    assert pro.offsets == [0, 3]


def test_report_rc_empty_result_is_none():
    pro = PagingProApi({})

    assert _adapter_with(pro).fetch_report_rc(report_date="20240101") is None
    assert pro.offsets == [0]


def test_report_rc_gives_up_at_max_pages(caplog):
    """An endpoint that ignores ``offset`` must not spin forever."""

    class NeverEmptyProApi:
        def query(self, name, **kw):
            return pd.DataFrame([{"ts_code": "000001.SZ", "quarter": "2026Q4"}])

    with caplog.at_level("WARNING"):
        df = _adapter_with(NeverEmptyProApi()).fetch_report_rc(report_date="20240101")

    assert len(df) == REPORT_RC_MAX_PAGES
    assert "max_pages" in caplog.text


# --------------------------------------------------------------------------- #
#  cn_pmi fields                                                               #
# --------------------------------------------------------------------------- #

class PmiProApi:
    def __init__(self):
        self.kw = None

    def cn_pmi(self, **kw):
        self.kw = kw
        return pd.DataFrame({
            "month": ["202609"],
            "pmi010000": [49.8],
            "pmi020100": [50.0],
        })


# --------------------------------------------------------------------------- #
#  dc_hot: 人气榜 must be requested explicitly                                #
# --------------------------------------------------------------------------- #

class DcHotProApi:
    """Records how ``dc_hot`` was called and serves one 人气榜 A-share list."""

    def __init__(self):
        self.calls = []

    def dc_hot(self, **kwargs):
        self.calls.append(kwargs)
        if kwargs.get("hot_type") == "人气榜":
            return pd.DataFrame([
                {"trade_date": "20261113", "data_type": "A股市场", "ts_code": "603366.SH",
                 "ts_name": "日出东方", "rank": 1, "pct_change": 5.54, "rank_time": "2026-11-13 22:30:03"},
            ])
        # Unfiltered: 人气榜 and 飙升榜 merged, same data_type.
        return pd.DataFrame([
            {"trade_date": "20261113", "data_type": "A股市场", "ts_code": "603366.SH",
             "ts_name": "日出东方", "rank": 1, "pct_change": 5.54, "rank_time": "2026-11-13 22:30:03"},
            {"trade_date": "20261113", "data_type": "A股市场", "ts_code": "301004.SZ",
             "ts_name": "嘉益股份", "rank": 1, "pct_change": 13.54, "rank_time": "2026-11-13 22:30:01"},
        ])


def test_fetch_dc_hot_requests_popularity_list_only():
    adapter = TushareAdapter(token="dummy", max_retries=0)
    pro = DcHotProApi()
    adapter._pro = pro
    adapter.enabled = True

    df = adapter.fetch_dc_hot("20261113")

    assert pro.calls == [{"trade_date": "20261113", "hot_type": "人气榜"}]
    # One list, not 人气榜 + 飙升榜 merged.
    assert len(df) == 1
    assert df.iloc[0]["ts_name"] == "日出东方"


def test_fetch_cn_pmi_passes_explicit_lowercase_fields():
    """Tushare cn_pmi 不传 fields 会报列名不匹配；必须显式传小写字段。"""
    pro = PmiProApi()

    df = _adapter_with(pro).fetch_cn_pmi(start_m="202501", end_m="202510")

    assert pro.kw["fields"] == "month,pmi010000,pmi020000,pmi020100"
    assert list(df.columns) == ["month", "pmi010000", "pmi020100"]
