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
