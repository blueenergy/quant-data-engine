"""Tests for CN report / gsrl calendar adapters."""

from __future__ import annotations

from datetime import datetime, timezone

from quant_data_engine.market_events.adapters import cn_gsrl, cn_report
from quant_data_engine.market_events.filters import MarketEventsFilterConfig


class _Resp:
    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self):
        return self._payload


def test_cn_report_empty_period_no_warning(monkeypatch, caplog):
    import logging

    def post(url, params=None, timeout=None):
        return _Resp({"prbookinfos": None})

    monkeypatch.setattr(cn_report.requests, "post", post)
    cfg = MarketEventsFilterConfig(min_importance=1, individual_require_whitelist=False)
    start = datetime(2026, 7, 1, tzinfo=timezone.utc)
    end = datetime(2026, 8, 31, tzinfo=timezone.utc)
    with caplog.at_level(logging.INFO):
        out = cn_report.fetch_cn_report(cfg, start, end)
    assert out == []
    assert "Length mismatch" not in caplog.text
    assert "failed" not in caplog.text.lower()
    assert "empty reservation" in caplog.text


def test_cn_report_parses_cninfo_rows(monkeypatch):
    def post(url, params=None, timeout=None):
        return _Resp(
            {
                "prbookinfos": [
                    {
                        "seccode": "000001",
                        "secname": "平安银行",
                        "f002d_0102": "2026-08-15",
                        "f006d_0102": "",
                        "f003d_0102": "",
                        "f004d_0102": "",
                        "f005d_0102": "",
                        "f001d_0102": "2026-06-30",
                    }
                ]
            }
        )

    monkeypatch.setattr(cn_report.requests, "post", post)
    cfg = MarketEventsFilterConfig(
        min_importance=1,
        symbol_whitelist={"000001.SZ"},
        individual_require_whitelist=True,
    )
    start = datetime(2026, 8, 1, tzinfo=timezone.utc)
    end = datetime(2026, 8, 31, tzinfo=timezone.utc)
    out = cn_report.fetch_cn_report(cfg, start, end)
    assert len(out) >= 1
    assert out[0]["symbol"] == "000001.SZ"
    assert out[0]["source_name"] == "cninfo_disclosure"


def test_cn_gsrl_null_result_and_weekend_skip(monkeypatch, caplog):
    import logging

    calls = {"n": 0}

    def get(url, params=None, headers=None, timeout=None):
        calls["n"] += 1
        return _Resp({"result": None})

    monkeypatch.setattr(cn_gsrl.requests, "get", get)
    cfg = MarketEventsFilterConfig(min_importance=1, individual_require_whitelist=False)
    # Sat/Sun should skip HTTP; Friday returns empty without warning.
    with caplog.at_level(logging.INFO):
        out = cn_gsrl.fetch_cn_gsrl(cfg, ["20260725", "20260726", "20260724"])
    assert out == []
    assert calls["n"] == 1
    assert "NoneType" not in caplog.text
    assert "failed" not in caplog.text.lower()
    assert "no company events" in caplog.text


def test_cn_gsrl_parses_rows(monkeypatch):
    def get(url, params=None, headers=None, timeout=None):
        return _Resp(
            {
                "result": {
                    "data": [
                        {
                            "SECURITY_CODE": "600000",
                            "SECURITY_NAME_ABBR": "浦发银行",
                            "EVENT_TYPE": "股东大会",
                            "EVENT_CONTENT": "2025年股东大会",
                            "TRADE_DATE": "2026-07-24",
                        }
                    ]
                }
            }
        )

    monkeypatch.setattr(cn_gsrl.requests, "get", get)
    cfg = MarketEventsFilterConfig(
        min_importance=1,
        symbol_whitelist={"600000.SH"},
        gsrl_important_types={"股东大会"},
    )
    out = cn_gsrl.fetch_cn_gsrl(cfg, ["20260724"])
    assert len(out) == 1
    assert out[0]["symbol"] == "600000.SH"
    assert "股东大会" in out[0]["title"]
