"""Tests for macro calendar HTTP adapters."""

from __future__ import annotations

from types import SimpleNamespace

import pandas as pd

from quant_data_engine.market_events.adapters import macro as macro_mod


class _Resp:
    def __init__(self, *, status_code=200, payload=None, text=""):
        self.status_code = status_code
        self._payload = payload or {}
        self.text = text

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self):
        return self._payload


class _Session:
    def __init__(self, handler):
        self.handler = handler

    def get(self, url, params=None, headers=None, timeout=None):
        return self.handler(url, params=params, headers=headers)


def test_fetch_macro_ws_empty_day_no_warning(monkeypatch, caplog):
    def handler(url, params=None, headers=None):
        return _Resp(payload={"code": 20000, "data": {"items": []}})

    monkeypatch.setattr(macro_mod.requests, "Session", lambda: _Session(handler))
    out = macro_mod.fetch_macro_ws(["20260802"])
    assert out == []
    assert "public_date" not in caplog.text
    assert "failed" not in caplog.text.lower()


def test_fetch_macro_ws_parses_items(monkeypatch):
    def handler(url, params=None, headers=None):
        return _Resp(
            payload={
                "data": {
                    "items": [
                        {
                            "public_date": 1784840400,
                            "country": "美国",
                            "title": "美国CPI年率",
                            "importance": 4,
                            "actual": None,
                            "forecast": 3.1,
                            "previous": 3.0,
                            "revised": None,
                            "uri": "https://example",
                        }
                    ]
                }
            }
        )

    monkeypatch.setattr(macro_mod.requests, "Session", lambda: _Session(handler))
    out = macro_mod.fetch_macro_ws(["20260724"])
    assert len(out) == 1
    assert out[0]["source_name"] == "wallstreetcn"
    assert out[0]["title"] == "美国CPI年率"
    assert out[0]["metrics"]["forecast"] == 3.1


def test_fetch_macro_baidu_circuit_breaks_on_soft_403(monkeypatch, caplog):
    calls = {"n": 0}

    def handler(url, params=None, headers=None):
        calls["n"] += 1
        return _Resp(payload={"QueryID": "0", "ResultCode": "403", "Result": []})

    monkeypatch.setattr(macro_mod.requests, "Session", lambda: _Session(handler))
    monkeypatch.setattr(macro_mod.time, "sleep", lambda *_a, **_k: None)
    out = macro_mod.fetch_macro_baidu(["20260723", "20260724"])
    assert out == []
    assert calls["n"] == 1
    assert "ResultCode=403" in caplog.text


def test_fetch_macro_baidu_maps_fields(monkeypatch):
    def handler(url, params=None, headers=None):
        return _Resp(
            payload={
                "Result": [
                    {
                        "date": "2026-07-23",
                        "list": [
                            {
                                "date": "2026-07-23",
                                "time": "20:30",
                                "title": "美国CPI年率",
                                "star": "3",
                                "formerVal": "3.0",
                                "indicateVal": "3.1",
                                "pubVal": "",
                                "region": "美国",
                            }
                        ],
                    }
                ]
            }
        )

    monkeypatch.setattr(macro_mod.requests, "Session", lambda: _Session(handler))
    monkeypatch.setattr(macro_mod.time, "sleep", lambda *_a, **_k: None)
    out = macro_mod.fetch_macro_baidu(["20260723"])
    assert len(out) == 1
    assert out[0]["source_name"] == "baidu"
    assert out[0]["metrics"]["forecast"] == 3.1
    assert out[0]["metrics"]["previous"] == 3.0
