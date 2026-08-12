"""Pytest hooks: stub optional third-party deps before package import.

`quant_data_engine` imports `TdxMinuteAdapter` at package load time, which
requires `pytdx`. CI / minimal venvs may not install it; tests that only need
Tushare/ETF paths should still collect and run.
"""
from __future__ import annotations

import sys
import types


def _ensure_module(name: str) -> types.ModuleType:
    mod = sys.modules.get(name)
    if mod is None:
        mod = types.ModuleType(name)
        sys.modules[name] = mod
    return mod


def _stub_pytdx_if_missing() -> None:
    if "pytdx" in sys.modules:
        return
    pytdx = _ensure_module("pytdx")
    reader = _ensure_module("pytdx.reader")
    hq = _ensure_module("pytdx.hq")

    class _DummyReader:
        pass

    class _DummyHq:
        pass

    reader.TdxExHqDailyBarReader = _DummyReader
    hq.TdxHq_API = _DummyHq
    pytdx.reader = reader
    pytdx.hq = hq


# Run at conftest import time so test modules can `import quant_data_engine`.
_stub_pytdx_if_missing()
