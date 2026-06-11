import sys
import types
from pathlib import Path

import pytest

from quant_data_engine.tasks import run_dispatcher


def _disable_library_catalog(monkeypatch):
    """Simulate an environment where the shared catalog library yields nothing,
    so resolution falls through to legacy paths / the built-in core catalog."""
    fake = types.ModuleType("stock_data_access")

    def _raise(*_args, **_kwargs):
        raise FileNotFoundError("no catalog available in test")

    fake.catalog_path = _raise  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "stock_data_access", fake)


def test_load_catalog_uses_builtin_core_when_default_catalog_missing(monkeypatch):
    monkeypatch.delenv(run_dispatcher.CATALOG_ENV_VAR, raising=False)
    monkeypatch.setattr(run_dispatcher, "SCRIPT_DIR", Path("/tmp/nonexistent-quant-data-engine"))
    _disable_library_catalog(monkeypatch)

    catalog = run_dispatcher.load_catalog()
    by_id = {entry["id"]: entry for entry in catalog}

    assert by_id["stock_daily"]["depends_on"] == ["tushare_daily_ready"]
    assert by_id["market_sentiment"]["depends_on"] == ["stock_daily"]
    assert all(entry["trigger_on_complete"] is True for entry in catalog)


def test_load_catalog_raises_when_explicit_catalog_missing(tmp_path):
    missing = tmp_path / "missing.yaml"

    with pytest.raises(FileNotFoundError):
        run_dispatcher.load_catalog(str(missing))


def test_find_catalog_path_prefers_env_path(monkeypatch, tmp_path):
    catalog_path = tmp_path / "data_pipelines_catalog.yaml"
    catalog_path.write_text("pipelines: []\n")
    monkeypatch.setenv(run_dispatcher.CATALOG_ENV_VAR, str(catalog_path))

    assert run_dispatcher._find_catalog_path() == Path(catalog_path)
