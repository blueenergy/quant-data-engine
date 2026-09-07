"""Tests for machine-local QMT instance config loader."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from quant_data_engine.bridges.bigqmt.qmt_instances import (
    get_instance,
    load_qmt_instances,
    quote_account_id_from_config,
    resolve_python_dir,
)


def _write_config(tmp_path: Path, payload: dict) -> Path:
    path = tmp_path / "qmt_instances.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_load_and_resolve_python_dir_from_root(tmp_path):
    path = _write_config(
        tmp_path,
        {
            "qmt_instances": {
                "quote": {"root": r"D:\国金QMT交易端模拟", "account_id": "62129066"},
                "trade": {"root": r"D:\国金证券QMT交易端"},
            }
        },
    )
    instances = load_qmt_instances(path)
    assert "quote" in instances
    assert Path(resolve_python_dir(instances["quote"])) == Path(r"D:\国金QMT交易端模拟") / "python"


def test_explicit_python_dir_overrides_root(tmp_path):
    path = _write_config(
        tmp_path,
        {
            "qmt_instances": {
                "quote": {
                    "root": r"D:\ignored",
                    "python_dir": r"D:\custom\python",
                }
            }
        },
    )
    quote = get_instance(path, "quote")
    assert resolve_python_dir(quote) == r"D:\custom\python"


def test_unknown_target_rejected(tmp_path):
    path = _write_config(tmp_path, {"qmt_instances": {"foo": {"root": "x"}}})
    with pytest.raises(ValueError, match="unknown qmt instance"):
        load_qmt_instances(path)


def test_quote_account_id_from_config(tmp_path):
    path = _write_config(
        tmp_path,
        {"qmt_instances": {"quote": {"root": "x", "account_id": "62129066"}}},
    )
    assert quote_account_id_from_config(path) == "62129066"


def test_missing_instance_rejected(tmp_path):
    path = _write_config(tmp_path, {"qmt_instances": {"quote": {"root": "x"}}})
    with pytest.raises(ValueError, match="trade not defined"):
        get_instance(path, "trade")
