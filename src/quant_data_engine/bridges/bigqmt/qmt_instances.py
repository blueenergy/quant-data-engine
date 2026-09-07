"""Load machine-local QMT quote/trade instance paths from JSON."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

VALID_TARGETS = frozenset({"quote", "trade"})


def load_qmt_instances(path: str | Path) -> dict[str, dict[str, Any]]:
    """Return ``qmt_instances`` mapping from a JSON config file."""
    config_path = Path(path)
    with config_path.open("r", encoding="utf-8") as fh:
        raw = json.load(fh)
    if not isinstance(raw, Mapping):
        raise ValueError("qmt instances config must be a JSON object")
    instances = raw.get("qmt_instances")
    if not isinstance(instances, Mapping):
        raise ValueError("qmt instances config requires top-level qmt_instances object")
    result: dict[str, dict[str, Any]] = {}
    for name, entry in instances.items():
        key = str(name).strip().lower()
        if key not in VALID_TARGETS:
            raise ValueError("unknown qmt instance %r (expected quote or trade)" % name)
        if not isinstance(entry, Mapping):
            raise ValueError("qmt_instances.%s must be an object" % key)
        result[key] = dict(entry)
    return result


def resolve_python_dir(instance: Mapping[str, Any]) -> str:
    """Return QMT ``python\\`` directory for sync_deployment."""
    explicit = str(instance.get("python_dir") or "").strip()
    if explicit:
        return explicit
    root = str(instance.get("root") or "").strip().rstrip("\\/")
    if not root:
        raise ValueError("qmt instance needs python_dir or root")
    return str(Path(root) / "python")


def get_instance(path: str | Path, target: str) -> dict[str, Any]:
    """Load one instance entry (quote or trade)."""
    key = str(target).strip().lower()
    if key not in VALID_TARGETS:
        raise ValueError("target must be quote or trade, got %r" % target)
    instances = load_qmt_instances(path)
    if key not in instances:
        raise ValueError("qmt_instances.%s not defined in %s" % (key, path))
    return instances[key]


def quote_account_id_from_config(path: str | Path) -> str | None:
    """Return quote account_id from config, or None if unset."""
    account_id = str(get_instance(path, "quote").get("account_id") or "").strip()
    return account_id or None
