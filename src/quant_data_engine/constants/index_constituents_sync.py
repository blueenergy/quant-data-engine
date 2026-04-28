"""Index constituents sync: Tushare index_weight -> Mongo index_constituents.

``store_key`` must match quantFinance ``IndexService`` / ``/api/index/*/constituents``
aliases (e.g. hs300) so API cache reads hit after this job runs.

Override the default profile list with env ``INDEX_CONSTITUENTS_PROFILES``:
JSON array of objects: [{"ts_code":"000300.SH","store_key":"hs300","alternates":["399300.SZ"]}, ...]
"""
from __future__ import annotations

import json
import os
from typing import Any, Dict, List

# Each profile: ts_code (Tushare index_weight), store_key (Mongo index_code / quantFinance alias), alternates?
ConstituentSyncProfile = Dict[str, Any]

# Core set (~5 indices). Extend via INDEX_CONSTITUENTS_PROFILES env (JSON).
DEFAULT_SYNC_PROFILES: List[ConstituentSyncProfile] = [
    {"ts_code": "000300.SH", "store_key": "hs300", "alternates": ["399300.SZ"]},
    {"ts_code": "000510.SH", "store_key": "a500", "alternates": []},  # 中证 A500
    {"ts_code": "000016.SH", "store_key": "sse50", "alternates": []},
    {"ts_code": "000905.SH", "store_key": "csi500", "alternates": []},
    {"ts_code": "000688.SH", "store_key": "star50", "alternates": []},
    {"ts_code": "000852.SH", "store_key": "csi1000", "alternates": []},
]


def load_constituent_sync_profiles() -> List[ConstituentSyncProfile]:
    raw = os.getenv("INDEX_CONSTITUENTS_PROFILES", "").strip()
    if not raw:
        return list(DEFAULT_SYNC_PROFILES)
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        raise ValueError(
            "INDEX_CONSTITUENTS_PROFILES must be valid JSON array of "
            "{ts_code, store_key, alternates?} objects"
        ) from e
    if not isinstance(data, list) or not data:
        raise ValueError("INDEX_CONSTITUENTS_PROFILES must be a non-empty JSON array")
    out: List[ConstituentSyncProfile] = []
    for i, item in enumerate(data):
        if not isinstance(item, dict):
            raise ValueError(f"INDEX_CONSTITUENTS_PROFILES[{i}] must be an object")
        ts = str(item.get("ts_code", "")).strip()
        sk = str(item.get("store_key", "")).strip()
        if not ts or not sk:
            raise ValueError(
                f"INDEX_CONSTITUENTS_PROFILES[{i}] needs non-empty ts_code and store_key"
            )
        alts = item.get("alternates") or []
        if alts is not None and not isinstance(alts, list):
            raise ValueError(f"INDEX_CONSTITUENTS_PROFILES[{i}].alternates must be a list")
        alt_list = [str(a).strip() for a in alts if str(a).strip()]
        out.append({"ts_code": ts, "store_key": sk, "alternates": alt_list})
    return out


__all__ = ["ConstituentSyncProfile", "DEFAULT_SYNC_PROFILES", "load_constituent_sync_profiles"]
