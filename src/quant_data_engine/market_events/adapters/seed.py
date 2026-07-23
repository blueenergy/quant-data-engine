"""YAML seed events adapter."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List

from quant_data_engine.market_events.identity import load_yaml_config, parse_shanghai_datetime

logger = logging.getLogger(__name__)


def fetch_seed_events() -> List[Dict[str, Any]]:
    raw = load_yaml_config("market_event_seeds.yaml")
    out: List[Dict[str, Any]] = []
    for row in raw.get("events") or []:
        try:
            event_time = parse_shanghai_datetime(str(row.get("event_time") or ""))
            out.append(
                {
                    "identity_kind": "seed",
                    "seed_id": str(row.get("id") or ""),
                    "source_name": "seed",
                    "title": str(row.get("title") or ""),
                    "event_time": event_time,
                    "region": str(row.get("region") or "GLOBAL"),
                    "category": str(row.get("category") or "other"),
                    "event_subtype": row.get("event_subtype"),
                    "importance": int(row.get("importance") or 3),
                    "status": str(row.get("status") or "scheduled"),
                }
            )
        except Exception as e:
            logger.warning("seed event skip %s: %s", row.get("id"), e)
    return out
