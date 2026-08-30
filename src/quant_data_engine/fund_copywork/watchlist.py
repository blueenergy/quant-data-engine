"""主动偏股观察名单：规模 Top N + yaml pin/exclude。"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Set

import yaml

from quant_data_engine.fund_inventory.active_funds import share_class_key
from quant_data_engine.fund_inventory.aggregate import stock_key

YI = 1e8
_DEFAULT_YAML = Path(__file__).with_name("watchlist.yaml")


@dataclass
class WatchlistConfig:
    top_n: int = 30
    aum_floor_yi: float = 20.0
    pin: List[str] = field(default_factory=list)
    exclude: List[str] = field(default_factory=list)


def load_watchlist_config(path: Optional[Path] = None) -> WatchlistConfig:
    target = path or _DEFAULT_YAML
    if not target.exists():
        return WatchlistConfig()
    raw = yaml.safe_load(target.read_text(encoding="utf-8")) or {}
    pin = [str(x).strip() for x in (raw.get("pin") or []) if str(x).strip()]
    exclude = [str(x).strip() for x in (raw.get("exclude") or []) if str(x).strip()]
    return WatchlistConfig(
        top_n=int(raw.get("top_n") or 30),
        aum_floor_yi=float(raw.get("aum_floor_yi") or 20.0),
        pin=pin,
        exclude=exclude,
    )


def _as_float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def strategy_aum_from_rows(
    rows: Iterable[Mapping[str, Any]],
    fund_meta: Mapping[str, Mapping[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    """A/C 同一股票取最大披露市值，再按产品加总。不可把 A/C 市值相加。"""
    by_key_stock: Dict[tuple, float] = {}
    meta_by_key: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        ts_code = str(row.get("ts_code") or "").strip()
        info = fund_meta.get(ts_code) or {}
        key = share_class_key(
            name=info.get("name") or row.get("fund_name"),
            ts_code=ts_code,
            management=info.get("management"),
        )
        symbol = stock_key(row.get("symbol") or row.get("stock_code"))
        if not key or not symbol:
            continue
        mkv = _as_float(row.get("mkv"))
        slot = (key, symbol)
        if mkv > by_key_stock.get(slot, 0.0):
            by_key_stock[slot] = mkv
        current = meta_by_key.get(key) or {
            "fund_key": key,
            "ts_code": ts_code,
            "name": info.get("name") or row.get("fund_name"),
            "management": info.get("management"),
        }
        if mkv >= _as_float(current.get("_rep_mkv")):
            current = {
                "fund_key": key,
                "ts_code": ts_code,
                "name": info.get("name") or row.get("fund_name"),
                "management": info.get("management"),
                "_rep_mkv": mkv,
            }
        meta_by_key[key] = current

    out: Dict[str, Dict[str, Any]] = {}
    for (key, _symbol), mkv in by_key_stock.items():
        rec = out.get(key)
        if rec is None:
            rec = dict(meta_by_key.get(key) or {"fund_key": key})
            rec["aum"] = 0.0
            rec.pop("_rep_mkv", None)
            out[key] = rec
        rec["aum"] = float(rec.get("aum") or 0.0) + mkv
    return out


def _matches_override(token: str, rec: Mapping[str, Any]) -> bool:
    needle = token.strip()
    if not needle:
        return False
    return needle in {
        str(rec.get("fund_key") or ""),
        str(rec.get("ts_code") or ""),
        str(rec.get("name") or ""),
    }


def build_watchlist(
    aum_by_key: Mapping[str, Mapping[str, Any]],
    config: Optional[WatchlistConfig] = None,
) -> List[Dict[str, Any]]:
    cfg = config or WatchlistConfig()
    exclude_keys: Set[str] = set()
    ranked = sorted(
        aum_by_key.values(),
        key=lambda rec: float(rec.get("aum") or 0.0),
        reverse=True,
    )
    for rec in ranked:
        if any(_matches_override(token, rec) for token in cfg.exclude):
            exclude_keys.add(str(rec.get("fund_key") or ""))

    floor = cfg.aum_floor_yi * YI
    auto: List[Dict[str, Any]] = []
    for rec in ranked:
        key = str(rec.get("fund_key") or "")
        if not key or key in exclude_keys:
            continue
        if float(rec.get("aum") or 0.0) < floor:
            continue
        auto.append(dict(rec, source="auto"))
        if len(auto) >= cfg.top_n:
            break

    selected = {str(r.get("fund_key") or "") for r in auto}
    for rec in ranked:
        key = str(rec.get("fund_key") or "")
        if key in selected or key in exclude_keys:
            continue
        if any(_matches_override(token, rec) for token in cfg.pin):
            auto.append(dict(rec, source="pin"))
            selected.add(key)

    for idx, rec in enumerate(auto, start=1):
        rec["rank"] = idx
        rec["aum_yi"] = round(float(rec.get("aum") or 0.0) / YI, 2)
    return auto
