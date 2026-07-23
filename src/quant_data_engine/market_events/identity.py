"""Identity key generation and metric normalization for market events."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml

_CONFIG_DIR = Path(__file__).resolve().parent / "config"


@dataclass(frozen=True)
class MetricPattern:
    fragments: Tuple[str, ...]
    metric_code: str


def _load_metric_patterns() -> List[MetricPattern]:
    path = _CONFIG_DIR / "macro_metric_map.yaml"
    if not path.exists():
        return []
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    patterns: List[MetricPattern] = []
    for row in raw.get("patterns") or []:
        code = str(row.get("metric_code") or "").strip().upper()
        frags = tuple(str(x) for x in (row.get("match") or []))
        if code and frags:
            patterns.append(MetricPattern(fragments=frags, metric_code=code))
    return patterns


_METRIC_PATTERNS = _load_metric_patterns()


def normalize_title(text: str) -> str:
    s = re.sub(r"\s+", "", (text or "").strip().lower())
    return re.sub(r"[^\w\u4e00-\u9fff]", "", s)


def resolve_metric_code(title: str) -> Optional[str]:
    """Resolve metric code; prefer specific YoY/MoM/core variants via pattern order."""
    t = title or ""
    t_lower = t.lower()
    for pat in _METRIC_PATTERNS:
        for frag in pat.fragments:
            frag_l = frag.lower()
            if frag_l in t_lower or frag in t:
                code = pat.metric_code
                # Disambiguate bare CPI/PPI/GDP when title implies YoY/MoM/core.
                if code in {"CPI", "PPI", "GDP"}:
                    refined = _refine_macro_code(code, t, t_lower)
                    return refined
                return code
    return None


def _refine_macro_code(base: str, title: str, title_lower: str) -> str:
    if "核心" in title or "core" in title_lower:
        if base in {"CPI", "PPI"}:
            return f"CORE_{base}"
    if "同比" in title or "年率" in title or "yoy" in title_lower or "year" in title_lower:
        return f"{base}_YOY"
    if "环比" in title or "mom" in title_lower or "qoq" in title_lower or "month" in title_lower:
        return f"{base}_MOM" if base != "GDP" else "GDP_QOQ"
    return base


def extract_ref_period(title: str, event_time: datetime) -> Optional[str]:
    """Best-effort reference period from Chinese/English macro titles."""
    t = title or ""
    year = event_time.year
    m = re.search(r"(20\d{2})年?(\d{1,2})月", t)
    if m:
        return f"{m.group(1)}-{int(m.group(2)):02d}"
    m = re.search(r"(\d{1,2})月", t)
    if m:
        return f"{year}-{int(m.group(1)):02d}"
    if "一季" in t or "Q1" in t.upper():
        return f"{year}-Q1"
    if "二季" in t or "半年" in t or "Q2" in t.upper():
        return f"{year}-Q2"
    if "三季" in t or "Q3" in t.upper():
        return f"{year}-Q3"
    if "四季" in t or "年报" in t or "Q4" in t.upper():
        return f"{year}-Q4"
    return None


def sha1_hex(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


def event_date_utc8(event_time: datetime) -> str:
    from zoneinfo import ZoneInfo

    dt = event_time
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=ZoneInfo("UTC"))
    local = dt.astimezone(ZoneInfo("Asia/Shanghai"))
    return local.strftime("%Y-%m-%d")


def build_identity_key(
    *,
    kind: str,
    region: str = "",
    metric_code: Optional[str] = None,
    ref_period: Optional[str] = None,
    symbol: Optional[str] = None,
    event_subtype: Optional[str] = None,
    event_date: Optional[str] = None,
    title: str = "",
    seed_id: Optional[str] = None,
) -> str:
    if seed_id:
        return f"seed|{seed_id}"
    if kind == "macro" and metric_code and ref_period:
        return sha1_hex(f"macro|{region}|{metric_code}|{ref_period}")
    if kind == "macro":
        norm_title = normalize_title(title)
        ed = event_date or ""
        return sha1_hex(f"macro|{region}|{norm_title}|{ed}")
    if kind == "cn_report" and symbol and ref_period:
        return sha1_hex(f"cn_report|{symbol}|{ref_period}")
    if kind == "corporate" and symbol and event_subtype and event_date:
        return sha1_hex(f"{event_subtype}|{symbol}|{event_date}")
    if kind == "aggregate" and event_subtype and event_date:
        return sha1_hex(f"aggregate|{event_subtype}|{event_date}|{region}")
    fallback = "|".join(
        x for x in [kind, region, symbol or "", event_subtype or "", event_date or "", normalize_title(title)] if x
    )
    return sha1_hex(fallback)


def normalize_region(name: str) -> str:
    mapping = {
        "中国": "CN",
        "美国": "US",
        "欧元区": "EU",
        "欧洲": "EU",
        "日本": "JP",
        "英国": "UK",
        "韩国": "KR",
        "澳大利亚": "AU",
    }
    n = (name or "").strip()
    if n in mapping:
        return mapping[n]
    if len(n) == 2 and n.isupper():
        return n
    return n or "GLOBAL"


def normalize_symbol(code: str) -> str:
    code = str(code or "").strip().upper()
    if not code:
        return ""
    if "." in code:
        return code
    if code.startswith("6"):
        return f"{code}.SH"
    if code.startswith(("0", "3")):
        return f"{code}.SZ"
    if code.startswith(("8", "4")):
        return f"{code}.BJ"
    return code


def parse_shanghai_datetime(value: str, default_date: Optional[str] = None) -> datetime:
    from zoneinfo import ZoneInfo

    s = (value or "").strip()
    if not s and default_date:
        s = f"{default_date[:4]}-{default_date[4:6]}-{default_date[6:8]} 09:00:00"
    if not s:
        raise ValueError("empty datetime")
    formats = ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d")
    for fmt in formats:
        try:
            chunk = s[:10] if fmt == "%Y-%m-%d" else s[:19]
            naive = datetime.strptime(chunk, fmt)
            return naive.replace(tzinfo=ZoneInfo("Asia/Shanghai")).astimezone(ZoneInfo("UTC"))
        except ValueError:
            continue
    raise ValueError(f"unparseable datetime: {value!r}")


def load_yaml_config(name: str) -> Dict[str, Any]:
    path = _CONFIG_DIR / name
    if not path.exists():
        return {}
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}
