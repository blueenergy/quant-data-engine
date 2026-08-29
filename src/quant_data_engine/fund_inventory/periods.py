"""报告期披露口径：半年报/年报为全持仓，一季报/三季报多为前十大。"""
from __future__ import annotations

from datetime import date, datetime
from typing import Iterable, List, Optional, Sequence, Tuple

FULL_MONTHDAYS = {"0630", "1231"}
TOP10_MONTHDAYS = {"0331", "0930"}

CompareMode = str  # "full_vs_full" | "top10_vs_top10"


def _period_str(period: str) -> str:
    return "".join(c for c in str(period or "") if c.isdigit())[:8]


def disclosure_kind(period: str) -> str:
    """``full`` = 半年报/年报全部持仓；``top10`` = 季报前十大；否则 ``unknown``。"""
    p = _period_str(period)
    if len(p) < 8:
        return "unknown"
    md = p[4:8]
    if md in FULL_MONTHDAYS:
        return "full"
    if md in TOP10_MONTHDAYS:
        return "top10"
    return "unknown"


def disclosure_deadline(period: str) -> Optional[date]:
    """Return the statutory disclosure deadline for a standard fund report."""
    p = _period_str(period)
    if len(p) != 8:
        return None
    year = int(p[:4])
    md = p[4:8]
    if md == "0331":
        return date(year, 4, 30)
    if md == "0630":
        return date(year, 8, 31)
    if md == "0930":
        return date(year, 10, 31)
    if md == "1231":
        return date(year + 1, 3, 31)
    return None


def is_period_mature(period: str, as_of: Optional[str] = None) -> bool:
    """A report period is usable only after its disclosure deadline."""
    deadline = disclosure_deadline(period)
    if deadline is None:
        return False
    if as_of:
        text = _period_str(as_of)
        if len(text) != 8:
            return False
        current = datetime.strptime(text, "%Y%m%d").date()
    else:
        current = date.today()
    return current >= deadline


def latest_mature_full_period(as_of: Optional[str] = None) -> str:
    """Return the newest H1/annual period whose disclosure deadline passed."""
    if as_of:
        text = _period_str(as_of)
        current = datetime.strptime(text, "%Y%m%d").date()
    else:
        current = date.today()
    candidates = []
    for year in range(current.year - 2, current.year + 1):
        for md in FULL_MONTHDAYS:
            period = f"{year}{md}"
            if is_period_mature(period, current.strftime("%Y%m%d")):
                candidates.append(period)
    return max(candidates)


def pick_comparable_pair(
    available: Sequence[str],
    pair_mode: str = "previous_full",
    as_of: Optional[str] = None,
) -> Optional[Tuple[str, str, CompareMode]]:
    """从已有 ``end_date`` 里挑 (prev, curr, compare_mode)。

    ``previous_full``（默认）：优先最新一期全持仓 vs 上一期全持仓
    （年报↔半年报），避免 Q1 前十大对 Q2 全持仓把「新进」系统性抬高。

    ``previous_quarter``：相邻两期；若任一侧不是全持仓则两侧都按前十大对齐。
    """
    periods = sorted(
        {
            _period_str(p)
            for p in available
            if len(_period_str(p)) == 8 and is_period_mature(p, as_of)
        }
    )
    if len(periods) < 2:
        return None

    if pair_mode == "previous_full":
        fulls = [p for p in periods if disclosure_kind(p) == "full"]
        if len(fulls) >= 2:
            return fulls[-2], fulls[-1], "full_vs_full"
        return None

    curr = periods[-1]
    prev = periods[-2]
    if disclosure_kind(prev) == "full" and disclosure_kind(curr) == "full":
        return prev, curr, "full_vs_full"
    return prev, curr, "top10_vs_top10"


def list_period_meta(periods: Iterable[str]) -> List[dict]:
    out = []
    for p in sorted({_period_str(x) for x in periods if len(_period_str(x)) == 8}):
        out.append({"period": p, "disclosure": disclosure_kind(p)})
    return out
