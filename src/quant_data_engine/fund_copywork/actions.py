"""相邻报告期持仓动作：新买、加仓、减仓、清仓。"""
from __future__ import annotations

from typing import Any, Optional

ACTION_NEW = "new"
ACTION_INCREASED = "increased"
ACTION_DECREASED = "decreased"
ACTION_EXITED = "exited"
ACTION_UNCHANGED = "unchanged"

ACTION_LABELS = {
    ACTION_NEW: "新买",
    ACTION_INCREASED: "加仓",
    ACTION_DECREASED: "减仓",
    ACTION_EXITED: "清仓",
    ACTION_UNCHANGED: "不变",
}

_SHARE_EPS = 0.01


def _as_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def holding_change(
    prev_shares: Any,
    curr_shares: Any,
    prev_mkv: Any = None,
    curr_mkv: Any = None,
) -> Optional[float]:
    """相对上期的变动比例。优先股数，否则用市值（含股价，仅作后备）。"""
    prev_qty = _as_float(prev_shares)
    curr_qty = _as_float(curr_shares)
    if prev_qty is None or curr_qty is None:
        prev_qty = _as_float(prev_mkv)
        curr_qty = _as_float(curr_mkv)
    if prev_qty is None or curr_qty is None:
        return None
    if prev_qty <= 0:
        return None if curr_qty <= 0 else 1.0
    return (curr_qty / prev_qty) - 1.0


def classify_action(
    *,
    prev_exists: bool,
    curr_exists: bool,
    change: Optional[float],
    threshold: float = _SHARE_EPS,
) -> str:
    if curr_exists and not prev_exists:
        return ACTION_NEW
    if prev_exists and not curr_exists:
        return ACTION_EXITED
    if not prev_exists and not curr_exists:
        return ACTION_UNCHANGED
    if change is None:
        return ACTION_UNCHANGED
    if change >= threshold:
        return ACTION_INCREASED
    if change <= -threshold:
        return ACTION_DECREASED
    return ACTION_UNCHANGED
