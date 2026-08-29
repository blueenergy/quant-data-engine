"""主动偏股公募过滤，以及 A/C 份额合并键。"""
from __future__ import annotations

import re
from typing import Any, Mapping, Optional

_INDEX_INVEST = {"被动指数型", "增强指数型", "股票指数", "债券指数"}
_EXCLUDE_FUND_TYPES = {
    "货币型",
    "债券型",
    "货币市场型",
    "保本型",
    "商品型",
    "REITs",
    "REIT",
}
_EXCLUDE_NAME_RE = re.compile(
    r"ETF|联接|指数|货币|短债|纯债|偏债|QDII|FOF|商品|黄金|REIT|债基|中债|利率债",
    re.I,
)
_BOND_IN_TYPE_RE = re.compile(r"债券|偏债|货币|FOF", re.I)
_SHARE_CLASS_PAREN_RE = re.compile(r"[（(][ABCDabcd][)）]\s*$")
_SHARE_CLASS_TAIL_RE = re.compile(r"[ABCDabcd]\s*$")

INCLUDE_INVEST_TYPES = {
    "普通股票型",
    "偏股混合型",
    "平衡混合型",
    "灵活配置型",
    "偏股型",
    "股票型",
}


def _text(value: Any) -> str:
    return str(value or "").strip()


def is_active_equity_fund(row: Mapping[str, Any]) -> bool:
    """股票型 / 偏股混合等主动产品；剔除指数、ETF、联接、债基、货币、QDII。"""
    fund_type = _text(row.get("fund_type"))
    invest_type = _text(row.get("invest_type"))
    name = _text(row.get("name"))
    if any(token in fund_type for token in _EXCLUDE_FUND_TYPES):
        return False
    if invest_type in _INDEX_INVEST or "指数" in invest_type:
        return False
    if _BOND_IN_TYPE_RE.search(fund_type) or _BOND_IN_TYPE_RE.search(invest_type):
        return False
    if _EXCLUDE_NAME_RE.search(name):
        return False
    if fund_type in {"股票型", "混合型"}:
        return True
    if invest_type in INCLUDE_INVEST_TYPES:
        return True
    return False


def share_class_key(
    name: Optional[str] = None,
    ts_code: Optional[str] = None,
    management: Optional[str] = None,
) -> str:
    """同一策略的 A/C 份额合并为一条「机构」身份。"""
    raw_name = _text(name)
    stripped = _SHARE_CLASS_PAREN_RE.sub("", raw_name)
    stripped = _SHARE_CLASS_TAIL_RE.sub("", stripped).strip()
    base = stripped or _text(ts_code) or raw_name
    mgr = _text(management)
    return f"{mgr}|{base}" if mgr else base
