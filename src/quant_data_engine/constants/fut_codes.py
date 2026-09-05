"""Futures whitelist for selective Tushare fut_daily ingestion.

Main-contract ts_codes (e.g. IF.CFX) return continuous daily bars from Tushare
``fut_daily``. ``fut_mapping`` is used only to annotate the underlying month
contract per trade_date.
"""
from __future__ import annotations

from typing import Dict, List, TypedDict


class FutVariety(TypedDict):
    ts_code: str
    name: str
    group: str
    spot_ts_code: str | None


# CFFEX index futures
FUT_INDEX: List[FutVariety] = [
    {"ts_code": "IF.CFX", "name": "沪深300期货", "group": "index", "spot_ts_code": "000300.SH"},
    {"ts_code": "IH.CFX", "name": "上证50期货", "group": "index", "spot_ts_code": "000016.SH"},
    {"ts_code": "IC.CFX", "name": "中证500期货", "group": "index", "spot_ts_code": "000905.SH"},
    {"ts_code": "IM.CFX", "name": "中证1000期货", "group": "index", "spot_ts_code": "000852.SH"},
]

# Commodity main contracts (suffix verified via Tushare probe 2026-09-05)
FUT_COMMODITY: List[FutVariety] = [
    {"ts_code": "AU.SHF", "name": "黄金", "group": "commodity", "spot_ts_code": None},
    {"ts_code": "CU.SHF", "name": "沪铜", "group": "commodity", "spot_ts_code": None},
    {"ts_code": "AL.SHF", "name": "沪铝", "group": "commodity", "spot_ts_code": None},
    {"ts_code": "RB.SHF", "name": "螺纹钢", "group": "commodity", "spot_ts_code": None},
    {"ts_code": "I.DCE", "name": "铁矿石", "group": "commodity", "spot_ts_code": None},
    {"ts_code": "SC.INE", "name": "原油", "group": "commodity", "spot_ts_code": None},
    {"ts_code": "J.DCE", "name": "焦炭", "group": "commodity", "spot_ts_code": None},
    {"ts_code": "JM.DCE", "name": "焦煤", "group": "commodity", "spot_ts_code": None},
    {"ts_code": "M.DCE", "name": "豆粕", "group": "commodity", "spot_ts_code": None},
    {"ts_code": "CF.ZCE", "name": "棉花", "group": "commodity", "spot_ts_code": None},
    {"ts_code": "TA.ZCE", "name": "PTA", "group": "commodity", "spot_ts_code": None},
    {"ts_code": "LC.GFE", "name": "碳酸锂", "group": "commodity", "spot_ts_code": None},
]

FUT_VARIETIES: List[FutVariety] = FUT_INDEX + FUT_COMMODITY

FUT_WHITELIST_CODES: List[str] = [v["ts_code"] for v in FUT_VARIETIES]

FUT_NAME_MAP: Dict[str, str] = {v["ts_code"]: v["name"] for v in FUT_VARIETIES}

FUT_SPOT_MAP: Dict[str, str] = {
    v["ts_code"]: v["spot_ts_code"]
    for v in FUT_VARIETIES
    if v.get("spot_ts_code")
}

__all__ = [
    "FUT_COMMODITY",
    "FUT_INDEX",
    "FUT_NAME_MAP",
    "FUT_SPOT_MAP",
    "FUT_VARIETIES",
    "FUT_WHITELIST_CODES",
    "FutVariety",
]
