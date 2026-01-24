"""Sector ETF code constants & groupings.

Codes are Tushare ts_code identifiers for popular Chinese sector/style ETFs.
Extend or adjust based on coverage needs.
"""
from __future__ import annotations

ETF_CODES = {
    # 核心行业/主题 ETF (示例挑选)
    "PHARMA": "512880.SH",        # 医药ETF
    "MEDIA": "512980.SH",         # 传媒ETF
    "LIQUOR": "512690.SH",        # 白酒ETF
    "SEMICON": "512760.SH",       # 半导体ETF
    "NEV": "515790.SH",           # 新能源车ETF
    "PV_NEWENERGY": "516160.SH",  # 光伏及新能源
    "BROKER": "512000.SH",        # 证券ETF
    "BANK": "512800.SH",          # 银行ETF
    "CHIP": "159995.SZ",          # 芯片ETF
    "CONSUMER": "159928.SZ",      # 消费ETF (沪深300消费)
    "DIGITAL": "159941.SZ",       # 数字经济相关ETF
}

ETF_GROUPS = {
    "core_sectors": [
        ETF_CODES["PHARMA"],
        ETF_CODES["LIQUOR"],
        ETF_CODES["SEMICON"],
        ETF_CODES["NEV"],
        ETF_CODES["BROKER"],
        ETF_CODES["BANK"],
        ETF_CODES["CHIP"],
    ],
    "extended": list(ETF_CODES.values()),
}

ETF_NAME_MAP = {
    ETF_CODES["PHARMA"]: "医药ETF",
    ETF_CODES["MEDIA"]: "传媒ETF",
    ETF_CODES["LIQUOR"]: "白酒ETF",
    ETF_CODES["SEMICON"]: "半导体ETF",
    ETF_CODES["NEV"]: "新能源车ETF",
    ETF_CODES["PV_NEWENERGY"]: "光伏新能源ETF",
    ETF_CODES["BROKER"]: "证券ETF",
    ETF_CODES["BANK"]: "银行ETF",
    ETF_CODES["CHIP"]: "芯片ETF",
    ETF_CODES["CONSUMER"]: "消费ETF",
    ETF_CODES["DIGITAL"]: "数字经济ETF",
}

__all__ = ["ETF_CODES", "ETF_GROUPS", "ETF_NAME_MAP"]
