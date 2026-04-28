"""Index code constants & groupings (internal to quant-data-engine to avoid cross-repo coupling).

These mirror original constants but are intentionally minimal. Extend as needed.
"""
from __future__ import annotations

INDEX_CODES = {
    "HS300": "000300.SH",      # 沪深300
    "SSE50": "000016.SH",      # 上证50
    "CSI500": "000905.SH",     # 中证500
    "STAR50": "000688.SH",     # 科创50
    "CSI1000": "000852.SH",    # 中证1000
    "CSI_ALL": "000985.CSI",   # 中证全指等（与 CSI_A500 不同）
    "SSE_INDEX": "000001.SH",  # 上证指数（上证综指）
    "SZ_INDEX": "399001.SZ",   # 深圳成分指数（深证成指）
    "GEM_INDEX": "399006.SZ",  # 创业板指数
    "STAR100": "588013.SH",    # 科创100指数
    "CSI_A500": "000510.SH",   # 中证A500（上交所 000510；勿与 000985 全指混淆）
}

INDEX_GROUPS = {
    "core": [
        INDEX_CODES["HS300"],
        INDEX_CODES["SSE50"],
        INDEX_CODES["CSI500"],
        INDEX_CODES["STAR50"],
        INDEX_CODES["CSI1000"],
    ],
    "extended": [
        INDEX_CODES["HS300"],
        INDEX_CODES["SSE50"],
        INDEX_CODES["CSI500"],
        INDEX_CODES["STAR50"],
        INDEX_CODES["CSI1000"],
        INDEX_CODES["CSI_ALL"],
        INDEX_CODES["SSE_INDEX"],
        INDEX_CODES["SZ_INDEX"],
        INDEX_CODES["GEM_INDEX"],
        INDEX_CODES["STAR100"],
        INDEX_CODES["CSI_A500"],
    ],
}

__all__ = ["INDEX_CODES", "INDEX_GROUPS"]
