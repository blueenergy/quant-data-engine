"""股东名称归一：国际知名机构与常见通道关键词，用于季度环比与「新进外资」识别。"""
from __future__ import annotations

import re
from typing import List, Pattern, Tuple

# (canonical_key, regex) 按顺序匹配，先匹配先返回（A 股公告里外资常见中英混写，可按实际再增）
_PATTERNS: List[Tuple[str, Pattern[str]]] = [
    ("MorganStanley", re.compile(r"MORGAN\s*STANLEY|摩根士丹利", re.I)),
    ("JPMorgan", re.compile(r"J\.?\s*P\.?\s*MORGAN|JP\s*MORGAN|摩根大通|JPM", re.I)),
    ("GoldmanSachs", re.compile(r"GOLDMAN|高盛", re.I)),
    ("UBS", re.compile(r"\bUBS\b|瑞银", re.I)),
    # 常见外资金融机构（Tushare 常显示英文全称或简写，未列出的仍为 OTHER，可再扩）
    (
        "Citi",
        re.compile(
            r"花旗(银行|环球|有限公司)?|CITI(GROUP|BANK|CORP|GLOBAL|SECUR)|\bCITI\b",
            re.I,
        ),
    ),
    (
        "StandardChartered",
        re.compile(
            r"STANDARD\s*CHARTERED|渣打(银行(香港)?|环球)?|SCB\s*HK|SC\s*BANK",
            re.I,
        ),
    ),
    (
        "BNPParibas",
        re.compile(
            r"BNP|PARIBAS|法巴|巴黎(银行|帕斯卡)?\s*法巴|BNP\s*PAR",
            re.I,
        ),
    ),
    ("Barclays", re.compile(r"BARCLAYS?|巴克莱", re.I)),
    (
        "DeutscheBank",
        re.compile(
            r"DEUTSCHE|德意志(银行)?|德银|DB\s*GLOBAL|DEUTSCH",
            re.I,
        ),
    ),
    (
        "SocGen",
        re.compile(
            r"SOCIE?TE?\s*GENERALE|法兴(银行)?|法国兴业|SG\s*SEC|SG\s*PARIS",
            re.I,
        ),
    ),
    ("CreditSuisse", re.compile(r"CREDIT\s*SUISSE|瑞信|瑞士(信贷|银行)", re.I)),
    ("BankOfAmerica", re.compile(r"BANK\s*OF\s*AMERICA|BAML|美银|美国银行", re.I)),
    ("MSCI_Benchmark", re.compile(r"MSCI|MORGAN\s*STANLEY.*CAP", re.I)),
    ("BlackRock", re.compile(r"BLACKROCK|贝莱德", re.I)),
    ("Vanguard", re.compile(r"VANGUARD|先锋", re.I)),
    ("Fidelity", re.compile(r"FIDELITY|富达", re.I)),
    ("Nomura", re.compile(r"NOMURA|野村", re.I)),
    ("HSBC", re.compile(r"HSBC|汇丰(?!金)", re.I)),
    ("Schroders", re.compile(r"SCHRODER|施罗德", re.I)),
    ("FIL", re.compile(r"FIL|富达(国际|香港)", re.I)),
    ("HKSC", re.compile(r"香港中央结算(有限公司)?|HONG\s*KONG.*CLEARING|CHINAAMC.*CONNECT", re.I)),
]

INTL_LABELS = tuple(p[0] for p in _PATTERNS)

# 同一集团在十大里常以不同 norm 出现，实体计数时合并（不影响 diff 槽位键）
_ENTITY_COUNT_MERGE: dict[str, str] = {
    "FIL": "Fidelity",
}


def entity_count_bucket(norm: str) -> str:
    """
    将 ``normalize_institution_key`` 的 canonical key 映射为「经济实体」计数桶，
    用于 ``new_intl`` 聚合与机会分（与 diff 用的 norm 槽位可不同）。
    """
    if not norm or norm == "OTHER":
        return "OTHER"
    return _ENTITY_COUNT_MERGE.get(norm, norm)


def normalize_institution_key(holder_name: str) -> str:
    """
    将股东显示名归一为可比较的 canonical key；无法识别则返回 ``OTHER``。
    """
    if holder_name is None or (isinstance(holder_name, float) and str(holder_name) == "nan"):
        return "OTHER"
    s = str(holder_name).strip()
    if not s:
        return "OTHER"
    for key, pat in _PATTERNS:
        if pat.search(s):
            return key
    return "OTHER"
