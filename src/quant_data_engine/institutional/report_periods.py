"""最近 N 个已结束自然季度的报告期截止日 YYYYMMDD（用于 top10 按 period 拉取）。"""
from __future__ import annotations

from datetime import date, datetime
from typing import List, Optional, Union

import pandas as pd


def last_n_completed_quarter_ends(
    n: int,
    as_of: Optional[Union[date, datetime, str]] = None,
) -> List[str]:
    """
    返回以 ``as_of`` 为参考的、最近 N 个**已完整结束**自然季度的**期末日** YYYYMMDD。

    例：as_of=2025-04-20 → 先取「当前日所在季度之前」的上一完整季度末为最近一期，
    再向前共 N 个季度（20241231, 20240930, 20240630, 20240331, …）。
    """
    if n <= 0:
        return []
    if as_of is None:
        t = pd.Timestamp.now()
    else:
        t = pd.Timestamp(as_of)
    # 最近一个已结束季度：若今天还在本季度内，则「最近完整季度」为上一季
    q = t.to_period("Q")
    # t 所在季未结束时，以「上一季末」为最近一期
    p0 = q - 1
    out: List[str] = []
    for i in range(n):
        p = p0 - i
        end = p.to_timestamp(how="end").normalize()
        out.append(end.strftime("%Y%m%d"))
    return out
