"""自有序 ``(trade_date, close)`` 序列计算 N 个交易日涨跌幅(%)，与机构模块算法一致。"""
from __future__ import annotations

from typing import List, Optional, Tuple


def n_trading_day_return_percent(
    series: List[Tuple[str, float]], end_date: str, n: int = 60
) -> Optional[float]:
    """
    ``series`` 按 trade_date 升序；在 ``<= end_date`` 的序列中取最近一段，
    若条数 < n+1 则无法计算。收益 = (close_end / close_start - 1) * 100。
    """
    if n < 1 or not series:
        return None
    end_s = str(end_date).replace("-", "")[:8]
    have: List[Tuple[str, float]] = [(a, c) for a, c in series if a <= end_s]
    if len(have) < n + 1:
        return None
    # 找到 <= end 的最后一根
    last = None
    for i in range(len(have) - 1, -1, -1):
        if have[i][0] <= end_s:
            last = i
            break
    if last is None or last < n:
        return None
    c_end = have[last][1]
    c_start = have[last - n][1]
    if c_start and c_start > 0 and c_end == c_end:
        return (c_end / c_start - 1.0) * 100.0
    return None


__all__ = ["n_trading_day_return_percent"]
