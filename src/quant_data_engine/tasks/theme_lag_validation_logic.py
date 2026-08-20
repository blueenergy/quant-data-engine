"""主题补涨候选验证：纯规则引擎（无 I/O），与 llm-wiki theme-lag-candidate-validation 笔记对齐。"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Literal, Optional, Tuple

Outcome = Literal["take_profit", "stop_loss", "expire", "insufficient_data"]

# 期满：第几根「有效收盘」触发 expire（见 llm-wiki theme-lag-candidate-validation §规则）
EXPIRE_AFTER_VALID_CLOSE_COUNT = 2


@dataclass
class SymbolValidationResult:
    symbol: str
    p0: float
    outcome: Outcome
    exit_trade_date: Optional[str]
    exit_price: Optional[float]
    return_pct: Optional[float]
    eod_float_path: List[Dict[str, Any]]


def trading_slots_after_anchor(
    open_days_sorted: List[str], anchor_trade_date: str, max_slots: int
) -> List[str]:
    """从已排序的开市日列表中取严格晚于 anchor 的前 ``max_slots`` 个交易日（字符串 YYYYMMDD）。"""
    td = "".join(c for c in str(anchor_trade_date) if c.isdigit())[:8]
    after = [d for d in open_days_sorted if d > td]
    return after[:max_slots]


def validate_one_symbol_stream(
    symbol: str,
    p0: float,
    slot_trade_dates: List[str],
    get_close: Callable[[str], Optional[float]],
) -> SymbolValidationResult:
    """§4.1 + §5 流式：``slot_trade_dates`` 为锚定日后至多 ``max_slots`` 个开市日（由调用方传入）。"""
    if p0 <= 0:
        return SymbolValidationResult(
            symbol=symbol,
            p0=p0,
            outcome="insufficient_data",
            exit_trade_date=None,
            exit_price=None,
            return_pct=None,
            eod_float_path=[],
        )

    valid_count = 0
    path: List[Dict[str, float]] = []
    for d in slot_trade_dates:
        c = get_close(d)
        if c is None:
            continue
        valid_count += 1
        path.append({"trade_date": d, "close": c, "float_vs_p0": (c - p0) / p0})
        if c >= 1.03 * p0:
            return SymbolValidationResult(
                symbol=symbol,
                p0=p0,
                outcome="take_profit",
                exit_trade_date=d,
                exit_price=c,
                return_pct=(c - p0) / p0,
                eod_float_path=path,
            )
        if c <= 0.97 * p0:
            return SymbolValidationResult(
                symbol=symbol,
                p0=p0,
                outcome="stop_loss",
                exit_trade_date=d,
                exit_price=c,
                return_pct=(c - p0) / p0,
                eod_float_path=path,
            )
        if valid_count == EXPIRE_AFTER_VALID_CLOSE_COUNT:
            return SymbolValidationResult(
                symbol=symbol,
                p0=p0,
                outcome="expire",
                exit_trade_date=d,
                exit_price=c,
                return_pct=(c - p0) / p0,
                eod_float_path=path,
            )

    return SymbolValidationResult(
        symbol=symbol,
        p0=p0,
        outcome="insufficient_data",
        exit_trade_date=None,
        exit_price=None,
        return_pct=None,
        eod_float_path=path,
    )


def aggregate_mvp(results: List[SymbolValidationResult]) -> Dict[str, object]:
    """§5.5 MVP：命中率分母为「有完整 §5 终态」的标的数。"""
    terminal = [r for r in results if r.outcome != "insufficient_data"]
    insuf = sum(1 for r in results if r.outcome == "insufficient_data")
    denom = len(terminal)
    tp = sum(1 for r in terminal if r.outcome == "take_profit")
    sl = sum(1 for r in terminal if r.outcome == "stop_loss")
    ex = sum(1 for r in terminal if r.outcome == "expire")
    rets = sorted([r.return_pct for r in terminal if r.return_pct is not None])
    median_ret: Optional[float] = None
    if rets:
        mid = len(rets) // 2
        if len(rets) % 2 == 1:
            median_ret = float(rets[mid])
        else:
            median_ret = float((rets[mid - 1] + rets[mid]) / 2.0)
    hit_rate = (tp / denom) if denom else None
    return {
        "denominator": denom,
        "take_profit_count": tp,
        "stop_loss_count": sl,
        "expire_count": ex,
        "insufficient_data_count": insuf,
        "hit_rate_take_profit": hit_rate,
        "median_return": median_ret,
    }


__all__ = [
    "EXPIRE_AFTER_VALID_CLOSE_COUNT",
    "SymbolValidationResult",
    "aggregate_mvp",
    "trading_slots_after_anchor",
    "validate_one_symbol_stream",
]
