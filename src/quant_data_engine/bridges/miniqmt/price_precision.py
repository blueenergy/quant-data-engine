"""A 股价格精度：按标的的最小变动价位（tick）取整。

QMT 对 ETF / 基金 / 可转债给的是 **3 位小数**（tick 0.001，实测
``159766.SZ 20260918`` 原生 ``0.547/0.548/0.549``），对股票是 2 位（tick 0.01）。
两条 QMT 写入路径早先统一 ``round(x, 2)``，把 ETF 的 ``0.561`` 压成 ``0.56`` ——
最大误差 0.005，在 0.42 的价位上约 1.2%，而回放消费的正是 ``close``。

默认 **2 位，即既有行为**：只有显式认得的基金 / 债券前缀才放宽到 3 位，
所以遗漏一个前缀的后果只是「维持现状」，不会静默改变未识别标的的精度。

**方向性约束：精度只能往细里调。** ``kline_bars.is_filler_bar`` 用精确相等判
「四价齐平」，``split_real_days`` 据此按天丢弃填充日 —— 取整变粗会把窄幅 bar
压成齐平，导致整天数据被静默丢弃。
"""
from __future__ import annotations

# 0.001 最小变动价位的两位前缀，跨市场无歧义：股票用 60/68/00/30/8/4/92，
# 指数用 000/399，与下表均不相交，所以不必看交易所后缀。
#   11x / 12x / 13x          可转债、可交换债
#   15x / 16x / 18x          深市 ETF / LOF / 封闭式基金
#   50x / 51x / 52x / 56x / 58x  沪市 封闭式基金 / ETF / LOF（588 为科创板 ETF）
_FINE_PREFIXES = ("11", "12", "13", "15", "16", "18", "50", "51", "52", "56", "58")

DEFAULT_PRICE_DIGITS = 2
FINE_PRICE_DIGITS = 3


def price_digits(symbol: object) -> int:
    """标的的价格小数位：基金 / 债券 3 位，其余（股票、指数）2 位。"""
    text = str(symbol or "").strip().upper()
    code = text.rsplit(".", 1)[0]  # 去掉 .SH / .SZ 后缀（没有后缀也无妨）
    if len(code) > 2 and code.startswith(("SH", "SZ", "BJ")):
        code = code[2:]  # 容忍 SH600428 这种前置形式
    if code[:2] in _FINE_PREFIXES:
        return FINE_PRICE_DIGITS
    return DEFAULT_PRICE_DIGITS


def round_price(value: float, symbol: object) -> float:
    """按标的 tick 取整，顺带消掉浮点噪声。"""
    return round(float(value), price_digits(symbol))
