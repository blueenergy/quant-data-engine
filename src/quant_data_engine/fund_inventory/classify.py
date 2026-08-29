"""四态分类：存量强化 / 新建库存 / 库存换手 / 库存退出。"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

STATE_STRENGTHEN = "incumbent_strengthen"
STATE_NEW_INVENTORY = "new_inventory"
STATE_TURNOVER = "inventory_turnover"
STATE_EXIT = "inventory_exit"

STATE_LABELS = {
    STATE_STRENGTHEN: "存量强化",
    STATE_NEW_INVENTORY: "新建库存",
    STATE_TURNOVER: "库存换手",
    STATE_EXIT: "库存退出",
}


@dataclass(frozen=True)
class ClassifyParams:
    """阈值按元。净增持时新进占净增 ≥80% 算新建；净减持时新进 ≥50% 卖出算换手。"""

    new_share_of_net: float = 0.80
    turnover_new_vs_sell: float = 0.50


def classify_inventory_state(
    net_ex_price: float,
    new_inflow: float,
    incumbent_delta: Optional[float] = None,
    params: Optional[ClassifyParams] = None,
) -> Dict[str, Any]:
    """根据剥股价后的净增减、新进规模、存量变化打四态。

    恒等式：``incumbent_delta = net_ex_price - new_inflow``（存量含整段退出）。
    金额单位为元。
    """
    p = params or ClassifyParams()
    net = float(net_ex_price or 0.0)
    new = max(float(new_inflow or 0.0), 0.0)
    if incumbent_delta is None:
        inc = net - new
    else:
        inc = float(incumbent_delta)

    if net > 0:
        if new >= p.new_share_of_net * net:
            state = STATE_NEW_INVENTORY
        elif inc > 0:
            state = STATE_STRENGTHEN
        else:
            state = STATE_NEW_INVENTORY
    else:
        sell = abs(min(inc, 0.0))
        if new > 0 and sell > 0 and new >= p.turnover_new_vs_sell * sell:
            state = STATE_TURNOVER
        else:
            state = STATE_EXIT

    return {
        "state": state,
        "state_label": STATE_LABELS[state],
        "net_ex_price": net,
        "new_inflow": new,
        "incumbent_delta": inc,
    }
