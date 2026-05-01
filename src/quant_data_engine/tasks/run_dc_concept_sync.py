"""
已废弃：东财概念 ``dc_index`` / ``dc_member`` 不再落库到 ``dc_concept_index`` / ``dc_concept_member``。

``run_theme_lag_candidates`` 在运行时会直接调用 Tushare；缺数据即抛错退出。
保留本模块仅为避免旧 cron / 脚本调用 ``python -m ...run_dc_concept_sync`` 时静默失败。
"""
from __future__ import annotations

import argparse
import logging
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_DEPRECATION_MSG = (
    "run_dc_concept_sync 已移除：不再同步 dc_concept_index / dc_concept_member。"
    "概念板块与成分请在 run_theme_lag_candidates 中由 Tushare 在线获取。"
)


def main() -> None:
    p = argparse.ArgumentParser(description=_DEPRECATION_MSG)
    p.add_argument("--sync", choices=("index", "members", "all"), default="all", help="已忽略")
    p.add_argument("--trade-date", default="", help="已忽略")
    p.add_argument("--as-of-date", default="", help="已忽略")
    p.add_argument("--dry-run", action="store_true", help="已忽略")
    p.add_argument("--max-concepts", type=int, default=0, help="已忽略")
    p.parse_args()
    logger.error(_DEPRECATION_MSG)
    print(_DEPRECATION_MSG, file=sys.stderr)
    raise SystemExit(2)


if __name__ == "__main__":
    main()
