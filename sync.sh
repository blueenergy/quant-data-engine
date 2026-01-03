#!/usr/bin/env bash
set -euo pipefail

cd /app/quant_data_engine

# 载入 .env（若存在）
if [ -f /app/.env ]; then
  set -a
  source /app/.env
  set +a
fi

LOCK_FILE="/tmp/quant_sync.lock"

# 防重入
exec 9>"${LOCK_FILE}"
flock -n 9 || { echo "[sync] $(date) another run in progress"; exit 0; }

echo "[sync] $(date) start"

# 股票日线（当天）
echo "[sync] $(date) run_stock_daily --mode by-date --days 1"
python -m quant_data_engine.tasks.run_stock_daily --mode by-date --days 1

# 资金流
echo "[sync] $(date) run_moneyflow_sync --auto"
python -m quant_data_engine.tasks.run_moneyflow_sync --auto

# 指数
echo "[sync] $(date) run_index_daily --core --backfill-days 1"
python -m quant_data_engine.tasks.run_index_daily --core --backfill-days 1

# 市场情绪（日线，当日）
TODAY=$(date +%Y%m%d)
echo "[sync] $(date) analyze_market_sentiment daily ${TODAY}"
python -m quant_data_engine.tasks.analyze_market_sentiment \
  --granularity daily \
  --start "${TODAY}" \
  --end "${TODAY}" \
  --ma-period 5

echo "[sync] $(date) done"
