#!/bin/bash
set -e

# 1. Start Supercronic in background (for daily scheduled tasks)
echo "[Entrypoint] Starting Supercronic..."
supercronic /app/crontab.txt &
SUPERCRONIC_PID=$!

# 2. Start Ladder Sync in Loop Mode (foreground)
# Reads interval from env var or defaults to 60s
INTERVAL=${LADDER_SYNC_INTERVAL:-60}
echo "[Entrypoint] Starting Ladder Sync Loop (Interval: ${INTERVAL}s)..."
python -m quant_data_engine.tasks.run_ladder_sync --loop --interval ${INTERVAL} &
LADDER_PID=$!

# 3. Start Global Market Sync in Loop Mode (background)
# Default to 300s (5 minutes) if not set
MARKET_INTERVAL=${GLOBAL_MARKET_SYNC_INTERVAL:-300}
echo "[Entrypoint] Starting Global Market Sync Loop (Interval: ${MARKET_INTERVAL}s)..."
python -m quant_data_engine.tasks.run_global_market_sync --interval ${MARKET_INTERVAL} &
MARKET_PID=$!

# 4. Start Industry Sync in Loop Mode (background)
INDUSTRY_INTERVAL=${INDUSTRY_SYNC_INTERVAL:-3600}
echo "[Entrypoint] Starting Industry Sync Loop (Interval: ${INDUSTRY_INTERVAL}s)..."
python -m quant_data_engine.tasks.run_industry_sync --loop --interval ${INDUSTRY_INTERVAL} &
INDUSTRY_PID=$!

# 5. Wait for processes and handle signals
echo "[Entrypoint] All services started."

trap "kill $SUPERCRONIC_PID $LADDER_PID $MARKET_PID $INDUSTRY_PID; exit 0" SIGINT SIGTERM

wait -n
