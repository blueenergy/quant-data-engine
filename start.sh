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

# 3. Wait for processes and handle signals
echo "[Entrypoint] All services started."

trap "kill $SUPERCRONIC_PID $LADDER_PID; exit 0" SIGINT SIGTERM

wait -n
