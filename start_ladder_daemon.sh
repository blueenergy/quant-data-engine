#!/bin/bash

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_DIR="$SCRIPT_DIR"
PYTHON_CMD="python3" # Or absolute path to your venv python
LOG_FILE="$APP_DIR/logs/ladder_daemon.log"
PID_FILE="$APP_DIR/run_ladder_sync.pid"

# Ensure log directory exists
mkdir -p "$APP_DIR/logs"

# Check if already running
if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    if ps -p "$PID" > /dev/null; then
        echo "Crawler is already running with PID $PID"
        exit 1
    else
        echo "Stale PID file found. Removing..."
        rm "$PID_FILE"
    fi
fi

# Set PYTHONPATH
export PYTHONPATH="$APP_DIR/src:$PYTHONPATH"

# Start process
echo "Starting Ladder Sync Daemon..."
cd "$APP_DIR"
nohup $PYTHON_CMD -m quant_data_engine.tasks.run_ladder_sync --loop --interval 60 --source eastmoney >> "$LOG_FILE" 2>&1 &
echo $! > "$PID_FILE"

echo "Daemon started with PID $(cat $PID_FILE)"
echo "Logs: $LOG_FILE"
