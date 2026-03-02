#!/bin/bash
set -e

# DEPLOY_MODE controls which services start:
#   domestic  — Tushare sync + industry + ladder (no proxy needed, runs on cloud)
#   overseas  — Yahoo Finance global market sync only (needs proxy, runs local)
#   full      — everything (default, backward-compatible)
DEPLOY_MODE=${DEPLOY_MODE:-full}

echo "[Entrypoint] DEPLOY_MODE=${DEPLOY_MODE}"

# ── Select crontab file ────────────────────────────────────────────────────
if [ "$DEPLOY_MODE" = "domestic" ]; then
    CRONTAB_FILE="/app/crontab_domestic.txt"
elif [ "$DEPLOY_MODE" = "overseas" ]; then
    CRONTAB_FILE="/app/crontab_overseas.txt"
else
    CRONTAB_FILE="/app/crontab.txt"  # full mode: original crontab
fi

# 1. Start Supercronic
echo "[Entrypoint] Starting Supercronic with ${CRONTAB_FILE}..."
supercronic "${CRONTAB_FILE}" &
SUPERCRONIC_PID=$!

# ── Domestic services (Tushare, ladder, industry, sentiment) ───────────────
if [ "$DEPLOY_MODE" = "domestic" ] || [ "$DEPLOY_MODE" = "full" ]; then
    # 2. Ladder Sync Loop
    INTERVAL=${LADDER_SYNC_INTERVAL:-60}
    echo "[Entrypoint] Starting Ladder Sync Loop (${INTERVAL}s)..."
    python -m quant_data_engine.tasks.run_ladder_sync --loop --interval ${INTERVAL} &
    LADDER_PID=$!

    # 3. Industry Sync Loop
    INDUSTRY_INTERVAL=${INDUSTRY_SYNC_INTERVAL:-3600}
    echo "[Entrypoint] Starting Industry Sync Loop (${INDUSTRY_INTERVAL}s)..."
    python -m quant_data_engine.tasks.run_industry_sync --loop --interval ${INDUSTRY_INTERVAL} &
    INDUSTRY_PID=$!
else
    LADDER_PID=""
    INDUSTRY_PID=""
fi

# ── Overseas services (Yahoo Finance, international market data) ───────────
if [ "$DEPLOY_MODE" = "overseas" ] || [ "$DEPLOY_MODE" = "full" ]; then
    SYNC_ARGS=""
    if [ -n "$SECONDARY_MONGO_URI" ]; then
        SYNC_ARGS="--sync-uri ${SECONDARY_MONGO_URI}"
        echo "[Entrypoint] Secondary MongoDB Sync Enabled: ${SECONDARY_MONGO_URI}"
    fi

    # 4. Global Market Sync Loop
    MARKET_INTERVAL=${GLOBAL_MARKET_SYNC_INTERVAL:-300}
    echo "[Entrypoint] Starting Global Market Sync Loop (${MARKET_INTERVAL}s)..."
    python -m quant_data_engine.tasks.run_global_market_sync --interval ${MARKET_INTERVAL} ${SYNC_ARGS} &
    MARKET_PID=$!

    # 5. International News Cache Sync Loop (BBC/CNBC/Yahoo RSS → MongoDB)
    INTL_NEWS_INTERVAL=${INTL_NEWS_SYNC_INTERVAL:-1800}
    echo "[Entrypoint] Starting International News Sync Loop (${INTL_NEWS_INTERVAL}s)..."
    python -m quant_data_engine.tasks.run_international_news_sync --loop --interval ${INTL_NEWS_INTERVAL} ${SYNC_ARGS} &
    INTL_NEWS_PID=$!
else
    MARKET_PID=""
    INTL_NEWS_PID=""
fi

# ── Signal handling ────────────────────────────────────────────────────────
echo "[Entrypoint] All services started (mode=${DEPLOY_MODE})."
PIDS="${SUPERCRONIC_PID} ${LADDER_PID} ${INDUSTRY_PID} ${MARKET_PID} ${INTL_NEWS_PID}"

trap "kill ${PIDS} 2>/dev/null; exit 0" SIGINT SIGTERM

wait -n
