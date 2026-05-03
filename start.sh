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

# ── Optional cron overrides (per task) ──────────────────────────────────────
# Allow ops to tune task times without rebuilding image.
# Example:
#   CRON_STOCKINFO_DAILY="20 17 * * 1-5"
#   CRON_STOCK_DAILY="40 17 * * 1-5"
#   CRON_MONEYFLOW_DAILY="50 18 * * 1-5"
#   CRON_EARNINGS_DAILY="30 18 * * 1-5"
RUNTIME_CRONTAB="/tmp/crontab_runtime.txt"
cp "${CRONTAB_FILE}" "${RUNTIME_CRONTAB}"

override_cron_line() {
    local cron_expr="$1"
    local task_cmd="$2"
    if [ -z "$cron_expr" ]; then
        return 0
    fi

    # Replace existing line for task command; keep comments as-is.
    # NOTE: cron_expr should be a valid 5-field cron expression.
    sed -i -E "s|^[[:space:]]*[0-9\\*\\/,-]+[[:space:]]+[0-9\\*\\/,-]+[[:space:]]+[0-9\\*\\/,-]+[[:space:]]+[0-9\\*\\/,-]+[[:space:]]+[0-9\\*\\/,-]+[[:space:]]+/app/daily_market_sync\\.sh ${task_cmd}[[:space:]]*$|${cron_expr} /app/daily_market_sync.sh ${task_cmd}|" "${RUNTIME_CRONTAB}"
    echo "[Entrypoint] Override cron for ${task_cmd}: ${cron_expr}"
}

override_cron_line "${CRON_STOCKINFO_DAILY:-}" "stockinfo"
override_cron_line "${CRON_STOCK_DAILY:-}" "stock"
override_cron_line "${CRON_INDEX_DAILY:-}" "index"
override_cron_line "${CRON_INDEX_CONSTITUENTS_DAILY:-}" "index_constituents"
override_cron_line "${CRON_ETF_DAILY:-}" "etf"
override_cron_line "${CRON_FINANCIAL_DAILY:-}" "financial"
override_cron_line "${CRON_MONEYFLOW_DAILY:-}" "moneyflow"
override_cron_line "${CRON_EARNINGS_DAILY:-}" "earnings"
override_cron_line "${CRON_FUND_PORTFOLIO:-}" "fund_portfolio"
override_cron_line "${CRON_SENTIMENT_DAILY:-}" "sentiment"
override_cron_line "${CRON_HOTSTOCK_DAILY:-}" "hotstock"

# 1. Start Supercronic
echo "[Entrypoint] Starting Supercronic with ${RUNTIME_CRONTAB} (base=${CRONTAB_FILE})..."
supercronic "${RUNTIME_CRONTAB}" &
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

    # 6. Watchlist intraday (East Money snapshot -> quant_data.volume_price, 9:30–15:00 CN)
    # 默认不启动；显式 ENABLE_WATCHLIST_EASTMONEY_INTRADAY=1 才拉起常驻进程。
    if [ "${ENABLE_WATCHLIST_EASTMONEY_INTRADAY:-0}" = "1" ]; then
        W_EM_IDLE=${WATCHLIST_EASTMONEY_IDLE_INTERVAL:-120}
        W_EM_ACTIVE=${WATCHLIST_EASTMONEY_ACTIVE_INTERVAL:-60}
        echo "[Entrypoint] Starting Watchlist East Money intraday (idle=${W_EM_IDLE}s active=${W_EM_ACTIVE}s)..."
        python -m quant_data_engine.tasks.run_watchlist_intraday_eastmoney --interval "${W_EM_IDLE}" --active-interval "${W_EM_ACTIVE}" &
        WATCHLIST_EM_PID=$!
    else
        WATCHLIST_EM_PID=""
    fi
else
    LADDER_PID=""
    INDUSTRY_PID=""
    WATCHLIST_EM_PID=""
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
PIDS="${SUPERCRONIC_PID} ${LADDER_PID} ${INDUSTRY_PID} ${WATCHLIST_EM_PID} ${MARKET_PID} ${INTL_NEWS_PID}"

trap "kill ${PIDS} 2>/dev/null; exit 0" SIGINT SIGTERM

wait -n
