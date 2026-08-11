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
#   CRON_REALTIME_DAILY_K="*/5 9-15 * * 1-5"
RUNTIME_CRONTAB="/tmp/crontab_runtime.txt"
cp "${CRONTAB_FILE}" "${RUNTIME_CRONTAB}"

override_cron_line() {
    local cron_expr="$1"
    local task_cmd="$2"
    
    # Strip any surrounding quotes (double or single) that might come from k8s/docker env vars
    cron_expr="${cron_expr%\"}"
    cron_expr="${cron_expr#\"}"
    cron_expr="${cron_expr%\'}"
    cron_expr="${cron_expr#\'}"
    
    if [ -z "$cron_expr" ]; then
        return 0
    fi

    # Replace active lines for task command; keep comments as-is.
    # NOTE: cron_expr should be a valid 5-field cron expression.
    sed -i -E "\\|^[[:space:]]*[0-9\\*\\/,-]+[[:space:]]+[0-9\\*\\/,-]+[[:space:]]+[0-9\\*\\/,-]+[[:space:]]+[0-9\\*\\/,-]+[[:space:]]+[0-9\\*\\/,-]+[[:space:]]+/app/daily_market_sync\\.sh ${task_cmd}[[:space:]]*$|d" "${RUNTIME_CRONTAB}"
    printf '%s /app/daily_market_sync.sh %s\n' "${cron_expr}" "${task_cmd}" >> "${RUNTIME_CRONTAB}"
    echo "[Entrypoint] Override cron for ${task_cmd}: ${cron_expr}"
}

override_cron_line "${CRON_STOCKINFO_DAILY:-}" "stockinfo"
override_cron_line "${CRON_STOCK_DAILY:-}" "stock"
override_cron_line "${CRON_INDEX_DAILY:-}" "index"
override_cron_line "${CRON_INDEX_CONSTITUENTS_DAILY:-}" "index_constituents"
override_cron_line "${CRON_ETF_DAILY:-}" "etf"
override_cron_line "${CRON_FINANCIAL_DAILY:-}" "financial"
# Collapses both scheduled sw_daily passes into the single time given.
override_cron_line "${CRON_SW_DAILY:-}" "sw_daily"
override_cron_line "${CRON_MONEYFLOW_DAILY:-}" "moneyflow"
override_cron_line "${CRON_EARNINGS_DAILY:-}" "earnings"
override_cron_line "${CRON_FUND_PORTFOLIO:-}" "fund_portfolio"
override_cron_line "${CRON_SENTIMENT_DAILY:-}" "sentiment"
override_cron_line "${CRON_HOTSTOCK_DAILY:-}" "hotstock"
# When ENABLE_REALTIME_DAILY_K=0, remove the job from the runtime crontab
# entirely so supercronic never sees it. Using an impossible date (0 0 30 2 *)
# as the override leaves a registered-but-never-fired entry that causes
# supercronic to emit a misleading "job took too long" warning.
if [ "${ENABLE_REALTIME_DAILY_K:-0}" = "0" ]; then
    sed -i -E '\|/app/daily_market_sync\.sh realtime_daily_k[[:space:]]*$|d' "${RUNTIME_CRONTAB}"
    echo "[Entrypoint] realtime_daily_k disabled (ENABLE_REALTIME_DAILY_K=0); removed from crontab"
else
    override_cron_line "${CRON_REALTIME_DAILY_K:-}" "realtime_daily_k"
fi

# ── Cold-start data bootstrap (contract reconciler) ────────────────────────
# On a fresh/incomplete system, auto-backfill all contracted data to spec.
# Gate is O(1): a marker doc in data_contract_state. Absent (exit 10) => run
# --bootstrap, which fills missing data dependency-ordered and writes the marker
# on success. If the marker exists, the daily cron reconcile keeps data fresh.
# Runs detached so the container does not block on a long backfill, and so it
# never trips the `wait -n` below (which would exit the container). Idempotent
# and resumable: safe if interrupted by a restart. Disable with
# DATA_CONTRACT_BOOTSTRAP=0.
if [ "${DATA_CONTRACT_BOOTSTRAP:-1}" = "1" ] && \
   { [ "$DEPLOY_MODE" = "domestic" ] || [ "$DEPLOY_MODE" = "full" ]; }; then
    (
        if python -m quant_data_engine.tasks.run_data_contract_reconcile --check-bootstrap; then
            echo "[Entrypoint] Data contracts already bootstrapped; skipping cold-start backfill."
        else
            echo "[Entrypoint] Fresh/incomplete data detected — running cold-start bootstrap..."
            python -m quant_data_engine.tasks.run_data_contract_reconcile \
                --bootstrap ${DATA_CONTRACT_BOOTSTRAP_ARGS:-} \
                && echo "[Entrypoint] Cold-start bootstrap finished." \
                || echo "[Entrypoint] Cold-start bootstrap incomplete (see logs / data-pulse)."
        fi
    ) &
    disown %% 2>/dev/null || true
fi

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

    # 7. Market Events Calendar Sync Loop (macro + A-share events → MongoDB)
    MARKET_EVENTS_INTERVAL=${MARKET_EVENTS_SYNC_INTERVAL:-21600}
    echo "[Entrypoint] Starting Market Events Sync Loop (${MARKET_EVENTS_INTERVAL}s)..."
    python -m quant_data_engine.tasks.run_market_events_sync --loop --interval ${MARKET_EVENTS_INTERVAL} &
    MARKET_EVENTS_PID=$!
else
    LADDER_PID=""
    INDUSTRY_PID=""
    WATCHLIST_EM_PID=""
    MARKET_EVENTS_PID=""
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

# ── X (Twitter) 大 V 推文同步 — 默认不拉循环，避免无 API credits/权限时持续失败 ─
# 开启：TWITTER_BEARER_TOKEN 非空 且 ENABLE_X_INFLUENCER_SYNC=1
# 单次手动：docker exec … python -m quant_data_engine.tasks.run_x_influencer_sync
if [ -n "${TWITTER_BEARER_TOKEN:-}" ] && [ "${ENABLE_X_INFLUENCER_SYNC:-0}" = "1" ]; then
    X_INT=${X_INFLUENCER_SYNC_INTERVAL:-180}
    echo "[Entrypoint] Starting X influencer sync loop (${X_INT}s)..."
    python -m quant_data_engine.tasks.run_x_influencer_sync --loop --interval "${X_INT}" &
    X_INFLUENCER_PID=$!
else
    X_INFLUENCER_PID=""
    if [ -n "${TWITTER_BEARER_TOKEN:-}" ]; then
        echo "[Entrypoint] X influencer sync loop disabled (set ENABLE_X_INFLUENCER_SYNC=1 to enable after API access is ready)."
    else
        echo "[Entrypoint] X influencer sync skipped (TWITTER_BEARER_TOKEN unset)."
    fi
fi

# ── Signal handling ────────────────────────────────────────────────────────
echo "[Entrypoint] All services started (mode=${DEPLOY_MODE})."
PIDS="${SUPERCRONIC_PID} ${LADDER_PID} ${INDUSTRY_PID} ${WATCHLIST_EM_PID} ${MARKET_EVENTS_PID} ${MARKET_PID} ${INTL_NEWS_PID} ${X_INFLUENCER_PID}"

trap "kill ${PIDS} 2>/dev/null; exit 0" SIGINT SIGTERM

wait -n
