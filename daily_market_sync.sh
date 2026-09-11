#!/bin/bash
# 脚本用途：quant_data_engine 项目每日市场数据同步任务
# 维护人：shuyolin
# 执行时间：周一到周五，按不同时段执行对应任务

# 不再需要激活虚拟环境 (Docker中直接运行)

# 定义通用变量（方便后续修改路径）
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="${SCRIPT_DIR}/logs"
DATE=$(date +%Y%m%d)
mkdir -p "$LOG_DIR"  # 确保日志目录存在

# 辅助函数：同时输出到控制台和日志文件
log_msg() {
    local msg="[$(date +'%Y-%m-%d %H:%M:%S')] $1"
    echo "$msg"
}

# 检查并安装项目（如果尚未安装）
check_and_install_package() {
    if ! python -c "import quant_data_engine" &> /dev/null; then
        log_msg "未检测到已安装的 quant_data_engine 包，尝试安装..."
        pip install -e . &> /dev/null
        if [ $? -ne 0 ]; then
            log_msg "安装 quant_data_engine 包失败！"
            exit 1
        fi
        log_msg "quant_data_engine 包安装成功！"
        if [ $? -ne 0 ]; then
            log_msg "安装 quant_data_engine 包失败！"
            exit 1
        fi
        log_msg "quant_data_engine 包安装成功！"
    fi
}

check_and_install_quant_analyzer() {
    if ! python -c "import quant_analyzer" &> /dev/null; then
        log_msg "未检测到已安装的 quant_analyzer 包，尝试安装..."
        cd "${SCRIPT_DIR}/../quantAnalyzer" && pip install -e . &> /dev/null
        if [ $? -ne 0 ]; then
            log_msg "安装 quant_analyzer 包失败！"
            # Don't exit, just warn, as analysis is optional for sync success
        else
             log_msg "quant_analyzer 包安装成功！"
        fi
    fi
}

# ==================== 任务1：17:30 批量股票日线数据同步 ====================
run_stock_info_sync() {
    log_msg "开始执行股票基础信息(stock_info)同步任务..."
    check_and_install_package
    cd "$SCRIPT_DIR" && python -m quant_data_engine.tasks.run_stock_info_sync 2>&1 | tee -a ${LOG_DIR}/stock_info_${DATE}.log
    if [ ${PIPESTATUS[0]} -eq 0 ]; then
        log_msg "股票基础信息(stock_info)同步任务执行成功！"
        run_stock_info_pinyin_backfill
    else
        log_msg "股票基础信息(stock_info)同步任务执行失败！查看日志：${LOG_DIR}/stock_info_${DATE}.log"
    fi
}

run_stock_info_delisted_sync() {
    # bak_basic 只返回当天有交易的票，退市股永远进不来，且在市票退市后
    # list_status 会一直停在 L。每周从 stock_basic 补一次身份字段。
    log_msg "开始执行退市/暂停股基础信息补录..."
    check_and_install_package
    cd "$SCRIPT_DIR" && python -m quant_data_engine.tasks.run_stock_info_sync --delisted-only 2>&1 | tee -a ${LOG_DIR}/stock_info_delisted_${DATE}.log
    if [ ${PIPESTATUS[0]} -eq 0 ]; then
        log_msg "退市/暂停股基础信息补录成功！"
    else
        log_msg "退市/暂停股基础信息补录失败！查看日志：${LOG_DIR}/stock_info_delisted_${DATE}.log"
    fi
}

run_stock_info_pinyin_backfill() {
    local QF_DIR
    QF_DIR="$(cd "$SCRIPT_DIR/../quantFinance" && pwd)"
    if [ ! -f "$QF_DIR/tools/backfill_stock_info_pinyin.py" ]; then
        log_msg "跳过 stock_info 拼音回填：未找到 $QF_DIR/tools/backfill_stock_info_pinyin.py"
        return 0
    fi
    log_msg "开始执行 stock_info 拼音字段增量回填..."
    cd "$QF_DIR" && python tools/backfill_stock_info_pinyin.py --only-missing 2>&1 | tee -a ${LOG_DIR}/stock_info_pinyin_${DATE}.log
    if [ ${PIPESTATUS[0]} -eq 0 ]; then
        log_msg "stock_info 拼音回填执行成功！"
    else
        log_msg "stock_info 拼音回填执行失败！查看日志：${LOG_DIR}/stock_info_pinyin_${DATE}.log"
    fi
}

# ==================== 任务2：17:30 批量股票日线数据同步 ====================
run_stock_daily_sync() {
    log_msg "开始执行股票日线数据同步任务..."
    check_and_install_package
    cd "$SCRIPT_DIR" && python -m quant_data_engine.tasks.run_stock_daily --mode by-date --days 1 2>&1 | tee -a ${LOG_DIR}/stock_daily_${DATE}.log
    if [ ${PIPESTATUS[0]} -eq 0 ]; then
        log_msg "股票日线数据同步任务执行成功！"
    else
        log_msg "股票日线数据同步任务执行失败！查看日志：${LOG_DIR}/stock_daily_${DATE}.log"
    fi
}

run_stock_adj_factor_sync() {
    log_msg "开始执行股票复权因子同步任务（Tushare adj_factor -> Mongo stock_adj_factor）..."
    check_and_install_package
    cd "$SCRIPT_DIR" && python -m quant_data_engine.tasks.run_stock_adj_factor_sync --mode by-date --days 1 2>&1 | tee -a ${LOG_DIR}/stock_adj_factor_${DATE}.log
    if [ ${PIPESTATUS[0]} -eq 0 ]; then
        log_msg "股票复权因子同步任务执行成功！"
    else
        log_msg "股票复权因子同步任务执行失败！查看日志：${LOG_DIR}/stock_adj_factor_${DATE}.log"
    fi
}

# ==================== 任务3：17:45 指数日线数据同步 ====================
run_index_daily_sync() {
    log_msg "开始执行指数日线数据同步任务..."
    check_and_install_package
    cd "$SCRIPT_DIR" && python -m quant_data_engine.tasks.run_index_daily --core --end $(date +%Y%m%d) 2>&1 | tee -a ${LOG_DIR}/index_daily_${DATE}.log
    if [ ${PIPESTATUS[0]} -eq 0 ]; then
        log_msg "指数日线数据同步任务执行成功！"
    else
        log_msg "指数日线数据同步任务执行失败！查看日志：${LOG_DIR}/index_daily_${DATE}.log"
    fi
}

run_fut_daily_sync() {
    log_msg "开始执行期货主力日线数据同步任务..."
    check_and_install_package
    cd "$SCRIPT_DIR" && python -m quant_data_engine.tasks.run_fut_daily --core --end $(date +%Y%m%d) 2>&1 | tee -a ${LOG_DIR}/fut_daily_${DATE}.log
    if [ ${PIPESTATUS[0]} -eq 0 ]; then
        log_msg "期货主力日线数据同步任务执行成功！"
    else
        log_msg "期货主力日线数据同步任务执行失败！查看日志：${LOG_DIR}/fut_daily_${DATE}.log"
    fi
}

run_index_dailybasic_sync() {
    log_msg "开始执行指数估值指标同步（index_dailybasic）..."
    check_and_install_package
    cd "$SCRIPT_DIR" && python -m quant_data_engine.tasks.run_index_dailybasic_sync \
        2>&1 | tee -a ${LOG_DIR}/index_dailybasic_${DATE}.log
    if [ ${PIPESTATUS[0]} -eq 0 ]; then
        log_msg "指数估值指标同步任务执行成功！"
    else
        log_msg "指数估值指标同步任务执行失败！查看日志：${LOG_DIR}/index_dailybasic_${DATE}.log"
    fi
}

run_margin_sync() {
    log_msg "开始执行沪深两融汇总同步（margin）..."
    check_and_install_package
    cd "$SCRIPT_DIR" && python -m quant_data_engine.tasks.run_margin_sync \
        2>&1 | tee -a ${LOG_DIR}/margin_sync_${DATE}.log
    if [ ${PIPESTATUS[0]} -eq 0 ]; then
        log_msg "沪深两融汇总同步任务执行成功！"
    else
        log_msg "沪深两融汇总同步任务执行失败！查看日志：${LOG_DIR}/margin_sync_${DATE}.log"
    fi
}

run_market_pe_aggregate() {
    log_msg "开始执行全A市盈率预聚合（financial_daily_basic -> market_pe_daily）..."
    check_and_install_package
    cd "$SCRIPT_DIR" && python -m quant_data_engine.tasks.run_market_pe_aggregate \
        2>&1 | tee -a ${LOG_DIR}/market_pe_aggregate_${DATE}.log
    if [ ${PIPESTATUS[0]} -eq 0 ]; then
        log_msg "全A市盈率预聚合任务执行成功！"
    else
        log_msg "全A市盈率预聚合任务执行失败！查看日志：${LOG_DIR}/market_pe_aggregate_${DATE}.log"
    fi
}

# ==================== 任务3b：指数成分与权重 index_weight -> index_constituents ====================
run_index_constituents_sync() {
    log_msg "开始执行指数成分股同步（Tushare index_weight -> Mongo index_constituents）..."
    check_and_install_package
    cd "$SCRIPT_DIR" && python -m quant_data_engine.tasks.run_index_constituents_sync 2>&1 | tee -a ${LOG_DIR}/index_constituents_${DATE}.log
    if [ ${PIPESTATUS[0]} -eq 0 ]; then
        log_msg "指数成分股同步任务执行成功！"
    else
        log_msg "指数成分股同步任务执行失败（退出码非0）！查看日志：${LOG_DIR}/index_constituents_${DATE}.log"
    fi
}

# ==================== 任务3e：主题补涨候选（天梯主线 ∩ 东财 ∩ 指数成分）====================
# 候选成功后串联 theme_lag_candidate_validation（需与候选同一锚定日与 index_store_key）。
# 可选环境变量：THEME_LAG_TRADE_DATE=YYYYMMDD（显式业务日）、THEME_LAG_INDEX_STORE_KEY（默认 csi1000）。
run_theme_lag_candidates_sync() {
    log_msg "开始执行主题补涨候选（theme_lag_candidates）..."
    check_and_install_package
    local TL_INDEX="${THEME_LAG_INDEX_STORE_KEY:-csi1000}"
    local TL_TD=""
    if [ -n "${THEME_LAG_TRADE_DATE:-}" ]; then
        TL_TD="${THEME_LAG_TRADE_DATE}"
    else
        TL_TD=$(cd "$SCRIPT_DIR" && python -c 'import sys
from quant_data_engine.config.settings import get_settings
from quant_data_engine.adapters.stock_adapter import StockAdapter
from quant_data_engine.tasks.run_theme_lag_candidates import resolve_trade_date
s = get_settings()
sa = StockAdapter(s.tushare_token or "", rate_limit_per_sec=s.rate_limit_per_sec)
if not sa.enabled:
    sys.exit(2)
print(resolve_trade_date(sa, None))') || true
    fi

    if [ -n "$TL_TD" ]; then
        log_msg "主题补涨：trade_date=${TL_TD} index_store_key=${TL_INDEX}（候选与验证共用）"
        cd "$SCRIPT_DIR" && python -m quant_data_engine.tasks.run_theme_lag_candidates \
            --trade-date "$TL_TD" --index-store-key "$TL_INDEX" 2>&1 | tee -a ${LOG_DIR}/theme_lag_${DATE}.log
    else
        log_msg "警告：无法解析业务日（可设置 THEME_LAG_TRADE_DATE）；以无参方式仅执行 theme_lag_candidates，不串联验证。"
        cd "$SCRIPT_DIR" && python -m quant_data_engine.tasks.run_theme_lag_candidates 2>&1 | tee -a ${LOG_DIR}/theme_lag_${DATE}.log
    fi

    if [ ${PIPESTATUS[0]} -ne 0 ]; then
        log_msg "主题补涨候选任务执行失败！查看日志：${LOG_DIR}/theme_lag_${DATE}.log"
        return
    fi
    log_msg "主题补涨候选任务执行成功！"

    if [ -z "$TL_TD" ]; then
        return
    fi

    log_msg "开始执行主题补涨候选验证（theme_lag_candidate_validation）..."
    cd "$SCRIPT_DIR" && python -m quant_data_engine.tasks.run_theme_lag_candidate_validation \
        --trade-date "$TL_TD" --index-store-key "$TL_INDEX" 2>&1 | tee -a ${LOG_DIR}/theme_lag_validation_${DATE}.log
    if [ ${PIPESTATUS[0]} -eq 0 ]; then
        log_msg "主题补涨候选验证任务执行成功！"
    else
        log_msg "主题补涨候选验证任务失败（退出码非0）。查看日志：${LOG_DIR}/theme_lag_validation_${DATE}.log"
    fi
}

# ==================== 任务4：18:00 ETF日线数据同步 ====================
run_etf_daily_sync() {
    log_msg "开始执行ETF日线数据同步任务（全量，自动发现Tushare所有在市ETF）..."
    check_and_install_package
    cd "$SCRIPT_DIR" && python -m quant_data_engine.tasks.run_etf_daily --all --start $(date +%Y%m%d) --end $(date +%Y%m%d) 2>&1 | tee -a ${LOG_DIR}/etf_daily_${DATE}.log
    if [ ${PIPESTATUS[0]} -eq 0 ]; then
        log_msg "ETF日线数据同步任务执行成功！"
    else
        log_msg "ETF日线数据同步任务执行失败！查看日志：${LOG_DIR}/etf_daily_${DATE}.log"
    fi
}

# ==================== 任务4b：19:05 ETF 份额规模（etf_share_size，Tushare 建议 19 点后拉取）====================
run_etf_share_size_sync() {
    log_msg "开始执行 ETF etf_share_size 同步（全市场在市 ETF，当日区间）..."
    check_and_install_package
    cd "$SCRIPT_DIR" && python -m quant_data_engine.tasks.run_etf_share_size --all --start $(date +%Y%m%d) --end $(date +%Y%m%d) 2>&1 | tee -a ${LOG_DIR}/etf_share_size_${DATE}.log
    if [ ${PIPESTATUS[0]} -eq 0 ]; then
        log_msg "ETF etf_share_size 同步任务执行成功！"
    else
        log_msg "ETF etf_share_size 同步任务执行失败！查看日志：${LOG_DIR}/etf_share_size_${DATE}.log"
    fi
}

# ==================== 任务5：18:15 财务数据同步 ====================
run_financial_sync() {
    log_msg "开始执行财务数据同步任务..."
    check_and_install_package
    # 默认：最近上市 + 测试条数（轻量）。若设置 FINANCIAL_SYNC_PERIOD=YYYYMMDD（如 20240930），则改为
    # 全市场单季 VIP（income_vip / balancesheet_vip / cashflow_vip / fina_indicator_vip），需 Tushare VIP 权限。
    if [ -n "${FINANCIAL_SYNC_PERIOD:-}" ]; then
        cd "$SCRIPT_DIR" && python -m quant_data_engine.tasks.run_financial_sync --all --period "${FINANCIAL_SYNC_PERIOD}" 2>&1 | tee -a ${LOG_DIR}/financial_${DATE}.log
    else
        cd "$SCRIPT_DIR" && python -m quant_data_engine.tasks.run_financial_sync --recent-ipo 30 --test --data-types financial_indicator 2>&1 | tee -a ${LOG_DIR}/financial_${DATE}.log
    fi
    if [ ${PIPESTATUS[0]} -eq 0 ]; then
        log_msg "财务数据同步任务执行成功！"
    else
        log_msg "财务数据同步任务执行失败！查看日志：${LOG_DIR}/financial_${DATE}.log"
    fi
}

# ==================== 任务6：18:30 资金流数据同步 ====================
run_moneyflow_sync() {
    log_msg "开始执行资金流数据同步任务..."
    check_and_install_package
    cd "$SCRIPT_DIR" && python -m quant_data_engine.tasks.run_moneyflow_sync --days 1 2>&1 | tee -a ${LOG_DIR}/moneyflow_${DATE}.log
    if [ ${PIPESTATUS[0]} -eq 0 ]; then
        log_msg "资金流数据同步任务执行成功！"
    else
        log_msg "资金流数据同步任务执行失败！查看日志：${LOG_DIR}/moneyflow_${DATE}.log"
    fi
}

# ==================== 任务6b：21:30 神奇九转同步 ====================
run_nineturn_sync() {
    log_msg "开始执行神奇九转（stk_nineturn）同步任务..."
    check_and_install_package
    cd "$SCRIPT_DIR" && python -m quant_data_engine.tasks.run_nineturn_sync --days 1 --freq daily 2>&1 | tee -a ${LOG_DIR}/nineturn_${DATE}.log
    if [ ${PIPESTATUS[0]} -eq 0 ]; then
        log_msg "神奇九转同步任务执行成功！"
    else
        log_msg "神奇九转同步任务执行失败！查看日志：${LOG_DIR}/nineturn_${DATE}.log"
    fi
}

# ==================== 任务7：18:30 业绩数据同步 ====================
run_earnings_sync() {
    log_msg "开始执行业绩数据同步任务..."
    check_and_install_package
    cd "$SCRIPT_DIR" && python -m quant_data_engine.tasks.run_earnings_sync --days 1 2>&1 | tee -a ${LOG_DIR}/earnings_${DATE}.log
    if [ ${PIPESTATUS[0]} -eq 0 ]; then
        log_msg "业绩数据同步任务执行成功！"
    else
        log_msg "业绩数据同步任务执行失败！查看日志：${LOG_DIR}/earnings_${DATE}.log"
    fi
}

# ==================== 任务：公募基金持仓（fund_portfolio，建议每周） ====================
run_fund_portfolio_sync() {
    log_msg "开始执行公募基金持仓（fund_portfolio -> fund_holdings）同步..."
    check_and_install_package
    DAYS="${FUND_PORTFOLIO_ANN_DAYS:-14}"
    cd "$SCRIPT_DIR" && python -m quant_data_engine.tasks.run_fund_portfolio_sync --ann-days "${DAYS}" 2>&1 | tee -a ${LOG_DIR}/fund_portfolio_${DATE}.log
    if [ ${PIPESTATUS[0]} -eq 0 ]; then
        log_msg "公募基金持仓同步任务执行成功！"
    else
        log_msg "公募基金持仓同步任务执行失败！查看日志：${LOG_DIR}/fund_portfolio_${DATE}.log"
        return 1
    fi
}

run_overseas_equity_sync() {
    local market="$1"
    local dataset="$2"
    local extra="${3:-}"
    log_msg "开始执行 ${market} ${dataset} 同步..."
    check_and_install_package
    cd "$SCRIPT_DIR" && python -m quant_data_engine.tasks.run_overseas_equity_sync --market "$market" --dataset "$dataset" $extra 2>&1 | tee -a ${LOG_DIR}/${market}_${dataset}_${DATE}.log
    if [ ${PIPESTATUS[0]} -eq 0 ]; then
        log_msg "${market} ${dataset} 同步任务执行成功！"
    else
        log_msg "${market} ${dataset} 同步任务执行失败！查看日志：${LOG_DIR}/${market}_${dataset}_${DATE}.log"
    fi
}

run_fund_basic_sync() {
    log_msg "开始执行公募基金基础信息（fund_basic）同步..."
    check_and_install_package
    cd "$SCRIPT_DIR" && python -m quant_data_engine.tasks.run_fund_basic_sync 2>&1 | tee -a ${LOG_DIR}/fund_basic_${DATE}.log
    if [ ${PIPESTATUS[0]} -eq 0 ]; then
        log_msg "公募基金基础信息同步任务执行成功！"
    else
        log_msg "公募基金基础信息同步任务执行失败！查看日志：${LOG_DIR}/fund_basic_${DATE}.log"
        return 1
    fi
}

run_fund_inventory_snapshot() {
    log_msg "开始生成主动公募库存四态快照..."
    check_and_install_package
    cd "$SCRIPT_DIR" && python -m quant_data_engine.tasks.run_fund_inventory_snapshot 2>&1 | tee -a ${LOG_DIR}/fund_inventory_${DATE}.log
    if [ ${PIPESTATUS[0]} -eq 0 ]; then
        log_msg "主动公募库存快照任务执行成功！"
    else
        log_msg "主动公募库存快照任务执行失败！查看日志：${LOG_DIR}/fund_inventory_${DATE}.log"
        return 1
    fi
}

run_fund_copywork_snapshot() {
    log_msg "开始生成公募抄作业快照（正式全持仓 + 未到期/前十大预览）..."
    check_and_install_package
    cd "$SCRIPT_DIR" && python -m quant_data_engine.tasks.run_fund_copywork_snapshot 2>&1 | tee -a ${LOG_DIR}/fund_copywork_${DATE}.log
    if [ ${PIPESTATUS[0]} -eq 0 ]; then
        log_msg "公募抄作业快照任务执行成功！"
    else
        log_msg "公募抄作业快照任务执行失败！查看日志：${LOG_DIR}/fund_copywork_${DATE}.log"
        return 1
    fi
}

# ==================== 任务8：18:45 市场情绪分析（阴阳谱，日线级） ====================
# Default: MA5 + MA30 (comma-separated). Override with SENTIMENT_MA_PERIODS.
run_sentiment_daily() {
    log_msg "开始执行市场情绪分析任务（阴阳谱 daily）..."
    check_and_install_package
    local MA_PERIODS="${SENTIMENT_MA_PERIODS:-5,30}"
    cd "$SCRIPT_DIR" && python -m quant_data_engine.tasks.analyze_market_sentiment \
        --granularity daily \
        --latest 1 \
        --ma-periods "${MA_PERIODS}" \
        2>&1 | tee -a ${LOG_DIR}/sentiment_${DATE}.log
    if [ ${PIPESTATUS[0]} -eq 0 ]; then
        log_msg "市场情绪分析任务执行成功！(ma_periods=${MA_PERIODS})"
    else
        log_msg "市场情绪分析任务执行失败！查看日志：${LOG_DIR}/sentiment_${DATE}.log"
    fi
}

run_realtime_daily_k_sync() {
    log_msg "开始执行实时日K同步与实时日K阴阳谱任务..."
    check_and_install_package
    local EXTRA_ARGS=""
    if [ "${REALTIME_DAILY_K_ALLOW_OUTSIDE_SESSION:-0}" = "1" ]; then
        EXTRA_ARGS="--ignore-session"
    fi
    # Prefer multi-period list; fall back to single REALTIME_DAILY_K_MA_PERIOD for compat.
    local MA_PERIODS="${REALTIME_DAILY_K_MA_PERIODS:-}"
    if [ -z "${MA_PERIODS}" ]; then
        MA_PERIODS="${REALTIME_DAILY_K_MA_PERIOD:-5,30}"
    fi
    cd "$SCRIPT_DIR" && python -m quant_data_engine.tasks.run_realtime_daily_k_sync \
        --ma-periods "${MA_PERIODS}" \
        --batch-size "${REALTIME_DAILY_K_BATCH_SIZE:-500}" \
        ${EXTRA_ARGS} \
        2>&1 | tee -a ${LOG_DIR}/realtime_daily_k_${DATE}.log
    if [ ${PIPESTATUS[0]} -eq 0 ]; then
        log_msg "实时日K同步与实时日K阴阳谱任务执行成功！(ma_periods=${MA_PERIODS})"
    else
        log_msg "实时日K同步与实时日K阴阳谱任务执行失败！查看日志：${LOG_DIR}/realtime_daily_k_${DATE}.log"
        return 1
    fi
}

run_realtime_daily_k_compare() {
    log_msg "开始执行实时日K与盘后日线等价性对比任务..."
    check_and_install_package
    cd "$SCRIPT_DIR" && python -m quant_data_engine.tasks.compare_realtime_daily_k_eod \
        2>&1 | tee -a ${LOG_DIR}/realtime_daily_k_compare_${DATE}.log
    if [ ${PIPESTATUS[0]} -eq 0 ]; then
        log_msg "实时日K等价性对比任务执行成功！"
    else
        log_msg "实时日K等价性对比任务执行失败！查看日志：${LOG_DIR}/realtime_daily_k_compare_${DATE}.log"
    fi
}

# ==================== 任务9：19:30 市场风险预警扫描（全行业） ====================
run_risk_scan() {
    log_msg "开始执行市场风险预警扫描任务（全行业）..."
    check_and_install_package
    cd "$SCRIPT_DIR" && python -m quant_data_engine.tasks.run_risk_scan \
        2>&1 | tee -a ${LOG_DIR}/risk_scan_${DATE}.log
    if [ ${PIPESTATUS[0]} -eq 0 ]; then
        log_msg "市场风险预警扫描任务执行成功！"
    else
        log_msg "市场风险预警扫描任务执行失败！查看日志：${LOG_DIR}/risk_scan_${DATE}.log"
    fi
}

run_us_risk_scan() {
    log_msg "开始执行美股板块风险扫描（美东收盘后）..."
    check_and_install_package
    cd "$SCRIPT_DIR" && python -m quant_data_engine.tasks.run_risk_scan \
        --sectors us_semicon us_tech us_macro \
        2>&1 | tee -a ${LOG_DIR}/risk_scan_us_${DATE}.log
    if [ ${PIPESTATUS[0]} -eq 0 ]; then
        log_msg "美股板块风险扫描成功！"
    else
        log_msg "美股板块风险扫描失败！查看日志：${LOG_DIR}/risk_scan_us_${DATE}.log"
    fi
}

run_a_risk_scan() {
    log_msg "开始执行A股板块风险扫描（A股收盘后）..."
    check_and_install_package
    cd "$SCRIPT_DIR" && python -m quant_data_engine.tasks.run_risk_scan \
        --sectors semicon semicon_eq finance consumer healthcare nev robot tourism aerospace \
        2>&1 | tee -a ${LOG_DIR}/risk_scan_a_${DATE}.log
    if [ ${PIPESTATUS[0]} -eq 0 ]; then
        log_msg "A股板块风险扫描成功！"
    else
        log_msg "A股板块风险扫描失败！查看日志：${LOG_DIR}/risk_scan_a_${DATE}.log"
    fi
}

run_market_breadth_daily() {
    log_msg "开始执行A股每日广度原子指标预计算（涨跌/新高新低）..."
    check_and_install_package
    cd "$SCRIPT_DIR" && python -m quant_data_engine.tasks.run_market_breadth_daily \
        2>&1 | tee -a ${LOG_DIR}/market_breadth_${DATE}.log
    if [ ${PIPESTATUS[0]} -eq 0 ]; then
        log_msg "A股每日广度原子指标预计算任务执行成功！"
    else
        log_msg "A股每日广度原子指标预计算任务执行失败！查看日志：${LOG_DIR}/market_breadth_${DATE}.log"
    fi
}

run_regime_scan() {
    log_msg "开始执行A股广度与估值扫描（MCL/兴登堡/CAPE）..."
    check_and_install_package
    cd "$SCRIPT_DIR" && python -m quant_data_engine.tasks.run_market_regime_scan \
        2>&1 | tee -a ${LOG_DIR}/regime_scan_${DATE}.log
    if [ ${PIPESTATUS[0]} -eq 0 ]; then
        log_msg "A股广度与估值扫描任务执行成功！"
    else
        log_msg "A股广度与估值扫描任务执行失败！查看日志：${LOG_DIR}/regime_scan_${DATE}.log"
    fi
}

# ==================== 美股日线同步（美东收盘后，北京时间 06:30）====================
run_global_market_daily() {
    log_msg "开始执行美股/全球市场日线历史数据同步（yfinance）..."
    check_and_install_package
    # Pass secondary MongoDB URI for dual-write if configured
    SYNC_URI_ARG=""
    if [ -n "${SECONDARY_MONGO_URI:-}" ]; then
        SYNC_URI_ARG="--sync-uri ${SECONDARY_MONGO_URI}"
    fi
    cd "$SCRIPT_DIR" && python -m quant_data_engine.tasks.run_global_market_daily_sync \
        ${SYNC_URI_ARG} \
        2>&1 | tee -a ${LOG_DIR}/global_market_daily_${DATE}.log
    if [ ${PIPESTATUS[0]} -eq 0 ]; then
        log_msg "美股日线数据同步成功！"
    else
        log_msg "美股日线数据同步失败！查看日志：${LOG_DIR}/global_market_daily_${DATE}.log"
    fi
}

# ==================== 任务：15:45 行业与ETF数据同步及聚合分析 ====================
run_industry_sync() {
    log_msg "开始执行行业与ETF数据同步及聚合分析任务..."
    check_and_install_package
    cd "$SCRIPT_DIR" && python -m quant_data_engine.tasks.run_industry_sync --auto 2>&1 | tee -a ${LOG_DIR}/industry_${DATE}.log
    if [ ${PIPESTATUS[0]} -eq 0 ]; then
        log_msg "行业与ETF数据同步及聚合分析任务执行成功！"
    else
        log_msg "行业与ETF数据同步及聚合分析任务执行失败！查看日志：${LOG_DIR}/industry_${DATE}.log"
    fi
}

# ==================== 任务6：15:30 连板天梯数据同步 ====================
run_ladder_sync() {
    log_msg "开始执行连板天梯数据同步任务..."
    check_and_install_package
    cd "$SCRIPT_DIR" && python -m quant_data_engine.tasks.run_ladder_sync 2>&1 | tee -a ${LOG_DIR}/ladder_${DATE}.log
    if [ ${PIPESTATUS[0]} -eq 0 ]; then
        log_msg "连板天梯数据同步任务执行成功！"
        
        # Trigger Analysis immediately
        check_and_install_quant_analyzer
        log_msg "触发连板天梯AI分析..."
        # Go to quantAnalyzer dir or use -m if installed
        # We need to ensure we use the same python env and config
        
        # Run analysis (capture output to existing log or new log)
        python -m quant_analyzer.main --action analyze_daily 2>&1 | tee -a ${LOG_DIR}/ladder_${DATE}.log
        
    else
        log_msg "连板天梯数据同步任务执行失败！查看日志：${LOG_DIR}/ladder_${DATE}.log"
    fi
}

run_limit_up_narrative() {
    log_msg "开始执行涨停全景市场动向总结（ladder_sync → narrative 入队）..."
    check_and_install_package

    # Step 1: 先同步连板天梯（依赖 daily_limit_up + limit_up_ladder）
    log_msg "[1/2] 同步连板天梯数据..."
    cd "$SCRIPT_DIR" && python -m quant_data_engine.tasks.run_ladder_sync 2>&1 | tee -a ${LOG_DIR}/limit_up_narrative_${DATE}.log
    if [ ${PIPESTATUS[0]} -ne 0 ]; then
        log_msg "连板天梯同步失败，终止 narrative 入队。查看日志：${LOG_DIR}/limit_up_narrative_${DATE}.log"
        return 1
    fi

    # Step 2: 入队 narrative 任务
    log_msg "[2/2] 入队涨停全景市场动向总结任务..."
    cd "$SCRIPT_DIR" && python -m quant_data_engine.tasks.run_limit_up_narrative 2>&1 | tee -a ${LOG_DIR}/limit_up_narrative_${DATE}.log
    if [ ${PIPESTATUS[0]} -eq 0 ]; then
        log_msg "涨停全景总结任务已入队，等待 quantAnalyzer 消费"
    else
        log_msg "涨停全景总结入队失败！查看日志：${LOG_DIR}/limit_up_narrative_${DATE}.log"
    fi
}

# ==================== 任务8b：周六 周线（Tushare weekly → stock_weekly）====================
run_stock_weekly_sync() {
    log_msg "开始执行股票周线同步（Tushare weekly，最近 1 个完整 ISO 周）..."
    check_and_install_package
    cd "$SCRIPT_DIR" && python -m quant_data_engine.tasks.run_stock_weekly --end "$(date +%Y%m%d)" --recent-weeks 1 2>&1 | tee -a ${LOG_DIR}/stock_weekly_${DATE}.log
    if [ ${PIPESTATUS[0]} -eq 0 ]; then
        log_msg "股票周线同步任务执行成功！"
    else
        log_msg "股票周线同步任务执行失败！查看日志：${LOG_DIR}/stock_weekly_${DATE}.log"
    fi
}

# ==================== 任务8c：每月 1 日 月线（Tushare monthly → stock_monthly）====================
run_stock_monthly_sync() {
    log_msg "开始执行股票月线同步（Tushare monthly，上一自然月）..."
    check_and_install_package
    cd "$SCRIPT_DIR" && python -m quant_data_engine.tasks.run_stock_monthly --end "$(date +%Y%m%d)" --recent-months 1 2>&1 | tee -a ${LOG_DIR}/stock_monthly_${DATE}.log
    if [ ${PIPESTATUS[0]} -eq 0 ]; then
        log_msg "股票月线同步任务执行成功！"
    else
        log_msg "股票月线同步任务执行失败！查看日志：${LOG_DIR}/stock_monthly_${DATE}.log"
    fi
}

# ==================== 任务8d：申万行业指数日线（Tushare sw_daily，每日自动）====================
run_sw_daily_sync() {
    log_msg "开始执行申万行业指数日线同步（Tushare sw_daily）..."
    check_and_install_package
    cd "$SCRIPT_DIR" && python -m quant_data_engine.tasks.run_sw_industry_backfill --sync daily --resume 2>&1 | tee -a ${LOG_DIR}/sw_daily_${DATE}.log
    if [ ${PIPESTATUS[0]} -eq 0 ]; then
        log_msg "申万行业指数日线同步任务执行成功！"
    else
        log_msg "申万行业指数日线同步任务执行失败！查看日志：${LOG_DIR}/sw_daily_${DATE}.log"
    fi
}

# ==================== 任务8e：申万行业指数周线（sw_daily → sw_weekly，pandas resample，每日自动）====================
run_sw_weekly_sync() {
    log_msg "开始执行申万行业指数周线同步（sw_daily → sw_weekly，pandas resample）..."
    check_and_install_package
    cd "$SCRIPT_DIR" && python -m quant_data_engine.tasks.run_sw_industry_backfill --sync weekly --resume 2>&1 | tee -a ${LOG_DIR}/sw_weekly_${DATE}.log
    if [ ${PIPESTATUS[0]} -eq 0 ]; then
        log_msg "申万行业指数周线同步任务执行成功！"
    else
        log_msg "申万行业指数周线同步任务执行失败！查看日志：${LOG_DIR}/sw_weekly_${DATE}.log"
    fi
}

# ==================== 任务8f：申万行业指数月线（sw_daily → sw_monthly，pandas resample，每日自动）====================
run_sw_monthly_sync() {
    log_msg "开始执行申万行业指数月线同步（sw_daily → sw_monthly，pandas resample）..."
    check_and_install_package
    cd "$SCRIPT_DIR" && python -m quant_data_engine.tasks.run_sw_industry_backfill --sync monthly --resume 2>&1 | tee -a ${LOG_DIR}/sw_monthly_${DATE}.log
    if [ ${PIPESTATUS[0]} -eq 0 ]; then
        log_msg "申万行业指数月线同步任务执行成功！"
    else
        log_msg "申万行业指数月线同步任务执行失败！查看日志：${LOG_DIR}/sw_monthly_${DATE}.log"
    fi
}

# ==================== 任务8g：申万行业成分股（Tushare index_member_all → sw_index_member，每日自动）====================
run_sw_member_sync() {
    log_msg "开始执行申万行业成分股同步（Tushare index_member_all → sw_index_member）..."
    check_and_install_package
    cd "$SCRIPT_DIR" && python -m quant_data_engine.tasks.run_sw_industry_backfill --sync members --src SW2021 2>&1 | tee -a ${LOG_DIR}/sw_member_${DATE}.log
    if [ ${PIPESTATUS[0]} -eq 0 ]; then
        log_msg "申万行业成分股同步任务执行成功，开始物化 stock_info SW 字段（sw_l1/l2/l3）..."
        cd "$SCRIPT_DIR" && python -m quant_data_engine.tasks.backfill_stock_info_sw_fields --src SW2021 2>&1 | tee -a ${LOG_DIR}/sw_stock_info_${DATE}.log
        if [ ${PIPESTATUS[0]} -eq 0 ]; then
            log_msg "stock_info SW 字段物化任务执行成功！"
        else
            log_msg "stock_info SW 字段物化任务执行失败！查看日志：${LOG_DIR}/sw_stock_info_${DATE}.log"
        fi
    else
        log_msg "申万行业成分股同步任务执行失败！查看日志：${LOG_DIR}/sw_member_${DATE}.log"
    fi
}

# ==================== 宏观数据同步：国内宏观（月/季，策略B跳过）====================
run_cn_macro_sync() {
    log_msg "开始执行国内宏观数据同步（shibor/LPR/PMI/CPI/PPI/M2/社融/GDP）..."
    check_and_install_package
    cd "$SCRIPT_DIR" && python -m quant_data_engine.tasks.run_cn_macro_sync 2>&1 | tee -a ${LOG_DIR}/cn_macro_${DATE}.log
    if [ ${PIPESTATUS[0]} -eq 0 ]; then
        log_msg "国内宏观数据同步任务执行成功！"
    else
        log_msg "国内宏观数据同步任务执行失败！查看日志：${LOG_DIR}/cn_macro_${DATE}.log"
    fi
}

# ==================== 宏观数据同步：美国利率（日频）====================
run_us_rates_sync() {
    log_msg "开始执行美国利率数据同步（tycr/trycr/tbr/tltr/trltr）..."
    check_and_install_package
    cd "$SCRIPT_DIR" && python -m quant_data_engine.tasks.run_us_rates_sync 2>&1 | tee -a ${LOG_DIR}/us_rates_${DATE}.log
    if [ ${PIPESTATUS[0]} -eq 0 ]; then
        log_msg "美国利率数据同步任务执行成功！"
    else
        log_msg "美国利率数据同步任务执行失败！查看日志：${LOG_DIR}/us_rates_${DATE}.log"
    fi
}

# ==================== 任务9：热股数据同步 (THS + DC) ====================
run_hot_stock_sync() {
    log_msg "开始执行热股数据同步任务..."
    check_and_install_package
    cd "$SCRIPT_DIR" && python -m quant_data_engine.tasks.run_hot_stock_sync --source all 2>&1 | tee -a ${LOG_DIR}/hot_stock_${DATE}.log
    if [ ${PIPESTATUS[0]} -eq 0 ]; then
        log_msg "热股数据同步任务执行成功！"
    else
        log_msg "热股数据同步任务执行失败！查看日志：${LOG_DIR}/hot_stock_${DATE}.log"
    fi
}

# ==================== 数据运维任务：MongoDB 夜间备份 ====================
run_mongodb_backup() {
    log_msg "开始执行 MongoDB 夜间备份任务..."
    check_and_install_package
    cd "$SCRIPT_DIR" && python -m quant_data_engine.tasks.run_mongodb_backup 2>&1 | tee -a ${LOG_DIR}/mongodb_backup_${DATE}.log
    if [ ${PIPESTATUS[0]} -eq 0 ]; then
        log_msg "MongoDB 夜间备份任务执行成功！"
    else
        log_msg "MongoDB 夜间备份任务执行失败！查看日志：${LOG_DIR}/mongodb_backup_${DATE}.log"
    fi
}

# ==================== 任务调用入口（按参数执行对应任务） ====================
case "$1" in
    stockinfo)
        run_stock_info_sync
        ;;
    stockinfo_delisted)
        run_stock_info_delisted_sync
        ;;
    stock)
        run_stock_daily_sync
        ;;
    stock_adj_factor)
        run_stock_adj_factor_sync
        ;;
    index)
        run_index_daily_sync
        ;;
    index_dailybasic)
        run_index_dailybasic_sync
        ;;
    margin_sync)
        run_margin_sync
        ;;
    market_pe_aggregate)
        run_market_pe_aggregate
        ;;
    index_constituents)
        run_index_constituents_sync
        ;;
    theme_lag)
        run_theme_lag_candidates_sync
        ;;
    etf)
        run_etf_daily_sync
        ;;
    fut)
        run_fut_daily_sync
        ;;
    etf_share_size)
        run_etf_share_size_sync
        ;;
    financial)
        run_financial_sync
        ;;
    moneyflow)
        run_moneyflow_sync
        ;;
    nineturn)
        run_nineturn_sync
        ;;
    earnings)
        run_earnings_sync
        ;;
    fund_portfolio)
        run_fund_portfolio_sync
        ;;
    fund_basic)
        run_fund_basic_sync
        ;;
    hk_basic)
        run_overseas_equity_sync hk basic
        ;;
    hk_tradecal)
        run_overseas_equity_sync hk tradecal
        ;;
    hk_daily)
        run_overseas_equity_sync hk daily "--days 1"
        ;;
    us_basic)
        run_overseas_equity_sync us basic
        ;;
    us_tradecal)
        run_overseas_equity_sync us tradecal
        ;;
    us_daily)
        run_overseas_equity_sync us daily "--days 1"
        ;;
    fund_inventory)
        run_fund_inventory_snapshot
        ;;
    fund_copywork)
        run_fund_copywork_snapshot
        ;;
    sentiment)
        run_sentiment_daily
        ;;
    realtime_daily_k)
        run_realtime_daily_k_sync
        ;;
    realtime_daily_k_compare)
        run_realtime_daily_k_compare
        ;;
    industry)
        run_industry_sync
        ;;
    ladder)
        run_ladder_sync
        ;;
    limit_up_narrative)
        run_limit_up_narrative
        ;;
    risk_scan)
        run_risk_scan
        ;;
    risk_scan_us)
        run_us_risk_scan
        ;;
    risk_scan_a)
        run_a_risk_scan
        ;;
    market_breadth)
        run_market_breadth_daily
        ;;
    regime_scan)
        run_regime_scan
        ;;
    global_market_daily)
        run_global_market_daily
        ;;
    cn_macro_sync)
        run_cn_macro_sync
        ;;
    us_rates_sync)
        run_us_rates_sync
        ;;
    hotstock)
        run_hot_stock_sync
        ;;
    mongodb_backup)
        run_mongodb_backup
        ;;
    stock_weekly)
        run_stock_weekly_sync
        ;;
    stock_monthly)
        run_stock_monthly_sync
        ;;
    sw_daily)
        run_sw_daily_sync
        ;;
    sw_weekly)
        run_sw_weekly_sync
        ;;
    sw_monthly)
        run_sw_monthly_sync
        ;;
    sw_member)
        run_sw_member_sync
        ;;
    daily)
        # 执行所有日线相关任务
        run_stock_daily_sync
        sleep 30
        run_stock_adj_factor_sync
        sleep 30
        run_index_daily_sync
        sleep 30
        run_index_constituents_sync
        sleep 30
        run_etf_daily_sync
        ;;
    all)
        # 一次性执行所有任务（测试用）
        run_ladder_sync
        sleep 60
        run_stock_daily_sync
        sleep 60
        run_stock_adj_factor_sync
        sleep 60
        run_index_daily_sync
        sleep 60
        run_index_constituents_sync
        sleep 60
        run_etf_daily_sync
        sleep 60
        run_financial_sync
        sleep 60
        run_moneyflow_sync
        ;;
    *)
        echo "用法：$0 {stockinfo|stockinfo_delisted|stock|stock_adj_factor|stock_weekly|stock_monthly|sw_daily|sw_weekly|sw_monthly|sw_member|index|index_dailybasic|index_constituents|margin_sync|market_pe_aggregate|theme_lag|etf|etf_share_size|financial|moneyflow|nineturn|earnings|fund_portfolio|fund_basic|fund_inventory|fund_copywork|ladder|limit_up_narrative|risk_scan|industry|sentiment|realtime_daily_k|realtime_daily_k_compare|hotstock|mongodb_backup|hk_basic|hk_tradecal|hk_daily|us_basic|us_tradecal|us_daily|daily|all}"
        echo "  stockinfo - 执行股票基础信息(stock_info)同步任务"
        echo "  stockinfo_delisted - 从 stock_basic 补录退市/暂停股身份字段（每周）"
        echo "  stock     - 执行股票日线数据同步任务"
        echo "  stock_adj_factor - 股票复权因子（Tushare adj_factor -> Mongo stock_adj_factor）"
        echo "  stock_weekly  - 股票周线（weekly → stock_weekly，建议每周六 cron）"
        echo "  stock_monthly - 股票月线（monthly → stock_monthly，建议每月 1 日 cron）"
        echo "  sw_daily      - 申万行业指数日线（Tushare sw_daily，每日自动同步）"
        echo "  sw_weekly     - 申万行业指数周线（sw_daily → sw_weekly，每日自动汇聚）"
        echo "  sw_monthly    - 申万行业指数月线（sw_daily → sw_monthly，每日自动汇聚）"
        echo "  sw_member     - 申万行业成分股（index_member_all → sw_index_member，并物化 stock_info SW 字段）"
        echo "  index     - 执行指数日线数据同步任务"
        echo "  index_dailybasic - 指数估值指标（index_dailybasic -> index_daily_basic，沪深300/上证/创业板）"
        echo "  margin_sync - 沪深两融交易汇总（Tushare margin -> margin_market_daily，建议 18:05 cron）"
        echo "  market_pe_aggregate - 全A谐波市盈率（financial_daily_basic -> market_pe_daily ALLA.WI，建议 19:35 cron）"
        echo "  index_constituents - 指数成分与权重（index_weight -> Mongo index_constituents，建议每日指数日线之后）"
        echo "  dc_concept_index|dc_concept_members - 已废弃（空操作）；概念由 theme_lag 内 Tushare 在线拉取"
        echo "  theme_lag       - 主题补涨候选；成功后同锚定日跑 theme_lag_candidate_validation（日志 theme_lag_validation_*.log）"
        echo "  etf       - 执行ETF日线数据同步任务"
        echo "  etf_share_size - 执行 Tushare etf_share_size -> Mongo etf_share_size（建议 19 点后）"
        echo "  financial - 执行财务数据同步任务"
        echo "  moneyflow - 执行资金流数据同步任务"
        echo "  nineturn  - 执行神奇九转（stk_nineturn）同步任务"
        echo "  earnings  - 执行业绩数据同步任务"
        echo "  fund_portfolio - 公募基金持仓（Tushare fund_portfolio -> fund_holdings）"
        echo "  fund_basic     - 公募基金基础信息（Tushare fund_basic 全市场 -> fund_basic）"
        echo "  hk_basic / hk_tradecal / hk_daily - 港股列表/日历/日线（独立集合，不混入 A 股）"
        echo "  us_basic / us_tradecal / us_daily - 美股列表/日历/日线（us_daily 需单独权限）"
        echo "  fund_inventory - 主动公募库存四态快照（fund_holdings -> fund_inventory_snapshots）"
        echo "  fund_copywork  - 公募抄作业快照（正式全持仓 + 未到期/前十大预览）"
        echo "  ladder    - 执行连板天梯数据同步任务"
        echo "  limit_up_narrative - 入队涨停全景市场动向总结任务（14:50 尾盘）"
        echo "  risk_scan - 执行市场风险预警扫描（全行业，建议 19:30 cron）"
        echo "  global_market_daily - 美股/全球市场日线同步（yfinance，建议 06:30 cron）"
        echo "  cn_macro_sync   - 国内宏观数据同步（shibor/LPR/PMI/CPI/PPI/M2/社融/GDP，建议 19:40 cron；月/季数据策略B跳过）"
        echo "  us_rates_sync   - 美国利率数据同步（日频收益率曲线，建议 06:35 紧跟 global_market_daily）"
        echo "  industry  - 执行行业与ETF数据同步及聚合任务"
        echo "  sentiment - 执行市场情绪（阴阳谱）分析任务"
        echo "  realtime_daily_k - 执行实时日K同步与实时日K阴阳谱任务"
        echo "  realtime_daily_k_compare - 对比实时日K快照与盘后Tushare日线"
        echo "  hotstock  - 执行热股数据同步任务（THS + DC）"
        echo "  mongodb_backup - 执行 MongoDB 夜间备份并上传到 TOS/S3"
        echo "  daily     - 执行所有日线数据同步任务（股票+指数+ETF）"
        echo "  all       - 执行所有任务（测试用）"
        exit 1
        ;;
esac

task_status=$?
exit "$task_status"