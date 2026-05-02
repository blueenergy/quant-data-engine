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
    else
        log_msg "股票基础信息(stock_info)同步任务执行失败！查看日志：${LOG_DIR}/stock_info_${DATE}.log"
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
    fi
}

# ==================== 任务8：18:45 市场情绪分析（阴阳谱，日线级） ====================
run_sentiment_daily() {
    log_msg "开始执行市场情绪分析任务（阴阳谱 daily）..."
    check_and_install_package
    cd "$SCRIPT_DIR" && python -m quant_data_engine.tasks.analyze_market_sentiment \
        --granularity daily \
        --latest 1 \
        --ma-period 5 \
        2>&1 | tee -a ${LOG_DIR}/sentiment_${DATE}.log
    if [ ${PIPESTATUS[0]} -eq 0 ]; then
        log_msg "市场情绪分析任务执行成功！"
    else
        log_msg "市场情绪分析任务执行失败！查看日志：${LOG_DIR}/sentiment_${DATE}.log"
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

# ==================== 任务8d：申万行业指数周线（sw_daily → sw_weekly，pandas resample，每日自动）====================
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

# ==================== 任务8e：申万行业指数月线（sw_daily → sw_monthly，pandas resample，每日自动）====================
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

# ==================== 任务9：22:35 热股数据同步 (THS + DC) ====================
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

# ==================== 任务调用入口（按参数执行对应任务） ====================
case "$1" in
    stockinfo)
        run_stock_info_sync
        ;;
    stock)
        run_stock_daily_sync
        ;;
    index)
        run_index_daily_sync
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
    etf_share_size)
        run_etf_share_size_sync
        ;;
    financial)
        run_financial_sync
        ;;
    moneyflow)
        run_moneyflow_sync
        ;;
    earnings)
        run_earnings_sync
        ;;
    fund_portfolio)
        run_fund_portfolio_sync
        ;;
    sentiment)
        run_sentiment_daily
        ;;
    industry)
        run_industry_sync
        ;;
    ladder)
        run_ladder_sync
        ;;
    hotstock)
        run_hot_stock_sync
        ;;
    stock_weekly)
        run_stock_weekly_sync
        ;;
    stock_monthly)
        run_stock_monthly_sync
        ;;
    sw_weekly)
        run_sw_weekly_sync
        ;;
    sw_monthly)
        run_sw_monthly_sync
        ;;
    daily)
        # 执行所有日线相关任务
        run_stock_daily_sync
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
        echo "用法：$0 {stockinfo|stock|stock_weekly|stock_monthly|index|index_constituents|theme_lag|etf|etf_share_size|financial|moneyflow|earnings|fund_portfolio|ladder|industry|sentiment|hotstock|daily|all}"
        echo "  stockinfo - 执行股票基础信息(stock_info)同步任务"
        echo "  stock     - 执行股票日线数据同步任务"
        echo "  stock_weekly  - 股票周线（weekly → stock_weekly，建议每周六 cron）"
        echo "  stock_monthly - 股票月线（monthly → stock_monthly，建议每月 1 日 cron）"
        echo "  sw_weekly     - 申万行业指数周线（sw_daily → sw_weekly，每日自动汇聚）"
        echo "  sw_monthly    - 申万行业指数月线（sw_daily → sw_monthly，每日自动汇聚）"
        echo "  index     - 执行指数日线数据同步任务"
        echo "  index_constituents - 指数成分与权重（index_weight -> Mongo index_constituents，建议每日指数日线之后）"
        echo "  dc_concept_index|dc_concept_members - 已废弃（空操作）；概念由 theme_lag 内 Tushare 在线拉取"
        echo "  theme_lag       - 主题补涨候选；成功后同锚定日跑 theme_lag_candidate_validation（日志 theme_lag_validation_*.log）"
        echo "  etf       - 执行ETF日线数据同步任务"
        echo "  etf_share_size - 执行 Tushare etf_share_size -> Mongo etf_share_size（建议 19 点后）"
        echo "  financial - 执行财务数据同步任务"
        echo "  moneyflow - 执行资金流数据同步任务"
        echo "  earnings  - 执行业绩数据同步任务"
        echo "  fund_portfolio - 公募基金持仓（Tushare fund_portfolio -> fund_holdings）"
        echo "  ladder    - 执行连板天梯数据同步任务"
        echo "  industry  - 执行行业与ETF数据同步及聚合任务"
        echo "  sentiment - 执行市场情绪（阴阳谱）分析任务"
        echo "  hotstock  - 执行热股数据同步任务（THS + DC）"
        echo "  daily     - 执行所有日线数据同步任务（股票+指数+ETF）"
        echo "  all       - 执行所有任务（测试用）"
        exit 1
        ;;
esac

exit 0