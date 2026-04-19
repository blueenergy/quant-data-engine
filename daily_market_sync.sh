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
    # 只同步最近上市的股票的财务数据（测试模式）
    cd "$SCRIPT_DIR" && python -m quant_data_engine.tasks.run_financial_sync --recent-ipo 30 --test --data-types financial_indicator 2>&1 | tee -a ${LOG_DIR}/financial_${DATE}.log
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
    daily)
        # 执行所有日线相关任务
        run_stock_daily_sync
        sleep 30
        run_index_daily_sync
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
        run_etf_daily_sync
        sleep 60
        run_financial_sync
        sleep 60
        run_moneyflow_sync
        ;;
    *)
        echo "用法：$0 {stockinfo|stock|index|etf|etf_share_size|financial|moneyflow|earnings|fund_portfolio|ladder|industry|sentiment|hotstock|daily|all}"
        echo "  stockinfo - 执行股票基础信息(stock_info)同步任务"
        echo "  stock     - 执行股票日线数据同步任务"
        echo "  index     - 执行指数日线数据同步任务"
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