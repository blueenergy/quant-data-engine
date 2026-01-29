#!/bin/bash
# 脚本用途：quant_data_engine 项目每日市场数据同步任务
# 维护人：shuyolin
# 执行时间：周一到周五，按不同时段执行对应任务

# 激活虚拟环境（如果存在）
if [ -d ".venv" ]; then
    source .venv/bin/activate
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] 已激活虚拟环境"
fi

# 定义通用变量（方便后续修改路径）
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="${SCRIPT_DIR}/logs"
DATE=$(date +%Y%m%d)
mkdir -p "$LOG_DIR"  # 确保日志目录存在

# 检查并安装项目（如果尚未安装）
check_and_install_package() {
    if ! python -c "import quant_data_engine" &> /dev/null; then
        echo "[$(date +'%Y-%m-%d %H:%M:%S')] 未检测到已安装的 quant_data_engine 包，尝试安装..."
        pip install -e . &> /dev/null
        if [ $? -ne 0 ]; then
            echo "[$(date +'%Y-%m-%d %H:%M:%S')] 安装 quant_data_engine 包失败！"
            exit 1
        fi
        echo "[$(date +'%Y-%m-%d %H:%M:%S')] quant_data_engine 包安装成功！"
    fi
}

# ==================== 任务1：17:30 批量股票日线数据同步 ====================
run_stock_daily_sync() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] 开始执行股票日线数据同步任务..."
    check_and_install_package
    cd "$SCRIPT_DIR" && python -m quant_data_engine.tasks.run_stock_daily --mode by-date --days 1 >> ${LOG_DIR}/stock_daily_${DATE}.log 2>&1
    if [ $? -eq 0 ]; then
        echo "[$(date +'%Y-%m-%d %H:%M:%S')] 股票日线数据同步任务执行成功！"
    else
        echo "[$(date +'%Y-%m-%d %H:%M:%S')] 股票日线数据同步任务执行失败！查看日志：${LOG_DIR}/stock_daily_${DATE}.log"
    fi
}

# ==================== 任务2：17:45 指数日线数据同步 ====================
run_index_daily_sync() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] 开始执行指数日线数据同步任务..."
    check_and_install_package
    cd "$SCRIPT_DIR" && python -m quant_data_engine.tasks.run_index_daily --core --end $(date +%Y%m%d) >> ${LOG_DIR}/index_daily_${DATE}.log 2>&1
    if [ $? -eq 0 ]; then
        echo "[$(date +'%Y-%m-%d %H:%M:%S')] 指数日线数据同步任务执行成功！"
    else
        echo "[$(date +'%Y-%m-%d %H:%M:%S')] 指数日线数据同步任务执行失败！查看日志：${LOG_DIR}/index_daily_${DATE}.log"
    fi
}

# ==================== 任务3：18:00 ETF日线数据同步 ====================
run_etf_daily_sync() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] 开始执行ETF日线数据同步任务..."
    check_and_install_package
    cd "$SCRIPT_DIR" && python -m quant_data_engine.tasks.run_etf_daily --core --end $(date +%Y%m%d) >> ${LOG_DIR}/etf_daily_${DATE}.log 2>&1
    if [ $? -eq 0 ]; then
        echo "[$(date +'%Y-%m-%d %H:%M:%S')] ETF日线数据同步任务执行成功！"
    else
        echo "[$(date +'%Y-%m-%d %H:%M:%S')] ETF日线数据同步任务执行失败！查看日志：${LOG_DIR}/etf_daily_${DATE}.log"
    fi
}

# ==================== 任务4：18:15 财务数据同步 ====================
run_financial_sync() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] 开始执行财务数据同步任务..."
    check_and_install_package
    # 只同步最近上市的股票的财务数据（测试模式）
    cd "$SCRIPT_DIR" && python -m quant_data_engine.tasks.run_financial_sync --recent-ipo 30 --test --data-types financial_indicator >> ${LOG_DIR}/financial_${DATE}.log 2>&1
    if [ $? -eq 0 ]; then
        echo "[$(date +'%Y-%m-%d %H:%M:%S')] 财务数据同步任务执行成功！"
    else
        echo "[$(date +'%Y-%m-%d %H:%M:%S')] 财务数据同步任务执行失败！查看日志：${LOG_DIR}/financial_${DATE}.log"
    fi
}

# ==================== 任务5：18:30 资金流数据同步 ====================
run_moneyflow_sync() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] 开始执行资金流数据同步任务..."
    check_and_install_package
    cd "$SCRIPT_DIR" && python -m quant_data_engine.tasks.run_moneyflow_sync --days 1 >> ${LOG_DIR}/moneyflow_${DATE}.log 2>&1
    if [ $? -eq 0 ]; then
        echo "[$(date +'%Y-%m-%d %H:%M:%S')] 资金流数据同步任务执行成功！"
    else
        echo "[$(date +'%Y-%m-%d %H:%M:%S')] 资金流数据同步任务执行失败！查看日志：${LOG_DIR}/moneyflow_${DATE}.log"
    fi
}

# ==================== 任务调用入口（按参数执行对应任务） ====================
case "$1" in
    stock)
        run_stock_daily_sync
        ;;
    index)
        run_index_daily_sync
        ;;
    etf)
        run_etf_daily_sync
        ;;
    financial)
        run_financial_sync
        ;;
    moneyflow)
        run_moneyflow_sync
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
        echo "用法：$0 {stock|index|etf|financial|moneyflow|daily|all}"
        echo "  stock     - 执行股票日线数据同步任务"
        echo "  index     - 执行指数日线数据同步任务"
        echo "  etf       - 执行ETF日线数据同步任务"
        echo "  financial - 执行财务数据同步任务"
        echo "  moneyflow - 执行资金流数据同步任务"
        echo "  daily     - 执行所有日线数据同步任务（股票+指数+ETF）"
        echo "  all       - 执行所有任务（测试用）"
        exit 1
        ;;
esac

exit 0