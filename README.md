# quant-data-engine

统一的量化数据采集 / 处理 / 质量监控引擎 (Plan B 重构)。

## 目录结构
```
config/         # 全局配置与日志
adapters/       # 外部数据源适配 (Tushare、TDX、miniQMT 等)
repositories/   # 数据持久化与读取抽象
pipelines/      # 端到端数据管线 (采集→转换→写库)
models/         # 数据结构契约 (Pydantic/TypedDict)
quality/        # 验证与监控组件
tasks/          # 可调度任务入口 (CLI / cron / systemd)
utils/          # 通用工具函数
tools/          # 数据管理工具
tests/          # 自动化测试
```

## 快速开始
```bash
# 安装 (开发模式)
pip install -e ./quant-data-engine

# 配置环境变量（创建 .env 文件）
MONGO_URL=mongodb://localhost:27017
MONGO_DB=finance
TUSHARE_TOKEN=your_token_here
RATE_LIMIT_PER_SEC=4

# 运行指数日线采集
python -m quant_data_engine.tasks.run_index_daily --codes 000300.SH 000905.SH --backfill-days 3

# 运行股票日线采集（新增）
python -m quant_data_engine.tasks.run_stock_daily --mode by-date --days 5

# 运行财务数据同步（新增）
python -m quant_data_engine.tasks.run_financial_sync --codes 000001.SZ --start 20230101

# 运行资金流数据同步（新增）
python -m quant_data_engine.tasks.run_moneyflow_sync --auto

# 运行实时数据采集 (Windows Bridge)
python -m quant_data_engine.tasks.run_realtime_bridge --auto
```

## 数据采集功能

### 1. 指数日线数据
采集主要指数（沪深300、中证500等）的日线数据。

**使用**：
```bash
python -m quant_data_engine.tasks.run_index_daily --core --backfill-days 3
```

### 2. ETF 日线数据
采集 ETF 的日线数据。

**使用**：
```bash
python -m quant_data_engine.tasks.run_etf_daily --codes 510300.SH 159915.SZ
```

### 3. 股票日线数据（OHLCV + 基础指标）
批量采集股票日线 OHLCV 数据和基础指标（PE、PB、换手率等）。

**特性**：
- 支持按日期批量采集（推荐，效率高）
- 支持按股票逐个采集
- 自动处理交易日历
- 同时采集日线和基础指标

**使用**：
```bash
# 按日期批量采集（推荐）
python -m quant_data_engine.tasks.run_stock_daily --mode by-date --start 20240101 --end 20240110

# 自动同步最近 5 个交易日
python -m quant_data_engine.tasks.run_stock_daily --mode by-date --days 5

# 按股票采集
python -m quant_data_engine.tasks.run_stock_daily --mode by-stock --codes 000001.SZ 000002.SZ --days 30
```

### 4. 财务数据同步
采集财务报表（利润表、资产负债表、现金流量表）和财务指标。

**特性**：
- 支持选择性数据采集
- 批量处理多只股票
- 自动去重和更新

**使用**：
```bash
# 同步指定股票的所有财务数据
python -m quant_data_engine.tasks.run_financial_sync --codes 000001.SZ 000002.SZ --start 20230101

# 只同步特定数据类型
python -m quant_data_engine.tasks.run_financial_sync --codes 000001.SZ --data-types income,balance

# 同步所有上市股票（测试模式，仅前10只）
python -m quant_data_engine.tasks.run_financial_sync --all --test

# 同步最近上市的股票
python -m quant_data_engine.tasks.run_financial_sync --recent-ipo 90
```

### 5. 资金流数据同步
采集全市场资金流向数据。

**特性**：
- 按日期批量采集
- 支持断点续传
- 自动记录同步进度

**使用**：
```bash
# 同步指定日期范围
python -m quant_data_engine.tasks.run_moneyflow_sync --start 20240101 --end 20240110

# 自动续传模式（从上次同步位置继续）
python -m quant_data_engine.tasks.run_moneyflow_sync --auto

# 同步最近 5 个交易日
python -m quant_data_engine.tasks.run_moneyflow_sync --days 5
```

### 6. 实时数据采集（Windows Bridge）
实时订阅 miniQMT tick 数据并聚合为分钟线。

**特性**：
- 实时 tick 数据订阅
- 自动聚合为分钟 Bar
- 交易时间调度
- 自动从数据库加载股票列表

**使用**：
```bash
# 自动模式（从 watchlist_strategies 加载股票）
python -m quant_data_engine.tasks.run_realtime_bridge --auto

# 手动指定股票
python -m quant_data_engine.tasks.run_realtime_bridge --symbols 002050.SZ 600618.SH

# 双写同步数据到远程云服务器
python -m quant_data_engine.tasks.run_realtime_bridge --auto --sync-uri "mongodb://admin:pwd@remote-ip:27017"

# Debug 模式
python -m quant_data_engine.tasks.run_realtime_bridge --auto --debug
```

### 7. 分钟级历史下载（miniQMT）
使用 miniQMT 下载分钟级历史数据并写入 minute_bars/minute_bars_external。

**使用**：
```bash
# 指定代码
python -m quant_data_engine.tasks.download_minute_data --symbols 300722.SZ 000001.SZ --start 20250101 --end 20250105 --period 1m

# 全市场 A 股（stock_info 提取）批量模式
python -m quant_data_engine.tasks.download_minute_data --all-a-shares --period 1m --batch
```

### 8. 市场情绪分析（阳谱/阴谱）
基于日线或分钟线计算市场情绪，支持实时（日线 MA + 分钟收盘）。

**使用**：
```bash
# 日线区间
python -m quant_data_engine.tasks.analyze_market_sentiment --granularity daily --start 20251201 --end 20251215 --ma-period 5

# 分钟线最新 60 分钟
python -m quant_data_engine.tasks.analyze_market_sentiment --granularity minute --latest 60 --ma-period 5

# 日线 MA + 分钟收盘实时视角
python -m quant_data_engine.tasks.analyze_market_sentiment --granularity daily_realtime --latest 120 --ma-period 5 --collection minute_bars_external
```

### 9. 全市场分钟连续同步
批量轮询全市场分钟数据（miniQMT），可控制批大小、目标延迟，并可选实时情绪分析。

**使用**：
```bash
# 默认全市场，批大小 400，目标延迟 300s
python -m quant_data_engine.tasks.continuous_market_minute_sync --batch-size 400 --target-latency 300 --count 10

# 指定代码文件并启用情绪分析
python -m quant_data_engine.tasks.continuous_market_minute_sync --symbols-file stocks.txt --analyze-sentiment --sentiment-ma-period 5

# 仅查看批次规划（不下载）
python -m quant_data_engine.tasks.continuous_market_minute_sync --dry-run
```

## 数据工具

### 符号标准化工具
用于将数据库中的股票代码统一添加交易所后缀。

**使用**：
```bash
cd quant_data_engine/tools
python update_symbol_with_suffix_generic.py
```

详见 [tools/README.md](tools/README.md)

## 迁移说明

如果你之前使用 `quantFinance/tools` 中的数据采集脚本，请参考 [MIGRATION.md](MIGRATION.md) 了解如何迁移到新架构。

**迁移的脚本**：
- `bulk_daily_collector.py` → `run_stock_daily.py`
- `bulk_daily_basic_sync.py` → `run_stock_daily.py`
- `sync_financial_data.py` → `run_financial_sync.py`
- `money_flow_collector.py` → `run_moneyflow_sync.py`
- `update_symbol_with_suffix_generic.py` → `tools/update_symbol_with_suffix_generic.py`

## 后续计划
- ✅ 股票日线数据采集
- ✅ 财务数据同步
- ✅ 资金流数据同步
- 增加异步抓取支持
- Prometheus 指标导出
- 数据质量监控
- 更多数据管线 (板块 / 因子)

## 实时数据采集 (Windows Bridge)

实时数据采集功能集成了原 stock-execution-system 中的 Windows Bridge，
采用统一的架构模式 (Adapter → Pipeline → Repository)。

**功能特性**：
- 实时订阅 miniQMT tick 数据
- 自动聚合为分钟 Bar
- 交易时间调度 (9:30-15:00)
- 自动从数据库加载股票列表
- 统一存储到 MongoDB minute_bars

**使用示例**：
```bash
# 自动模式（从 watchlist_strategies 加载股票）
python -m quant_data_engine.tasks.run_realtime_bridge --auto

# 手动指定股票
python -m quant_data_engine.tasks.run_realtime_bridge --symbols 002050.SZ 600618.SH

# Debug 模式
python -m quant_data_engine.tasks.run_realtime_bridge --auto --debug

# 24/7 运行（禁用交易时间限制）
python -m quant_data_engine.tasks.run_realtime_bridge --auto --no-schedule
```
