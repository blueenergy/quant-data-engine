# 股票数据采集脚本迁移文档

## 迁移概述

本文档记录了将 `quantFinance/tools` 中的数据采集脚本迁移到 `quant_data_engine` 的过程。

**迁移日期**: 2026-01-03  
**目标**: 将独立的数据采集脚本统一到 `quant_data_engine` 架构下，实现更好的代码复用和维护性

## 原始文件列表

以下文件从 `quantFinance/tools` 迁移到 `quant_data_engine`：

1. **bulk_daily_basic_sync.py** - 批量同步股票日线基础指标（PE、PB等）
2. **bulk_daily_collector.py** - 批量同步股票日线 OHLCV 数据
3. **money_flow_collector.py** - 资金流向数据采集
4. **volume_price_collector.py** - 量价数据采集器（已弃用，功能被 bulk_daily_collector 覆盖）
5. **sync_financial_data.py** - 财务数据同步（利润表、资产负债表、现金流量表、财务指标）
6. **update_symbol_with_suffix_generic.py** - 符号标准化工具

## 新架构设计

迁移后的代码遵循 `quant_data_engine` 的三层架构：

```
quant_data_engine/
├── adapters/           # 数据源适配层
│   └── stock_adapter.py          # 新增：股票数据 Tushare API 封装
├── repositories/       # 数据持久化层
│   ├── stock_repository.py       # 新增：股票日线数据存储
│   ├── financial_repository.py   # 新增：财务数据存储
│   └── moneyflow_repository.py   # 新增：资金流数据存储
├── pipelines/          # 数据管道层
│   ├── stock_daily_pipeline.py   # 新增：股票日线数据管道
│   ├── financial_pipeline.py     # 新增：财务数据管道
│   └── moneyflow_pipeline.py     # 新增：资金流数据管道
├── tasks/              # 任务入口层
│   ├── run_stock_daily.py        # 新增：股票日线采集任务
│   ├── run_financial_sync.py     # 新增：财务数据同步任务
│   └── run_moneyflow_sync.py     # 新增：资金流同步任务
└── tools/              # 工具脚本
    └── update_symbol_with_suffix_generic.py  # 迁移：符号标准化工具
```

### 核心组件说明

#### 1. StockAdapter (adapters/stock_adapter.py)

统一封装所有股票相关的 Tushare API 调用：

**功能**：
- 日线 OHLCV 数据获取 (`fetch_daily`)
- 日线基础指标获取 (`fetch_daily_basic`)
- 资金流数据获取 (`fetch_moneyflow`)
- 财务报表获取（利润表、资产负债表、现金流量表）
- 财务指标获取 (`fetch_fina_indicator`)
- 股票列表和交易日历获取

**特性**：
- 内置速率限制（默认 4 次/秒）
- 自动重试机制（默认 2 次）
- 统一的错误处理

**示例**：
```python
from quant_data_engine.adapters.stock_adapter import StockAdapter

adapter = StockAdapter(token="your_token", rate_limit_per_sec=4.0)

# 获取单只股票日线数据
df = adapter.fetch_daily(ts_code="000001.SZ", start_date="20240101", end_date="20240110")

# 获取某日全市场数据
df = adapter.fetch_daily(trade_date="20240110")

# 获取财务数据
df_income = adapter.fetch_income(ts_code="000001.SZ", start_date="20230101")
```

#### 2. StockRepository (repositories/stock_repository.py)

处理股票日线数据的持久化：

**集合映射**：
- `volume_price` - 存储日线 OHLCV 数据
- `financial_daily_basic` - 存储日线基础指标

**主要方法**：
- `upsert_daily()` - 批量写入/更新 OHLCV 数据
- `upsert_daily_basic()` - 批量写入/更新基础指标
- `get_latest_trade_date()` - 查询最新交易日期
- `get_stock_data()` - 查询股票历史数据

**特性**：
- 自动创建索引
- 使用 bulk_write 提升性能
- 自动处理 NaN 值
- 添加数据源和时间戳元信息

#### 3. FinancialRepository (repositories/financial_repository.py)

处理财务数据的持久化：

**集合映射**：
- `financial_income` - 利润表
- `financial_balance` - 资产负债表
- `financial_cashflow` - 现金流量表
- `financial_indicator` - 财务指标

**主要方法**：
- `upsert_income()` / `upsert_balance()` / `upsert_cashflow()` / `upsert_indicators()`
- 支持 DataFrame 或 records 列表两种输入格式
- 提供查询方法获取历史财务数据

#### 4. MoneyFlowRepository (repositories/moneyflow_repository.py)

处理资金流数据的持久化：

**集合映射**：
- `money_flow` - 资金流数据
- `collect_state` - 采集状态记录

**主要方法**：
- `upsert_moneyflow()` - 批量写入资金流数据
- `update_last_sync_date()` / `get_last_sync_date()` - 管理同步进度
- `get_moneyflow_data()` - 查询资金流数据

#### 5. Pipelines

**StockDailyPipeline** (pipelines/stock_daily_pipeline.py)
- 支持两种模式：`run_by_stock()` 和 `run_by_date()`
- `by-date` 模式更高效，适合批量采集
- 自动处理数据转换和验证

**FinancialPipeline** (pipelines/financial_pipeline.py)
- 支持选择性数据采集（只采集需要的报表类型）
- 统一处理四类财务数据
- 返回详细的执行结果统计

**MoneyFlowPipeline** (pipelines/moneyflow_pipeline.py)
- 支持按日期批量采集（推荐）
- 支持按股票逐个采集（备选）
- 自动管理同步进度状态

## 使用示例

### 1. 股票日线数据采集

```bash
# 按日期批量采集（推荐，效率高）
python -m quant_data_engine.tasks.run_stock_daily \
  --mode by-date \
  --start 20240101 \
  --end 20240110

# 自动同步最近 5 个交易日
python -m quant_data_engine.tasks.run_stock_daily \
  --mode by-date \
  --days 5

# 按股票采集（适合少量股票）
python -m quant_data_engine.tasks.run_stock_daily \
  --mode by-stock \
  --codes 000001.SZ 000002.SZ \
  --start 20240101 \
  --end 20240110

# 采集所有上市股票
python -m quant_data_engine.tasks.run_stock_daily \
  --mode by-stock \
  --all \
  --start 20240101 \
  --end 20240110
```

**对应原脚本**：
- 替代 `bulk_daily_collector.py`（日线 OHLCV）
- 替代 `bulk_daily_basic_sync.py`（日线基础指标）
- 替代 `volume_price_collector.py`（已弃用）

### 2. 财务数据同步

```bash
# 同步指定股票的财务数据
python -m quant_data_engine.tasks.run_financial_sync \
  --codes 000001.SZ 000002.SZ \
  --start 20230101

# 同步所有上市股票（测试模式）
python -m quant_data_engine.tasks.run_financial_sync \
  --all \
  --test

# 只同步特定数据类型
python -m quant_data_engine.tasks.run_financial_sync \
  --codes 000001.SZ \
  --data-types income,balance

# 同步最近上市的股票
python -m quant_data_engine.tasks.run_financial_sync \
  --recent-ipo 90 \
  --start 20230101
```

**对应原脚本**：
- 替代 `sync_financial_data.py`

### 3. 资金流数据同步

```bash
# 同步指定日期范围
python -m quant_data_engine.tasks.run_moneyflow_sync \
  --start 20240101 \
  --end 20240110

# 自动续传模式（从上次同步位置继续）
python -m quant_data_engine.tasks.run_moneyflow_sync --auto

# 同步最近 5 个交易日
python -m quant_data_engine.tasks.run_moneyflow_sync --days 5
```

**对应原脚本**：
- 替代 `money_flow_collector.py`

### 4. 符号标准化工具

```bash
# 进入工具目录
cd quant_data_engine/tools

# 运行符号标准化工具
python update_symbol_with_suffix_generic.py
```

**功能**：
- 将数据库中的股票代码统一添加交易所后缀
- 支持单个字段和数组字段
- 交互式操作，安全可控

## 架构优势

### 相比原脚本的改进

1. **代码复用**
   - 所有 Tushare API 调用集中在 `StockAdapter`
   - 数据库操作封装在 Repository 层
   - 避免重复代码

2. **职责分离**
   - Adapter：只负责数据获取
   - Repository：只负责数据存储
   - Pipeline：负责业务流程编排
   - Task：提供命令行接口

3. **易于测试**
   - 各层独立，可单独测试
   - Mock 友好
   - 清晰的接口定义

4. **易于扩展**
   - 新增数据源：添加新 Adapter
   - 新增存储：添加新 Repository
   - 新增业务逻辑：添加新 Pipeline

5. **统一配置**
   - 通过 `settings.py` 统一管理配置
   - 环境变量驱动
   - 无需修改代码

6. **错误处理**
   - 统一的重试机制
   - 详细的错误日志
   - 优雅的降级策略

## 迁移对照表

| 原脚本 | 新任务脚本 | 功能说明 |
|--------|-----------|---------|
| `bulk_daily_collector.py` | `run_stock_daily.py --mode by-date` | 按日期批量采集日线数据 |
| `bulk_daily_basic_sync.py` | `run_stock_daily.py --mode by-date` | 日线基础指标（已集成） |
| `volume_price_collector.py` | `run_stock_daily.py --mode by-stock` | 按股票采集（已弃用） |
| `sync_financial_data.py` | `run_financial_sync.py` | 财务数据同步 |
| `money_flow_collector.py` | `run_moneyflow_sync.py` | 资金流数据同步 |
| `update_symbol_with_suffix_generic.py` | `tools/update_symbol_with_suffix_generic.py` | 符号标准化工具（直接迁移） |

## 数据库集合映射

| 集合名称 | 数据内容 | Repository | 说明 |
|---------|---------|-----------|------|
| `volume_price` | 股票日线 OHLCV | StockRepository | 与原脚本一致 |
| `financial_daily_basic` | 日线基础指标 | StockRepository | 与原脚本一致 |
| `financial_income` | 利润表 | FinancialRepository | 与原脚本一致 |
| `financial_balance` | 资产负债表 | FinancialRepository | 与原脚本一致 |
| `financial_cashflow` | 现金流量表 | FinancialRepository | 与原脚本一致 |
| `financial_indicator` | 财务指标 | FinancialRepository | 与原脚本一致 |
| `money_flow` | 资金流数据 | MoneyFlowRepository | 与原脚本一致 |
| `collect_state` | 采集状态 | MoneyFlowRepository | 与原脚本一致 |

## 配置说明

所有配置通过环境变量管理，在项目根目录创建 `.env` 文件：

```bash
# MongoDB 连接
MONGO_URI=mongodb://localhost:27017
MONGO_DB=finance

# Tushare Token
TUSHARE_TOKEN=your_tushare_token_here

# API 速率限制（次/秒）
RATE_LIMIT_PER_SEC=4
```

## 编程接口使用

除了命令行接口，也可以在 Python 代码中直接使用：

```python
from quant_data_engine import (
    create_stock_daily_pipeline,
    create_financial_pipeline,
    create_moneyflow_pipeline,
    get_settings
)

# 使用工厂函数创建 pipeline
pipeline = create_stock_daily_pipeline()

# 执行采集
result = pipeline.run_by_date(['20240110', '20240111'])

print(f"采集完成：{result['total_daily_records']} 条日线数据")
```

## 注意事项

1. **依赖安装**
   ```bash
   cd quant_data_engine
   pip install -e .
   ```

2. **Tushare Token**
   - 必须配置有效的 Tushare Token
   - 注意 API 调用次数限制
   - 建议使用付费账户以获得更高限额

3. **速率限制**
   - 默认 4 次/秒，可通过 `RATE_LIMIT_PER_SEC` 调整
   - 过高可能导致 API 封禁
   - 过低影响采集效率

4. **数据一致性**
   - 使用 upsert 操作，重复运行不会产生重复数据
   - 建议定期运行确保数据完整性

5. **错误恢复**
   - 资金流数据支持断点续传（`--auto` 参数）
   - 其他任务建议使用 `--dry-run` 先预览

## 后续规划

1. **性能优化**
   - 引入异步采集支持
   - 实现并发写入优化
   - 添加数据缓存机制

2. **功能增强**
   - 添加数据质量检查
   - 实现智能增量更新
   - 支持自定义字段选择

3. **监控告警**
   - Prometheus 指标导出
   - 采集失败告警
   - 数据质量监控

4. **文档完善**
   - API 参考文档
   - 故障排查指南
   - 最佳实践指南

## 反馈与支持

如有问题或建议，请通过以下方式反馈：
- 项目 Issues
- 代码 Review
- 团队沟通渠道

---

**文档版本**: 1.0  
**最后更新**: 2026-01-03  
**维护者**: quant_data_engine team
