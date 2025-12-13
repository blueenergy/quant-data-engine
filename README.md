# quant-data-engine

统一的量化数据采集 / 处理 / 质量监控引擎 (Plan B 重构)。

## 目录结构
```
config/         # 全局配置与日志
adapters/       # 外部数据源适配 (Tushare 等)
repositories/   # 数据持久化与读取抽象
pipelines/      # 端到端数据管线 (采集→转换→写库)
models/         # 数据结构契约 (Pydantic/TypedDict)
quality/        # 验证与监控组件
tasks/          # 可调度任务入口 (CLI / cron / systemd)
utils/          # 通用工具函数
tests/          # 自动化测试
```

## 快速开始
```bash
# 安装 (开发模式)
pip install -e ./quant-data-engine

# 运行指数日线采集
python -m quant_data_engine.tasks.run_index_daily --codes 000300.SH 000905.SH --backfill-days 3

# 运行实时数据采集 (Windows Bridge)
python -m quant_data_engine.tasks.run_realtime_bridge --auto
```

## 后续计划
- 增加速率限制与重试策略
- 增加异步抓取支持
- Prometheus 指标导出
- 更多数据管线 (财务 / 板块 / 因子)

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
