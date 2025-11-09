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
```

## 后续计划
- 增加速率限制与重试策略
- 增加异步抓取支持
- Prometheus 指标导出
- 更多数据管线 (财务 / 板块 / 因子)
