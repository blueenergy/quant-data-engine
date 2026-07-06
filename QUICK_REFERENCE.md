# 数据采集快速参考

## 常用命令速查

### 股票日线数据（OHLCV + 基础指标）

```bash
# 最近 5 个交易日（推荐）
python -m quant_data_engine.tasks.run_stock_daily --mode by-date --days 5

# 指定日期范围
python -m quant_data_engine.tasks.run_stock_daily --mode by-date --start 20240101 --end 20240110

# 单只股票最近 30 天
python -m quant_data_engine.tasks.run_stock_daily --mode by-stock --codes 000001.SZ --days 30

# 预览模式（不写入）
python -m quant_data_engine.tasks.run_stock_daily --mode by-date --days 1 --dry-run
```

### 股票复权因子（adj_factor）

```bash
# 最近 5 个交易日（Tushare adj_factor -> Mongo stock_adj_factor）
python -m quant_data_engine.tasks.run_stock_adj_factor_sync --mode by-date --days 5

# 2016 至今指定股票回填
python -m quant_data_engine.tasks.run_stock_adj_factor_sync \
  --mode by-stock \
  --codes 000001.SZ \
  --start 20160101

# 预览模式（不写入）
python -m quant_data_engine.tasks.run_stock_adj_factor_sync --mode by-date --days 1 --dry-run
```

### volume_price 原始价重刷（P5.2 迁移）

将 `volume_price` 历史段统一为 Tushare `pro.daily` 原始不复权价（默认 2016→今，仅 OHLCV）：

```bash
# 预览全量范围
python -m quant_data_engine.tasks.run_volume_price_backfill --dry-run

# 执行重刷（按交易日逐日写入并断点）
python -m quant_data_engine.tasks.run_volume_price_backfill --start 20160101

# 中断后续跑
python -m quant_data_engine.tasks.run_volume_price_backfill --resume

# 校验负价与 update_time 类型
python -m quant_data_engine.tasks.run_volume_price_validate --start 20160101
```

### 财务数据

```bash
# 同步单只股票全部财务数据
python -m quant_data_engine.tasks.run_financial_sync --codes 000001.SZ --start 20230101

# 只同步利润表和资产负债表
python -m quant_data_engine.tasks.run_financial_sync \
  --codes 000001.SZ \
  --data-types income,balance \
  --start 20230101

# 同步多只股票
python -m quant_data_engine.tasks.run_financial_sync \
  --codes 000001.SZ 000002.SZ 600519.SH \
  --start 20230101

# 同步所有股票（测试模式，仅前 10 只）
python -m quant_data_engine.tasks.run_financial_sync --all --test

# 同步最近 90 天上市的股票
python -m quant_data_engine.tasks.run_financial_sync --recent-ipo 90
```

### X（Twitter）大 V 推文（API v2）

写入 `quant_data.x_influencer_posts`，新帖入队 `finance.analysis_tasks`（`x_influencer_post_analysis`，由 quantAnalyzer 写 `quant_analyzer.x_influencer_post_analysis`）。需 `TWITTER_BEARER_TOKEN`；建议可访问 `api.twitter.com`。Docker 内 **`start.sh` 常驻循环默认关闭**，避免无 API credits 时反复失败；有权限后设 **`ENABLE_X_INFLUENCER_SYNC=1`** 再重启 `quant-data-engine`。

```bash
python -m quant_data_engine.tasks.run_x_influencer_sync
python -m quant_data_engine.tasks.run_x_influencer_sync --loop --interval 180
```

### 资金流数据

```bash
# 自动续传（从上次位置继续）
python -m quant_data_engine.tasks.run_moneyflow_sync --auto

# 最近 5 个交易日
python -m quant_data_engine.tasks.run_moneyflow_sync --days 5

# 指定日期范围
python -m quant_data_engine.tasks.run_moneyflow_sync --start 20240101 --end 20240110

# 预览模式
python -m quant_data_engine.tasks.run_moneyflow_sync --days 1 --dry-run
```

### 指数数据

```bash
# 核心指数
python -m quant_data_engine.tasks.run_index_daily --core --backfill-days 3

# 指定指数
python -m quant_data_engine.tasks.run_index_daily \
  --codes 000300.SH 000905.SH \
  --backfill-days 3

# 核心指数成分与权重（Tushare index_weight -> Mongo index_constituents）
# 默认 7 个（含中证 A500 / a500、中证2000 / csi2000）；覆盖或合并见 env INDEX_CONSTITUENTS_PROFILES
# 若设置 INDEX_CONSTITUENTS_PROFILES（JSON 数组），默认**整表替换**；漏写 a500 则不会同步 A500。
# 需要「自定义 + 补全默认」时设 INDEX_CONSTITUENTS_PROFILES_MERGE_DEFAULTS=1
python -m quant_data_engine.tasks.run_index_constituents_sync
python -m quant_data_engine.tasks.run_index_constituents_sync --dry-run
```

### 东方财富概念（dc_index / dc_member）

不再落库 ``dc_concept_index`` / ``dc_concept_member``。``run_theme_lag_candidates`` 在运行时会直接调 Tushare；缺数据即退出。

旧命令 ``python -m quant_data_engine.tasks.run_dc_concept_sync`` 已废弃（退出码 2）。

### 主题补涨候选（theme_lag_candidates）

```bash
python -m quant_data_engine.tasks.run_theme_lag_candidates --trade-date 20250425
python -m quant_data_engine.tasks.run_theme_lag_candidates --dry-run
# 默认：0 < pct_chg < 3（百分点），主线 Top 5，每桶最多 3 只；见 --help
# 环境变量：THEME_CANDIDATE_MIN_PCT_CHG、THEME_CANDIDATE_MAX_PCT_CHG、THEME_MAINLINE_TOP_K、THEME_PER_BUCKET_MAX
# 概念板块/成分在任务内在线 Tushare（需 TUSHARE_TOKEN）；无精确 trade_date 或接口无数据则失败退出
# 默认成功后入队 finance.analysis_tasks（type=theme_lag_narrative）；--skip-narrative-task 跳过
./daily_market_sync.sh theme_lag
```

### 主题补涨候选验证（theme_lag_candidate_validation）

规格见 [docs/THEME_LAG_CANDIDATE_VALIDATION_SPEC.md](docs/THEME_LAG_CANDIDATE_VALIDATION_SPEC.md)。结果写入 `quant_data.theme_lag_candidate_validation`。就绪门闩：优先看 `finance.pipeline_tasks` 中 `theme_lag_candidates` 与 `stock_daily` 在锚定日为 `complete`；若缺失但 `quant_data.theme_lag_candidates` / `volume_price` 已有对应数据则仍允许运行（打 `readiness fallback` 日志）。`--no-pipeline-check` 跳过检查。

```bash
python -m quant_data_engine.tasks.run_theme_lag_candidate_validation --trade-date 20250425 --index-store-key csi1000
python -m quant_data_engine.tasks.run_theme_lag_candidate_validation --trade-date 20250425 --index-store-key csi1000 --dry-run
# --skip-empty：无候选时不写库（§3.1 策略 B）
```

### ETF 数据

```bash
# 指定 ETF
python -m quant_data_engine.tasks.run_etf_daily \
  --codes 510300.SH 159915.SZ \
  --backfill-days 3
```

### 分钟历史下载（miniQMT）

```bash
# 指定代码与区间
python -m quant_data_engine.tasks.download_minute_data \
  --symbols 300722.SZ 000001.SZ \
  --start 20250101 --end 20250105 \
  --period 1m

# 全市场 A 股 + 批量模式
python -m quant_data_engine.tasks.download_minute_data --all-a-shares --period 1m --batch
```

### 市场情绪分析

```bash
# 日线区间
python -m quant_data_engine.tasks.analyze_market_sentiment \
  --granularity daily \
  --start 20251201 --end 20251215 \
  --ma-period 5

# 分钟最新 N 分钟
python -m quant_data_engine.tasks.analyze_market_sentiment \
  --granularity minute \
  --latest 60 \
  --ma-period 5

# 日线 MA + 分钟收盘实时视角
python -m quant_data_engine.tasks.analyze_market_sentiment \
  --granularity daily_realtime \
  --latest 120 \
  --ma-period 5 \
  --collection minute_bars_external
```

### 全市场分钟连续同步

```bash
# 默认全市场，批 400，目标延迟 300s
python -m quant_data_engine.tasks.continuous_market_minute_sync \
  --batch-size 400 \
  --target-latency 300 \
  --count 10

# 指定代码文件 + 情绪分析
python -m quant_data_engine.tasks.continuous_market_minute_sync \
  --symbols-file stocks.txt \
  --analyze-sentiment \
  --sentiment-ma-period 5

# 仅查看批次规划
python -m quant_data_engine.tasks.continuous_market_minute_sync --dry-run
```

## 配置文件 (.env)

```bash
# MongoDB 配置
MONGO_URI=mongodb://localhost:27017
MONGO_DB=finance

# Tushare 配置
TUSHARE_TOKEN=your_token_here

# 速率限制（次/秒）
RATE_LIMIT_PER_SEC=4
```

## 数据类型说明

### 财务数据类型
- `income` - 利润表
- `balance` - 资产负债表
- `cashflow` - 现金流量表
- `financial_indicator` - 财务指标

### 采集模式
- `by-date` - 按日期批量采集（推荐，效率高 10 倍）
- `by-stock` - 按股票逐个采集（适合少量股票）

## 数据库集合

| 集合名称 | 数据内容 |
|---------|---------|
| `volume_price` | 股票日线 OHLCV |
| `financial_daily_basic` | 日线基础指标（PE、PB等）|
| `financial_income` | 利润表 |
| `financial_balance` | 资产负债表 |
| `financial_cashflow` | 现金流量表 |
| `financial_indicator` | 财务指标 |
| `money_flow` | 资金流数据 |
| `collect_state` | 采集状态记录 |

## Cron 定时任务示例

```bash
# 每天收盘后 16:00 采集日线数据
0 16 * * 1-5 cd /path/to/quant_data_engine && python -m quant_data_engine.tasks.run_stock_daily --mode by-date --days 1

# 每天收盘后 16：15 同步资金流数据
15 16 * * 1-5 cd /path/to/quant_data_engine && python -m quant_data_engine.tasks.run_moneyflow_sync --auto

# 每季度第一天 02:00 更新财务数据
0 2 1 1,4,7,10 * cd /path/to/quant_data_engine && python -m quant_data_engine.tasks.run_financial_sync --all

# 每天 16:30 更新指数数据
30 16 * * 1-5 cd /path/to/quant_data_engine && python -m quant_data_engine.tasks.run_index_daily --core --backfill-days 1
```

## 常见问题

### Q: 如何检查上次同步时间？
```python
from pymongo import MongoClient
client = MongoClient('mongodb://localhost:27017')
db = client['finance']

# 查看最新的日线数据日期
latest = db['volume_price'].find_one(sort=[('trade_date', -1)])
print(f"最新日线数据: {latest['trade_date']}")

# 查看资金流同步状态
state = db['collect_state'].find_one({'_id': 'money_flow_last_date'})
print(f"资金流上次同步: {state['last_date']}")
```

### Q: 如何验证数据完整性？
```bash
# 使用 dry-run 模式预览
python -m quant_data_engine.tasks.run_stock_daily --mode by-date --days 1 --dry-run

# 使用 verbose 模式查看详细日志
python -m quant_data_engine.tasks.run_stock_daily --mode by-date --days 1 --verbose
```

### Q: API 调用超限怎么办？
调整速率限制：
```bash
# 在 .env 文件中设置
RATE_LIMIT_PER_SEC=2  # 降低到 2 次/秒
```

### Q: 如何同步历史数据？
```bash
# 日线数据：按日期批量，效率高
python -m quant_data_engine.tasks.run_stock_daily \
  --mode by-date \
  --start 20230101 \
  --end 20231231

# 财务数据：指定起始日期
python -m quant_data_engine.tasks.run_financial_sync \
  --all \
  --start 20200101
```

## 工具脚本

### 符号标准化
```bash
cd quant_data_engine/tools
python update_symbol_with_suffix_generic.py
```

## 帮助信息

```bash
# 查看命令帮助
python -m quant_data_engine.tasks.run_stock_daily --help
python -m quant_data_engine.tasks.run_financial_sync --help
python -m quant_data_engine.tasks.run_moneyflow_sync --help
```

## 监控与 job_status

定时任务（如 `daily_market_sync.sh` 调用的各 `python -m quant_data_engine.tasks.*`）在运行结束时会向 **`finance` 库的 `job_status` 集合** 写入摘要（`job_type`、`date`、`status`、`records_changed`、`completed_at` 等）。`job_type` 命名与共享库 catalog [`data-access-lib/src/stock_data_access/catalogs/data_pipelines_catalog.yaml`](data-access-lib/src/stock_data_access/catalogs/data_pipelines_catalog.yaml) 中的 `job_status_key` 一致。运维面板：quantFinance Admin「数据采集」、API `GET /api/admin/data-monitoring/overview`、`GET /api/data-pulse/overview`。

**双写监控库（海外采集 → 云上 Dashboard）**：若主 `MONGO_URI` 为本地/海外，而 Dashboard 连云上 `finance`，可设置环境变量 **`JOB_STATUS_SYNC_URI`**（云上 Mongo 连接串），`job_status` 会在主库写入后再 upsert 一份到该 URI（库名仍用 `MONGO_DB`，默认 `finance`）。未设置时若已配置 **`SECONDARY_MONGO_URI`**（与国际新闻镜像一致），会自动用作 `job_status` 镜像目标。

**常驻 loop 任务**（`start.sh` 中的连板天梯、行业同步）：每次成功同步后会 `record_ingestion_run`（按当日 `YYYYMMDD` 覆盖同一文档）；失败时写 `status=failed`。国际新闻 loop 本身已在 `run_once` 内写 `job_status`。

**综合评分**（`job_type` 与 `data_pipelines_catalog` 的 `job_status_key` 均为 `stock_composite_scoring`）：由 **stock-scoring-system** 的评分任务在跑批结束时写入同形的 `finance.job_status` 与 `pipeline_tasks`（`domain=data_collection`）；**不是** 本仓 quant-data-engine 的采集任务。

## 相关文档

- [MIGRATION.md](MIGRATION.md) - 完整迁移指南
- [README.md](README.md) - 项目概览
- [MIGRATION_SUMMARY.md](MIGRATION_SUMMARY.md) - 迁移总结
- [tools/README.md](tools/README.md) - 工具说明

---

**最后更新**: 2026-01-03
