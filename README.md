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
DATA_DB_NAME=quant_data
TUSHARE_TOKEN=your_token_here
RATE_LIMIT_PER_SEC=4

# 运行指数日线采集
python -m quant_data_engine.tasks.run_index_daily --codes 000300.SH 000905.SH --backfill-days 3

# 运行股票日线采集（新增）
python -m quant_data_engine.tasks.run_stock_daily --mode by-date --days 5

# 股票周线 / 月线（Tushare weekly / monthly → quant_data）
python -m quant_data_engine.tasks.run_stock_weekly --end $(date +%Y%m%d) --recent-weeks 1
python -m quant_data_engine.tasks.run_stock_monthly --end $(date +%Y%m%d) --recent-months 1

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

从 Tushare 采集全市场 ETF 数据，日线任务写入以下 MongoDB 集合（另见 §2b `etf_share_size`）：

| 集合 | 键 | 内容 |
|---|---|---|
| `etf_basic` | `ts_code` (唯一) | ETF 基本信息：名称、类型、成立日期、规模等 |
| `etf_daily` | `(ts_code, trade_date)` (唯一) | 日线 OHLCV 数据（开高低收成交量成交额） |
| `etf_adj_factor` | `(ts_code, trade_date)` (唯一) | 复权因子，用于计算前复权价格 |
| `etf_shares` | `(ts_code, trade_date)` (唯一) | 基金份额（`fund_share` / 万份 `fd_share`） |

所有写入均使用 `upsert=True`，**完全幂等，多次运行不产生重复数据**。

**使用方式**：

```bash
cd quant-data-engine

# 【推荐】全量 ETF，只同步今天（每日定时任务）
python -m quant_data_engine.tasks.run_etf_daily --all \
    --start $(date +%Y%m%d) --end $(date +%Y%m%d)

# 首次历史回填（只需跑一次，会花较长时间）
python -m quant_data_engine.tasks.run_etf_daily --all \
    --start 20250101 --end $(date +%Y%m%d)

# 指定白名单 ETF（少量）
python -m quant_data_engine.tasks.run_etf_daily --codes 510300.SH 159919.SZ

# 核心行业 ETF 白名单（11只）
python -m quant_data_engine.tasks.run_etf_daily --core

# 扩展白名单（21只）
python -m quant_data_engine.tasks.run_etf_daily --extended
```

**`run_etf_daily --all` 说明**（与 `etf_share_size --all` 同一思路）：
- **不传 `ts_code`**：在 `start`～`end` 的每个自然日依次调用 **`fund_daily` / `fund_adj` / `fund_share`** 的 **`trade_date=当日`**，一次写入**当日全市场 ETF** 日线、复权、份额；`fund_adj`/`fund_share` 单次 2000 行，脚本内已 **offset 分页**。
- **`etf_basic`**：仍用一次 **`fund_basic(market=E, status=L)`** 全量 upsert（场内、在市基金列表即 ETF 基础信息来源）。
- **指数日线**（`run_index_daily` 等）仍多为**显式指数 ts_code 列表**，与上述 ETF 接口「按日全市场」习惯不一定相同；若要对齐需单独改指数任务。

### 2b. ETF 份额规模（etf_share_size）

Tushare 接口 `etf_share_size`，写入集合 **`etf_share_size`**。与 `fund_share` 写入的 `etf_shares` 并存，便于对比规模字段（`total_size`）及后续按规模排序。

```bash
cd quant-data-engine

# 每日增量（与 cron 一致：全市场 + 当天）
python -m quant_data_engine.tasks.run_etf_share_size --all \
    --start $(date +%Y%m%d) --end $(date +%Y%m%d)

# 历史回填（耗时长、注意积分与限频）
python -m quant_data_engine.tasks.run_etf_share_size --all \
    --start 20250101 --end $(date +%Y%m%d)

# 对比 etf_share_size 与 etf_shares（Mongo）
python tools/compare_etf_share_size_vs_fund_share.py --start 20250401 --end 20250418

# 可选：按区间 deleteMany / drop 集合后再全市场重拉（见 tools/rebuild_etf_share_size_range.py --help）
PYTHONPATH=src python tools/rebuild_etf_share_size_range.py \
    --start 20250401 --end 20250430 --drop range --dry-run
```

Shell 调度：`./daily_market_sync.sh etf_share_size`（建议 **19:05** 以后，见 `crontab.txt`）。

**说明**：

- **`run_etf_share_size --all`**：**不传 `ts_code`**，按 **`trade_date`** 在 `start`～`end` 每个自然日调用一次 **`etf_share_size`**，拉取**当日全市场 ETF**；**不需要**事先拉代码列表。
- **`fund_basic` 是什么**：Tushare 的**基金基础信息**接口；在 **`run_etf_daily --all`** 里只用于**一次**写入 **`etf_basic`**（`market=E, status=L`），**不再**用来驱动 `fund_daily` 的逐只循环。
- **`total_share` / `total_size` 等**：入库使用 **`Decimal128`**，减轻 `float64` 大数与网页展示不一致的问题；重新同步会覆盖旧文档。
- **`nav` / `close`**：Tushare 文档中均为**可选输出**，接口**没有**「强制返回某列」的参数；网页勾选列只是展示。若 SDK 返回的 DataFrame 里无该列，说明当次接口未带该字段（与积分/入库批次/数据源有关），可升级 `tushare` 版本后重试。

#### 公募基金持仓（fund_portfolio → `fund_holdings`）

Tushare `fund_portfolio`（季报/年报披露），写入 **外部数据标准库** `DATA_DB_NAME`（默认 `quant_data`）下的 `fund_holdings`，与 `etf_*` 等一致、**不写入** `MONGO_DB`（`finance`）。默认可按最近若干 **公告日 `ann_date`** 逐日拉取（适合每周定时任务）。需要 [Tushare 积分权限](https://tushare.pro/document/2?doc_id=121)。

```bash
# 默认：从今天往前 14 个日历日，逐日 ann_date 拉取
python -m quant_data_engine.tasks.run_fund_portfolio_sync

python -m quant_data_engine.tasks.run_fund_portfolio_sync --ann-days 10

# 仅同步单只基金（.OF）
python -m quant_data_engine.tasks.run_fund_portfolio_sync --ts-code 001753.OF
```

Shell 入口：`./daily_market_sync.sh fund_portfolio`（环境变量 `FUND_PORTFOLIO_ANN_DAYS` 覆盖默认 14）。镜像内默认 **每周日 19:00** 跑一次（`crontab*.txt`），可用 `CRON_FUND_PORTFOLIO` 覆盖时间。

### 2c. 申万行业指数（Tushare → `quant_data`）

从 Tushare 拉取申万行业分类、指数成分、行业指数日线，写入 **`DATA_DB_NAME` 所指的库**（`get_settings().data_db`，与 ETF/股票等原始行情落库一致；**默认** `quant_data`，与 `MONGO_DB=finance` 可并存为不同库）。

| 集合 | 说明 |
|------|------|
| `sw_index_classify` | `index_classify`（L1/L2/L3 行业/指数树），键含 `index_code` + `src` |
| `sw_index_member` | `index_member_all` 成分（按 L1 拉取，含 `con_code` 等），`src` 区分申万版本 |
| `sw_daily` | 申万行业指数**日线**（`trade_date` 为 YYYYMMDD 字符串） |
| `sw_industry_sync_meta` | 断点元数据，如 `sw_daily_backfill` 已写到哪一天，配合 `--resume` 续拉 |

**任务入口**：`python -m quant_data_engine.tasks.run_sw_industry_backfill`（实现见 `tasks/run_sw_industry_backfill.py`）。

**推荐顺序**（首次全量时）：

```bash
cd quant-data-engine
# 已 pip install -e . 时可直接 -m；否则: PYTHONPATH=src python -m quant_data_engine.tasks... 

# 1) 行业/指数分类（SW2021 或 SW2014）
python -m quant_data_engine.tasks.run_sw_industry_backfill --sync classify --src SW2021

# 2) 成分：按 L1 指数拉 index_member_all
python -m quant_data_engine.tasks.run_sw_industry_backfill --sync members --src SW2021

# 3) 日线：按**交易日**逐日拉全市场行业指数 K 线（历史区间可改，耗时长、注意限频/积分）
python -m quant_data_engine.tasks.run_sw_industry_backfill --sync daily --start 20160425 --end $(date +%Y%m%d)
```

- **`--sync all`**：依实现依次执行 `classify` + `members` + `daily`（见脚本中 `choices`）。
- **`--resume`**：从 `sw_industry_sync_meta` 里记录的 `sw_daily` 下一天起继续拉 `daily`。
- **`--dry-run`**：只打印/计数，不落库（实现以脚本为准）。

**与前端/ API 的关系**：`quantFinance` 的申万行业指数相关路由（如 `shenwan_index`）在**同一份行情库**中**只读** `sw_index_classify`、`sw_daily`；无数据时错误提示会指向本任务。机构深度管线里 L1 行业超额等逻辑依赖上述集合已回补。

### 2d. 东方财富概念板块（Tushare `dc_index` / `dc_member` → `quant_data`）

一期**不落库** `dc_daily`；板块涨跌幅等以 **`dc_index`** 返回字段为准。写入 **`DATA_DB_NAME`**（默认 `quant_data`），与申万/ETF 一致。

| 集合 | 说明 |
|------|------|
| `dc_concept_index` | `dc_index` 截面；唯一键 `(ts_code, trade_date)`；含 `sync_time`、`source` |
| `dc_concept_member` | `dc_member` 成分；唯一键 `(concept_ts_code, as_of_date, con_code)`，便于保留历史版本 |
| `dc_concept_sync_meta` | 元数据，如 `index_last_trade_date`、`members_last_as_of` |

**任务入口**：`python -m quant_data_engine.tasks.run_dc_concept_sync`（`tasks/run_dc_concept_sync.py`）。

```bash
cd quant-data-engine

# 日更：板块列表（推荐每个交易日盘后）
python -m quant_data_engine.tasks.run_dc_concept_sync --sync index

# 周更：全市场概念成分（调用次数多，注意积分与限频；可先 --max-concepts 20 试跑）
python -m quant_data_engine.tasks.run_dc_concept_sync --sync members

# 一次拉 index 再拉 members（同进程内 members 可用刚拉到的概念列表，含 --dry-run）
python -m quant_data_engine.tasks.run_dc_concept_sync --sync all --dry-run

# 指定 index 业务日 / 成分版本戳
python -m quant_data_engine.tasks.run_dc_concept_sync --sync index --trade-date 20250425
python -m quant_data_engine.tasks.run_dc_concept_sync --sync members --trade-date 20250425 --as-of-date 20250427
```

- **`--trade-date`**：未传时取最近 A 股开市日（`trade_cal`）；`dc_index` 若不含该日则回退到数据源中的最新 `trade_date` 并打日志。
- **`--dry-run`**：不写 Mongo、不写 `job_status`；`--sync all` 时仍会对 Tushare 拉数以便打印规模。
- **Shell**：`./daily_market_sync.sh dc_concept_index`（日）、`./daily_market_sync.sh dc_concept_members`（周）。示例 cron 见 `crontab.txt`（`dc_concept_index` 17:55、`dc_concept_members` 周日 04:00）。

**排查**：无数据时检查 `TUSHARE_TOKEN`、`dc_index`/`dc_member` 积分权限；成员任务依赖当日或历史上已有 `dc_concept_index`（`--sync members` 单独跑时从库读概念列表）。

### 2e. 主题补涨候选（`theme_lag_candidates`）

在 **`quant_data`** 中写入 **`theme_lag_candidates`**：从 **`limit_up_ladder.sector_ranking`** 取主线，将 `sector` 名称与 **`dc_concept_index.name`** 对齐（精确或唯一模糊匹配），用 **`dc_concept_member`** 与 **`index_constituents`**（默认 `csi1000`）求交，剔除当日 **`daily_limit_up`** 涨停股，按 **`volume_price.pct_chg`** **严格小于**可配置阈值（默认 3，百分点）筛选，每桶最多 3 只（可配）。

**任务入口**：`python -m quant_data_engine.tasks.run_theme_lag_candidates`（`tasks/run_theme_lag_candidates.py`）。

```bash
python -m quant_data_engine.tasks.run_theme_lag_candidates --trade-date 20250425
python -m quant_data_engine.tasks.run_theme_lag_candidates --index-store-key csi1000 --dry-run
# 环境变量：THEME_CANDIDATE_MAX_PCT_CHG、THEME_MAINLINE_TOP_K、THEME_PER_BUCKET_MAX
```

**调度**：`./daily_market_sync.sh theme_lag`；建议晚于日线、`dc_concept_index`、连板天梯与 `volume_price`（示例见 `crontab.txt` 22:50）。

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

### 3a. 股票周线 / 月线（Tushare `weekly` / `monthly`）

写入 **`quant_data.stock_weekly`**、**`quant_data.stock_monthly`**，与日线 `volume_price` 分离；`trade_date` 为 Tushare 定义的**周线 / 月线结束交易日**（周线为当周最后交易日，月线为当月最后交易日）。全市场按 `trade_date` 拉取时单次最多 6000 行，任务内已 **offset 分页**。

**权限**：Tushare 文档要求约 **2000 积分** 以上。

**使用**：
```bash
# 指定某一周线结束日（全市场）
python -m quant_data_engine.tasks.run_stock_weekly --trade-date 20250411

# 锚定日期前最近 N 个完整 ISO 周（常用于定时任务）
python -m quant_data_engine.tasks.run_stock_weekly --end 20250418 --recent-weeks 2

# 单只股票区间回补
python -m quant_data_engine.tasks.run_stock_weekly --ts-code 000001.SZ --start 20200101 --end-range 20250418

# 月线：上一自然月（锚定日所在月之前的已完成月份）
python -m quant_data_engine.tasks.run_stock_monthly --end 20250401 --recent-months 1

# 指定某一月线结束日（全市场）
python -m quant_data_engine.tasks.run_stock_monthly --trade-date 20250331
```

Shell：`./daily_market_sync.sh stock_weekly` / `stock_monthly`。镜像内建议 **周六 02:30** 周线、**每月 1 日 03:30** 月线（见 `crontab.txt` / `crontab_domestic.txt`）。

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
