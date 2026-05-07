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

# 可选：将美股/全球数据同步双写到远端 MongoDB（海外/国内两地部署）
SECONDARY_MONGO_URI=mongodb://user:pwd@remote-host:27017

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

# 自选股东财盘中快照 → quant_data.volume_price（手工跑一轮，见 §6b）
python -m quant_data_engine.tasks.run_watchlist_intraday_eastmoney --once
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

### 2d. 东方财富概念（`dc_index` / `dc_member`）

**已不再**通过 `run_dc_concept_sync` 写入 `dc_concept_index` / `dc_concept_member`。历史若曾落库，Mongo 中旧集合可忽略或自行清理。

**`run_theme_lag_candidates`** 在运行时会直接调用 Tushare **`dc_index`**（按锚定 `trade_date` 精确截面，缺则报错退出）与 **`dc_member`**（按概念 `ts_code` + 业务日；空或无列则报错退出）。

旧入口 `python -m quant_data_engine.tasks.run_dc_concept_sync` 仅打印废弃说明并以退出码 2 结束。

### 2e. 主题补涨候选（`theme_lag_candidates`）

在 **`quant_data`** 中写入 **`theme_lag_candidates`**：从 **`limit_up_ladder.sector_ranking`** 取主线，将 `sector` 名称与 **Tushare `dc_index`** 返回的板块名对齐（精确或唯一模糊匹配），用 **`dc_member`** 与 **`index_constituents`**（默认 `csi1000`）求交，剔除当日 **`daily_limit_up`** 涨停股，按 **`volume_price.pct_chg`** 落在 **(min, max)** 开区间内筛选（默认 **大于 0 且小于 3** 个百分点：只保留当日上涨且涨幅相对温和的成分，排除下跌/平盘及涨幅过大），每桶最多 3 只（可配）。需 **`TUSHARE_TOKEN`**。

**任务入口**：`python -m quant_data_engine.tasks.run_theme_lag_candidates`（`tasks/run_theme_lag_candidates.py`）。

```bash
python -m quant_data_engine.tasks.run_theme_lag_candidates --trade-date 20250425
python -m quant_data_engine.tasks.run_theme_lag_candidates --index-store-key csi1000 --dry-run
# 环境变量：THEME_CANDIDATE_MIN_PCT_CHG、THEME_CANDIDATE_MAX_PCT_CHG、THEME_MAINLINE_TOP_K、THEME_PER_BUCKET_MAX
```

**调度**：`./daily_market_sync.sh theme_lag`；建议晚于连板天梯与 `volume_price`（示例见 `crontab.txt` 22:50）。默认成功后会向 **`finance.analysis_tasks`** 入队 **`theme_lag_narrative`**（`quantAnalyzer` 消费后将说明写入该任务 `result`）；`--skip-narrative-task` 可跳过，`--force-narrative-task` 会先删同日同指数既有叙事任务再入队。

### 2f. 主题补涨候选验证（`theme_lag_candidate_validation`）

对给定锚定业务日与指数，用 **`quant_data.theme_lag_candidates`** 与 **`quant_data.volume_price`** 的日收盘做确定性规则验证（止盈 / 止损 / 期满 / 数据不足），写入 **`quant_data.theme_lag_candidate_validation`**。自然键 **`(anchor_trade_date, index_store_key)`** 重跑为整文档替换。

**任务入口**：

```bash
python -m quant_data_engine.tasks.run_theme_lag_candidate_validation --trade-date 20250425 --index-store-key csi1000
# --skip-empty          无标的不写主文档（§3.1 策略 B）
# --no-pipeline-check   不检查 finance.pipeline_tasks（仍可用候选表 / volume_price 就绪回退）
# --dry-run             只打印 JSON，不写库
```

**环境变量（节选）**：

| 变量 | 含义 |
|------|------|
| `THEME_LAG_VALIDATE_MAX_TRADING_DAYS` | 锚定日 **T 之后**参与扫描的开市日个数上限（默认 `10`） |
| `THEME_LAG_WRITE_SIZED_TRADE_EXECUTIONS` | 设为 `1`/`true` 时，验证落库后向 **`finance.trade_executions`** 写入纸面 BUY/SELL（默认关闭） |
| `THEME_LAG_NOTIONAL_CNY_PER_SYMBOL` | 纸面成交每标的计划名义本金（CNY），默认 `10000` |

开启纸面写入时，**`user_id`** 来自 **`finance.theme_lag_validation_subscriptions`**：同一 **`index_store_key`** 下启用订阅的用户各一套；**无订阅则跳过**纸面写入（打日志）。

规则口径、**期满（当前为第 2 根有效收盘）**、`insufficient_data`、就绪门闩与 **`trade_date`** 存储形态兼容等，以 **[docs/THEME_LAG_CANDIDATE_VALIDATION_SPEC.md](docs/THEME_LAG_CANDIDATE_VALIDATION_SPEC.md)** 与 `tasks/theme_lag_validation_logic.py` 为准。

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
- 支持选择性数据采集（`--data-types income,balance,cashflow,financial_indicator`）
- 批量处理多只股票
- 全量 upsert，多次运行不产生重复数据

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

#### 4a. fina_indicator 全量覆盖回填

`fetch_fina_indicator` 调用 `fina_indicator_vip`，使用显式 `fields=` 参数拉取约 80 个字段（含
`rd_exp`、`ebit`、`ebitda`、`fcff`、`fcfe`、`current_ratio`、`quick_ratio`、`debt_to_assets`、
全系列 YoY/QoQ 单季增速等）。若之前未指定 `fields=` 导致缺字段，可用以下命令全量覆盖：

```bash
cd quant-data-engine

# 全市场回填，只拉 fina_indicator（推荐；按季末逐期覆盖）
python -m quant_data_engine.tasks.run_financial_sync \
    --all \
    --period-range 20200331:20260331 \
    --data-types financial_indicator

# 只补近几年
python -m quant_data_engine.tasks.run_financial_sync \
    --all \
    --period-range 20230331:20260331 \
    --data-types financial_indicator

# 只补指定个股
python -m quant_data_engine.tasks.run_financial_sync \
    --codes 000001.SZ 600036.SH 300750.SZ \
    --start 20200101 \
    --data-types financial_indicator
```

**说明**：
- `--period-range START:END` 会展开为 `0331/0630/0930/1231` 逐季调用，保证每期覆盖。
- `financial_indicator` 主键为 `(ts_code, end_date)`，`$set` upsert 写入新字段、保留旧字段，完全幂等。
- `fina_indicator_vip` 无 `report_type` 维度，每期只有一行，不需要 `--no-single-quarter`。
- 需要 Tushare **2000 积分**以上权限，否则接口会报权限错误。

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

### 6b. 自选股东财盘中快照（`run_watchlist_intraday_eastmoney`）

交易日 **9:30–15:00（上海时区）** 轮询东方财富 **`push2.eastmoney.com/api/qt/ulist.np/get`**，将**全站用户自选股**（`MONGO_DB` 下 `user_watchlists.symbols` 去重并规范为 `ts_code`）的当日 OHLCV 快照写入 **`DATA_DB_NAME`（默认 `quant_data`）的 `volume_price`**，`source` 为 **`eastmoney_snapshot`**。与 Tushare 日线共用 `(symbol, trade_date)` upsert；夜间 `run_stock_daily` 可能覆盖同日文档，属预期。

**字段映射（ulist diff）**：`f2` 现价、`f15` 昨收、`f16` 最低、`f17` 今开、`f18` 最高；`StockRepository.upsert_daily` 会保留记录中的 `source` 字段。

**手动运行**：

```bash
cd quant-data-engine

# 跑一轮即退出（忽略是否在交易时段；适合联调 / 补数）
python -m quant_data_engine.tasks.run_watchlist_intraday_eastmoney --once

# 常驻：非交易时段 sleep --interval 秒；交易时段内用 --active-interval
python -m quant_data_engine.tasks.run_watchlist_intraday_eastmoney --interval 120 --active-interval 60

# 每批 secids 数量（默认 80）
python -m quant_data_engine.tasks.run_watchlist_intraday_eastmoney --once --batch 80
```

**Docker / `start.sh`**：`DEPLOY_MODE=domestic` 或 `full` 时，设置 **`ENABLE_WATCHLIST_EASTMONEY_INTRADAY=1`** 会拉起上述任务；可选 **`WATCHLIST_EASTMONEY_IDLE_INTERVAL`** / **`WATCHLIST_EASTMONEY_ACTIVE_INTERVAL`**（秒）覆盖默认休眠与盘中间隔。

**消费方**：`quantFinance` 自选股实时接口会优先读当日 `minute_bars`，否则读当日（或近日）`volume_price` 东财快照。

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

### 9a. 美股 / 全球市场日线数据同步（yfinance → `global_market_daily`）

通过 **yfinance** 抓取美股 ETF、指数、宏观锚定品种的日线 OHLCV 历史，写入 **`quant_data.global_market_daily`**。数据用于美股板块风险扫描（§9b）。

**默认同步品种（44 个）**，按类别分组：

| 类别 | 品种 |
|------|------|
| 美股宽基 | `SPY`、`^GSPC`、`^DJI`、`^RUT` |
| 纳指 / 科技 | `QQQ`、`XLK`、`^IXIC` |
| 半导体 | `SOXX`、`SMH`、`^SOX` |
| 行业 ETF | `XLE`、`XLF`、`XLV`、`XLI`、`XLU`、`XLRE`、`ARKK` |
| 波动率 / 利率 | `^VIX`、`^TNX`、`^TYX` |
| 美元指数 | `DX-Y.NYB` |
| 全球指数 | `^N225`（日经）、`^HSI`（恒生）、`000001.SS`（上证）、`FXI`、`EWJ`、`EWG`、`EWY` |
| 商品 | `GC=F`（黄金）、`CL=F`（原油）、`SI=F`（白银） |
| 加密货币 | `BTC-USD`、`ETH-USD` |
| 美股指数期货 | `ES=F`、`NQ=F` |
| 科技巨头 | `NVDA`、`AMD`、`AAPL`、`MSFT`、`TSLA`、`GOOG`、`META`、`AMZN` |
| 中概股 | `BABA`、`PDD`、`JD` |

**文档结构**：

| 字段 | 说明 |
|------|------|
| `symbol` | 品种代码（如 `SOXX`、`^VIX`） |
| `date` | 交易日 `YYYYMMDD`，与 A 股日线格式一致 |
| `open/high/low/close` | 日线 OHLC（auto_adjust=True 前复权） |
| `volume` | 成交量 |
| `updated_at` | 写入时间戳（UTC） |

唯一索引：`(symbol, date)`，全部写入 `upsert=True`，**完全幂等**。

**使用**：

```bash
# 每日增量（默认抓最近 5 个交易日，供 cron 任务调用）
python -m quant_data_engine.tasks.run_global_market_daily_sync

# 初次全量历史回填（约 2 年 ≈ 504 个交易日）
python -m quant_data_engine.tasks.run_global_market_daily_sync --backfill

# 指定日期区间
python -m quant_data_engine.tasks.run_global_market_daily_sync --start 20240101 --end 20260430

# 仅指定品种
python -m quant_data_engine.tasks.run_global_market_daily_sync --symbols SOXX QQQ ^VIX

# Dry-run：拉取但不写库
python -m quant_data_engine.tasks.run_global_market_daily_sync --dry-run

# 双写到 secondary MongoDB（国内/海外两地部署）
python -m quant_data_engine.tasks.run_global_market_daily_sync \
    --sync-uri "mongodb://user:pwd@remote-host:27017"
# 或设置环境变量（daily_market_sync.sh 会自动读取）
export SECONDARY_MONGO_URI="mongodb://user:pwd@remote-host:27017"
```

**双写说明**：若设置 `SECONDARY_MONGO_URI` 环境变量（或通过 `--sync-uri` 显式指定），每个品种写入主库成功后，会同步 upsert 到 secondary MongoDB，异常只打警告不中断主流程。`daily_market_sync.sh global_market_daily` 已自动读取该环境变量。

Shell 入口：`./daily_market_sync.sh global_market_daily`（需 `HTTP_PROXY`/`HTTPS_PROXY` 正确配置，yfinance 需访问 Yahoo Finance）。镜像内默认 **北京时间 06:30 每日** 运行（美东夏令 04:00 收盘，06:30 BJT 数据已就绪）。

### 9b. 市场板块风险扫描（`run_risk_scan`）

计算 A 股与美股板块的多维度风险信号，写入 **`quant_data.risk_reports`**。前端「市场风险看板」读取该集合展示最新风险评分。

**支持板块（共 12 个）**：

| 类型 | 板块标识 | 说明 |
|------|---------|------|
| A 股 | `semiconductor`、`pharma`、`new_energy`、`military`、`ai_cloud`、`consumer`、`finance_real_estate`、`cycle`、`small_cap` | 申万行业映射，9 个 |
| 美股 | `us_semicon`、`us_tech`、`us_macro` | 半导体 ETF/指数、科技 ETF/指数、宏观锚定 |

**风险信号**：

| 信号 | 计算逻辑 |
|------|----------|
| RSI 超买 | 近 14 日 RSI > 70 |
| 因子拥挤度 | 动量 / 波动率 / 换手率百分位 |
| A 股 VIX 类拥挤 | 沪深 300 ETF + 成分股 ATM 期权隐含波动率代理 |
| 美股 VIX 拥挤 | `^VIX` 当前值 + 历史百分位 |

**使用**：

```bash
# 扫描全部 12 个板块（默认）
python -m quant_data_engine.tasks.run_risk_scan

# 只扫描美股板块（crontab 06:30 之后使用）
python -m quant_data_engine.tasks.run_risk_scan --sectors us_semicon us_tech us_macro

# 只扫描 A 股板块
python -m quant_data_engine.tasks.run_risk_scan \
    --sectors semiconductor pharma new_energy military ai_cloud \
                consumer finance_real_estate cycle small_cap

# 指定日期（读取该日历史数据）
python -m quant_data_engine.tasks.run_risk_scan --date 20260501

# Dry-run（不写 MongoDB）
python -m quant_data_engine.tasks.run_risk_scan --dry-run
```

Shell 快捷入口：

```bash
./daily_market_sync.sh risk_scan_us   # 仅美股 3 个板块
./daily_market_sync.sh risk_scan_a    # 仅 A 股 9 个板块
```

**依赖**：
- A 股扫描需 `quant_data.volume_price`（日线已就绪）
- 美股扫描需 `quant_data.global_market_daily`（§9a 同步后可用）

**调度顺序**（crontab）：

```
06:30 → global_market_daily    # yfinance 同步美股日线
07:00 → risk_scan_us           # 美股风险扫描，结果在 09:30 A 股开盘前就绪
19:30 → risk_scan_a            # A 股风险扫描
```

### 9c. 涨停全景市场动向总结（`run_limit_up_narrative`）

每个交易日 **14:50**（尾盘前）自动运行，先同步当日连板天梯，再向 **`finance.analysis_tasks`** 入队 `limit_up_daily_narrative` 任务，由 **quantAnalyzer** 消费调用 LLM 生成总结报告，写入 **`quant_analyzer.limit_up_daily_narrative`**，通过 `GET /ladder/narrative` 暴露给前端。

**执行链路**：

```
14:50 cron → daily_market_sync.sh limit_up_narrative
    └─ [1/2] run_ladder_sync          # 同步 limit_up_ladder（天梯聚合）
    └─ [2/2] run_limit_up_narrative   # 组装 payload，入队任务
               ↓
         finance.analysis_tasks       # type=limit_up_daily_narrative
               ↓
         quantAnalyzer 消费 → LLM
               ↓
         quant_analyzer.limit_up_daily_narrative
```

**使用**：

```bash
# 今日（自动取 daily_limit_up 最新日期）
python -m quant_data_engine.tasks.run_limit_up_narrative

# 指定日期
python -m quant_data_engine.tasks.run_limit_up_narrative --date 20260507

# 强制重跑（删除已有任务后重新入队）
python -m quant_data_engine.tasks.run_limit_up_narrative --force

# 仅打印 payload，不入队（调试用）
python -m quant_data_engine.tasks.run_limit_up_narrative --dry-run
```

Shell 入口：`./daily_market_sync.sh limit_up_narrative`（内部会先跑 `run_ladder_sync`，天梯同步失败则终止）。

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
- ✅ 美股 / 全球市场日线数据同步（yfinance）
- ✅ A 股 + 美股板块风险扫描
- ✅ Secondary MongoDB 双写（美股日线数据）
- ✅ 涨停全景市场动向总结（LLM，每日 14:50 自动入队）
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
