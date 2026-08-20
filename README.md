# quant-data-engine

统一的量化数据采集 / 处理 / 质量监控引擎（Plan B 重构）。

## 目录

- [目录结构](#目录结构)
- [环境变量](#环境变量)
- [快速开始](#快速开始)
- [数据采集任务](#数据采集任务)
  - [1. 申万行业指数](#1-申万行业指数)
  - [2. 指数日线](#2-指数日线)
  - [3. ETF 日线 / 份额规模 / 基金持仓](#3-etf-日线--份额规模--基金持仓)
  - [4. 股票日线 / 周线 / 月线](#4-股票日线--周线--月线)
  - [5. 财务数据](#5-财务数据)
  - [6. 资金流](#6-资金流)
  - [7. 东方财富概念 & 主题补涨](#7-东方财富概念--主题补涨)
  - [8. 美股 / 全球市场日线](#8-美股--全球市场日线)
  - [9. 板块风险扫描](#9-板块风险扫描)
  - [10. 涨停动向总结（LLM）](#10-涨停动向总结llm)
  - [11. 实时数据采集（miniQMT Bridge）](#11-实时数据采集miniqmt-bridge)
  - [12. 自选股盘中快照（东财）](#12-自选股盘中快照东财)
  - [13. 分钟级历史下载](#13-分钟级历史下载)
  - [14. 全市场分钟连续同步](#14-全市场分钟连续同步)
  - [15. 市场情绪分析](#15-市场情绪分析)
- [Pipeline Cascade Dispatcher](#pipeline-cascade-dispatcher)
- [数据工具](#数据工具)
- [迁移说明](#迁移说明)
- [后续计划](#后续计划)

---

## 目录结构

```
config/         # 全局配置与日志
adapters/       # 外部数据源适配（Tushare、TDX、miniQMT 等）
repositories/   # 数据持久化与读取抽象
pipelines/      # 端到端数据管线（采集 → 转换 → 写库）
models/         # 数据结构契约（Pydantic / TypedDict）
quality/        # 验证与监控组件
tasks/          # 可调度任务入口（CLI / cron / systemd）
utils/          # 通用工具函数
tools/          # 数据管理工具
tests/          # 自动化测试
```

---

## 环境变量

在项目根目录创建 `.env`：

| 变量 | 必须 | 默认值 | 说明 |
|------|:----:|--------|------|
| `MONGO_URI` | ✅ | — | MongoDB 主库连接字符串 |
| `MONGO_DB` | ✅ | — | 业务库名（任务队列、用户等），如 `finance` |
| `DATA_DB_NAME` | ✅ | — | 行情数据库名，如 `quant_data` |
| `TUSHARE_TOKEN` | ✅ | — | Tushare API Token |
| `RATE_LIMIT_PER_SEC` | | `4` | Tushare 限频（次/秒） |
| `SECONDARY_MONGO_URI` | | — | 双写目标库（海外部署 / 国内备份） |
| `FUND_PORTFOLIO_ANN_DAYS` | | `14` | 基金持仓回溯公告日天数 |
| `CRON_FUND_PORTFOLIO` | | `0 19 * * 0` | 基金持仓 cron 表达式（镜像内） |
| `ENABLE_WATCHLIST_EASTMONEY_INTRADAY` | | `0` | 设为 `1` 启用东财盘中快照常驻 |
| `WATCHLIST_EASTMONEY_IDLE_INTERVAL` | | `120` | 非交易时段轮询间隔（秒） |
| `WATCHLIST_EASTMONEY_ACTIVE_INTERVAL` | | `60` | 盘中轮询间隔（秒） |
| `THEME_CANDIDATE_MIN_PCT_CHG` | | `0` | 主题补涨候选最小涨幅 |
| `THEME_CANDIDATE_MAX_PCT_CHG` | | `3` | 主题补涨候选最大涨幅 |
| `THEME_MAINLINE_TOP_K` | | — | 取前 K 条主线（实现默认） |
| `THEME_PER_BUCKET_MAX` | | `3` | 每桶最多候选数 |
| `THEME_LAG_VALIDATE_MAX_TRADING_DAYS` | | `10` | 补涨验证扫描最大交易日数 |
| `THEME_LAG_WRITE_SIZED_TRADE_EXECUTIONS` | | `0` | 设为 `1` 启用纸面成交写入 |
| `THEME_LAG_NOTIONAL_CNY_PER_SYMBOL` | | `10000` | 纸面成交每标的名义本金（CNY） |

---

## 快速开始

```bash
# 安装（开发模式）
pip install -e ./quant-data-engine

# 配置环境变量（参见上方环境变量表）
# cp .env.example .env && vim .env

# 常用任务示例
python -m quant_data_engine.tasks.run_stock_daily --mode by-date --days 3
python -m quant_data_engine.tasks.run_sw_industry_backfill --sync daily \
    --start 20040101 --end $(date +%Y%m%d)
python -m quant_data_engine.tasks.run_global_market_daily_sync --backfill
```

> 所有任务均支持 `--dry-run`（只打印，不写库）；大多数支持 `--resume`（断点续传）。  
> 写库均使用 `upsert=True`，**完全幂等，多次运行不产生重复数据**。  
> Shell 快捷入口统一为 `./daily_market_sync.sh <task_name>`（详见 `crontab.txt`）。

---

## 数据采集任务

---

### 1. 申万行业指数

从 Tushare 拉取申万行业分类、指数成分、行业指数日线，写入 `DATA_DB_NAME`（默认 `quant_data`）。

#### 写入集合

| 集合 | 说明 |
|------|------|
| `sw_index_classify` | L1/L2/L3 行业/指数树，键含 `index_code` + `src` |
| `sw_index_member` | 成分（按 L1 拉取，含 `con_code`），`src` 区分申万版本 |
| `sw_daily` | 行业指数日线，`trade_date` 为 `YYYYMMDD` 字符串 |
| `sw_weekly` | 由 `sw_daily` 经 pandas resample（`W-FRI`）派生 |
| `sw_monthly` | 由 `sw_daily` 经 pandas resample（`ME`）派生 |
| `sw_industry_sync_meta` | 断点元数据（`sw_daily_backfill` 记录最后写入日期） |

#### 使用

```bash
cd quant-data-engine
# 已 pip install -e . 时可直接 -m；否则加 PYTHONPATH=src

# 首次全量（推荐顺序）
# 1) 行业/指数分类
python -m quant_data_engine.tasks.run_sw_industry_backfill --sync classify --src SW2021

# 2) 指数成分
python -m quant_data_engine.tasks.run_sw_industry_backfill --sync members --src SW2021

# 3) 日线历史（Tushare sw_daily 最早支持约 2004 年，注意限频与积分）
python -m quant_data_engine.tasks.run_sw_industry_backfill --sync daily \
    --start 20040101 --end $(date +%Y%m%d)

# 断点续传（从 meta 记录的下一天继续）
python -m quant_data_engine.tasks.run_sw_industry_backfill --sync daily --resume

# 派生周线 / 月线
python -m quant_data_engine.tasks.run_sw_industry_backfill --sync weekly \
    --start 20040101 --end $(date +%Y%m%d)
python -m quant_data_engine.tasks.run_sw_industry_backfill --sync monthly \
    --start 20040101 --end $(date +%Y%m%d)

# 一键执行 classify + members + daily
python -m quant_data_engine.tasks.run_sw_industry_backfill --sync all --src SW2021
```

> `quantFinance` 的 `shenwan_index` 路由**只读**此库；无数据时错误提示将指向本任务。

---

### 2. 指数日线

采集沪深 300、中证 500 等主要指数日线数据。

```bash
python -m quant_data_engine.tasks.run_index_daily --core --backfill-days 3
python -m quant_data_engine.tasks.run_index_daily --codes 000300.SH 000905.SH --backfill-days 3
```

---

### 3. ETF 日线 / 份额规模 / 基金持仓

#### 3.1 ETF 日线

写入集合：

| 集合 | 键 | 内容 |
|------|----|------|
| `etf_basic` | `ts_code`（唯一） | ETF 基础信息：名称、类型、成立日期、规模等 |
| `etf_daily` | `(ts_code, trade_date)` | 日线 OHLCV |
| `etf_adj_factor` | `(ts_code, trade_date)` | 复权因子 |
| `etf_shares` | `(ts_code, trade_date)` | 基金份额（`fund_share` / 万份 `fd_share`） |

```bash
cd quant-data-engine

# 每日增量（推荐，全市场当天）
python -m quant_data_engine.tasks.run_etf_daily --all \
    --start $(date +%Y%m%d) --end $(date +%Y%m%d)

# 首次历史回填
python -m quant_data_engine.tasks.run_etf_daily --all \
    --start 20250101 --end $(date +%Y%m%d)

# 白名单模式
python -m quant_data_engine.tasks.run_etf_daily --codes 510300.SH 159919.SZ
python -m quant_data_engine.tasks.run_etf_daily --core       # 11 只核心行业 ETF
python -m quant_data_engine.tasks.run_etf_daily --extended   # 21 只扩展白名单
```

> **`--all` 说明**：按 `trade_date` 逐日调用 `fund_daily` / `fund_adj` / `fund_share`，无需预先拉代码列表；`fund_adj` / `fund_share` 已做 offset 分页。`etf_basic` 通过一次 `fund_basic(market=E, status=L)` 全量 upsert。

#### 3.2 ETF 份额规模（`etf_share_size`）

Tushare `etf_share_size` → 集合 `etf_share_size`。与 `etf_shares` 并存，便于对比规模字段（`total_size`）。

```bash
# 每日增量
python -m quant_data_engine.tasks.run_etf_share_size --all \
    --start $(date +%Y%m%d) --end $(date +%Y%m%d)

# 历史回填
python -m quant_data_engine.tasks.run_etf_share_size --all \
    --start 20250101 --end $(date +%Y%m%d)

# 与 etf_shares 对比
python tools/compare_etf_share_size_vs_fund_share.py --start 20250401 --end 20250418

# 按区间重建（--help 查看参数）
PYTHONPATH=src python tools/rebuild_etf_share_size_range.py \
    --start 20250401 --end 20250430 --drop range --dry-run
```

Shell 调度：`./daily_market_sync.sh etf_share_size`（建议 **19:05** 之后）。

> `total_share` / `total_size` 等大数字段入库使用 `Decimal128`；`nav` / `close` 为 Tushare 可选输出，SDK 未返回时说明当次接口未带该字段。

#### 3.3 公募基金持仓（`fund_holdings`）

Tushare `fund_portfolio`（季报/年报披露） → `quant_data.fund_holdings`。需要 [Tushare 积分权限](https://tushare.pro/document/2?doc_id=121)。

```bash
# 默认：近 14 个日历日的公告日
python -m quant_data_engine.tasks.run_fund_portfolio_sync

python -m quant_data_engine.tasks.run_fund_portfolio_sync --ann-days 10

# 单只基金（.OF）
python -m quant_data_engine.tasks.run_fund_portfolio_sync --ts-code 001753.OF
```

Shell 入口：`./daily_market_sync.sh fund_portfolio`。镜像默认 **每周日 19:00** 运行，可用 `CRON_FUND_PORTFOLIO` 覆盖。

---

### 4. 股票日线 / 周线 / 月线

#### 4.1 股票日线（OHLCV + 基础指标）

采集全市场股票日线 OHLCV 及基础指标（PE、PB、换手率等），写入 `quant_data.volume_price` / `financial_daily_basic`。

```bash
# 按日期批量（推荐）
python -m quant_data_engine.tasks.run_stock_daily --mode by-date --start 20240101 --end 20240110

# 最近 N 个交易日
python -m quant_data_engine.tasks.run_stock_daily --mode by-date --days 5

# 只回补 daily_basic 基础指标（例如新增 dv_ratio/dv_ttm 字段时，避免重写 OHLCV）
python -m quant_data_engine.tasks.run_stock_daily --mode by-date --start 20240101 --end 20240110 --basic-only

# 按股票逐只
python -m quant_data_engine.tasks.run_stock_daily --mode by-stock --codes 000001.SZ 000002.SZ --days 30
```

#### 4.2 股票周线 / 月线

写入 `quant_data.stock_weekly` / `quant_data.stock_monthly`。`trade_date` 为周线/月线结束交易日。全市场按 `trade_date` 拉取时单次最多 6000 行，任务内已 offset 分页。需要 Tushare **2000 积分**以上权限。

```bash
# 周线：指定结束日（全市场）
python -m quant_data_engine.tasks.run_stock_weekly --trade-date 20250411

# 最近 N 个完整 ISO 周
python -m quant_data_engine.tasks.run_stock_weekly --end 20250418 --recent-weeks 2

# 单只股票区间回补
python -m quant_data_engine.tasks.run_stock_weekly --ts-code 000001.SZ \
    --start 20200101 --end-range 20250418

# 月线：上一自然月
python -m quant_data_engine.tasks.run_stock_monthly --end 20250401 --recent-months 1

# 月线：指定结束日
python -m quant_data_engine.tasks.run_stock_monthly --trade-date 20250331
```

Shell：`./daily_market_sync.sh stock_weekly` / `stock_monthly`。建议 **周六 02:30** 周线、**每月 1 日 03:30** 月线（见 `crontab.txt` / `crontab_domestic.txt`）。

---

### 5. 财务数据

采集财务报表（利润表、资产负债表、现金流量表）和财务指标，写入 `quant_data.financial_*`。

```bash
# 指定股票，同步所有报表类型
python -m quant_data_engine.tasks.run_financial_sync --codes 000001.SZ 000002.SZ --start 20230101

# 只同步特定类型
python -m quant_data_engine.tasks.run_financial_sync \
    --codes 000001.SZ --data-types income,balance

# 全市场（测试模式，仅前 10 只）
python -m quant_data_engine.tasks.run_financial_sync --all --test

# 最近上市股票
python -m quant_data_engine.tasks.run_financial_sync --recent-ipo 90
```

#### 5.1 fina_indicator 全量覆盖回填

`fina_indicator_vip` 接口用显式 `fields=` 拉取约 80 个字段（含 `rd_exp`、`ebit`、`ebitda`、`fcff`、`fcfe`、`current_ratio`、`quick_ratio`、`debt_to_assets`、全系列 YoY/QoQ 单季增速等）。若历史数据缺字段，运行以下命令全量覆盖：

```bash
# 全市场回填（按季末逐期覆盖，推荐）
python -m quant_data_engine.tasks.run_financial_sync \
    --all --period-range 20200331:20260331 --data-types financial_indicator

# 只补近几年
python -m quant_data_engine.tasks.run_financial_sync \
    --all --period-range 20230331:20260331 --data-types financial_indicator

# 只补指定个股
python -m quant_data_engine.tasks.run_financial_sync \
    --codes 000001.SZ 600036.SH 300750.SZ --start 20200101 \
    --data-types financial_indicator
```

> - `--period-range START:END` 展开为 `0331/0630/0930/1231` 逐季调用。
> - 主键 `(ts_code, end_date)`，`$set` upsert，完全幂等。
> - 需要 Tushare **2000 积分**以上权限。

---

### 6. 资金流

采集全市场资金流向，写入 `quant_data.moneyflow`。

```bash
# 指定日期范围
python -m quant_data_engine.tasks.run_moneyflow_sync --start 20240101 --end 20240110

# 自动续传（从上次位置继续）
python -m quant_data_engine.tasks.run_moneyflow_sync --auto

# 最近 N 个交易日
python -m quant_data_engine.tasks.run_moneyflow_sync --days 5
```

---

### 7. 东方财富概念 & 主题补涨

#### 7.1 东方财富概念（废弃入库，运行时实时调用）

> **注意**：`run_dc_concept_sync` 已废弃（退出码 2），`dc_concept_index` / `dc_concept_member` 集合不再维护。
>
> `run_theme_lag_candidates` 运行时直接调用 Tushare **`dc_index`**（按锚定 `trade_date`）与 **`dc_member`**（按概念 `ts_code` + 业务日），两者缺失则报错退出。

#### 7.2 主题补涨候选（`theme_lag_candidates`）

逻辑：从 `limit_up_ladder.sector_ranking` 取主线 → 与 `dc_index` 板块名对齐 → 用 `dc_member` 与 `index_constituents`（默认 `csi1000`）求交 → 剔除当日涨停股 → 按 `volume_price.pct_chg` 在 `(min, max)` 开区间筛选（默认大于 0 且小于 3 个百分点） → 每桶最多 N 只。写入 `quant_data.theme_lag_candidates`。

```bash
python -m quant_data_engine.tasks.run_theme_lag_candidates --trade-date 20250425
python -m quant_data_engine.tasks.run_theme_lag_candidates --index-store-key csi1000 --dry-run
```

Shell 调度：`./daily_market_sync.sh theme_lag`（建议晚于连板天梯与 `volume_price`，见 `crontab.txt` 22:50）。

成功后默认向 `finance.analysis_tasks` 入队 `theme_lag_narrative`（quantAnalyzer 消费后写 LLM 说明）；`--skip-narrative-task` 可跳过，`--force-narrative-task` 先删同日既有任务再入队。

#### 7.3 主题补涨候选验证（`theme_lag_candidate_validation`）

对锚定日与指数，用 `volume_price` 日收盘做规则验证（止盈 / 止损 / 期满 / 数据不足），整文档替换（自然键 `(anchor_trade_date, index_store_key)`）。

```bash
python -m quant_data_engine.tasks.run_theme_lag_candidate_validation \
    --trade-date 20250425 --index-store-key csi1000
# --skip-empty          无标的时不写主文档
# --no-pipeline-check   不检查 finance.pipeline_tasks
# --dry-run             只打印 JSON，不写库
```

相关环境变量（见[环境变量](#环境变量)表）：`THEME_LAG_VALIDATE_MAX_TRADING_DAYS`、`THEME_LAG_WRITE_SIZED_TRADE_EXECUTIONS`、`THEME_LAG_NOTIONAL_CNY_PER_SYMBOL`。

开启纸面写入时，`user_id` 来自 `finance.theme_lag_validation_subscriptions`（同一 `index_store_key` 下启用订阅的用户各一套；无订阅则跳过）。

规则口径详见 llm-wiki `projects/trading/repos/quant-data-engine/theme-lag-candidate-validation.md` 与 `tasks/theme_lag_validation_logic.py`。

---

### 8. 美股 / 全球市场日线

通过 **yfinance** 抓取 44 个美股 ETF / 指数 / 宏观锚定品种的日线 OHLCV，写入 `quant_data.global_market_daily`，供板块风险扫描使用。唯一索引：`(symbol, date)`，`date` 格式 `YYYYMMDD`，`auto_adjust=True`（前复权）。

#### 默认品种（44 个）

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

#### 使用

```bash
# 每日增量（抓最近 5 个交易日）
python -m quant_data_engine.tasks.run_global_market_daily_sync

# 初次全量回填（约 2 年）
python -m quant_data_engine.tasks.run_global_market_daily_sync --backfill

# 指定区间
python -m quant_data_engine.tasks.run_global_market_daily_sync --start 20240101 --end 20260430

# 指定品种 / Dry-run
python -m quant_data_engine.tasks.run_global_market_daily_sync --symbols SOXX QQQ ^VIX
python -m quant_data_engine.tasks.run_global_market_daily_sync --dry-run

# 双写到 secondary MongoDB
python -m quant_data_engine.tasks.run_global_market_daily_sync \
    --sync-uri "mongodb://user:pwd@remote-host:27017"
```

Shell 入口：`./daily_market_sync.sh global_market_daily`（需配置 `HTTP_PROXY`/`HTTPS_PROXY`）。镜像默认 **北京时间 06:30** 运行（美东夏令收盘后约 2.5 h）。

> **双写**：设置 `SECONDARY_MONGO_URI` 后每个品种写入主库后同步 upsert 到 secondary MongoDB；异常仅打警告，不中断主流程。`daily_market_sync.sh` 会自动读取该环境变量。

---

### 9. 板块风险扫描

计算 A 股与美股板块多维度风险信号，写入 `quant_data.risk_reports`，供前端「市场风险看板」展示。

**依赖**：`volume_price`（A 股）、`global_market_daily`（美股，见 [§8](#8-美股--全球市场日线)）。

#### 支持板块（12 个）

| 类型 | 板块标识 |
|------|---------|
| A 股（9 个） | `semiconductor`、`pharma`、`new_energy`、`military`、`ai_cloud`、`consumer`、`finance_real_estate`、`cycle`、`small_cap` |
| 美股（3 个） | `us_semicon`、`us_tech`、`us_macro` |

#### 风险信号

| 信号 | 计算逻辑 |
|------|----------|
| RSI 超买 | 近 14 日 RSI > 70 |
| 因子拥挤度 | 动量 / 波动率 / 换手率百分位 |
| A 股 VIX 代理 | 沪深 300 ETF + 成分股 ATM 期权隐含波动率代理 |
| 美股 VIX | `^VIX` 当前值 + 历史百分位 |

#### 使用

```bash
# 全部 12 个板块
python -m quant_data_engine.tasks.run_risk_scan

# 仅美股 / 仅 A 股
python -m quant_data_engine.tasks.run_risk_scan --sectors us_semicon us_tech us_macro
python -m quant_data_engine.tasks.run_risk_scan \
    --sectors semiconductor pharma new_energy military ai_cloud \
              consumer finance_real_estate cycle small_cap

# 指定日期 / Dry-run
python -m quant_data_engine.tasks.run_risk_scan --date 20260501
python -m quant_data_engine.tasks.run_risk_scan --dry-run
```

Shell 快捷入口：`./daily_market_sync.sh risk_scan_us` / `risk_scan_a`。

**推荐调度顺序**：

```
06:30 → global_market_daily    # 美股日线同步
07:00 → risk_scan_us           # 美股风险，09:30 前就绪
19:30 → risk_scan_a            # A 股风险
```

---

### 10. 涨停动向总结（LLM）

每个交易日 **14:50** 同步连板天梯后，向 `finance.analysis_tasks` 入队 `limit_up_daily_narrative` 任务，由 quantAnalyzer 调用 LLM 生成总结，写入 `quant_analyzer.limit_up_daily_narrative`，通过 `GET /ladder/narrative` 暴露给前端。

**执行链路**：

```
14:50 cron → daily_market_sync.sh limit_up_narrative
    ├─ run_ladder_sync            # 同步 limit_up_ladder（天梯）
    └─ run_limit_up_narrative     # 组装 payload，入队
               ↓
         finance.analysis_tasks   # type=limit_up_daily_narrative
               ↓
         quantAnalyzer → LLM
               ↓
         quant_analyzer.limit_up_daily_narrative
```

```bash
# 今日（自动取最新日期）
python -m quant_data_engine.tasks.run_limit_up_narrative

# 指定日期
python -m quant_data_engine.tasks.run_limit_up_narrative --date 20260507

# 强制重跑（删已有任务再入队）
python -m quant_data_engine.tasks.run_limit_up_narrative --force

# 仅打印 payload（调试）
python -m quant_data_engine.tasks.run_limit_up_narrative --dry-run
```

Shell 入口：`./daily_market_sync.sh limit_up_narrative`（天梯同步失败则终止）。

---

### 11. 实时数据采集（miniQMT Bridge）

实时订阅 miniQMT tick 数据并聚合为分钟 Bar，写入 `quant_data.minute_bars`。交易时间为 9:30–15:00（上海时区）。

```bash
# 自动模式（从 watchlist_strategies 加载股票列表）
python -m quant_data_engine.tasks.run_realtime_bridge --auto

# 手动指定股票
python -m quant_data_engine.tasks.run_realtime_bridge --symbols 002050.SZ 600618.SH

# 双写到远端
python -m quant_data_engine.tasks.run_realtime_bridge \
    --auto --sync-uri "mongodb://admin:pwd@remote-ip:27017"

# Debug 模式 / 24/7 运行（禁用交易时间限制）
python -m quant_data_engine.tasks.run_realtime_bridge --auto --debug
python -m quant_data_engine.tasks.run_realtime_bridge --auto --no-schedule
```

#### 11.1 miniQMT standalone bridge（全市场盘中日K，替代 Tushare `rt_k`）

当 Tushare `rt_k` 不可用或要切到 miniQMT 时，用 Windows 常驻进程 `get_full_tick()` 维护全市场盘中日K，并跑与 Tushare 路径相同的后处理：阴阳谱、板块强度、`a_daily_realtime_sync` job_status、T+0 / 火眼金睛触发。

这是 **Windows + miniQMT/xtquant 环境下运行的 standalone bridge**，只是放在本 repo 里复用 schema/repository 代码；它不是 `quant-data-engine` 容器任务，不会由 `start.sh`、cron 或 Docker Compose 自动启动。核心类位于 `src/quant_data_engine/bridges/miniqmt/`，入口脚本是 `scripts/run_miniqmt_bridge.py`。

**切流约定（不要并行写 `volume_price`）：**

1. 容器侧关掉 Tushare cron：`ENABLE_REALTIME_DAILY_K=0`
2. Windows 只跑 **一个** full_market bridge，**停掉** targeted（自选股/策略池）bridge
3. 全市场已覆盖在市 A 股，不必再跑 targeted 补数据

默认写库间隔 **60 秒**，对齐原来的 Tushare 每分钟 cron。压测通过后可用 `--daily-k-interval 30`（或 JSON）加快。

```bash
# 推荐：全市场配置
cp scripts/miniqmt_bridge.full_market.example.json scripts/miniqmt_bridge.json

python scripts/run_miniqmt_bridge.py --config scripts/miniqmt_bridge.json --loop

# 或 CLI（未指定间隔时自动套用 60s 写库 / 2s poll / 3600s 刷新标的）
python scripts/run_miniqmt_bridge.py --loop --universe full_market --sync-uri "mongodb://admin:pwd@remote-ip:27017"

# 以后要更快：把写库间隔降到 30 秒
python scripts/run_miniqmt_bridge.py --loop --universe full_market --daily-k-interval 30
```

`--poll-interval` 是向 miniQMT 拉 tick 的频率；`--daily-k-interval` 才是写入 Mongo / 生成阴阳谱的间隔。`--poll-batch-size`（默认全市场 400）控制单次 `get_full_tick` 标的数。

火眼金睛 / T+0 触发也写在同一份 JSON（复制到 gitignored 的 `scripts/miniqmt_bridge.json` 后填 key）。quant-api 若在同机 WSL/Docker 且 3001 已发布到 Windows，用 `127.0.0.1`，不要用 compose 主机名 `quant-api`：

```json
"market_insight_trigger_url": "http://127.0.0.1:3001/api/market-insights/generate-internal",
"market_insight_trigger_key": "",
"intraday_t0_trigger_url": "http://127.0.0.1:3001/api/intraday-t0/signals/generate-all",
"intraday_t0_trigger_key": ""
```

空 key 回退环境变量 `MARKET_INSIGHT_TRIGGER_KEY` / `HERMES_QUANT_INTERNAL_KEY`。优先级：CLI > JSON > `.env` / 进程环境。

写入位置：

- `quant_data.volume_price`
- `quant_realtime.volume_price_realtime_latest`
- `quant_realtime.market_spectrum_realtime_daily`
- `quant_realtime.realtime_sector_strength`

关键标记：

- `source=miniqmt`
- `source_mode=miniqmt_full_market_daily`
- `universe_type=full_market_a_share`
- 任务状态：`a_daily_realtime_sync`（与原 Tushare 全市场任务同一 key）

容器侧停用 Tushare `rt_k` cron：

```env
ENABLE_REALTIME_DAILY_K=0
```

##### 11.1.1 遗留：targeted 目标池模式

`--universe targeted`（默认）只订阅自选股 + enabled 策略股 + 活跃组合计划 + 模拟持仓，任务类型为 `a_daily_realtime_targeted_sync`，**不**生成全市场阴阳谱。全市场切流后不要再跑。示例配置见 `scripts/miniqmt_bridge.example.json`。

```bash
python scripts/run_miniqmt_bridge.py --once --ignore-session --symbols 000001.SZ 600000.SH
```

配置优先级：CLI 参数 > JSON 配置文件 > `.env` / 进程环境 > 代码默认值。

`--sync-uri` 双写 `volume_price` 与 `volume_price_realtime_latest`，并把本任务的 `job_status` / `pipeline_tasks` 镜像到同一集群。填了 `--sync-uri` 时，状态镜像目标以它为准（覆盖 `JOB_STATUS_SYNC_URI`）。

---

### 12. 自选股盘中快照（东财）

交易日 9:30–15:00 轮询东方财富 API，将全站用户自选股（`user_watchlists.symbols` 去重后规范为 `ts_code`）的当日 OHLCV 快照写入 `quant_data.volume_price`（`source=eastmoney_snapshot`）。与 Tushare 日线共用 `(symbol, trade_date)` upsert；夜间 `run_stock_daily` 可能覆盖同日文档，属预期行为。

**字段映射**：`f2` 现价、`f15` 昨收、`f16` 最低、`f17` 今开、`f18` 最高。

```bash
# 单次运行即退出（忽略交易时段限制，适合补数 / 联调）
python -m quant_data_engine.tasks.run_watchlist_intraday_eastmoney --once

# 常驻（非交易时段 sleep）
python -m quant_data_engine.tasks.run_watchlist_intraday_eastmoney \
    --interval 120 --active-interval 60

# 每批 secids 数量（默认 80）
python -m quant_data_engine.tasks.run_watchlist_intraday_eastmoney --once --batch 80
```

**Docker 启动**：`DEPLOY_MODE=domestic` 或 `full` 时，设置 `ENABLE_WATCHLIST_EASTMONEY_INTRADAY=1` 会拉起此任务；间隔由 `WATCHLIST_EASTMONEY_IDLE_INTERVAL` / `WATCHLIST_EASTMONEY_ACTIVE_INTERVAL` 控制（见[环境变量](#环境变量)表）。

> **消费方**：`quantFinance` 自选股实时接口优先读当日 `minute_bars`，否则读 `volume_price` 东财快照。

---

### 13. 分钟级历史下载（miniQMT）

使用 miniQMT 下载分钟级历史数据，写入 `minute_bars` / `minute_bars_external`。

```bash
# 指定代码与区间
python -m quant_data_engine.tasks.download_minute_data \
    --symbols 300722.SZ 000001.SZ --start 20250101 --end 20250105 --period 1m

# 全市场 A 股批量模式
python -m quant_data_engine.tasks.download_minute_data --all-a-shares --period 1m --batch
```

---

### 14. 全市场分钟连续同步

批量轮询全市场分钟数据（miniQMT），支持批大小控制、目标延迟、可选实时情绪分析。

```bash
# 默认：全市场，批 400，目标延迟 300 s，跑 10 轮
python -m quant_data_engine.tasks.continuous_market_minute_sync \
    --batch-size 400 --target-latency 300 --count 10

# 指定代码文件，启用情绪分析
python -m quant_data_engine.tasks.continuous_market_minute_sync \
    --symbols-file stocks.txt --analyze-sentiment --sentiment-ma-period 5

# 仅查看批次规划（不下载）
python -m quant_data_engine.tasks.continuous_market_minute_sync --dry-run
```

---

### 15. 市场情绪分析（阳谱 / 阴谱）

基于日线或分钟线计算市场情绪，支持实时（日线 MA + 分钟收盘）模式。

```bash
# 日线区间
python -m quant_data_engine.tasks.analyze_market_sentiment \
    --granularity daily --start 20251201 --end 20251215 --ma-period 5

# 分钟线最新 60 分钟
python -m quant_data_engine.tasks.analyze_market_sentiment \
    --granularity minute --latest 60 --ma-period 5

# 日线 MA + 分钟收盘实时视角
python -m quant_data_engine.tasks.analyze_market_sentiment \
    --granularity daily_realtime --latest 120 --ma-period 5 \
    --collection minute_bars_external
```

---

## Pipeline Cascade Dispatcher

`run_dispatcher.py` 轮询 `pipeline_tasks`，当某个管线的所有上游依赖都 `complete` 时自动触发该管线，实现盘后任务的自动级联执行（无需固定 cron 等待时间）。

### 前置条件

在共享库 catalog `data-access-lib/src/stock_data_access/catalogs/data_pipelines_catalog.yaml` 中将目标管线的 `trigger_on_complete` 改为 `true`：

```yaml
- id: market_sentiment
  trigger_on_complete: true   # 改为 true 后 dispatcher 将在 stock_daily 完成后立即触发
  depends_on: [stock_daily]
```

> 所有管线默认 `trigger_on_complete: false`，dispatcher 启动不会执行任何操作，可安全观察。

### 使用

```bash
# dry-run：只打印会触发哪些任务，不执行任何命令（默认行为）
python -m quant_data_engine.tasks.run_dispatcher --dry-run --once

# dry-run 持续轮询（60s 间隔，Ctrl-C 停止）
python -m quant_data_engine.tasks.run_dispatcher --dry-run

# live 模式：真实调用 daily_market_sync.sh <id>
python -m quant_data_engine.tasks.run_dispatcher --live

# 指定业务日期（默认北京时间今日）
python -m quant_data_engine.tasks.run_dispatcher --dry-run --date 20260512

# 自定义 catalog 路径
python -m quant_data_engine.tasks.run_dispatcher --catalog /path/to/data_pipelines_catalog.yaml
```

### 选项

| 选项 | 说明 | 默认 |
|------|------|------|
| `--live` | 禁用 dry-run，真实执行 | 关（dry-run 开） |
| `--date YYYYMMDD` | 覆盖业务日期 | 今日北京时间 |
| `--poll-interval N` | 轮询间隔（秒） | 60 |
| `--once` | 执行一次轮询后退出 | 关 |
| `--catalog PATH` | 覆盖 catalog YAML 路径 | 自动定位 |

### 防重复触发

使用 MongoDB `find_one_and_update` + `$setOnInsert` 原子操作在 `pipeline_tasks` 中写入 `status=dispatching` 占位，即使多个 dispatcher 实例同时运行也只有一个能触发同一管线。

### 管线 id → shell 参数映射

`SHELL_ID_MAP` 中定义了 catalog id 与 `daily_market_sync.sh` case 标签的映射（当两者不一致时需要显式填写）。当前已内置：

| catalog id | shell 参数 |
|---|---|
| `market_sentiment` | `sentiment` |
| `theme_lag_candidates` | `theme_lag` |
| `theme_lag_candidate_validation` | `theme_lag` |
| `risk_scan_a` | `risk_scan_a` |

---

## 数据工具

### 符号标准化

将数据库中的股票代码统一添加交易所后缀。

```bash
cd quant-data-engine/tools
python update_symbol_with_suffix_generic.py
```

详见 [tools/README.md](tools/README.md)。

---

## 迁移说明

如果你之前使用 `quantFinance/tools` 中的数据采集脚本，对照下表迁移到新架构。详细历史见 llm-wiki：`projects/trading/repos/quant-data-engine/2026-01-03-plan-b-tools-migration.md`。

| 旧脚本 | 新入口 |
|--------|--------|
| `bulk_daily_collector.py` | `run_stock_daily.py` |
| `bulk_daily_basic_sync.py` | `run_stock_daily.py` |
| `sync_financial_data.py` | `run_financial_sync.py` |
| `money_flow_collector.py` | `run_moneyflow_sync.py` |
| `update_symbol_with_suffix_generic.py` | `tools/update_symbol_with_suffix_generic.py` |

---

## 后续计划

- ✅ 股票日线数据采集
- ✅ 财务数据同步
- ✅ 资金流数据同步
- ✅ 美股 / 全球市场日线同步（yfinance）
- ✅ A 股 + 美股板块风险扫描
- ✅ Secondary MongoDB 双写（美股日线）
- ✅ 涨停全景市场动向总结（LLM，每日 14:50 自动入队）
- ✅ Pipeline Cascade Dispatcher（`run_dispatcher.py`，dry-run 安全，支持上游完成自动触发下游）
- 异步抓取支持
- Prometheus 指标导出
- 数据质量监控
- 更多数据管线（板块 / 因子）
