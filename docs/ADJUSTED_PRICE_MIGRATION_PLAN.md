# 复权口径改造实施计划（原始价 + adj_factor，读取侧后复权）

> 目标：统一 `volume_price` 的复权口径，消除历史负价与「前复权/不复权」交叠带，
> 让评分与回测使用稳定、可增量、永不为负的价格序列。

## 背景与问题（体检结论）

对 `quant_data.volume_price`（约 1476 万行，覆盖 1990-12-19 → 2026-06-30）的审计发现口径不统一：

| 写入来源（按 `update_time` 类型） | 行数 | trade_date 范围 | 口径 |
|---|---|---|---|
| `datetime`（批量回填） | 13,063,995 | 1990-12-19 → 2025-09-10 | 前复权 |
| `string`（日增量/后续写入） | 3,863,353 | 2021-01-04 → 2026-06-30 | 不复权 |

- 两者在 **2021–2025 区间交叠**，边界模糊（同一时段部分行前复权、部分不复权）。
- **负价**：567,027 行、656 只股票有负 close，负价 trade_date 一直延伸到 **2021-10-20**（前复权对长历史高分红股累减分红的必然产物，非脏数据）。
- 各指数最新成分中「含负价历史」的占比：hs300 44%、a500 39%、csi500 29%、csi1000 15%、csi2000 5.4%、star50 0%。

### 采集现状

- 日增量任务 `run_stock_daily → StockAdapter.fetch_daily` 调用 Tushare `pro.daily`（**原始不复权**），`_transform_daily` 原样写入 `volume_price`，无复权处理。→ 日增量本身是对的。
- 历史段则被按前复权批量回填过（负价来源）。
- 股票**没有** `adj_factor` 集合（仅 ETF 有 `etf_adj_factor` + `fetch_fund_adj` + `upsert_adj_factors` 可作范式参考）。

## 设计原则

1. `volume_price` 只存**原始不复权**价（日增量已符合，历史需重刷统一）。
2. 新增 `stock_adj_factor` 集合存 Tushare 复权因子。
3. **读取侧实时计算复权**（不物化复权集合）：
   - 后复权（回测/因子，恒正）：`hfq_close = raw_close × adj_factor`
   - 前复权（仅展示）：`qfq_close = raw_close × adj_factor / 最新adj_factor`
4. 回测/因子统一用**后复权**或复权因子收益率；负价问题随之消除。

### 已确认决策

- **重刷/回填范围：2016-01-01 → 今**（覆盖现有评分与回测区间；更深历史可后续再补）。
- **复权计算方式：读取时实时计算**（集中在数据访问层，不维护物化复权表）。
- **`stock_adj_factor` 回填范围：与 `volume_price` 重刷范围一致（2016→今）。**

## 目标数据模型

- `quant_data.volume_price`：原始不复权 OHLCV（现有 schema 不变）。
- `quant_data.stock_adj_factor`：`{ symbol, ts_code, trade_date, adj_factor, update_time }`，
  索引 `[(symbol,1),(trade_date,-1)]`（唯一）、`[(trade_date,-1)]`。

## 实施阶段

### Phase 1 — quant-data-engine：adj_factor 采集与存储

- `adapters/stock_adapter.py`：新增 `fetch_adj_factor(ts_code="", trade_date="", start_date="", end_date="")` → `pro.adj_factor`，
  支持 by-date 全市场与 by-stock 区间分页（参照 `tushare_adapter.fetch_fund_adj`）。
- `repositories/stock_repository.py`：新增 `stock_adj_factor` 集合 + `upsert_adj_factors()` + 索引（参照 `etf_repository`）。
- `pipelines/stock_daily_pipeline.py`：`run_by_date` / `run_by_stock` 增加 adj_factor 抓取与写入分支。
- 新任务 `tasks/run_stock_adj_factor_sync.py`：by-date 日更 + by-stock 历史回填；接入 crontab / `daily_market_sync.sh`。
- `fetch_daily` 维持 `pro.daily`（原始），**不改**。
- catalog（独立 `data-access-lib` 仓库）：`data_pipelines_catalog.yaml` 增加 `stock_adj_factor` pipeline 条目；提交后在 consumers bump submodule。

### Phase 2 — data-access-lib（共享层）：复权计算工具

- 新增 `stock_data_access/prices.py`：`load_adjusted_ohlc(db, symbol, start, end, adjust="hfq"|"qfq"|"none")`，
  join `volume_price` + `stock_adj_factor`：
  - `hfq_close = raw_close × adj_factor`（恒正）
  - `qfq_close = raw_close × adj_factor / 最新adj_factor`（展示）
  - 无 adj_factor 时降级为 raw 并标注。
- 单元测试：hfq 恒正、无因子降级、与 Tushare `pro_bar(adj=...)` 抽样比对。
- data-access-lib 提交后 consumers bump submodule。

### Phase 3 — stock-scoring-system 读取侧（单点改造）

- **choke point**：`DatabaseManager.get_volume_price_data`（被 `base_scorer.get_volume_price_data` 调用）。
  让其默认返回**后复权** OHLC（复用 Phase 2 helper），加参数 `adjust="hfq"`（默认）/ `"none"`（逃生）。
- 经由 base_scorer 取价的评分器（technical/cycle/cycle_v2/money_flow/volatility/entropy 等）**自动切换**，无需逐个改。
- 回测 `analysis/score_backtester.py`、`analysis/portfolio_backtester.py`：确认收益率走后复权价，或改用复权因子收益率
  `ret_t = (c_t·af_t)/(c_{t-1}·af_{t-1}) − 1`。
- 负价兜底：读取层对 `close<=0`（重刷前残留）置为缺失。

### Phase 4 — quantFinance 读取侧

- 盘点价格消费点（`routers/market.py` 等 `volume_price` 读取）：因子/收益类用后复权；K 线展示端加 `adjust` 参数默认 qfq。
- 统一走 data-access-lib helper，避免重复实现。

### Phase 5 — 数据迁移与回填（按序）

1. 全量回填 `stock_adj_factor`（2016→今）。
2. **重刷 `volume_price` 为原始价**（`run_stock_daily --mode by-date` 覆盖 2016→今），消除负价与交叠带。
3. 校验通过后，**重跑之前暂停的 csi2000 金榜/熵值回填**（口径已统一；pilot 已验证链路可用）。

### Phase 6 — 校验

- 重刷范围内 `volume_price` 负价行数 = 0。
- 抽样（如 600519）：读取层 hfq/qfq 与 Tushare `pro_bar(adj=...)` 比对，日收益率一致。
- 重刷范围内 `update_time` 类型不再混用。
- 回测跨 2021/长窗口收益率连续、无跳变。

## 执行顺序 / 风险 / 回滚 / 工作量

- **顺序**：P1 采集 → P2 helper → P5.1 adj_factor 回填 → P3/P4 读取切换 → P5.2 volume_price 重刷（2016→今）→ P5.3 csi2000 回填 → P6 校验。
- **风险**：读取层默认口径切换会改变历史评分数值（属修正，预期内）；用 `adjust` 开关灰度、保留 raw 逃生路径。volume_price 重刷是重操作，范围限定 2016→今 以降风险。
- **回滚**：读取层加开关可秒回退；volume_price 重刷前可先按 symbol/date 范围导出备份。
- **工作量（粗估）**：P1 ~0.5–1d，P2 ~0.5d，P3 ~0.5–1d，P4 ~0.5d，P5 数据回填按机器/限速（adj_factor 快、volume_price 重刷数小时），P6 ~0.5d。

## 附：命令与接口备忘

- Tushare：`pro.adj_factor(ts_code=..., trade_date=... | start_date=..., end_date=...)` → `ts_code, trade_date, adj_factor`。
- 换算：`hfq = close × adj_factor`；`qfq = close × adj_factor / adj_factor_latest`；日收益率两者一致。
- 现有范式：ETF 的 `fetch_fund_adj` / `etf_adj_factor` / `upsert_adj_factors`。
- 关键读取入口：`stock-scoring-system` 的 `DatabaseManager.get_volume_price_data`（评分单点）；`quantFinance/routers/market.py`（展示/行情）。
