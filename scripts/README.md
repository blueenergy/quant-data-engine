# quant-data-engine scripts

本目录收纳 **仓库外可执行脚本**（非 `python -m quant_data_engine.tasks.*` 容器/cron 任务）。

## 布局

| 路径 | 用途 |
|------|------|
| `qmt/` | Windows QMT bridge、本机实例配置、RPC 探针 |
| `data/` | Mongo 迁移、Tushare 探针、符号规范化等数据运维 |
| `backfill_report_type2.sh` | 财务 `report_type=2` 历史回填 shell |

**容器任务**（日频同步、分钟下载、回填 CLI 等）仍在 `src/quant_data_engine/tasks/`，通过 `python -m quant_data_engine.tasks.<name>` 调用。

## QMT（`scripts/qmt/`）

| 文件 | 说明 |
|------|------|
| `run_miniqmt_bridge.py` | miniQMT / Big QMT RPC 盘中 bridge 入口 |
| `qmt_instances.example.json` | 本机 quote/trade 安装路径模板 → 复制为 `qmt_instances.local.json` |
| `miniqmt_bridge*.example.json` | targeted / full_market bridge 配置模板 |
| `bigqmt_rpc_bridge.full_market.example.json` | 双实例 Big QMT RPC shadow/切流模板 |
| `probe_bigqmt_rpc_ticks.py` | RPC `get_ticks` 延迟与 payload 探针 |
| `probe_tick_source_alignment.py` | miniQMT vs Big QMT 同票对齐探针 |

gitignored 本机文件（从 example 复制后编辑）：

- `qmt/qmt_instances.local.json`
- `qmt/miniqmt_bridge.json`
- `qmt/bigqmt_rpc_bridge.json`

`quantTrader/scripts/sync_qmt_rpc_deployment.py` 读取 `qmt_instances.local.json` 同步 RPC 包到 QMT `python\\`。

## 数据运维（`scripts/data/`）

详见各脚本头部 docstring。常用：

- `update_symbol_with_suffix_generic.py` — 为 Mongo 符号补 `.SH`/`.SZ` 后缀
- `restore_collection_to_remote.sh` — 本地 collection dump 后经 SSH 恢复到远端
- `migrate_to_quant_data.py` / `cleanup_old_collections.py` — 库迁移与清理
- `probe_tushare_*.py` / `probe_fina_indicator_fields.py` — Tushare 字段探针

## 从旧路径迁移

若本机已有 gitignored 配置在旧位置，请移动到 `scripts/qmt/`：

```text
scripts/qmt_instances.local.json      → scripts/qmt/qmt_instances.local.json
scripts/miniqmt_bridge.json           → scripts/qmt/miniqmt_bridge.json
scripts/bigqmt_rpc_bridge.json        → scripts/qmt/bigqmt_rpc_bridge.json
```

原 `tools/` 下脚本已迁至 `scripts/data/` 或 `scripts/qmt/`（探针）。
