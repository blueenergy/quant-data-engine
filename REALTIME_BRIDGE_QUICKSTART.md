# Windows Bridge 快速开始 ⚡

> 实时数据采集从未如此简单！

## 🚀 60 秒快速启动

### 1️⃣ 确保环境配置

```bash
# 检查 .env 文件
cat .env
# 应该包含：
# MONGO_URL=mongodb://localhost:27017
# MONGO_DB=quant_db
```

### 2️⃣ 启动 Bridge

```bash
cd quant_data_engine

# 自动模式（推荐）- 从数据库加载股票
python -m quant_data_engine.tasks.run_realtime_bridge --auto

# 手动模式 - 指定股票
python -m quant_data_engine.tasks.run_realtime_bridge --symbols 002050.SZ 600618.SH
```

### 3️⃣ 查看输出

```
================================================================================
🌉 MiniQMT Realtime Bridge (quant_data_engine)
================================================================================
📊 Auto mode: loaded 5 symbols from database
Symbols: 002050.SZ, 600618.SH, 000001.SZ, 600519.SH, 000858.SZ
Interval: 1.0s
Schedule: Enabled (9:30-15:00)
Debug: Off
================================================================================
[MiniQMTRealtime] Starting for 5 symbols
[MiniQMTRealtime] Subscribed to: 002050.SZ, 600618.SH, ...
[MiniQMTRealtime] Setting volume baseline...
[MiniQMTRealtime] Baseline set
[MiniQMTRealtime] Started
[RealtimePipeline] Starting...
[RealtimePipeline] Running
✅ Bridge running (Ctrl+C to stop)
================================================================================
✅ Saved 002050 202512131030 | C=15.23 V=12500
✅ Saved 600618 202512131030 | C=42.15 V=8900
```

---

## 📚 常用命令

### 自动模式（推荐）

```bash
# 标准运行
python -m quant_data_engine.tasks.run_realtime_bridge --auto

# Debug 模式（查看详细日志）
python -m quant_data_engine.tasks.run_realtime_bridge --auto --debug

# 24/7 运行（测试用，不限制交易时间）
python -m quant_data_engine.tasks.run_realtime_bridge --auto --no-schedule

# 调整轮询间隔（默认 1 秒）
python -m quant_data_engine.tasks.run_realtime_bridge --auto --interval 0.5

# 双写同步到远程云服务器数据库 (跨机房容灾 / 将本地 QMT 数据同步上云)
python -m quant_data_engine.tasks.run_realtime_bridge --auto --sync-uri "mongodb://admin:pwd@1.2.3.4:27017"
```

### 手动模式

```bash
# 单个股票
python -m quant_data_engine.tasks.run_realtime_bridge --symbols 002050.SZ

# 多个股票
python -m quant_data_engine.tasks.run_realtime_bridge --symbols 002050.SZ 600618.SH 000001.SZ

# 不带后缀也可以（自动添加）
python -m quant_data_engine.tasks.run_realtime_bridge --symbols 002050 600618
```

---

## 🐍 程序化使用

### 简单示例

```python
from quant_data_engine import create_realtime_pipeline

# 创建管线
pipeline = create_realtime_pipeline(
    symbols=["002050.SZ", "600618.SH"],
    interval_sec=1.0,
    enable_schedule=True
)

# 启动
pipeline.start()

# 运行...
import time
time.sleep(3600)  # 运行 1 小时

# 停止
pipeline.stop()
```

### 完整示例

```python
from quant_data_engine import (
    create_realtime_pipeline,
    get_settings,
    TradingScheduler
)
import signal
import sys

# 检查交易时间
scheduler = TradingScheduler()
if not scheduler.is_trading_time():
    print("非交易时间，等待市场开盘...")
    scheduler.wait_until_trading_time()

# 创建管线
pipeline = create_realtime_pipeline(
    symbols=["002050.SZ"],
    interval_sec=1.0,
    enable_schedule=True,
    debug=True
)

# 优雅停机
def signal_handler(sig, frame):
    print("\n停止中...")
    pipeline.stop()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

# 启动
pipeline.start()
print("Bridge 运行中...")

# 保持运行
signal.pause()
```

---

## ⚙️ 配置数据库股票列表

Bridge 从 `watchlist_strategies` 集合读取股票：

```javascript
// MongoDB 插入示例
db.watchlist_strategies.insertMany([
    {
        symbol: "002050",
        enabled: true,
        strategy: "grid",
        created_at: new Date()
    },
    {
        symbol: "600618",
        enabled: true,
        strategy: "grid",
        created_at: new Date()
    }
]);

// 启用/禁用股票
db.watchlist_strategies.updateOne(
    { symbol: "002050" },
    { $set: { enabled: true } }
);
```

---

## 🔍 检查数据

### MongoDB 查询

```javascript
// 查看最新的分钟数据
db.minute_bars.find().sort({ trade_date: -1 }).limit(10);

// 查看特定股票
db.minute_bars.find({ symbol: "002050" }).sort({ trade_date: -1 }).limit(10);

// 统计数据量
db.minute_bars.countDocuments({ symbol: "002050" });
```

### Python 查询

```python
from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017")
db = client["quant_db"]

# 最新的 10 条数据
latest = list(db.minute_bars.find().sort("trade_date", -1).limit(10))
for bar in latest:
    print(f"{bar['symbol']} {bar['trade_date']} C={bar['close']}")

# 特定股票
bars = list(db.minute_bars.find({"symbol": "002050"}).sort("trade_date", -1).limit(10))
```

---

## 🐛 故障排除

### 问题：xtquant 不可用

```
❌ Error: xtquant not available
```

**解决方案**：
1. 确保在 Windows 环境运行
2. 确保已安装 miniQMT
3. 检查 Python 环境是否能导入 xtquant

```python
# 测试 xtquant
python -c "from xtquant import xtdata; print('OK')"
```

### 问题：MongoDB 连接失败

```
❌ Error: connection refused
```

**解决方案**：
1. 检查 MongoDB 是否运行
2. 检查 `.env` 配置
3. 测试连接

```bash
# Windows
net start MongoDB

# 测试连接
mongosh mongodb://localhost:27017
```

### 问题：没有股票数据

```
⚠️ No symbols to monitor
```

**解决方案**：
1. 检查数据库 `watchlist_strategies` 集合
2. 确保有 `enabled: true` 的记录
3. 或使用手动模式指定股票

```bash
# 手动模式
python -m quant_data_engine.tasks.run_realtime_bridge --symbols 002050.SZ
```

---

## 📊 架构概览

```
miniQMT (Tick Data)
    ↓
MiniQMTRealtimeAdapter (聚合为 Bar)
    ↓
RealtimePipeline (数据处理)
    ↓
MinuteRepository (存储)
    ↓
MongoDB (minute_bars)
    ↓
UnifiedWorker (消费数据)
```

---

## 🎯 最佳实践

### ✅ 推荐做法

1. **使用自动模式**
   ```bash
   python -m quant_data_engine.tasks.run_realtime_bridge --auto
   ```

2. **启用交易时间调度**（省资源）
   ```python
   pipeline = create_realtime_pipeline(
       symbols=symbols,
       enable_schedule=True  # 非交易时间自动休眠
   )
   ```

3. **生产环境使用后台运行**
   ```bash
   # Linux/WSL
   nohup python -m quant_data_engine.tasks.run_realtime_bridge --auto > bridge.log 2>&1 &
   
   # Windows (PowerShell)
   Start-Process python -ArgumentList "-m quant_data_engine.tasks.run_realtime_bridge --auto" -WindowStyle Hidden
   ```

### ❌ 避免做法

1. **不要在非交易时间使用 --no-schedule**（浪费资源）
2. **不要设置过小的轮询间隔**（< 0.5s 可能影响性能）
3. **不要忘记配置 MongoDB**（会导致数据无法保存）

---

## 📖 更多资源

- [集成方案详细文档](file:///c:/Users/Shuyong%20Lin/code/stock-execution-system/docs/windows_bridge_integration_plan.md)
- [实施总结](file:///c:/Users/Shuyong%20Lin/code/stock-execution-system/docs/windows_bridge_implementation_summary.md)
- [使用示例](file:///c:/Users/Shuyong%20Lin/code/quant_data_engine/examples/realtime_bridge_example.py)
- [测试脚本](file:///c:/Users/Shuyong%20Lin/code/quant_data_engine/test_realtime.py)

---

**需要帮助？** 查看 [完整文档](file:///c:/Users/Shuyong%20Lin/code/quant_data_engine/README.md) 或运行测试：

```bash
python test_realtime.py
python examples/realtime_bridge_example.py --example 4
```
