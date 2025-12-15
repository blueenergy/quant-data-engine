# Timestamp & Volume Fix - Final Implementation

## 问题摘要

quant_data_engine中的实时数据采集存在两个问题：

### 问题1：时间戳错误
```
'trade_date': '197001211829'  # ❌ 错误的时间戳（1970年）
```

### 问题2：成交量计算不准确
与同花顺数据对比，成交量存在偏差

## 根本原因分析

### 时间戳问题

#### 原因1：miniQMT时间戳无效
- miniQMT有时返回 `time: 0` 或无效值
- 原始代码只检查 `int(time_ms) > 0`，但0被识别为布尔False
- 即使检查通过，0转换成1970年

#### 原因2：Repository时间戳单位错误
- Adapter输出的是**秒时间戳**（例如：1765763183.0）
- Repository假设是**毫秒时间戳**，使用 `unit='ms'` 转换
- 导致1765763.183毫秒 → 1970年1月21日

### 成交量问题

成交量计算逻辑本身是正确的（与旧版本一致），但可能存在以下问题：
1. Baseline设置时机
2. 跨天重置逻辑
3. 集合竞价成交量处理
4. 调试信息不足，难以排查问题

## 修复方案

### 修复1：mini_qmt_realtime_adapter.py - 时间戳验证

**改进的时间戳验证逻辑** (第186-220行)：

```python
def _process_tick(self, sym: str, tick: dict, now_ts: float):
    # Use exchange timestamp if available and valid
    time_ms = tick.get("time")
    ts = now_ts  # Default to current time
    
    if time_ms:
        try:
            time_ms_int = int(time_ms)
            # Only use if it's a reasonable timestamp (not 0, not negative, not from 1970)
            # Reasonable timestamps are after year 2000 (946684800 seconds since epoch)
            if time_ms_int > 946684800000:  # January 1, 2000 in milliseconds
                ts = float(time_ms_int) / 1000.0
        except (ValueError, TypeError):
            # If conversion fails, use current time
            ts = now_ts
```

**改进内容**：
- ✅ 检查时间戳是否>2000年（排除0和1970年）
- ✅ 处理转换异常
- ✅ 明确默认为当前时间

### 修复2：realtime_pipeline.py - 二层时间戳验证

**二层验证** (第37-60行)：

```python
def _on_bar(self, symbol: str, bar: Bar):
    # ...
    # Ensure bar.datetime is valid (not 0 or from 1970)
    bar_ts = float(bar.datetime)
    if bar_ts <= 0 or bar_ts < 946684800:  # Before year 2000
        bar_ts = time.time()
    
    # Use validated bar_ts for all calculations
```

**改进内容**：
- ✅ Pipeline对bar.datetime进行二次验证
- ✅ 双重防护：如果adapter传来无效时间戳，pipeline会修正

### 修复3：minute_repository.py - 自动检测时间戳单位

**智能时间戳单位检测** (第35-50行)：

```python
# Convert time column
if df['datetime'].dtype in ['float64', 'int64', 'float', 'int']:
    # Detect if it's millisecond or second timestamp
    # Timestamps < 100000000000 (100 billion) are likely seconds
    # Timestamps >= 100000000000 are likely milliseconds
    is_ms = (df['datetime'] >= 100000000000).any()
    
    if is_ms:
        # Convert from milliseconds to Beijing time (UTC+8)
        df['datetime'] = pd.to_datetime(df['datetime'], unit='ms') + pd.Timedelta(hours=8)
    else:
        # Convert from seconds to Beijing time (UTC+8)
        df['datetime'] = pd.to_datetime(df['datetime'], unit='s') + pd.Timedelta(hours=8)
```

**改进内容**：
- ✅ 自动检测时间戳是秒还是毫秒
- ✅ 兼容adapter输出的秒时间戳
- ✅ 兼容历史数据的毫秒时间戳

### 修复4：mini_qmt_realtime_adapter.py - 成交量调试

**增加成交量调试信息** (第220-237行)：

```python
# Debug volume calculation when enabled
if self.debug:
    print(
        f"[Volume Debug] {sym}: raw={cumulative_vol:,} "
        f"baseline={self._baseline_volume.get(sym, 0):,} "
        f"adjusted={adjusted_cumulative:,} prev={prev_vol:,} delta={vol_delta:,}"
    )
```

**改进内容**：
- ✅ 显示原始累计成交量
- ✅ 显示baseline值
- ✅ 显示调整后的累计成交量
- ✅ 显示增量计算过程

## 验证方法

### 1. 运行时间处理测试
```bash
cd c:\Users\Shuyong Lin\code\quant_data_engine
python test_time_fix.py
```

预期输出：
- ✅ 所有时间测试通过
- ✅ 无1970年日期

### 2. 启动实时adapter
```bash
python -m quant_data_engine.tasks.run_realtime_bridge --auto --debug --no-schedule
```

观察日志：
```
✅ Saved 002050 202512151000 | C=44.44 V=5377
```
- ✅ `trade_date` 格式正确
- ✅ 日期是当前日期，不是1970年

### 3. 检查MongoDB数据
```javascript
use finance
db.minute_bars.findOne({}, {_id: 0, symbol: 1, trade_date: 1})
```

预期结果：
```javascript
{
  symbol: '002050',
  trade_date: '202512151000'  // ✅ 当前日期时间，不是1970年
}
```

## 技术细节

### 时间戳验证标准

| 检查项 | 标准 | 说明 |
|--------|------|------|
| 非零值 | > 0 | 排除0和负数 |
| 合理范围 | > 946684800000ms (2000年) | 排除1970年的数据 |
| 格式 | 毫秒整数 | miniQMT格式 |

### 修复后的数据流

```
miniQMT Tick Data
    ↓ (可能包含time=0或无效值)
Adapter._process_tick()
    ↓ (进行严格时间戳验证)
    ✓ 若有效，使用miniQMT时间戳
    ✓ 若无效，使用系统当前时间
    ↓
Bar Object (包含有效的datetime)
    ↓
Pipeline._on_bar()
    ↓ (进行二次验证)
    ✓ 若bar.datetime有效，使用它
    ✓ 若bar.datetime无效，使用系统当前时间
    ↓
MongoDB (保证trade_date总是有效的当前日期)
```

## 相关文件

- [mini_qmt_realtime_adapter.py](./adapters/mini_qmt_realtime_adapter.py) - 第一层时间戳验证
- [realtime_pipeline.py](./pipelines/realtime_pipeline.py) - 第二层时间戳验证
- [test_time_fix.py](./test_time_fix.py) - 时间处理测试脚本

## 部署说明

修改后需要：
1. **重启adapter**：新的验证逻辑才能生效
2. **清理旧数据**（可选）：MongoDB中的1970年数据不会自动删除，需要手动清理
3. **验证新数据**：检查重启后生成的数据是否正确

## 后续监控

推荐在MongoDB中设置报警：

```javascript
// 检查是否有1970年的新数据
db.minute_bars.find({
  trade_date: /^1970/
})

// 应该返回空结果
```

---

**修复完成时间**：2025-12-15 10:25
**测试状态**：✅ 通过
**部署状态**：已修复

## 最新改进（针对成交量偏小问题）

### 问题：成交量比同花顺略小

**根本原因**：
- 轮询间隔1秒可能漏掉部分高频tick
- `get_full_tick()`只返回最新tick，两次轮询间的交易可能被漏掉

**解决方案**：
1. **缩短轮询间隔**：从1.0秒改为0.5秒
2. **添加tick采样统计**：显示tick覆盖率
3. **监控数据质量**：通过覆盖率判断是否漏tick

### 新增功能

**Tick Coverage统计**：
```
[1-Min Bar] 2025-12-15 10:30 | 002050.SZ
  Volume: 5,432 手
  📊 Tick Coverage: 58/60 polls (96.7%)
```

**解读**：
- 60次轮询中有58次成功获取到tick
- 覆盖率96.7%表示数据质量很好
- 如果覆盖率<90%，可能需要进一步缩短轮询间隔

### 使用建议

**标准模式**（推荐）：
```bash
python -m quant_data_engine.tasks.run_realtime_bridge --auto --no-schedule
# 默认0.5秒轮询
```

**高频模式**（成交极活跃的股票）：
```bash
python -m quant_data_engine.tasks.run_realtime_bridge --auto --no-schedule --interval 0.3
# 0.3秒轮询，更密集采样
```

**低频模式**（成交清淡的股票）：
```bash
python -m quant_data_engine.tasks.run_realtime_bridge --auto --no-schedule --interval 1.0
# 1.0秒轮询，节省资源
```
