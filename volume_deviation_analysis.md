# 成交量系统性偏差分析

## 问题描述

成交量比同花顺略少，例如：
- 实际：5085 手
- 同花顺：5140 手
- 偏差：-55 手 (-1.1%)

## 关键疑问

**系统性问题**：监控股票数量增加后，偏差是否会放大？

这是判断问题根源的关键：
- 如果偏差比例保持稳定（~1-2%）→ 数据源差异或采样频率不足
- 如果偏差随股票数增加而放大 → 系统资源瓶颈或tick竞争

## 可能原因分析

### 1. 数据源本身的差异（1-2%是正常的）

**说明**：
- miniQMT和同花顺可能连接不同的交易所服务器
- 数据到达时间略有差异
- 小额交易可能被不同程度地聚合

**判断方法**：
- 对比单只股票全天总成交量
- 如果差异稳定在1-2%，属于正常数据源差异

### 2. 轮询采样频率导致tick漏失

**说明**：
- 0.5秒轮询间隔内可能有多笔交易
- `get_full_tick()`只返回最新tick
- 中间的tick会被漏掉

**判断方法**：
- 检查Tick Coverage是否>95%
- 对比高频股票vs低频股票的偏差
- 如果高频股票偏差更大，说明采样频率不足

### 3. 多股票监控时的资源竞争

**关键测试**：
```
# 测试1：单股票监控
python -m quant_data_engine.tasks.run_realtime_bridge --symbols 002050 --no-schedule

# 测试2：10只股票监控
python -m quant_data_engine.tasks.run_realtime_bridge --auto --no-schedule

# 对比002050的偏差是否增大
```

**如果多股票时偏差增大**：
- 可能是网络带宽不足
- 可能是miniQMT响应变慢
- 可能是Python处理速度跟不上

### 4. 分钟聚合边界问题

**说明**：
- Tick时间戳不准确
- 导致边界tick被聚合到错误的分钟

**判断方法**：
- 检查Volume Debug中的delta是否合理
- 检查是否有异常的大跳变

## 对比测试方案

### 测试A：单只vs多只股票

```bash
# 1. 单只股票（002050）
python -m quant_data_engine.tasks.run_realtime_bridge --symbols 002050 --no-schedule --interval 0.5

# 观察10分钟，记录：
# - 每分钟成交量
# - Tick Coverage
# - 与同花顺的偏差

# 2. 10只股票
python -m quant_data_engine.tasks.run_realtime_bridge --auto --no-schedule --interval 0.5

# 观察同样的002050数据
# 对比偏差是否增大
```

### 测试B：不同轮询间隔

```bash
# 1. 0.3秒间隔
python -m quant_data_engine.tasks.run_realtime_bridge --symbols 002050 --no-schedule --interval 0.3

# 2. 0.5秒间隔（当前默认）
python -m quant_data_engine.tasks.run_realtime_bridge --symbols 002050 --no-schedule --interval 0.5

# 3. 1.0秒间隔
python -m quant_data_engine.tasks.run_realtime_bridge --symbols 002050 --no-schedule --interval 1.0

# 对比偏差变化
```

### 测试C：全天累计对比

```javascript
// MongoDB查询全天总成交量
use finance
db.minute_bars.aggregate([
  {$match: {symbol: '002050', trade_date: /^20251215/}},
  {$group: {_id: null, total: {$sum: "$volume"}}}
])

// 对比同花顺显示的全天总成交量
// 计算偏差百分比
```

## 诊断清单

- [ ] 单只股票的Tick Coverage是多少？
- [ ] 多只股票的Tick Coverage是否下降？
- [ ] 偏差是否随股票数量增加？
- [ ] 偏差是否随轮询间隔变化？
- [ ] 全天累计偏差是否稳定？
- [ ] 高频股票vs低频股票偏差对比？

## 预期结果判断

### 情况1：数据源差异（可接受）
- 单只和多只股票偏差相同（1-2%）
- 不同轮询间隔偏差相同
- 全天累计偏差稳定
- **结论**：这是miniQMT和同花顺的数据源差异，属于正常现象

### 情况2：采样频率不足（可优化）
- 轮询间隔越短偏差越小
- 高频股票偏差更大
- Tick Coverage <90%
- **解决**：缩短轮询间隔到0.2-0.3秒

### 情况3：系统瓶颈（需要优化架构）
- 多股票时偏差显著增大
- Tick Coverage随股票数下降
- **解决**：
  - 分拆adapter（每个adapter监控少量股票）
  - 优化miniQMT连接
  - 考虑使用更高性能的数据源

## 下一步行动

1. **先做测试A**：确认是否有系统性放大效应
2. **记录数据**：至少观察30分钟连续数据
3. **提供结果**：
   - Tick Coverage范围
   - 偏差变化趋势
   - 是否与股票数量相关

---

**关键判断**：
- 如果1-2%的偏差在任何情况下都稳定 → 这是数据源差异，无需优化
- 如果偏差随股票数增加 → 需要架构优化
- 如果偏差随间隔缩短而减小 → 需要更高采样频率
