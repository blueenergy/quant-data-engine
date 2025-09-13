# 使用示例
from quant_data_engine import create_data_engine

# 创建数据引擎
engine = create_data_engine('./config.json')

# 批量采集并存储数据
symbols = ['000001', '000002', '600036']
data_types = ['daily', 'financial', 'money_flow']

results = await engine.collect_and_store(symbols, data_types)

# 获取数据
daily_data = await engine.storage.get_stock_data('000001', 'daily', '2024-01-01', '2024-09-10')

# 启动 API 服务
if hasattr(engine, 'api'):
    import uvicorn
    uvicorn.run(engine.api.app, host="0.0.0.0", port=8000)