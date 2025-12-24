from quant_data_engine import create_baostock_minute_pipeline,create_minute_pipeline
if __name__ == "__main__":
    pipeline = create_minute_pipeline()
    # 示例：code='000001', market=1（深市），采集240条分钟数据
    df = pipeline.adapter.fetch_minute_bars(code='002050', market=0, start=0, count=720)
    print(df)
    pipeline.repository.upsert_minute_bars('002050', df)
    print("分钟数据采集与入库完成")