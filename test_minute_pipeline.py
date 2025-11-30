from quant_data_engine import create_minute_pipeline

if __name__ == "__main__":
    pipeline = create_minute_pipeline()
    # 示例：code='000001', market=1（深市），采集240条分钟数据
    df = pipeline.adapter.fetch_minute_bars(code='300722', market=0, start=0, count=24000)
    print(df)
    pipeline.repository.upsert_minute_bars('300722', df)
    print("分钟数据采集与入库完成")