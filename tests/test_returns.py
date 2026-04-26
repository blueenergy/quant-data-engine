from quant_data_engine.utils.returns import n_trading_day_return_percent


def test_n_trading_day_return():
    # 5 日一条，从 10 到 12；近 2 根相对前 2 根： (12/10-1)*100
    s = [("20240101", 10.0), ("20240102", 10.5), ("20240103", 11.0), ("20240104", 11.5), ("20240105", 12.0)]
    r = n_trading_day_return_percent(s, "20240105", n=4)
    assert r is not None
    assert abs(r - (12.0 / 10.0 - 1) * 100) < 1e-6
