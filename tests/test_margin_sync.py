import pandas as pd

from quant_data_engine.tasks.run_margin_sync import (
    aggregate_sse_szse,
    rows_from_df,
)


def test_rows_from_df_skips_nan_and_missing_keys():
    df = pd.DataFrame(
        [
            {"trade_date": "20260105", "exchange_id": "SSE", "rzye": 100.0, "rzrqye": 110.0, "rqye": float("nan")},
            {"trade_date": None, "exchange_id": "SZSE", "rzye": 1.0},
            {"trade_date": "20260105", "exchange_id": "SZSE", "rzye": 50.0, "rzrqye": 55.0, "rqye": 5.0},
        ]
    )
    rows = rows_from_df(df)
    assert len(rows) == 2
    sse = next(r for r in rows if r["exchange_id"] == "SSE")
    assert sse["rzye"] == 100.0
    assert sse["rqye"] is None
    assert sse["rzrqye"] == 110.0


def test_aggregate_sse_szse_requires_both_exchanges():
    rows = [
        {"trade_date": "20260105", "exchange_id": "SSE", "rzye": 100.0, "rzrqye": 110.0, "rqye": 10.0, "rzmre": 1.0, "rzche": 1.0, "rqmcl": 1.0, "rqyl": 1.0},
        {"trade_date": "20260105", "exchange_id": "SZSE", "rzye": 40.0, "rzrqye": 44.0, "rqye": 4.0, "rzmre": 2.0, "rzche": 2.0, "rqmcl": 2.0, "rqyl": 2.0},
        {"trade_date": "20260106", "exchange_id": "SSE", "rzye": 90.0, "rzrqye": 99.0, "rqye": 9.0, "rzmre": 1.0, "rzche": 1.0, "rqmcl": 1.0, "rqyl": 1.0},
        {"trade_date": "20260105", "exchange_id": "BSE", "rzye": 3.0, "rzrqye": 3.0, "rqye": 0.0, "rzmre": 0.0, "rzche": 0.0, "rqmcl": 0.0, "rqyl": 0.0},
    ]
    aggregated = aggregate_sse_szse(rows)
    assert len(aggregated) == 1
    item = aggregated[0]
    assert item["trade_date"] == "20260105"
    assert item["exchange_id"] == "ALL"
    assert item["rzye"] == 140.0
    assert item["rzrqye"] == 154.0
    assert item["rqye"] == 14.0
