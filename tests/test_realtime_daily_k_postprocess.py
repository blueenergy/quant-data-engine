from types import SimpleNamespace

from quant_data_engine.tasks.realtime_daily_k_postprocess import post_realtime_daily_k_snapshot


class _Client:
    def __init__(self):
        self.updates = []
        self.closed = False

    def __getitem__(self, name):
        return self

    def update_one(self, query, update):
        self.updates.append((query, update))
        return SimpleNamespace(matched_count=1)

    def close(self):
        self.closed = True


def test_post_realtime_daily_k_snapshot_writes_quality_and_triggers():
    repo = SimpleNamespace(
        upsert_daily=lambda records: 3,
        upsert_realtime_latest=lambda records: 2,
    )
    recorded = {}
    triggers = []
    client = _Client()
    settings = SimpleNamespace(
        mongo_uri="mongodb://localhost",
        data_db="quant_data",
        realtime_db="quant_realtime",
        realtime_daily_k_collection="market_spectrum_realtime_daily",
        realtime_daily_k_min_coverage=0.5,
        realtime_daily_k_min_field_coverage=0.5,
    )
    records = [
        {
            "symbol": "000001.SZ",
            "open": 10,
            "high": 11,
            "low": 9,
            "close": 10.5,
            "pre_close": 10,
            "pct_chg": 5,
            "volume": 1,
        }
    ]

    result = post_realtime_daily_k_snapshot(
        settings=settings,
        repo=repo,
        records=records,
        trade_date="20260619",
        snapshot_time="202606191030",
        expected_symbols=1,
        source_mode="miniqmt_full_market_daily",
        mongo_client_factory=lambda uri: client,
        sentiment_func=lambda **kwargs: {"success": True, "results": [{"yang_spectrum": 0.5}]},
        sector_func=lambda **kwargs: {"success": True, "written": 4, "results": []},
        record_func=lambda settings, task_type, trade_date, **kwargs: recorded.update(
            {"task_type": task_type, "trade_date": trade_date, **kwargs}
        ),
        trigger_t0_func=lambda *args: triggers.append(("t0", args)),
        trigger_insight_func=lambda *args: triggers.append(("insight", args)),
    )

    assert result.quality_passed is True
    assert result.status == "complete"
    assert recorded["task_type"] == "a_daily_realtime_sync"
    assert recorded["source_mode"] == "miniqmt_full_market_daily"
    assert recorded["full_market_derivatives_enabled"] is True
    assert recorded["fallback_decision"] == "skip_eod_allowed"
    assert client.updates
    assert [name for name, _ in triggers] == ["t0", "insight"]
