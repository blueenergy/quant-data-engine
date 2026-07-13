import mongomock

from quant_data_engine.services.internal_signal_enqueue import (
    enqueue_enabled,
    enqueue_internal_signal_refresh,
)


def test_enqueue_skips_pending_or_processing_duplicates(monkeypatch):
    monkeypatch.delenv("ENABLE_INTERNAL_SIGNAL_ENQUEUE", raising=False)
    client = mongomock.MongoClient()
    finance = client["finance"]
    col = finance["analysis_tasks"]
    col.insert_one(
        {
            "type": "internal_signal_refresh",
            "params": {"symbol": "600519.SH"},
            "status": "pending",
        }
    )

    result = enqueue_internal_signal_refresh(
        finance,
        ["600519.SH", "000001.SZ"],
    )

    assert result["enqueued"] == 1
    assert result["skipped"] == 1
    assert col.count_documents({"type": "internal_signal_refresh"}) == 2


def test_enqueue_enabled_defaults_to_false(monkeypatch):
    monkeypatch.delenv("ENABLE_INTERNAL_SIGNAL_ENQUEUE", raising=False)
    assert enqueue_enabled() is False
