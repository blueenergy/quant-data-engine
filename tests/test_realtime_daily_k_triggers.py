import json

from quant_data_engine.config import settings as settings_module
from quant_data_engine.tasks import run_realtime_daily_k_sync as realtime_sync


def test_market_insight_trigger_settings_fallback_to_internal_key(monkeypatch):
    monkeypatch.delenv("MARKET_INSIGHT_TRIGGER_KEY", raising=False)
    monkeypatch.setenv("HERMES_QUANT_INTERNAL_KEY", "shared-secret")
    monkeypatch.setenv("MARKET_INSIGHT_TRIGGER_URL", "http://127.0.0.1:3001/api/market-insights/generate-internal")
    monkeypatch.setenv("MARKET_INSIGHT_TRIGGER_TIMEOUT_SEC", "7")
    settings_module.get_settings.cache_clear()

    settings = settings_module.get_settings()

    assert settings.market_insight_trigger_url.endswith("/api/market-insights/generate-internal")
    assert settings.market_insight_trigger_key == "shared-secret"
    assert settings.market_insight_trigger_timeout_sec == 7.0


def test_market_insight_trigger_settings_prefers_specific_key(monkeypatch):
    monkeypatch.setenv("HERMES_QUANT_INTERNAL_KEY", "shared-secret")
    monkeypatch.setenv("MARKET_INSIGHT_TRIGGER_KEY", "market-secret")
    settings_module.get_settings.cache_clear()

    settings = settings_module.get_settings()

    assert settings.market_insight_trigger_key == "market-secret"


def test_post_realtime_trigger_sends_expected_payload_and_headers(monkeypatch):
    captured = {}

    class FakeResponse:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    def fake_urlopen(request, timeout):
        captured["url"] = request.full_url
        captured["method"] = request.get_method()
        captured["payload"] = json.loads(request.data.decode("utf-8"))
        captured["headers"] = dict(request.header_items())
        captured["timeout"] = timeout
        return FakeResponse()

    monkeypatch.setattr(realtime_sync.urllib.request, "urlopen", fake_urlopen)

    realtime_sync._post_realtime_trigger(
        name="market insight",
        url="http://127.0.0.1:3001/api/market-insights/generate-internal",
        key="shared-secret",
        timeout_sec=3.5,
        trade_date="20260515",
        snapshot_time="202605151030",
        status="complete",
    )

    assert captured["url"].endswith("/api/market-insights/generate-internal")
    assert captured["method"] == "POST"
    assert captured["timeout"] == 3.5
    assert captured["payload"] == {
        "trade_date": "20260515",
        "snapshot_time": "202605151030",
        "source": "auto",
        "sync_status": "complete",
    }
    assert captured["headers"]["Content-type"] == "application/json"
    assert captured["headers"]["X-hermes-quant-key"] == "shared-secret"


def test_post_realtime_trigger_noops_without_url(monkeypatch):
    called = False

    def fake_urlopen(*args, **kwargs):
        nonlocal called
        called = True

    monkeypatch.setattr(realtime_sync.urllib.request, "urlopen", fake_urlopen)

    realtime_sync._post_realtime_trigger(
        name="market insight",
        url=None,
        key="shared-secret",
        timeout_sec=3.5,
        trade_date="20260515",
        snapshot_time="202605151030",
        status="complete",
    )

    assert called is False
