"""Webhook alerting, which is the only channel that works from the cluster:
outbound is restricted to 80/443, so SMTP ports time out.
"""
import pytest

from quant_data_engine.contracts.notifier import ContractNotifier

WECOM_URL = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=abc123"
PLAIN_URL = "https://example.com/hooks/data-contract"


class _Resp:
    def __init__(self, status_code=200, body=None, json_raises=False):
        self.status_code = status_code
        self._body = body if body is not None else {}
        self._json_raises = json_raises

    def json(self):
        if self._json_raises:
            raise ValueError("not json")
        return self._body


@pytest.fixture
def notifier(monkeypatch):
    def _make(url):
        monkeypatch.setenv("DATA_CONTRACT_WEBHOOK_URL", url)
        return ContractNotifier()
    return _make


def _capture_post(monkeypatch, resp):
    """Stand in for requests.post and record what was sent."""
    sent = {}

    class _FakeRequests:
        @staticmethod
        def post(url, json=None, timeout=None):
            sent["url"] = url
            sent["json"] = json
            return resp

    monkeypatch.setitem(__import__("sys").modules, "requests", _FakeRequests)
    return sent


def test_wecom_url_gets_the_bot_markdown_schema(notifier, monkeypatch):
    sent = _capture_post(monkeypatch, _Resp(body={"errcode": 0, "errmsg": "ok"}))
    n = notifier(WECOM_URL)

    result = n.notify(["webhook"], "合约违约", {"contract": "hk_hold_sync"})

    assert result["webhook"] == "sent"
    assert sent["json"]["msgtype"] == "markdown"
    assert "合约违约" in sent["json"]["markdown"]["content"]
    assert "hk_hold_sync" in sent["json"]["markdown"]["content"]


def test_other_urls_keep_the_generic_body(notifier, monkeypatch):
    sent = _capture_post(monkeypatch, _Resp(status_code=204))
    n = notifier(PLAIN_URL)

    result = n.notify(["webhook"], "合约违约", {"contract": "hk_hold_sync"})

    assert result["webhook"] == "status:204"
    assert sent["json"] == {"subject": "合约违约", "summary": {"contract": "hk_hold_sync"}}


def test_wecom_rejection_is_reported_despite_http_200(notifier, monkeypatch):
    """WeCom answers 200 with an errcode when it refuses; that is not a success."""
    _capture_post(monkeypatch, _Resp(body={"errcode": 93000, "errmsg": "invalid webhook url"}))
    n = notifier(WECOM_URL)

    result = n.notify(["webhook"], "合约违约", {"contract": "hk_hold_sync"})

    assert result["webhook"].startswith("error:93000")


def test_wecom_non_json_response_is_an_error(notifier, monkeypatch):
    _capture_post(monkeypatch, _Resp(json_raises=True))
    n = notifier(WECOM_URL)

    result = n.notify(["webhook"], "合约违约", {"contract": "x"})

    assert result["webhook"].startswith("error:non-json")


def test_status_is_highlighted_in_the_markdown(notifier, monkeypatch):
    sent = _capture_post(monkeypatch, _Resp(body={"errcode": 0}))
    n = notifier(WECOM_URL)

    n.notify(["webhook"], "合约违约", {"status": "violated", "contract": "x"})

    content = sent["json"]["markdown"]["content"]
    assert '<font color="warning">violated</font>' in content
    assert "> contract: x" in content


def test_webhook_without_url_stays_disabled(notifier, monkeypatch):
    monkeypatch.delenv("DATA_CONTRACT_WEBHOOK_URL", raising=False)
    n = ContractNotifier()

    assert n.notify(["webhook"], "s", {})["webhook"] == "disabled"
