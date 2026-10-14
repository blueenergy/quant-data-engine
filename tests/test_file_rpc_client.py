"""The file mailbox client, against a stand-in for the strategy's own reader.

``FakeStrategy`` below is transcribed from ``FileTransport._serve_client`` in
xtquant_big_convert, same steps in the same order. That matters more than usual
here: the real server lives inside QMT and can only be redeployed by hand, so a
test that agreed with a *convenient* mock instead of the protocol would pass
while every production call timed out.
"""
from __future__ import annotations

import json
import threading
import time

import pytest

from quant_data_engine.bridges.bigqmt.file_rpc_client import (
    IN_PROGRESS_STATUS,
    REQUEST_NAME,
    RESPONSE_NAME,
    BigQmtFileRpcClient,
)
from quant_data_engine.bridges.bigqmt.rpc_client_factory import create_quote_rpc_client
from quant_data_engine.bridges.bigqmt.wire_codec import encode_rpc_request_payload
from quant_data_engine.bridges.bigqmt.zmq_rpc_client import (
    BigQmtZmqRpcClient,
    RpcTransportError,
    RpcTransportTimeout,
)


def _write(path, payload):
    path.write_bytes(json.dumps(payload, ensure_ascii=False).encode("utf-8"))


class FakeStrategy:
    """The strategy's mailbox reader, minus QMT."""

    def __init__(self, mailbox, handler=None):
        self.mailbox = mailbox
        self.handler = handler or (lambda method, params: {"echo": method})
        self.served = []
        self._stop = threading.Event()
        self._thread = None

    # -- the protocol, as FileTransport implements it ----------------------
    def serve_once(self):
        request = json.loads((self.mailbox / REQUEST_NAME).read_text(encoding="utf-8"))
        request_id = request["request_id"]
        response_path = self.mailbox / RESPONSE_NAME
        existing = None
        if response_path.exists():
            existing = json.loads(response_path.read_text(encoding="utf-8"))
        # Already answered -> skip, never re-run (the mailbox is stateless and
        # at-most-once; this is the whole of its bookkeeping).
        if existing is not None and existing.get("request_id") == request_id:
            return None
        # Claim it BEFORE running the handler, so a server that dies inside the
        # handler leaves the request marked handled.
        _write(response_path, {"schema_version": 1, "request_id": request_id,
                               "status": IN_PROGRESS_STATUS})
        self.before_answer()
        try:
            data = self.handler(request["method"], request.get("params") or {})
            ok, error = True, ""
        except Exception as exc:  # noqa: BLE001 - deliver() does the same
            # base.RpcTransport.deliver turns a handler exception into an
            # ok=False envelope so the receive loop survives.
            data, ok, error = None, False, "%s: %s" % (type(exc).__name__, exc)
        response = {
            "schema_version": 1,
            "request_id": request_id,
            "account_id": request.get("account_id", ""),
            "method": request["method"],
            "ok": ok,
            "data": data,
            "error": error,
            "server_error": "",
            "handled_at": "2026-10-15 09:31:00",
        }
        _write(response_path, response)
        self.served.append(request)
        return request

    def start(self, interval=0.005):
        def loop():
            while not self._stop.is_set():
                try:
                    if (self.mailbox / REQUEST_NAME).exists():
                        self.serve_once()
                except FileNotFoundError:
                    pass
                time.sleep(interval)

        self._thread = threading.Thread(target=loop, daemon=True)
        self._thread.start()
        return self

    def before_answer(self):
        """Hook to stall between claiming the request and answering it."""

    def stop(self):
        self._stop.set()
        if self._thread is not None:
            self._thread.join(2.0)


@pytest.fixture
def mailbox(tmp_path):
    """A mailbox directory plus the QMT-side root it lives under."""
    root = tmp_path / "bqbox"
    root.mkdir()
    box = root / "dayk"
    return box


def test_round_trip_through_a_real_mailbox(mailbox):
    strategy = FakeStrategy(mailbox, handler=lambda m, p: {"600000.SH": {"lastPrice": 10.5}})
    strategy.start()
    try:
        client = BigQmtFileRpcClient(mailbox=str(mailbox), account_id="8885156549")
        ticks = client.get_ticks(["600000.SH"], timeout_seconds=5.0)
    finally:
        strategy.stop()
    assert ticks == {"600000.SH": {"lastPrice": 10.5}}
    assert len(strategy.served) == 1
    assert strategy.served[0]["method"] == "get_ticks"
    assert strategy.served[0]["account_id"] == "8885156549"


def test_the_request_on_disk_is_plain_json_not_the_zmq_wire_format(mailbox):
    """The one asymmetry between the two transports, pinned.

    ZMQ frames go out base64-wrapped. The strategy reads req.json with a bare
    ``json.loads`` and *ignores* anything that is not a JSON object, so a base64
    string would sit there unread and every call would time out — with no error
    on either end, because nothing is technically broken. Reusing
    ``encode_rpc_request_payload`` here "for consistency" is exactly the change
    this test exists to catch.
    """
    strategy = FakeStrategy(mailbox)
    strategy.start()
    try:
        client = BigQmtFileRpcClient(mailbox=str(mailbox))
        client.ping(timeout_seconds=5.0)
    finally:
        strategy.stop()
    raw = (mailbox / REQUEST_NAME).read_text(encoding="utf-8")
    request = json.loads(raw)
    assert isinstance(request, dict), "the strategy ignores anything but a JSON object"
    assert request["method"] == "ping"
    assert raw != encode_rpc_request_payload(request)
    assert not raw.startswith("b64s:")


def test_the_in_progress_stub_is_not_mistaken_for_an_answer(mailbox):
    """A stall between claim and answer must not read as success.

    The stub carries the same request_id as the real envelope, so matching on
    the id alone would hand the caller an empty dict and the sink would write
    it. The client has to wait for the answer itself.
    """
    released = threading.Event()
    entered = threading.Event()

    class Slow(FakeStrategy):
        def before_answer(self):
            entered.set()
            released.wait(5.0)

    strategy = Slow(mailbox, handler=lambda m, p: {"600000.SH": {"lastPrice": 11.0}})
    strategy.start()
    try:
        client = BigQmtFileRpcClient(mailbox=str(mailbox))
        outcome = {}

        def call():
            outcome["ticks"] = client.get_ticks(["600000.SH"], timeout_seconds=5.0)

        caller = threading.Thread(target=call)
        caller.start()
        assert entered.wait(3.0), "the strategy never picked the request up"
        # Stub is on disk now and the handler has not run yet.
        stub = json.loads((mailbox / RESPONSE_NAME).read_text(encoding="utf-8"))
        assert stub["status"] == IN_PROGRESS_STATUS
        assert caller.is_alive(), "the stub was taken for the answer"
        released.set()
        caller.join(5.0)
        assert not caller.is_alive()
    finally:
        released.set()
        strategy.stop()
    assert outcome["ticks"] == {"600000.SH": {"lastPrice": 11.0}}


def test_a_timeout_raises_the_class_the_sources_catch(mailbox):
    """Identity, not likeness: source.py/kline_source.py catch this exact class.

    A look-alike defined in this module would turn every transport timeout into
    an unhandled exception that skips the retry path entirely.
    """
    client = BigQmtFileRpcClient(mailbox=str(mailbox))
    with pytest.raises(RpcTransportTimeout) as excinfo:
        client.get_ticks(["600000.SH"], timeout_seconds=0.05)
    assert isinstance(excinfo.value, RpcTransportError)
    assert "get_ticks" in str(excinfo.value)


def test_an_error_envelope_raises_a_transport_error(mailbox):
    strategy = FakeStrategy(mailbox, handler=None)
    strategy.handler = lambda m, p: (_ for _ in ()).throw(RuntimeError("boom"))
    strategy.start()
    try:
        client = BigQmtFileRpcClient(mailbox=str(mailbox))
        with pytest.raises(RpcTransportError):
            client.ping(timeout_seconds=5.0)
    finally:
        strategy.stop()


def test_a_dead_run_s_leftovers_are_gone_before_the_first_request(mailbox):
    """A stale req.json is an unanswered request whose client is gone.

    No strategy runs in this test on purpose: a live one could pick the leftover
    up in the ~20ms between process start and the first call, which is a real
    race the mailbox does not try to win. What the client owes us is that its
    own first request is not queued behind a dead one.
    """
    mailbox.mkdir()
    _write(mailbox / REQUEST_NAME, {"request_id": "dead", "method": "download_history_data"})
    _write(mailbox / RESPONSE_NAME, {"request_id": "dead", "ok": True})
    client = BigQmtFileRpcClient(mailbox=str(mailbox))
    with pytest.raises(RpcTransportTimeout):
        client.ping(timeout_seconds=0.05)
    on_disk = json.loads((mailbox / REQUEST_NAME).read_text(encoding="utf-8"))
    assert on_disk["method"] == "ping"
    assert on_disk["request_id"] != "dead"
    # Nothing answered, so the stale response is gone rather than replaced —
    # and its absence is what tells a reader no reply has arrived yet.
    assert not (mailbox / RESPONSE_NAME).exists()


def test_a_missing_mount_fails_loudly_instead_of_polling_an_empty_directory(tmp_path):
    """The mailbox root is normally a bind mount; without it makedirs would
    quietly build a private tree on the container's own filesystem and every
    call would time out with nothing to see. The QMT root really is absent from
    a container that was started without the volume."""
    client = BigQmtFileRpcClient(mailbox="/app/bqbox/dayk")
    with pytest.raises(RpcTransportError) as excinfo:
        client.ping(timeout_seconds=0.05)
    assert "does not exist" in str(excinfo.value)
    assert "/app/bqbox" in str(excinfo.value)


def test_the_mailbox_is_created_under_an_existing_parent(mailbox):
    client = BigQmtFileRpcClient(mailbox=str(mailbox))
    strategy = FakeStrategy(mailbox)
    strategy.start()
    try:
        assert not mailbox.exists()
        client.ping(timeout_seconds=5.0)
    finally:
        strategy.stop()
    assert mailbox.is_dir()


def test_download_history_data2_is_shaped_like_the_zmq_client(mailbox):
    seen = {}

    def handler(method, params):
        seen["method"] = method
        seen["params"] = params
        return {"queued": len(params["stock_list"])}

    strategy = FakeStrategy(mailbox, handler=handler)
    strategy.start()
    try:
        client = BigQmtFileRpcClient(mailbox=str(mailbox))
        result = client.download_history_data2(
            ["600000.SH", "000001.SZ"],
            period="1m",
            start_time="20260101",
            end_time="20260131",
            incrementally=True,
            timeout_seconds=5.0,
        )
    finally:
        strategy.stop()
    assert result == {"queued": 2}
    assert seen["method"] == "download_history_data2"
    assert seen["params"] == {
        "stock_list": ["600000.SH", "000001.SZ"],
        "period": "1m",
        "start_time": "20260101",
        "end_time": "20260131",
        "incrementally": True,
    }


def test_the_factory_picks_the_client_the_transport_names(mailbox):
    assert isinstance(
        create_quote_rpc_client(transport="zmq", connect_address="tcp://127.0.0.1:15609"),
        BigQmtZmqRpcClient,
    )
    assert isinstance(
        create_quote_rpc_client(transport="file", file_mailbox=str(mailbox)),
        BigQmtFileRpcClient,
    )
    # Default stays zmq: an unset transport must not silently move a deployment
    # onto a mailbox nobody is serving.
    assert isinstance(
        create_quote_rpc_client(connect_address="tcp://127.0.0.1:15609"),
        BigQmtZmqRpcClient,
    )


def test_the_factory_names_what_each_transport_is_missing(mailbox):
    with pytest.raises(ValueError, match="quote_rpc_file_mailbox"):
        create_quote_rpc_client(transport="file")
    with pytest.raises(ValueError, match="connect address"):
        create_quote_rpc_client(transport="zmq")
    with pytest.raises(ValueError, match="must be one of"):
        create_quote_rpc_client(transport="tcp")
