import json
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock
from zoneinfo import ZoneInfo

import pytest

from quant_data_engine.bridges.bigqmt.calendar import (
    MongoTradeCalendarChecker,
    make_range_trading_day_checker,
)
from quant_data_engine.bridges.bigqmt.shadow_sink import ShadowRealtimeDailyKSink
from quant_data_engine.bridges.bigqmt.kline_source import BigQmtRpcKlineSource
from quant_data_engine.bridges.bigqmt.source import BigQmtRpcTickSource
from quant_data_engine.bridges.bigqmt.tick_normalize import normalize_bigqmt_tick
from quant_data_engine.bridges.bigqmt.wire_codec import (
    SAFE_B64_PREFIX,
    decode_rpc_request_payload,
    encode_rpc_request_payload,
    loads_rpc_payload,
)
from quant_data_engine.bridges.bigqmt.zmq_rpc_client import (
    BigQmtZmqRpcClient,
    RpcTransportTimeout,
    default_zmq_port,
)
from quant_data_engine.bridges.miniqmt.config import BridgeConfig, UniverseConfig
from quant_data_engine.bridges.miniqmt.constants import TICK_SOURCE_BIGQMT_RPC
from quant_data_engine.bridges.miniqmt.runner import in_cn_stock_session, is_cn_trading_day
from quant_data_engine.bridges.miniqmt.universe import SymbolUniverse


class _FakeZmqModule:
    POLLIN = 1
    DEALER = "DEALER"
    IDENTITY = "IDENTITY"
    LINGER = "LINGER"

    class Poller:
        def __init__(self):
            self._sock = None

        def register(self, sock, flag):
            self._sock = sock

        def poll(self, timeout=0):
            return {self._sock: _FakeZmqModule.POLLIN} if self._sock is not None else {}

    class Context:
        @staticmethod
        def instance():
            return _FakeZmqModule.Context()

        def socket(self, kind):
            return _FakeZmqSocket()


class _FakeZmqSocket:
    def __init__(self):
        self.sent = []
        self.response_frames = []

    def setsockopt(self, *args, **kwargs):
        return None

    def connect(self, address):
        self.address = address

    def send(self, payload):
        self.sent.append(payload)

    def recv_multipart(self):
        return self.response_frames

    def close(self, linger=0):
        return None


def test_wire_codec_roundtrip():
    payload = {"method": "get_ticks", "params": {"codes": ["000001.SZ"]}, "request_id": "abc"}
    encoded = encode_rpc_request_payload(payload)
    assert encoded.startswith(SAFE_B64_PREFIX)
    decoded = json.loads(decode_rpc_request_payload(encoded))
    assert decoded == payload


def test_wire_codec_accepts_plain_json():
    text = '{"ok": true}'
    assert decode_rpc_request_payload(text) == text
    assert loads_rpc_payload(text)["ok"] is True


def test_default_zmq_port_uses_account_suffix():
    assert default_zmq_port("62129066") == 15560 + (62129066 % 100)


def test_bigqmt_rpc_client_matches_request_id():
    zmq = _FakeZmqModule()
    client = BigQmtZmqRpcClient(connect_address="tcp://127.0.0.1:15626", zmq_module=zmq)
    dealer = client._ensure_dealer(zmq)

    def send_and_arm(payload):
        sent = json.loads(decode_rpc_request_payload(payload.decode("utf-8")))
        response = {
            "ok": True,
            "request_id": sent["request_id"],
            "data": {"000001.SZ": {"lastPrice": 10.0}},
        }
        dealer.response_frames = [encode_rpc_request_payload(response).encode("utf-8")]

    dealer.send = send_and_arm

    ticks = client.get_ticks(["000001.SZ"], timeout_seconds=1.0)
    assert ticks["000001.SZ"]["lastPrice"] == 10.0


def test_bigqmt_rpc_client_get_market_data_ex():
    zmq = _FakeZmqModule()
    client = BigQmtZmqRpcClient(connect_address="tcp://127.0.0.1:15626", zmq_module=zmq)
    dealer = client._ensure_dealer(zmq)

    def send_and_arm(payload):
        sent = json.loads(decode_rpc_request_payload(payload.decode("utf-8")))
        assert sent["method"] == "get_market_data_ex"
        assert sent["params"]["period"] == "1m"
        response = {
            "ok": True,
            "request_id": sent["request_id"],
            "data": {
                "000001.SZ": {
                    "columns": ["stime", "close"],
                    "records": [{"stime": "20260908103000", "close": 10.2}],
                }
            },
        }
        dealer.response_frames = [encode_rpc_request_payload(response).encode("utf-8")]

    dealer.send = send_and_arm
    frames = client.get_market_data_ex(
        field_list=["open", "high", "low", "close", "volume"],
        stock_list=["000001.SZ"],
        period="1m",
        timeout_seconds=1.0,
    )
    assert frames["000001.SZ"]["records"][0]["close"] == 10.2


def test_bigqmt_rpc_client_download_history_data():
    zmq = _FakeZmqModule()
    client = BigQmtZmqRpcClient(connect_address="tcp://127.0.0.1:15626", zmq_module=zmq)
    dealer = client._ensure_dealer(zmq)

    def send_and_arm(payload):
        sent = json.loads(decode_rpc_request_payload(payload.decode("utf-8")))
        assert sent["method"] == "download_history_data"
        assert sent["params"]["stock_code"] == "000001.SZ"
        assert sent["params"]["period"] == "1m"
        assert sent["params"]["start_time"] == "20260720"
        assert sent["params"]["end_time"] == "20260908"
        response = {"ok": True, "request_id": sent["request_id"], "data": True}
        dealer.response_frames = [encode_rpc_request_payload(response).encode("utf-8")]

    dealer.send = send_and_arm
    assert client.download_history_data(
        "000001.SZ",
        period="1m",
        start_time="20260720",
        end_time="20260908",
        timeout_seconds=1.0,
    ) is True


def test_bigqmt_rpc_client_timeout():
    zmq = _FakeZmqModule()

    class EmptyPoller(_FakeZmqModule.Poller):
        def poll(self, timeout=0):
            return {}

    zmq.Poller = EmptyPoller
    client = BigQmtZmqRpcClient(connect_address="tcp://127.0.0.1:15626", zmq_module=zmq)
    with pytest.raises(RpcTransportTimeout):
        client.get_ticks(["000001.SZ"], timeout_seconds=0.01)


def test_bigqmt_rpc_client_timeout_drops_socket_and_next_call_reconnects():
    """A transport timeout must drop the DEALER so late replies cannot queue.

    First call times out (server never answers). The client must close the old
    socket and the next call must go out on a fresh one and succeed.
    """
    zmq = _FakeZmqModule()
    poller_instances = []

    class PhasePoller(_FakeZmqModule.Poller):
        """First instance never returns readable; later instances do."""

        _created = 0

        def __init__(self):
            super().__init__()
            PhasePoller._created += 1
            self._live = PhasePoller._created > 1
            poller_instances.append(self)

        def poll(self, timeout=0):
            if not self._live:
                return {}
            return {self._sock: _FakeZmqModule.POLLIN}

    class ArmContext(_FakeZmqModule.Context):
        """Arm the matching response on send, so a fresh socket answers."""

        @staticmethod
        def instance():
            # The base fake hardcodes its own class; return the armed context.
            return ArmContext()

        def socket(self, kind):
            sock = _FakeZmqSocket()

            def send_and_arm(payload):
                sent = json.loads(decode_rpc_request_payload(payload.decode("utf-8")))
                response = {
                    "ok": True,
                    "request_id": sent["request_id"],
                    "data": {"000001.SZ": {"lastPrice": 10.0}},
                }
                sock.response_frames = [encode_rpc_request_payload(response).encode("utf-8")]

            sock.send = send_and_arm
            return sock

    zmq.Poller = PhasePoller
    zmq.Context = ArmContext
    client = BigQmtZmqRpcClient(connect_address="tcp://127.0.0.1:15626", zmq_module=zmq)

    with pytest.raises(RpcTransportTimeout):
        client.get_ticks(["000001.SZ"], timeout_seconds=0.01)
    assert client._dealer is None, "timeout must drop the DEALER socket"

    ticks = client.get_ticks(["000001.SZ"], timeout_seconds=1.0)
    assert ticks["000001.SZ"]["lastPrice"] == 10.0


def test_bigqmt_rpc_tick_source_retries_batch_once():
    client = MagicMock()
    client.get_ticks.side_effect = [
        RpcTransportTimeout("zmq rpc timeout: get_ticks"),
        {"000001.SZ": {"lastPrice": 1.0}},
    ]
    source = BigQmtRpcTickSource(["000001.SZ"], rpc_client=client, poll_batch_size=1)
    ticks = source.poll()
    assert ticks["000001.SZ"]["lastPrice"] == 1.0
    assert client.get_ticks.call_count == 2


def test_bigqmt_rpc_tick_source_skips_batch_after_retry_and_merges_rest():
    client = MagicMock()
    client.get_ticks.side_effect = [
        RpcTransportTimeout("zmq rpc timeout: get_ticks"),
        RpcTransportTimeout("zmq rpc timeout: get_ticks"),
        {"600000.SH": {"lastPrice": 2.0}},
    ]
    source = BigQmtRpcTickSource(
        ["000001.SZ", "600000.SH"],
        rpc_client=client,
        poll_batch_size=1,
    )
    ticks = source.poll()
    assert "000001.SZ" not in ticks, "failed batch must be skipped, not sink the round"
    assert ticks["600000.SH"]["lastPrice"] == 2.0
    assert client.get_ticks.call_count == 3


def test_bigqmt_rpc_tick_source_single_batch_still_raises():
    client = MagicMock()
    client.get_ticks.side_effect = [
        RpcTransportTimeout("zmq rpc timeout: get_ticks"),
        RpcTransportTimeout("zmq rpc timeout: get_ticks"),
    ]
    source = BigQmtRpcTickSource(["000001.SZ"], rpc_client=client, poll_batch_size=0)
    with pytest.raises(RpcTransportTimeout):
        source.poll()
    assert client.get_ticks.call_count == 2


def test_bigqmt_rpc_tick_source_all_batches_fail_raises():
    client = MagicMock()
    client.get_ticks.side_effect = [
        RpcTransportTimeout("zmq rpc timeout: get_ticks"),
        RpcTransportTimeout("zmq rpc timeout: get_ticks"),
        RpcTransportTimeout("zmq rpc timeout: get_ticks"),
        RpcTransportTimeout("zmq rpc timeout: get_ticks"),
    ]
    source = BigQmtRpcTickSource(
        ["000001.SZ", "600000.SH"],
        rpc_client=client,
        poll_batch_size=1,
    )
    with pytest.raises(RpcTransportTimeout, match="all 2 batches skipped"):
        source.poll()
    assert source.last_poll_stats is not None
    assert source.last_poll_stats.batches_skipped == 2
    assert source.last_poll_stats.synced_symbols == 0


def test_bigqmt_rpc_tick_source_partial_poll_records_stats():
    client = MagicMock()
    client.get_ticks.side_effect = [
        RpcTransportTimeout("zmq rpc timeout: get_ticks"),
        RpcTransportTimeout("zmq rpc timeout: get_ticks"),
        {"600000.SH": {"lastPrice": 2.0}},
    ]
    source = BigQmtRpcTickSource(
        ["000001.SZ", "600000.SH"],
        rpc_client=client,
        poll_batch_size=1,
    )
    ticks = source.poll()
    assert ticks["600000.SH"]["lastPrice"] == 2.0
    assert source.last_poll_stats is not None
    assert source.last_poll_stats.batches_skipped == 1
    assert source.last_poll_stats.batches_ok == 1
    assert source.last_poll_stats.synced_symbols == 1
    assert source.last_poll_stats.expected_symbols == 2


def test_normalize_bigqmt_tick_maps_stime_to_time():
    tick = normalize_bigqmt_tick({"lastPrice": 1.0, "stime": "20250813102700"})
    assert tick["time"] == 1755052020000


def test_bigqmt_rpc_tick_source_batches():
    client = MagicMock()
    client.get_ticks.side_effect = [
        {"000001.SZ": {"lastPrice": 1.0}},
        {"600000.SH": {"lastPrice": 2.0}},
    ]
    source = BigQmtRpcTickSource(
        ["000001.SZ", "600000.SH", "300750.SZ"],
        rpc_client=client,
        poll_batch_size=2,
    )
    source.start()
    ticks = source.poll()
    assert ticks["000001.SZ"]["lastPrice"] == 1.0
    assert ticks["600000.SH"]["lastPrice"] == 2.0
    assert client.get_ticks.call_count == 2


def test_mongo_trade_calendar_checker():
    class _Col:
        def __init__(self, days):
            self.days = set(days)

        def find_one(self, query, projection=None):
            return {"_id": 1} if query.get("trade_date") in self.days else None

    class _Db:
        def __init__(self, col):
            self._col = col

        def __getitem__(self, name):
            return self._col

    checker = MongoTradeCalendarChecker(_Db(_Col({"20250901"})))
    open_day = datetime(2025, 9, 1, 10, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
    closed_day = datetime(2025, 9, 2, 10, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
    assert checker.is_trading_day(open_day) is True
    assert checker.is_trading_day(closed_day) is False


def test_make_range_trading_day_checker_reads_window_once():
    class _Cursor:
        def __init__(self, days):
            self.days = days

        def __iter__(self):
            return iter({"trade_date": day} for day in self.days)

    class _Col:
        def __init__(self, days):
            self.days = list(days)
            self.queries = []

        def find(self, query, projection=None):
            self.queries.append(query)
            low, high = query["trade_date"]["$gte"], query["trade_date"]["$lte"]
            return _Cursor([day for day in self.days if low <= day <= high])

    class _Db:
        def __init__(self, col):
            self._col = col

        def __getitem__(self, name):
            return self._col

    col = _Col({"20260907", "20260908", "20261001"})
    checker, days = make_range_trading_day_checker(_Db(col), "20260907", "20260909")

    # One query for the whole window, not one per day.
    assert len(col.queries) == 1
    assert days == ("20260907", "20260908")
    tz = ZoneInfo("Asia/Shanghai")
    assert checker(datetime(2026, 9, 8, 10, 0, tzinfo=tz)) is True
    # In range but not an open day (and 2026-10-01 is out of range entirely).
    assert checker(datetime(2026, 9, 9, 10, 0, tzinfo=tz)) is False
    assert checker(datetime(2026, 10, 1, 10, 0, tzinfo=tz)) is False


def test_make_range_trading_day_checker_clamps_unsettled_days():
    """Only sessions that can have a *complete* bar set may be expected.

    ``trade_calendar`` is populated through 20261231, so an unclamped window
    ending today would report every remaining day as missing and condemn
    today's session for every symbol while it is still running.
    """

    class _Cursor:
        def __init__(self, days):
            self.days = days

        def __iter__(self):
            return iter({"trade_date": day} for day in self.days)

    class _Col:
        def __init__(self, days):
            self.days = list(days)

        def find(self, query, projection=None):
            low, high = query["trade_date"]["$gte"], query["trade_date"]["$lte"]
            return _Cursor([day for day in self.days if low <= day <= high])

    class _Db:
        def __getitem__(self, name):
            return _Col({"20250901", "20250902", "20250903", "20251231"})

    tz = ZoneInfo("Asia/Shanghai")
    db = _Db()

    # Mid-session on 09-02: today is not settled, so it is not expected yet.
    _, mid_session = make_range_trading_day_checker(
        db, "20250901", "20250903", now=datetime(2025, 9, 2, 10, 0, tzinfo=tz)
    )
    assert mid_session == ("20250901",)

    # After the close it is.
    _, after_close = make_range_trading_day_checker(
        db, "20250901", "20250903", now=datetime(2025, 9, 2, 16, 0, tzinfo=tz)
    )
    assert after_close == ("20250901", "20250902")

    # A window reaching into the future stops at today.
    _, future_end = make_range_trading_day_checker(
        db, "20250901", "20251231", now=datetime(2025, 9, 2, 16, 0, tzinfo=tz)
    )
    assert future_end == ("20250901", "20250902")

    # A wholly future window has nothing to expect.
    checker, empty = make_range_trading_day_checker(
        db, "20250903", "20250910", now=datetime(2025, 9, 2, 16, 0, tzinfo=tz)
    )
    assert (checker, empty) == (None, ())


def test_make_range_trading_day_checker_falls_back_when_empty():
    class _Db:
        def __getitem__(self, name):
            raise RuntimeError("mongo down")

    checker, days = make_range_trading_day_checker(_Db(), "20260907", "20260909")
    assert checker is None
    assert days == ()
    assert make_range_trading_day_checker(None, "20260907", "20260909") == (None, ())


def test_is_cn_trading_day_uses_explicit_checker():
    open_day = datetime(2025, 9, 1, 10, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
    assert is_cn_trading_day(open_day, trading_day_checker=lambda _now: True) is True
    assert is_cn_trading_day(open_day, trading_day_checker=lambda _now: False) is False


def test_in_cn_stock_session_respects_checker_holiday():
    holiday = datetime(2025, 10, 1, 10, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
    assert in_cn_stock_session(holiday, trading_day_checker=lambda _now: False) is False


def test_bridge_config_requires_bigqmt_rpc_address_or_account():
    with pytest.raises(ValueError, match="bigqmt_rpc requires"):
        BridgeConfig.from_mapping({"tick_source": "bigqmt_rpc"}).validate()


def test_bridge_config_accepts_quote_account_id():
    cfg = BridgeConfig.from_mapping(
        {"tick_source": "bigqmt_rpc", "quote_account_id": "62129066"}
    ).validate()
    assert cfg.tick_source == TICK_SOURCE_BIGQMT_RPC


def test_symbol_universe_requires_stock_info_when_flag_set():
    class _EmptyCol:
        def find(self, query=None, projection=None):
            return []

    class _Db:
        def __getitem__(self, name):
            return _EmptyCol()

    universe = SymbolUniverse(
        finance_db=SimpleNamespace(),
        trading_db=SimpleNamespace(),
        data_db=_Db(),
        config=UniverseConfig(mode="full_market"),
        require_listed_from_db=True,
    )
    with pytest.raises(RuntimeError, match="stock_info"):
        universe.load_listed_a_shares()


def test_symbol_universe_filters_delisted_stock_info_rows():
    """Delisted (list_status="D") rows in stock_info added ~340 symbols to the
    full-market universe, pinning coverage_ratio at ~0.94 and failing the 0.98
    quality gate on every realtime spectrum snapshot."""
    captured = {}

    class _Col:
        ROWS = [
            {"ts_code": "600000.SH", "list_status": "L"},
            {"ts_code": "000003.SZ", "name": "PT金田A(退)", "list_status": "D"},
        ]

        def find(self, query=None, projection=None):
            captured["query"] = query
            query = query or {}
            return [
                row
                for row in self.ROWS
                if all(row.get(key) == value for key, value in query.items())
            ]

    class _Db:
        def __getitem__(self, name):
            return _Col()

    universe = SymbolUniverse(
        finance_db=SimpleNamespace(),
        trading_db=SimpleNamespace(),
        data_db=_Db(),
        config=UniverseConfig(mode="full_market"),
    )
    symbols = universe.load_listed_a_shares()
    assert captured["query"] == {"list_status": "L"}
    assert symbols == ["600000.SH"]


def test_shadow_sink_writes_shadow_collection_only():
    inserted = []

    class _ShadowCol:
        def insert_one(self, doc):
            inserted.append(doc)

    class _ShadowDb:
        def __init__(self, col):
            self._col = col

        def __getitem__(self, name):
            return self._col

    repo = MagicMock()
    repo.upsert_daily.return_value = 99
    sink = ShadowRealtimeDailyKSink(
        repo=repo,
        settings=None,
        write_interval=0,
        universe_type="full_market_a_share",
        source_mode="bigqmt_full_market_daily",
        task_type="a_daily_realtime_sync",
        shadow_db=_ShadowDb(_ShadowCol()),
        record_runs=False,
    )
    now = datetime(2025, 8, 31, 10, 30, tzinfo=ZoneInfo("Asia/Shanghai"))
    result = sink.process(
        ticks={"000001.SZ": {"lastPrice": 10.0, "lastClose": 9.0, "volume": 1}},
        symbols=["000001.SZ"],
        now=now,
        force=True,
    )
    assert result is not None
    assert result.daily_changed == 0
    repo.upsert_daily.assert_not_called()
    assert inserted[0]["synced_symbols"] == 1
    assert inserted[0]["source_mode"] == "bigqmt_full_market_daily"


def test_bigqmt_rpc_kline_source_polls_market_data_ex():
    client = MagicMock()
    client.get_market_data_ex.return_value = {
        "000001.SZ": {
            "records": [
                {
                    "stime": "20260908103000",
                    "open": 10.0,
                    "high": 10.2,
                    "low": 9.9,
                    "close": 10.1,
                    "volume": 100,
                }
            ]
        }
    }
    source = BigQmtRpcKlineSource(["000001.SZ"], rpc_client=client, poll_batch_size=0)
    frames = source.poll()
    assert source.payload_kind == "kline"
    assert frames["000001.SZ"]["records"][0]["close"] == 10.1
    client.get_market_data_ex.assert_called_once()
    kwargs = client.get_market_data_ex.call_args.kwargs
    assert kwargs["period"] == "1m"
    assert kwargs["stock_list"] == ["000001.SZ"]
