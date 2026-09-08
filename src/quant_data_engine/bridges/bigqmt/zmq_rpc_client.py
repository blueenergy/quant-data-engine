"""Thin ZMQ DEALER client for Big QMT RPC (get_ticks / get_market_data_ex)."""
from __future__ import annotations

import re
import threading
import time
import uuid
from typing import Any, Mapping, Sequence

from .constants import DEFAULT_QUOTE_RPC_HOST, DEFAULT_ZMQ_BASE_PORT
from .wire_codec import encode_rpc_request_payload, loads_rpc_payload


class RpcTransportError(RuntimeError):
    pass


class RpcTransportTimeout(RpcTransportError):
    pass


def default_zmq_port(account_id: str) -> int:
    digits = re.sub(r"\D", "", str(account_id or ""))
    suffix = int(digits) % 100 if digits else 0
    return DEFAULT_ZMQ_BASE_PORT + suffix


def default_zmq_address(account_id: str, host: str = DEFAULT_QUOTE_RPC_HOST) -> str:
    return "tcp://%s:%d" % (host, default_zmq_port(account_id))


class BigQmtZmqRpcClient:
    """Minimal RPC client: one persistent DEALER socket, request/response by request_id."""

    def __init__(
        self,
        *,
        connect_address: str,
        account_id: str = "",
        client_linger_ms: int = 0,
        zmq_module: Any = None,
    ) -> None:
        self.connect_address = connect_address
        self.account_id = str(account_id or "")
        self.client_linger_ms = int(client_linger_ms)
        self._zmq = zmq_module
        self._context: Any = None
        self._dealer: Any = None
        self._lock = threading.Lock()

    def close(self) -> None:
        with self._lock:
            self._drop_dealer()

    def _drop_dealer(self) -> None:
        """Close and forget the DEALER so the next call reconnects clean.

        Callers must hold ``self._lock``. After a transport timeout the server
        may still deliver the late reply; leaving the socket in place lets those
        stale frames queue up and delay every subsequent call, so a timeout
        tears the socket down instead.
        """
        if self._dealer is not None:
            try:
                self._dealer.close(linger=self.client_linger_ms)
            except Exception:
                pass
            self._dealer = None

    def ping(self, *, timeout_seconds: float = 6.0) -> dict[str, Any]:
        return self.call("ping", {}, timeout_seconds=timeout_seconds)

    def get_ticks(self, codes: Sequence[str], *, timeout_seconds: float = 15.0) -> dict[str, Any]:
        response = self.call("get_ticks", {"codes": list(codes)}, timeout_seconds=timeout_seconds)
        data = response.get("data")
        return data if isinstance(data, dict) else {}

    def get_market_data_ex(
        self,
        *,
        field_list: Sequence[str],
        stock_list: Sequence[str],
        period: str = "1m",
        count: int = 241,
        start_time: str = "",
        end_time: str = "",
        dividend_type: str = "none",
        fill_data: bool = True,
        timeout_seconds: float = 15.0,
    ) -> dict[str, Any]:
        response = self.call(
            "get_market_data_ex",
            {
                "field_list": list(field_list),
                "stock_list": list(stock_list),
                "period": period,
                "count": int(count),
                "start_time": start_time,
                "end_time": end_time,
                "dividend_type": dividend_type,
                "fill_data": fill_data,
            },
            timeout_seconds=timeout_seconds,
        )
        data = response.get("data")
        return data if isinstance(data, dict) else {}

    def call(self, method: str, params: Mapping[str, Any], *, timeout_seconds: float) -> dict[str, Any]:
        request = {
            "method": method,
            "params": dict(params),
            "request_id": uuid.uuid4().hex,
        }
        if self.account_id:
            request["account_id"] = self.account_id
        payload = encode_rpc_request_payload(request)
        request_id = request["request_id"]

        zmq = self._ensure_zmq()
        with self._lock:
            dealer = self._ensure_dealer(zmq)
            try:
                dealer.send(payload.encode("utf-8"))
            except Exception as exc:
                self._drop_dealer()
                raise RpcTransportError("zmq send failed: %s" % exc) from exc

            deadline = time.time() + float(timeout_seconds)
            poller = zmq.Poller()
            poller.register(dealer, zmq.POLLIN)
            while True:
                remaining = deadline - time.time()
                if remaining <= 0:
                    self._drop_dealer()
                    raise RpcTransportTimeout("zmq rpc timeout: %s" % method)
                events = dict(poller.poll(timeout=max(1, int(remaining * 1000))))
                if dealer not in events:
                    continue
                frames = dealer.recv_multipart()
                raw = frames[-1]
                response = loads_rpc_payload(raw)
                if response.get("request_id") != request_id:
                    continue
                if not response.get("ok", False):
                    error = str(response.get("error") or "rpc failed")
                    raise RpcTransportError(error)
                return response

    def _ensure_zmq(self) -> Any:
        if self._zmq is not None:
            return self._zmq
        try:
            import zmq  # type: ignore
        except Exception as exc:  # pragma: no cover - optional on Linux CI
            raise RpcTransportError(
                "pyzmq not available. Install pyzmq on the Windows bridge host."
            ) from exc
        self._zmq = zmq
        return zmq

    def _ensure_dealer(self, zmq: Any) -> Any:
        if self._dealer is not None:
            return self._dealer
        if self._context is None:
            self._context = zmq.Context.instance()
        sock = self._context.socket(zmq.DEALER)
        sock.setsockopt(zmq.IDENTITY, uuid.uuid4().hex.encode("utf-8")[:16])
        sock.setsockopt(zmq.LINGER, self.client_linger_ms)
        sock.connect(self.connect_address)
        self._dealer = sock
        return sock
