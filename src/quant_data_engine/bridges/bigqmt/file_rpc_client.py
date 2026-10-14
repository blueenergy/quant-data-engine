"""File-mailbox RPC client for Big QMT — the zero-socket counterpart of the ZMQ client.

Why this exists
---------------
The QMT client enforces a network policy (``TC::CStrategyTradeManager::
onIllegalIPFind``) that stops **every** running strategy the moment it flags a
TCP endpoint owned by the QMT process. A ZMQ client is useless there — and worse
than useless, since connecting is what gets the strategy killed. The strategy
side then serves RPC out of a plain file mailbox instead (``FileTransport`` in
xtquant_big_convert); this module is the client half.

Drop-in for ``BigQmtZmqRpcClient``: same method set, and — this matters — the
same ``RpcTransportError`` / ``RpcTransportTimeout`` classes. ``source.py`` and
``watchlist_minute_bigqmt.py`` catch those by identity, so a client raising its
own look-alike exceptions would not be retried or reported as a transport
failure at all.

The wire format is NOT the ZMQ one
----------------------------------
ZMQ frames go out base64-wrapped (``wire_codec.encode_rpc_request_payload``).
The mailbox does not: the strategy reads ``req.json`` with a bare ``json.loads``
and ignores anything that is not a JSON *object*, so a base64 string would sit
there unread and every single call would time out — with no error on either
side, because nothing is technically broken. Do not "unify" the two paths;
``test_file_rpc_client.py`` pins this.

Layout and ownership
--------------------
``<mailbox>/req.json``    this client -> strategy
``<mailbox>/resp.json``   strategy -> this client

``mailbox`` is this process's own directory. The server side names it
``<root>/<client_id>``, so the strategy's root is this directory's parent — but
the client only ever touches its own two files, so that split stays a
server-side detail. One deployment per mailbox: two processes sharing one
directory would overwrite each other's request and both stall.

Both files are written atomically (tmp + ``os.replace``), so neither end ever
observes a torn one. The mailbox is deliberately **at-most-once**: the strategy
publishes an ``in_progress`` stub before running a handler, so a strategy that
dies mid-handler leaves the request marked handled and this client times out
rather than the request being replayed. A timeout is the cheaper failure.
"""
from __future__ import annotations

import json
import os
import threading
import time
import uuid
from typing import Any, Mapping, Sequence

from .zmq_rpc_client import RpcTransportError, RpcTransportTimeout

REQUEST_NAME = "req.json"
RESPONSE_NAME = "resp.json"
IN_PROGRESS_STATUS = "in_progress"

# os.replace onto a file the other end is mid-read can transiently fail on
# Windows (a handle opened without FILE_SHARE_DELETE blocks MoveFileEx), and the
# mailbox lives on a Windows filesystem whenever one end runs inside QMT. Same
# retry budget as the server's own writer.
REPLACE_ATTEMPTS = 5
REPLACE_RETRY_SECONDS = 0.02

# The mailbox is a shared filesystem, so a poll costs a syscall (and, over a
# bind-mounted Windows path from a container, a 9p round trip). Start eager and
# back off: the response usually lands within a few ms of the request, and the
# in-progress stub that arrives first is tiny.
POLL_MIN_INTERVAL_SECONDS = 0.002
POLL_MAX_INTERVAL_SECONDS = 0.02
POLL_BACKOFF = 1.5


def _read_json(path: str) -> dict[str, Any] | None:
    """Read a JSON object, or None when absent, torn, or unreadable.

    Missing and half-written are the same thing to a poller: no data yet. Both
    only happen between the writer's tmp write and its replace.
    """
    try:
        with open(path, "rb") as handle:
            raw = handle.read()
    except Exception:
        return None
    if not raw:
        return None
    try:
        payload = json.loads(raw.decode("utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _atomic_write_json(path: str, payload: Mapping[str, Any]) -> None:
    """Publish ``payload`` at ``path`` atomically."""
    data = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
    tmp = "%s.tmp.%d" % (path, os.getpid())
    with open(tmp, "wb") as handle:
        handle.write(data)
    last_error: Exception | None = None
    for _attempt in range(REPLACE_ATTEMPTS):
        try:
            os.replace(tmp, path)
            return
        except OSError as exc:
            last_error = exc
            time.sleep(REPLACE_RETRY_SECONDS)
    raise RpcTransportError("file rpc replace failed for %s: %s" % (path, last_error))


class BigQmtFileRpcClient:
    """RPC over a per-process file mailbox.

    Quote-side methods used by data-engine: ``get_ticks``, ``get_market_data_ex``
    and history download (``download_history_data`` / ``download_history_data2``).
    """

    def __init__(self, *, mailbox: str, account_id: str = "") -> None:
        self.mailbox = str(mailbox or "").strip()
        if not self.mailbox:
            raise ValueError("file rpc client requires a mailbox directory")
        self.account_id = str(account_id or "")
        # Logged as the source's "address" — for a mailbox that is the directory,
        # and it is the one thing an operator needs to see when it misbehaves.
        self.connect_address = "file:%s" % self.mailbox
        self._lock = threading.Lock()
        self._mailbox_ready = False

    def close(self) -> None:
        """Nothing to release: the mailbox holds no handle and no connection.

        The two files are left in place on purpose. Deleting ``req.json`` here
        would race a strategy that is mid-handler, and the next process to claim
        this mailbox clears them at startup anyway.
        """

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

    def download_history_data(
        self,
        stock_code: str,
        *,
        period: str = "1m",
        start_time: str = "",
        end_time: str = "",
        incrementally: bool | None = None,
        timeout_seconds: float = 180.0,
    ) -> Any:
        params: dict[str, Any] = {
            "stock_code": stock_code,
            "period": period,
            "start_time": start_time,
            "end_time": end_time,
        }
        if incrementally is not None:
            params["incrementally"] = bool(incrementally)
        response = self.call("download_history_data", params, timeout_seconds=timeout_seconds)
        return response.get("data")

    def download_history_data2(
        self,
        stock_list: Sequence[str],
        *,
        period: str = "1m",
        start_time: str = "",
        end_time: str = "",
        incrementally: bool | None = None,
        timeout_seconds: float = 180.0,
    ) -> Any:
        params: dict[str, Any] = {
            "stock_list": list(stock_list),
            "period": period,
            "start_time": start_time,
            "end_time": end_time,
        }
        if incrementally is not None:
            params["incrementally"] = bool(incrementally)
        response = self.call("download_history_data2", params, timeout_seconds=timeout_seconds)
        return response.get("data")

    def call(self, method: str, params: Mapping[str, Any], *, timeout_seconds: float) -> dict[str, Any]:
        # Plain JSON, NOT wire_codec's base64 envelope — see the module docstring.
        request: dict[str, Any] = {
            "method": method,
            "params": dict(params),
            "request_id": uuid.uuid4().hex,
        }
        if self.account_id:
            request["account_id"] = self.account_id
        request_id = request["request_id"]

        request_path = os.path.join(self.mailbox, REQUEST_NAME)
        response_path = os.path.join(self.mailbox, RESPONSE_NAME)
        with self._lock:
            # One outstanding request per mailbox: the client blocks until its
            # response lands, so a second in flight could only clobber the
            # request the strategy has not picked up yet.
            self._ensure_mailbox(request_path)
            _atomic_write_json(request_path, request)

            # Monotonic, not wall clock: the bridge hosts slew their clock (a WSL
            # resume has been measured stepping it ~8%), and a backward step on a
            # wall-clock deadline silently extends every timeout.
            deadline = time.monotonic() + float(timeout_seconds)
            delay = POLL_MIN_INTERVAL_SECONDS
            while True:
                response = _read_json(response_path)
                if response is not None and str(response.get("request_id") or "") == request_id:
                    # The in-progress stub carries the id too; only a real
                    # envelope answers the call.
                    if str(response.get("status") or "") != IN_PROGRESS_STATUS:
                        if not response.get("ok", False):
                            raise RpcTransportError(str(response.get("error") or "rpc failed"))
                        return response
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise RpcTransportTimeout("file rpc timeout: %s" % method)
                time.sleep(min(delay, remaining))
                delay = min(delay * POLL_BACKOFF, POLL_MAX_INTERVAL_SECONDS)

    def _ensure_mailbox(self, request_path: str) -> None:
        """Create this client's mailbox, once, and clear a dead run's files.

        Only the last path component is created: the parent has to exist
        already. That parent is normally a bind mount of the QMT-side mailbox
        root, and if the mount is missing, ``makedirs`` would happily build a
        private directory tree on the container's own filesystem — every call
        would then poll an empty directory and time out, with nothing broken
        enough to log. Failing here instead names the actual problem.
        """
        if self._mailbox_ready:
            return
        parent = os.path.dirname(self.mailbox) or "."
        if not os.path.isdir(parent):
            raise RpcTransportError(
                "file rpc mailbox parent %s does not exist (is the QMT mailbox "
                "mounted into this container?)" % parent
            )
        try:
            os.makedirs(self.mailbox)
        except OSError:
            if not os.path.isdir(self.mailbox):
                raise RpcTransportError("file rpc cannot create mailbox %s" % self.mailbox)
        # A stale req.json is an unanswered request whose client is gone —
        # leaving it would have the strategy run it for nobody. The stale
        # response is harmless (its request_id never matches a new one) but
        # removing it keeps the directory honest. This client owns both files,
        # and deleting is the one mailbox operation that needs no help from the
        # strategy.
        for name in (REQUEST_NAME, RESPONSE_NAME):
            try:
                os.remove(os.path.join(self.mailbox, name))
            except OSError:
                pass
        self._mailbox_ready = True
