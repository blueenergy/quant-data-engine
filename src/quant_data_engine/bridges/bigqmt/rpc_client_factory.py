"""Pick the quote-RPC wire in one place.

Three consumers talk to the quote terminal — the daily-K bridge, the minute-bar
bridge, and the minute backfill task — and all of them have to speak whatever
the strategy inside QMT is serving. A deployment switches by config, so the
choice belongs in one function rather than in three call sites that can drift.

``zmq`` is the default and needs no explanation. ``file`` is the fallback for a
QMT client that enforces the illegal-IP policy: the strategy then serves RPC out
of a plain file mailbox and opens no socket at all, and every ZMQ client has to
stop — connecting is itself what gets the strategy killed. See
``file_rpc_client`` for the mailbox contract.
"""
from __future__ import annotations

from .constants import (
    DEFAULT_QUOTE_RPC_TRANSPORT,
    QUOTE_RPC_TRANSPORT_FILE,
    QUOTE_RPC_TRANSPORT_ZMQ,
)
from .file_rpc_client import BigQmtFileRpcClient
from .zmq_rpc_client import BigQmtZmqRpcClient

#: Every transport name a quote-RPC config may name.
QUOTE_RPC_TRANSPORTS = (QUOTE_RPC_TRANSPORT_ZMQ, QUOTE_RPC_TRANSPORT_FILE)


def normalize_quote_rpc_transport(value: str | None) -> str:
    name = str(value or "").strip().lower() or DEFAULT_QUOTE_RPC_TRANSPORT
    if name not in QUOTE_RPC_TRANSPORTS:
        raise ValueError(
            "quote_rpc_transport must be one of: %s" % ", ".join(QUOTE_RPC_TRANSPORTS)
        )
    return name


def create_quote_rpc_client(
    *,
    transport: str | None = None,
    connect_address: str = "",
    account_id: str = "",
    file_mailbox: str = "",
) -> BigQmtZmqRpcClient | BigQmtFileRpcClient:
    """Build the client for ``transport``, validating that transport's own fields.

    The two transports need different settings, so each one names what it is
    missing: a half-switched config (``transport: file`` with no mailbox) should
    fail at startup with that sentence, not later as timeouts against a
    directory nobody is writing to.
    """
    name = normalize_quote_rpc_transport(transport)
    if name == QUOTE_RPC_TRANSPORT_FILE:
        if not str(file_mailbox or "").strip():
            raise ValueError(
                "quote_rpc_transport=file requires quote_rpc_file_mailbox "
                "(this process's own mailbox directory)"
            )
        return BigQmtFileRpcClient(mailbox=file_mailbox, account_id=account_id)
    if not str(connect_address or "").strip():
        raise ValueError("quote_rpc_transport=zmq requires a connect address")
    return BigQmtZmqRpcClient(connect_address=connect_address, account_id=account_id)
