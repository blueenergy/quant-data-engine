"""Scope the HTTP proxy to the tasks that actually need it.

The data-engine container runs both domestic syncs (Tushare, East Money,
Baidu, CNINFO) and a handful of overseas ones (Yahoo, BBC/CNBC RSS).  Only
the latter need a proxy.

Setting ``HTTP_PROXY`` container-wide made *every* HTTP call — including the
domestic ones — depend on mihomo being alive, and made domestic traffic rely
on mihomo's geo rules to stay direct.  Instead the container only carries
``OVERSEAS_PROXY_URL``; each overseas task calls
:func:`enable_overseas_proxy` in its own process, so a proxy outage cannot
take the domestic sync down with it.
"""
from __future__ import annotations

import logging
import os

logger = logging.getLogger(__name__)

PROXY_ENV_VAR = "OVERSEAS_PROXY_URL"


def enable_overseas_proxy() -> str | None:
    """Route this process's HTTP(S) traffic through ``OVERSEAS_PROXY_URL``.

    Returns the proxy URL, or ``None`` when the variable is unset (e.g. when
    running overseas with no proxy at all).
    """
    url = (os.environ.get(PROXY_ENV_VAR) or "").strip()
    if not url:
        return None

    # requests/urllib check both cases; set both so every client agrees.
    for key in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"):
        os.environ[key] = url
    for key in ("NO_PROXY", "no_proxy"):
        os.environ[key] = "localhost,127.0.0.1"

    logger.info("Overseas proxy enabled: %s", url)
    return url
