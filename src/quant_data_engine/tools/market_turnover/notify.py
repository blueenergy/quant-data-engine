"""DingTalk notifier for the turnover alert (signed custom-robot webhook).

Mirrors ``notify_dingtalk.sh`` on the host — the container has no copy of that
script, so the signing is reimplemented here. Credentials come from
``DINGTALK_WEBHOOK_URL`` / ``DINGTALK_WEBHOOK_SECRET`` (local-only env file).
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import os
import time
import urllib.parse
import urllib.request
from typing import Optional

logger = logging.getLogger(__name__)


def _sign(secret: str, timestamp: str) -> str:
    digest = hmac.new(secret.encode("utf-8"), f"{timestamp}\n{secret}".encode("utf-8"), hashlib.sha256).digest()
    return urllib.parse.quote_plus(base64.b64encode(digest))


def send_dingtalk(content: str, *, timeout: float = 10.0) -> Optional[str]:
    """POST a text message. Returns ``None`` on success, otherwise an error string."""
    url = os.getenv("DINGTALK_WEBHOOK_URL", "").strip()
    secret = os.getenv("DINGTALK_WEBHOOK_SECRET", "").strip()
    if not url or url.startswith("<"):
        return "DINGTALK_WEBHOOK_URL 未配置"
    if secret:
        timestamp = str(int(time.time() * 1000))
        separator = "&" if "?" in url else "?"
        url = f"{url}{separator}timestamp={timestamp}&sign={_sign(secret, timestamp)}"

    payload = json.dumps({"msgtype": "text", "text": {"content": content}}, ensure_ascii=False).encode("utf-8")
    request = urllib.request.Request(
        url, data=payload, headers={"Content-Type": "application/json"}, method="POST"
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = response.read().decode("utf-8", "replace")
    except Exception as exc:  # network errors must never break the aggregator
        return f"发送失败: {exc}"
    try:
        if json.loads(body).get("errcode") == 0:
            return None
    except json.JSONDecodeError:
        pass
    return f"被拒绝: {body[:200]}"
