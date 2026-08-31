"""Big QMT RPC wire encoding compatible with xtquant_big_convert."""
from __future__ import annotations

import base64
import json
from typing import Any, Mapping

SAFE_B64_PREFIX = "b64s:"
SAFE_B64_DIGIT_ENCODE = str.maketrans("0123456789", "!#$%&()*~?")
SAFE_B64_DIGIT_DECODE = str.maketrans("!#$%&()*~?", "0123456789")


def encode_rpc_request_payload(request: Mapping[str, Any]) -> str:
    raw = json.dumps(request, ensure_ascii=False).encode("utf-8")
    encoded = base64.b64encode(raw).decode("ascii").translate(SAFE_B64_DIGIT_ENCODE)
    return SAFE_B64_PREFIX + encoded


def decode_rpc_request_payload(text: str) -> str:
    if not str(text).startswith(SAFE_B64_PREFIX):
        return str(text)
    encoded = str(text)[len(SAFE_B64_PREFIX) :].translate(SAFE_B64_DIGIT_DECODE)
    return base64.b64decode(encoded.encode("ascii")).decode("utf-8")


def loads_rpc_payload(raw: bytes | str | Mapping[str, Any]) -> dict[str, Any]:
    if isinstance(raw, dict):
        return dict(raw)
    text = raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else str(raw)
    text = decode_rpc_request_payload(text)
    payload = json.loads(text)
    if not isinstance(payload, dict):
        raise ValueError("rpc payload must be a json object")
    return payload
