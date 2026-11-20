"""Checking that a webhook delivery came from VerifyInboxes, before reading anything in it."""
from __future__ import annotations

import hashlib
import hmac
import math
import re
import time
from typing import List, Optional, Union

__all__ = ["verify_signature", "SIGNATURE_HEADER", "DELIVERY_HEADER", "DEFAULT_TOLERANCE"]

SIGNATURE_HEADER = "X-Boxverifier-Signature"
"""The header that carries a delivery's signature: `t=<unix seconds>,v1=<hex>`."""
DELIVERY_HEADER = "X-Boxverifier-Delivery"
"""The same on every retry of one delivery: deliveries come at least once, so drop the ones you have seen."""
DEFAULT_TOLERANCE = 300
"""How far, in seconds, a signature's time may be from yours (5 minutes)."""

_DIGITS = re.compile(r"[0-9]+")
_V1 = re.compile(r"[0-9a-f]{64}")


def verify_signature(
    raw_body: Union[bytes, str],
    header: str,
    secret: str,
    *,
    tolerance: float = DEFAULT_TOLERANCE,
    now: Optional[float] = None,
) -> bool:
    """Whether `header` (the delivery's `X-Boxverifier-Signature`) signs `raw_body` with your webhook's `secret`.

    Pass the body exactly as it arrived, before any JSON parsing: bytes, or text that is those bytes decoded as
    UTF-8. `secret` is the 64 characters `webhooks.create` returned. A signature more than `tolerance` seconds from
    `now` (default: your clock) is refused, so an old delivery cannot be sent again. Anything that cannot be read is
    `False`: this never raises.

        if not verify_signature(body, request.headers["X-Boxverifier-Signature"], secret):
            return 400
    """
    try:
        if isinstance(raw_body, str):
            body = raw_body.encode("utf-8")
        elif isinstance(raw_body, (bytes, bytearray, memoryview)):
            body = bytes(raw_body)
        else:
            return False
        if not isinstance(header, str) or not isinstance(secret, str) or not secret:
            return False
        if isinstance(tolerance, bool) or not isinstance(tolerance, (int, float)) or not math.isfinite(tolerance):
            return False
        moment = time.time() if now is None else now
        if isinstance(moment, bool) or not isinstance(moment, (int, float)) or not math.isfinite(moment):
            return False

        stamps: List[str] = []
        signatures: List[str] = []
        for part in header.split(","):
            name, sep, value = part.strip().partition("=")
            if not sep:
                continue
            name, value = name.strip(), value.strip()
            if name == "t":
                stamps.append(value)
            elif name == "v1" and _V1.fullmatch(value):
                signatures.append(value)
        if len(stamps) != 1 or not _DIGITS.fullmatch(stamps[0]) or not signatures:
            return False
        signed_at = int(stamps[0])
        if abs(moment - signed_at) > tolerance:
            return False

        expected = hmac.new(secret.encode("utf-8"), stamps[0].encode("ascii") + b"." + body, hashlib.sha256).hexdigest()
        matched = False
        for signature in signatures:
            # Every one compared, in constant time: which of them matched is not told by the time it took.
            matched = hmac.compare_digest(expected, signature) or matched
        return matched
    except Exception:  # anything unreadable is "not valid", never an exception
        return False
