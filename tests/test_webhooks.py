"""`verify_signature` beyond the shared vectors: it never raises, and it is the same from the client."""
from __future__ import annotations

import hashlib
import hmac
import time
from typing import Any

import pytest

from verifyinboxes import VerifyInboxes, verify_signature

SECRET = "6f1d0c9e8b7a6f5e4d3c2b1a0f9e8d7c6b5a4f3e2d1c0b9a8f7e6d5c4b3a2f1e"
BODY = '{"event":"job.completed","job_id":"019a3b71-3c4d-7e5f-8a6b-7c8d9e0f1a2b","status":"done"}'


def sign(body: str, t: int, secret: str = SECRET) -> str:
    return hmac.new(secret.encode(), f"{t}.{body}".encode(), hashlib.sha256).hexdigest()


def test_a_fresh_signature_is_valid_without_a_client_or_a_key() -> None:
    t = int(time.time())
    assert verify_signature(BODY, f"t={t},v1={sign(BODY, t)}", SECRET) is True
    assert verify_signature(BODY.encode(), f"t={t},v1={sign(BODY, t)}", SECRET) is True
    assert verify_signature(BODY, f"t={t - 301},v1={sign(BODY, t - 301)}", SECRET) is False
    assert verify_signature(BODY, f"t={t - 301},v1={sign(BODY, t - 301)}", SECRET, tolerance=600) is True


@pytest.mark.parametrize("header", [
    None, 7, b"t=1,v1=00", "t=1,v1=" + "é" * 64, "t=1,v1=" + "ü" * 32, "t=" + "²" * 3 + ",v1=" + "0" * 64, "t=,v1=",
    "=,=,=", ",,,", "t=1" * 1000, "t=99999999999999999999999999999999,v1=" + "0" * 64, "v1=" + "a" * 64, "\x00",
])
def test_anything_unreadable_is_not_valid_and_never_raises(header: Any) -> None:
    assert verify_signature(BODY, header, SECRET, now=1) is False


@pytest.mark.parametrize("args", [
    (None, "t=1,v1=" + "0" * 64, SECRET), (BODY, "t=1,v1=" + "0" * 64, None), (BODY, "t=1,v1=" + "0" * 64, 7),
    (12, "t=1,v1=" + "0" * 64, SECRET), (BODY, "t=1,v1=" + "0" * 64, "\udcff"),
])
def test_bad_arguments_are_not_valid(args: Any) -> None:
    assert verify_signature(*args, now=1) is False
    assert verify_signature(BODY, "t=1,v1=" + "0" * 64, SECRET, tolerance="a", now=1) is False  # type: ignore[arg-type]


def test_the_client_has_it_too() -> None:
    t = 1762858800
    header = f"t={t},v1={sign(BODY, t)}"
    with VerifyInboxes("bv_testtesttesttesttesttesttesttest") as client:
        assert client.webhooks.verify_signature(BODY, header, SECRET, now=t) is True
        assert client.webhooks.verify_signature(BODY + " ", header, SECRET, now=t) is False


@pytest.mark.parametrize("tolerance, now", [
    (float("nan"), 1762858800), (300, float("nan")), (float("inf"), 1762858800), (300, float("inf")),
    (float("-inf"), 1762858800),
])
def test_a_tolerance_or_a_clock_that_is_not_a_number_is_not_valid(tolerance: float, now: float) -> None:
    signed_at = 1762858800 - 1_000_000  # a million seconds old
    header = f"t={signed_at},v1={sign(BODY, signed_at)}"
    assert verify_signature(BODY, header, SECRET, tolerance=tolerance, now=now) is False


def test_names_the_headers_the_api_sends() -> None:
    from verifyinboxes import DELIVERY_HEADER, SIGNATURE_HEADER

    assert SIGNATURE_HEADER == "X-Boxverifier-Signature"
    assert DELIVERY_HEADER == "X-Boxverifier-Delivery"  # the same on every retry of one delivery
