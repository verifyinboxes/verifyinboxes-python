"""Shared by the tests: the made-up key, scripted answers, a fake API to serve them, and a clock that only sleeps on paper."""
from __future__ import annotations

import asyncio
import threading
from typing import Any, Callable, Dict, Iterator, List, Optional

import pytest

from fakeapi import FakeAPI
from verifyinboxes import AsyncVerifyInboxes, VerifyInboxes

# Made up: the API refuses it.
KEY = "bv_testtesttesttesttesttesttesttest"
JOB_ID = "019a3b71-3c4d-7e5f-8a6b-7c8d9e0f1a2b"
REQUEST_ID = "019a7e00-0000-7000-8000-0000000000aa"


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


def answer(status: int = 200, json: Any = None, *, request_id: str = REQUEST_ID, **extra: Any) -> Dict[str, Any]:
    """A scripted answer, in the scenario format."""
    response: Dict[str, Any] = {"operation": None, "status": status, "headers": {"x-request-id": request_id, **extra.pop("headers", {})}}
    if json is not None:
        response["json"] = json
    response.update(extra)
    return response


def exchange(method: str, path: str, response: Dict[str, Any], **request: Any) -> Dict[str, Any]:
    return {"request": {"method": method, "path": path, **request}, "response": response}


def error(code: str, message: str = "no") -> Dict[str, Any]:
    return {"error": {"code": code, "message": message}}


def result_json(email: str = "ann@example.com", verdict: str = "invalid", **over: Any) -> Dict[str, Any]:
    body: Dict[str, Any] = {
        "email": email, "verdict": verdict, "score": None, "safe_to_send": False, "flags": [], "provider": "google",
        "catch_all_domain": False, "verified_at": "2026-11-18T10:12:00.123Z", "source": "smtp",
        "reason": "mailbox_not_found",
    }
    body.update(over)
    return body


def single_job_id(n: int) -> str:
    return f"019a3b6e-8f0c-7d2e-9a41-5c3e2f1b0b{n:02d}"


def running_job(job_id: str, kind: str = "single") -> Dict[str, Any]:
    return {"id": job_id, "kind": kind, "status": "running", "total": 1, "done": 0,
            "created_at": "2026-11-18T10:00:00Z", "finished_at": None, "counts": None, "purged": False}


def mixed_singles(emails: List[str]) -> List[Dict[str, Any]]:
    """`verify_many` of 10, unordered: p3 is still running when the time runs out, p7 is refused (402), the others
    answer at once."""
    exchanges = []
    for n, email in enumerate(emails, 1):
        if n == 3:
            exchanges.append(exchange("POST", "/v1/verify", answer(202, {"status": "pending", "job_id": single_job_id(3)}),
                                      json={"email": email}))
        elif n == 7:
            exchanges.append(exchange("POST", "/v1/verify", answer(402, error("insufficient_credits")), json={"email": email}))
        else:
            exchanges.append(exchange("POST", "/v1/verify",
                                      answer(json={**result_json(email), "job_id": single_job_id(n)}), json={"email": email}))
    exchanges += [exchange("GET", f"/v1/jobs/{single_job_id(3)}", answer(json=running_job(single_job_id(3))))] * 20
    return exchanges


class PaperClock:
    """The client's `_sleep` and `_clock`: each sleep is written down and moves the clock, and nothing waits."""

    def __init__(self) -> None:
        self.now = 1_000_000.0
        self.slept: List[float] = []
        self._lock = threading.Lock()

    def sleep(self, seconds: float) -> None:
        with self._lock:
            self.slept.append(seconds)
            self.now += seconds

    async def asleep(self, seconds: float) -> None:
        self.sleep(seconds)
        await asyncio.sleep(0)

    def clock(self) -> float:
        with self._lock:
            return self.now


@pytest.fixture
def clock() -> PaperClock:
    return PaperClock()


Serve = Callable[..., FakeAPI]


@pytest.fixture
def serve() -> Iterator[Serve]:
    """`serve(exchanges, unordered=False, hold_until=0)`: a fake API, stopped when the test ends."""
    started: List[FakeAPI] = []

    def start(exchanges: List[Dict[str, Any]], **options: Any) -> FakeAPI:
        api = FakeAPI(exchanges, **options).__enter__()
        started.append(api)
        return api

    yield start
    for api in started:
        api.close()


def sync_client(api: FakeAPI, clock: Optional[PaperClock] = None, **options: Any) -> VerifyInboxes:
    client = VerifyInboxes(options.pop("api_key", KEY), base_url=api.url, **options)
    if clock is not None:
        client._sleep = clock.sleep
        client._clock = clock.clock
    return client


def async_client(api: FakeAPI, clock: Optional[PaperClock] = None, **options: Any) -> AsyncVerifyInboxes:
    client = AsyncVerifyInboxes(options.pop("api_key", KEY), base_url=api.url, **options)
    if clock is not None:
        client._sleep = clock.asleep
        client._clock = clock.clock
    return client
