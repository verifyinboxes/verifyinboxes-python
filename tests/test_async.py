"""The async client: the same surface, awaited, with `verify_many` sending its checks together."""
from __future__ import annotations

import asyncio
import json
import sys
from typing import Any, Dict, List, Optional

import httpx
import pytest

from conftest import (
    JOB_ID,
    KEY,
    PaperClock,
    Serve,
    answer,
    async_client,
    error,
    exchange,
    mixed_singles,
    result_json,
    running_job,
    single_job_id,
    sync_client,
)
import verifyinboxes
from verifyinboxes import AsyncVerifyInboxes, VerifyInboxesError, WaitTimeoutError

pytestmark = pytest.mark.anyio

EMAILS = [f"p{n}@example.com" for n in range(1, 11)]


def _singles() -> List[Dict[str, Any]]:
    return [
        exchange("POST", "/v1/verify", answer(json={**result_json(email), "job_id": f"019a3b6e-8f0c-7d2e-9a41-5c3e2f1b0b{n:02d}"}),
                 json={"email": email}, idempotency_key="new")
        for n, email in enumerate(EMAILS, 1)
    ]


async def test_verify_many_sends_ten_together(serve: Serve, clock: PaperClock) -> None:
    api = serve(_singles(), unordered=True, hold_until=10)
    async with async_client(api, clock) as client:
        results = await client.verify_many(EMAILS)
    assert not api.held_out, f"only {api.max_in_flight} checks were in flight at once"
    assert api.max_in_flight == 10
    assert [r.email for r in results] == EMAILS
    assert [r.job_id for r in results] == [f"019a3b6e-8f0c-7d2e-9a41-5c3e2f1b0b{n:02d}" for n in range(1, 11)]
    # Started 100 ms apart.
    assert clock.slept == [0.1] * 9


def test_sync_verify_many_sends_ten_together(serve: Serve, clock: PaperClock) -> None:
    api = serve(_singles(), unordered=True, hold_until=10)
    with sync_client(api, clock) as client:
        results = client.verify_many(EMAILS)
    assert not api.held_out, f"only {api.max_in_flight} checks were in flight at once"
    assert [r.email for r in results] == EMAILS


def _page(rows: List[str], start: int, next_cursor: Any) -> Dict[str, Any]:
    data = [{**result_json(email), "row": start + i} for i, email in enumerate(rows)]
    return {"data": data, "next_cursor": next_cursor, "complete": True}


async def test_async_for_over_results(serve: Serve, clock: PaperClock) -> None:
    path = f"/v1/jobs/{JOB_ID}/results"
    api = serve([
        exchange("GET", path, answer(json=_page(["a@example.com", "b@example.com"], 1, "c1")), query={"limit": "2"}),
        exchange("GET", path, answer(json=_page([], 3, "c2")), query={"limit": "2", "cursor": "c1"}),
        exchange("GET", path, answer(json=_page(["c@example.com"], 3, None)), query={"limit": "2", "cursor": "c2"}),
    ])
    async with async_client(api, clock) as client:
        rows = [row async for row in client.lists.results(JOB_ID, page_size=2)]
        assert len(api.requests) == 3
    assert [(r.row, r.email) for r in rows] == [(1, "a@example.com"), (2, "b@example.com"), (3, "c@example.com")]


async def test_results_are_read_lazily(serve: Serve) -> None:
    path = f"/v1/jobs/{JOB_ID}/results"
    api = serve([exchange("GET", path, answer(json=_page(["a@example.com"], 1, None)), query={"limit": "500"})])
    async with async_client(api) as client:
        rows = client.lists.results(JOB_ID)
        assert api.requests == []
        assert [r.email async for r in rows] == ["a@example.com"]


async def test_verify_many_error_names_every_job_made(serve: Serve, clock: PaperClock) -> None:
    api = serve(mixed_singles(EMAILS), unordered=True)
    async with async_client(api, clock) as client:
        with pytest.raises(VerifyInboxesError) as raised:
            await client.verify_many(EMAILS, timeout=5)
    # The first error in input order (p3's wait), naming every job the call made, in input order.
    assert isinstance(raised.value, WaitTimeoutError)
    assert raised.value.job_ids == [single_job_id(n) for n in range(1, 11) if n != 7]


async def _long_polls(seconds: float) -> None:
    """A `_sleep` whose starts are instant and whose waits between polls outlast the test's own timeout."""
    await asyncio.sleep(30 if seconds >= 1 else 0)


def _ids_in_chain(error: Optional[BaseException]) -> Optional[List[str]]:
    while error is not None:
        if getattr(error, "job_ids", None):
            return list(getattr(error, "job_ids"))
        error = error.__cause__ or error.__context__
    return None


async def test_verify_many_cancelled_names_the_checks_it_made(serve: Serve, caplog: pytest.LogCaptureFixture) -> None:
    pending = [exchange("POST", "/v1/verify", answer(202, {"status": "pending", "job_id": single_job_id(n)}),
                        json={"email": email}) for n, email in enumerate(EMAILS, 1)]
    api = serve(pending, unordered=True)
    async with async_client(api) as client:
        client._sleep = _long_polls
        with pytest.raises(asyncio.TimeoutError) as raised:
            await asyncio.wait_for(client.verify_many(EMAILS), 1.0)
    made = [single_job_id(n) for n in range(1, 11)]
    assert f"interrupted; still running: {', '.join(made)}" in caplog.text
    if sys.version_info >= (3, 12):
        assert _ids_in_chain(raised.value) == made


async def test_verify_many_cancelled_names_its_list(serve: Serve, caplog: pytest.LogCaptureFixture) -> None:
    emails = [f"p{n}@example.com" for n in range(1, 12)]
    api = serve([
        exchange("POST", "/v1/jobs", answer(201, {"id": JOB_ID, "status": "running", "total": 11}), json={"emails": emails}),
        exchange("GET", f"/v1/jobs/{JOB_ID}", answer(json=running_job(JOB_ID, "bulk"))),
    ])
    async with async_client(api) as client:
        client._sleep = _long_polls
        with pytest.raises(asyncio.TimeoutError) as raised:
            await asyncio.wait_for(client.verify_many(emails), 1.0)
    assert f"interrupted; still running: {JOB_ID}" in caplog.text
    if sys.version_info >= (3, 12):
        assert _ids_in_chain(raised.value) == [JOB_ID]


def _slow_api(answers: Dict[str, httpx.Response]) -> httpx.AsyncClient:
    """An API that holds every POST for 1.5 seconds before it answers: the address's (or the list's) answer."""

    async def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        await asyncio.sleep(1.5)
        return answers[body["email"] if "email" in body else "list"]

    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


async def test_verify_many_cancelled_while_its_checks_are_sent_names_them(caplog: pytest.LogCaptureFixture) -> None:
    ids = {"p1@example.com": single_job_id(1), "p2@example.com": single_job_id(2)}
    http = _slow_api({email: httpx.Response(202, json={"status": "pending", "job_id": job_id}) for email, job_id in ids.items()})
    async with AsyncVerifyInboxes(KEY, base_url="https://api.example.com", http_client=http) as client:
        with pytest.raises(asyncio.TimeoutError) as raised:
            await asyncio.wait_for(client.verify_many(list(ids)), 1.0)
    await http.aclose()
    made = list(ids.values())
    assert f"interrupted; still running: {', '.join(made)}" in caplog.text
    if sys.version_info >= (3, 12):
        assert _ids_in_chain(raised.value) == made


async def test_verify_many_cancelled_while_its_list_is_made_names_it(caplog: pytest.LogCaptureFixture) -> None:
    emails = [f"p{n}@example.com" for n in range(1, 12)]
    http = _slow_api({"list": httpx.Response(201, json={"id": JOB_ID, "status": "running", "total": 11})})
    async with AsyncVerifyInboxes(KEY, base_url="https://api.example.com", http_client=http) as client:
        with pytest.raises(asyncio.TimeoutError) as raised:
            await asyncio.wait_for(client.verify_many(emails), 1.0)
    await http.aclose()
    assert f"interrupted; still running: {JOB_ID}" in caplog.text
    if sys.version_info >= (3, 12):
        assert _ids_in_chain(raised.value) == [JOB_ID]


async def test_verify_cancelled_while_its_check_is_sent_names_it(caplog: pytest.LogCaptureFixture) -> None:
    http = _slow_api({"ann@example.com": httpx.Response(202, json={"status": "pending", "job_id": JOB_ID})})
    async with AsyncVerifyInboxes(KEY, base_url="https://api.example.com", http_client=http) as client:
        with pytest.raises(asyncio.TimeoutError) as raised:
            await asyncio.wait_for(client.verify("ann@example.com"), 1.0)
    await http.aclose()
    assert f"interrupted; still running: {JOB_ID}" in caplog.text
    if sys.version_info >= (3, 12):
        assert _ids_in_chain(raised.value) == [JOB_ID]


async def test_verify_cancelled_while_waiting_names_its_check(serve: Serve, caplog: pytest.LogCaptureFixture) -> None:
    api = serve([exchange("POST", "/v1/verify", answer(202, {"status": "pending", "job_id": JOB_ID}),
                          json={"email": "ann@example.com"})])
    async with async_client(api) as client:
        client._sleep = _long_polls
        with pytest.raises(asyncio.TimeoutError) as raised:
            await asyncio.wait_for(client.verify("ann@example.com"), 1.0)
    assert f"interrupted; still running: {JOB_ID}" in caplog.text
    if sys.version_info >= (3, 12):
        assert _ids_in_chain(raised.value) == [JOB_ID]


async def _caller_cancelled() -> bool:
    """For a fake API's handler, which runs in the request's own context: waits until the SDK has taken in that the
    request's caller was cancelled, so that the answer comes after; False if it never does."""
    interrupted = verifyinboxes._base.CALLER_INTERRUPTED.get()
    for _ in range(1000):
        if interrupted is None or interrupted.is_set():
            break
        await asyncio.sleep(0.01)
    return interrupted is not None and interrupted.is_set()


async def _instant(seconds: float) -> None:
    await asyncio.sleep(0)


async def test_verify_cancelled_while_its_check_is_sent_sends_it_no_more(caplog: pytest.LogCaptureFixture) -> None:
    arrived = asyncio.Event()
    posts: List[bool] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        if posts:
            return httpx.Response(202, json={"status": "pending", "job_id": JOB_ID})  # a retry makes the check
        arrived.set()
        posts.append(await _caller_cancelled())
        return httpx.Response(503, json=error("unavailable"))

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    async with AsyncVerifyInboxes(KEY, base_url="https://api.example.com", http_client=http, timeout=5) as client:
        client._sleep = _instant
        task = asyncio.ensure_future(client.verify("ann@example.com"))
        await asyncio.wait_for(arrived.wait(), 10)
        task.cancel()
        await asyncio.wait({task}, timeout=10)
    await http.aclose()
    # The attempt on its way is waited for; once it fails, nothing more is sent: a cancellation waits one attempt.
    assert task.cancelled() and posts == [True]
    assert "still running" not in caplog.text


async def test_verify_many_cancelled_while_its_list_is_made_sends_it_no_more(caplog: pytest.LogCaptureFixture) -> None:
    emails = [f"p{n}@example.com" for n in range(1, 12)]
    arrived = asyncio.Event()
    posts: List[bool] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.method != "POST":
            return httpx.Response(200, json=running_job(JOB_ID, "bulk"))
        if posts:
            return httpx.Response(201, json={"id": JOB_ID, "status": "running", "total": 11})  # a retry makes it
        arrived.set()
        posts.append(await _caller_cancelled())
        return httpx.Response(503, json=error("unavailable"))

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    async with AsyncVerifyInboxes(KEY, base_url="https://api.example.com", http_client=http, timeout=5) as client:
        client._sleep = _instant
        task = asyncio.ensure_future(client.verify_many(emails, timeout=5))
        await asyncio.wait_for(arrived.wait(), 10)
        task.cancel()
        await asyncio.wait({task}, timeout=10)
    await http.aclose()
    assert task.cancelled() and posts == [True]
    assert "still running" not in caplog.text
