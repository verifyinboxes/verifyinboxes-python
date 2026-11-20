"""The client's configuration, uploads, downloads and secrets, beyond what the conformance scenarios hold it to."""
from __future__ import annotations

import asyncio
import contextlib
import copy
import io
import logging
import pickle
import signal
import sys
import threading
import time
from pathlib import Path
from types import FrameType
from typing import Any, Dict, Iterator, List, Optional

import httpx
import pytest

import verifyinboxes
from conftest import (
    JOB_ID,
    KEY,
    PaperClock,
    Serve,
    answer,
    error,
    exchange,
    mixed_singles,
    result_json,
    running_job,
    single_job_id,
    sync_client,
)
from verifyinboxes import (
    APIConnectionError,
    APIError,
    APITimeoutError,
    AsyncVerifyInboxes,
    AuthenticationError,
    ConflictError,
    InsufficientCreditsError,
    IntegrationCreated,
    InvalidRequestError,
    MissingAPIKeyError,
    NotFoundError,
    PermissionDeniedError,
    RateLimitError,
    ServerError,
    VerifyInboxes,
    VerifyInboxesError,
    WaitTimeoutError,
    WebhookCreated,
)

CSV = "email\nann@example.com\nbob@example.com\n"
CREATED = {"id": JOB_ID, "status": "running", "total": 2}
SECRET = "6f1d0c9e8b7a6f5e4d3c2b1a0f9e8d7c6b5a4f3e2d1c0b9a8f7e6d5c4b3a2f1e"


@pytest.fixture(autouse=True)
def _no_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("VERIFYINBOXES_API_KEY", raising=False)
    monkeypatch.delenv("VERIFYINBOXES_BASE_URL", raising=False)


# --- configuration --------------------------------------------------------------------------------------------------


def test_missing_key_names_the_variable() -> None:
    for make in (VerifyInboxes, AsyncVerifyInboxes):
        with pytest.raises(MissingAPIKeyError) as raised:
            make()
        message = str(raised.value)
        assert "VERIFYINBOXES_API_KEY" in message
        assert "Developers" in message and "API keys" in message and "admin" in message
        with pytest.raises(MissingAPIKeyError):
            make("   ")
    assert issubclass(MissingAPIKeyError, VerifyInboxesError)


def test_key_from_environment_and_base_url_override(serve: Serve, monkeypatch: pytest.MonkeyPatch) -> None:
    account = {"name": "acme", "balance": 10, "reviewed": False, "test_emails": False, "safe_to_send_threshold": 0.8}
    from_env = serve([exchange("GET", "/v1/account", answer(json=account))])
    given = serve([exchange("GET", "/v1/account", answer(json=account))])
    monkeypatch.setenv("VERIFYINBOXES_API_KEY", f"  {KEY}\n")
    monkeypatch.setenv("VERIFYINBOXES_BASE_URL", from_env.url + "/")

    with VerifyInboxes() as client:
        assert client.base_url == from_env.url
        assert client.account.get().balance == 10
    assert from_env.requests[0].headers["authorization"] == f"Bearer {KEY}"
    assert from_env.requests[0].headers["accept"] == "application/json"
    assert from_env.requests[0].headers["user-agent"] == f"verifyinboxes-python/{verifyinboxes.__version__}"

    token = "vi_at_testtesttesttesttesttesttesttesttesttest"
    with VerifyInboxes(token, base_url=given.url) as client:
        client.account.get()
    assert given.requests[0].headers["authorization"] == f"Bearer {token}"
    assert len(from_env.requests) == 1

    assert VerifyInboxes().base_url == from_env.url
    monkeypatch.delenv("VERIFYINBOXES_BASE_URL")
    assert VerifyInboxes().base_url == "https://api.verifyinboxes.com"


def test_user_agent_puts_your_product_in_front(serve: Serve) -> None:
    account = {"name": "acme", "balance": 10, "reviewed": False, "test_emails": False, "safe_to_send_threshold": 0.8}
    api = serve([exchange("GET", "/v1/account", answer(json=account))] * 3)
    library = f"verifyinboxes-python/{verifyinboxes.__version__}"
    with sync_client(api, user_agent="my-app/1.2") as client:
        client.account.get()
    with sync_client(api, user_agent="") as client:
        client.account.get()

    async def from_async() -> None:
        async with AsyncVerifyInboxes(KEY, base_url=api.url, user_agent="my-app/1.2") as client:
            await client.account.get()

    asyncio.run(from_async())
    assert [r.headers["user-agent"] for r in api.requests] == [f"my-app/1.2 {library}", library, f"my-app/1.2 {library}"]

    for bad in ("my app/1.2", "app/1\r\nX-Other: 1", "app\u00e9/1", "app/1\x00", 7):
        for make in (VerifyInboxes, AsyncVerifyInboxes):
            with pytest.raises(ValueError) as raised:
                make(KEY, user_agent=bad)  # type: ignore[arg-type]
            assert "my-app/1.2" in str(raised.value)


def test_refuses_a_key_with_a_newline() -> None:
    for bad in (f"{KEY}\nX-Other: 1", f"bv_test test", f"{KEY}\x00", "bv_\ttest"):
        with pytest.raises(ValueError) as raised:
            VerifyInboxes(bad)
        assert KEY not in str(raised.value) and "bv_" not in str(raised.value)
        with pytest.raises(ValueError):
            AsyncVerifyInboxes(bad)


def test_refuses_a_key_that_is_not_ascii() -> None:
    for bad in (f"{KEY}\u200b", "bv_t\u00e9sttesttesttesttesttesttesttest", f"\u00a0{KEY}x\u00a0y"):
        with pytest.raises(ValueError) as raised:
            VerifyInboxes(bad)
        assert "copy it again" in str(raised.value) and "bv_" not in str(raised.value)


def test_base_url_must_be_http() -> None:
    for bad in ("ftp://api.example.com", "api.example.com", "file:///etc/passwd"):
        with pytest.raises(ValueError):
            VerifyInboxes(KEY, base_url=bad)
    assert VerifyInboxes(KEY, base_url="http://127.0.0.1:8080/").base_url == "http://127.0.0.1:8080"


def test_repr_hides_the_key_and_the_webhook_secret() -> None:
    client = VerifyInboxes(KEY)
    assert KEY not in repr(client) and KEY[:7] not in repr(client)
    assert KEY not in repr(AsyncVerifyInboxes(KEY))

    created = WebhookCreated.from_dict({"id": "019a4f10-2b3c-7d4e-8f5a-6b7c8d9e0f12", "url": "https://example.com/hook",
                                        "events": ["job.completed"], "secret": SECRET, "created_at": "2026-11-18T10:00:00Z"})
    assert SECRET not in repr(created) and SECRET not in str(created)
    assert created.secret == SECRET and created.to_dict()["secret"] == SECRET

    hook_url = "https://api.verifyinboxes.com/hooks/instantly/whk_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
    integration = IntegrationCreated.from_dict({"id": "019a4f22-7c1d-7e2f-9a3b-4c5d6e7f8a90", "provider": "instantly",
                                                "label": None, "url": hook_url, "created_at": "2026-11-18T10:00:00Z"})
    assert "whk_" not in repr(integration)
    assert integration.to_dict()["url"] == hook_url


ERRORS: List[VerifyInboxesError] = [
    VerifyInboxesError("the API returned the same cursor twice", job_ids=[JOB_ID]),
    MissingAPIKeyError("No API key."),
    APIConnectionError("Could not reach the VerifyInboxes API.", job_ids=[JOB_ID]),
    APITimeoutError(job_ids=[JOB_ID]),
    WaitTimeoutError("still running", job_ids=[JOB_ID, "019a3b6e-8f0c-7d2e-9a41-5c3e2f1b0a77"], timeout=5.0),
    *[cls("no", status=status, code="some_code", request_id="019a7e00-0000-7000-8000-0000000000cc", retry_after=1.0,
          body={"error": {"code": "some_code"}}, job_ids=[JOB_ID])
      for cls, status in ((APIError, 418), (InvalidRequestError, 422), (AuthenticationError, 401),
                          (InsufficientCreditsError, 402), (PermissionDeniedError, 403), (NotFoundError, 404),
                          (ConflictError, 409), (RateLimitError, 429), (ServerError, 502))],
]


@pytest.mark.parametrize("error", ERRORS, ids=[type(e).__name__ for e in ERRORS])
def test_errors_survive_pickle_and_copy(error: VerifyInboxesError) -> None:
    copies = [pickle.loads(pickle.dumps(error, protocol)) for protocol in range(2, pickle.HIGHEST_PROTOCOL + 1)]
    copies += [copy.copy(error), copy.deepcopy(error)]
    for again in copies:
        assert type(again) is type(error)
        assert str(again) == str(error) and again.args == error.args
        assert again.job_ids == error.job_ids and again.message == error.message
        for name in ("status", "code", "request_id", "retry_after", "body", "timeout"):
            assert getattr(again, name, None) == getattr(error, name, None), name


def test_unknown_fields_are_ignored_and_unknown_values_kept(serve: Serve) -> None:
    api = serve([exchange("POST", "/v1/verify", answer(json={**result_json(verdict="spam_trap", reason="new_reason"),
                                                                "job_id": JOB_ID, "something_new": 1}))])
    with sync_client(api) as client:
        result = client.verify("ann@example.com")
    assert (result.verdict, result.reason, result.job_id) == ("spam_trap", "new_reason", JOB_ID)
    assert "something_new" not in result.to_dict()


# --- uploads --------------------------------------------------------------------------------------------------------


def _upload_exchanges(first: Dict[str, Any], filename: str = "list.csv") -> List[Dict[str, Any]]:
    parts = [{"name": "email_column", "value": "email"},
             {"name": "file", "filename": filename, "content_type": "text/csv", "value": CSV}]
    return [
        exchange("POST", "/v1/jobs", first, multipart=parts, idempotency_key="new"),
        exchange("POST", "/v1/jobs", answer(201, CREATED), multipart=parts, idempotency_key="same"),
    ]


class OneWay(io.RawIOBase):
    """A stream that can be read once, from the start: no seek, no tell."""

    def __init__(self, data: bytes) -> None:
        self._data = io.BytesIO(data)

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return False

    def readinto(self, buffer: Any) -> int:
        chunk = self._data.read(len(buffer))
        buffer[: len(chunk)] = chunk
        return len(chunk)


def test_stream_body_not_resent(serve: Serve, clock: PaperClock) -> None:
    api = serve(_upload_exchanges({"operation": None, "drop": True}))
    with sync_client(api, clock) as client:
        with pytest.raises(APIConnectionError):
            client.lists.create(file=OneWay(CSV.encode()))
    assert len(api.requests) == 1
    assert api.requests[0].multipart is not None and api.requests[0].multipart[1]["value"] == CSV
    assert clock.slept == []


def test_seekable_file_rewound_on_retry(serve: Serve, clock: PaperClock) -> None:
    api = serve(_upload_exchanges(answer(503, error("unavailable"), headers={"retry-after": "1"})))
    stream = io.BytesIO(CSV.encode())
    with sync_client(api, clock) as client:
        created = client.lists.create(file=stream)
    assert created.id == JOB_ID
    assert [r.multipart[1]["value"] for r in api.requests if r.multipart] == [CSV, CSV]
    assert clock.slept == [1]


def test_a_path_is_opened_again_on_retry_and_named(serve: Serve, clock: PaperClock, tmp_path: Path) -> None:
    path = tmp_path / "prospects.csv"
    path.write_text(CSV, encoding="utf-8")
    api = serve(_upload_exchanges({"operation": None, "drop": True}, filename="prospects.csv"))
    with sync_client(api, clock) as client:
        assert client.lists.create(file=path).id == JOB_ID
    assert [r.multipart[1]["value"] for r in api.requests if r.multipart] == [CSV, CSV]


def test_a_file_open_in_text_mode_is_refused_clearly(serve: Serve, tmp_path: Path) -> None:
    path = tmp_path / "prospects.csv"
    path.write_text(CSV, encoding="utf-8")
    api = serve([])
    with sync_client(api) as client, open(path, encoding="utf-8") as text:
        for stream in (text, io.StringIO(CSV)):
            with pytest.raises(TypeError) as raised:
                client.lists.create(file=stream)  # type: ignore[arg-type]
            assert "open(path, 'rb')" in str(raised.value)
    assert api.requests == []


def test_create_takes_exactly_one_source(serve: Serve) -> None:
    api = serve([])
    with sync_client(api) as client:
        with pytest.raises(ValueError):
            client.lists.create()
        with pytest.raises(ValueError):
            client.lists.create(emails=["a@example.com"], csv=CSV)
    assert api.requests == []


# --- downloads ------------------------------------------------------------------------------------------------------

DOWNLOAD = f"/v1/jobs/{JOB_ID}/download"
BODY = "email,verdict\n" + "ann@example.com,invalid\n" * 50


def test_download_to_path_is_atomic(serve: Serve, clock: PaperClock, tmp_path: Path) -> None:
    target = tmp_path / "results.csv"
    target.write_text("the results from before\n", encoding="utf-8")
    cut = answer(200, headers={"content-type": "text/csv; charset=utf-8"}, text=BODY, cut_after=100)
    api = serve([exchange("GET", DOWNLOAD, cut, headers={"accept": "text/csv"})])
    with sync_client(api, clock, max_retries=0) as client:
        with pytest.raises(APIConnectionError):
            client.lists.download(JOB_ID, target)
    assert target.read_text("utf-8") == "the results from before\n"
    assert sorted(p.name for p in tmp_path.iterdir()) == ["results.csv"]

    whole = answer(200, headers={"content-type": "text/csv; charset=utf-8"}, text=BODY)
    api = serve([exchange("GET", DOWNLOAD, whole)])
    with sync_client(api, clock) as client:
        assert client.lists.download(JOB_ID, str(target)) == len(BODY.encode())
    assert target.read_text("utf-8") == BODY
    assert sorted(p.name for p in tmp_path.iterdir()) == ["results.csv"]


def test_download_to_a_stream_is_not_retried_after_its_first_byte(serve: Serve, clock: PaperClock) -> None:
    cut = answer(200, headers={"content-type": "text/csv; charset=utf-8"}, text=BODY, cut_after=100)
    api = serve([exchange("GET", DOWNLOAD, cut), exchange("GET", DOWNLOAD, cut)])
    sink = io.BytesIO()
    with sync_client(api, clock) as client:
        with pytest.raises(APIConnectionError):
            client.lists.download(JOB_ID, sink)
    assert len(api.requests) == 1

    whole = answer(200, headers={"content-type": "text/csv; charset=utf-8"}, text=BODY)
    api = serve([exchange("GET", DOWNLOAD, {"operation": None, "drop": True}), exchange("GET", DOWNLOAD, whole)])
    sink = io.BytesIO()
    with sync_client(api, clock) as client:
        assert client.lists.download(JOB_ID, sink) == len(BODY.encode())
    assert sink.getvalue() == BODY.encode() and len(api.requests) == 2


# --- verify_many ----------------------------------------------------------------------------------------------------


def test_verify_many_refuses_empty_input(serve: Serve) -> None:
    api = serve([])
    with sync_client(api) as client:
        for bad in ([], ["ann@example.com", ""], ["ann@example.com", None], ["ann@example.com", 7]):
            with pytest.raises(ValueError):
                client.verify_many(bad)  # type: ignore[arg-type]
        with pytest.raises(TypeError):
            client.verify_many("ann@example.com")
    assert api.requests == []


def test_verify_many_error_names_every_job_made(serve: Serve, clock: PaperClock) -> None:
    emails = [f"p{n}@example.com" for n in range(1, 11)]
    api = serve(mixed_singles(emails), unordered=True)
    with sync_client(api, clock) as client:
        with pytest.raises(VerifyInboxesError) as raised:
            client.verify_many(emails, timeout=5)
    made = [single_job_id(n) for n in range(1, 11) if n != 7]
    # The first error in input order (p3's wait), naming every job the call made, in input order.
    assert isinstance(raised.value, WaitTimeoutError)
    assert raised.value.job_ids == made


class Interrupting:
    """A `_sleep` that is interrupted (Ctrl-C) at its `at`-th call of `seconds` in the calling thread; the others pass."""

    def __init__(self, seconds: float, at: int) -> None:
        self.seconds, self.at, self.calls = seconds, at, 0

    def __call__(self, seconds: float) -> None:
        if seconds == self.seconds and threading.current_thread() is threading.main_thread():
            self.calls += 1
            if self.calls == self.at:
                raise KeyboardInterrupt
        time.sleep(0.01)


def test_verify_many_interrupted_names_the_checks_it_made(serve: Serve, caplog: pytest.LogCaptureFixture) -> None:
    emails = [f"p{n}@example.com" for n in range(1, 11)]
    pending = [exchange("POST", "/v1/verify", answer(202, {"status": "pending", "job_id": single_job_id(n)}),
                        json={"email": email}) for n, email in enumerate(emails, 1)]
    polls = [exchange("GET", f"/v1/jobs/{single_job_id(n)}", answer(json=running_job(single_job_id(n))))
             for n in range(1, 11) for _ in range(5)]
    api = serve(pending + polls, unordered=True)
    with sync_client(api) as client:
        client._sleep = Interrupting(0.1, at=3)  # Ctrl-C while starting the 4th check
        with pytest.raises(KeyboardInterrupt) as raised:
            client.verify_many(emails)
    started = [single_job_id(n) for n in (1, 2, 3)]
    assert getattr(raised.value, "job_ids") == started
    assert f"interrupted; still running: {', '.join(started)}" in caplog.text
    if sys.version_info >= (3, 11):
        assert any(", ".join(started) in note for note in getattr(raised.value, "__notes__", []))


def test_verify_many_interrupted_names_its_list(serve: Serve, caplog: pytest.LogCaptureFixture) -> None:
    emails = [f"p{n}@example.com" for n in range(1, 12)]
    created = {"id": JOB_ID, "status": "running", "total": 11}
    api = serve([
        exchange("POST", "/v1/jobs", answer(201, created), json={"emails": emails}),
        exchange("GET", f"/v1/jobs/{JOB_ID}", answer(json=running_job(JOB_ID, "bulk"))),
    ])
    with sync_client(api) as client:
        client._sleep = Interrupting(2.0, at=1)  # Ctrl-C while waiting for the list
        with pytest.raises(KeyboardInterrupt) as raised:
            client.verify_many(emails)
    assert getattr(raised.value, "job_ids") == [JOB_ID]
    assert f"interrupted; still running: {JOB_ID}" in caplog.text


def test_verify_interrupted_while_waiting_names_its_check(serve: Serve, caplog: pytest.LogCaptureFixture) -> None:
    api = serve([
        exchange("POST", "/v1/verify", answer(202, {"status": "pending", "job_id": JOB_ID}), json={"email": "ann@example.com"}),
        exchange("GET", f"/v1/jobs/{JOB_ID}", answer(json=running_job(JOB_ID))),
    ])
    with sync_client(api) as client:
        client._sleep = Interrupting(2.0, at=1)  # Ctrl-C while waiting for the check
        with pytest.raises(KeyboardInterrupt) as raised:
            client.verify("ann@example.com")
    assert getattr(raised.value, "job_ids", None) == [JOB_ID]
    assert f"interrupted; still running: {JOB_ID}" in caplog.text


def _waiting_in(caller: str, frame: Optional[FrameType]) -> bool:
    """The thread whose newest frame this is waits, in `caller`, for the answer of a request sent in a thread of its
    own (`Condition.wait`, in `Future.result`, in `caller`)."""
    names: List[str] = []
    while frame is not None and len(names) < 3:
        names.append(frame.f_code.co_name)
        frame = frame.f_back
    return names == ["wait", "result", caller]


@contextlib.contextmanager
def _ctrl_c_while_waiting_in(caller: str, arrived: threading.Event) -> Iterator[threading.Event]:
    """Ctrl-C, pressed once a request has reached the API (`arrived`) and the main thread waits for its answer in
    `caller`: never sooner, while the request is still being handed to its thread, and never by the clock. Yields an
    event set once the Ctrl-C has reached the main thread: the API holds its answer until then."""
    main = threading.main_thread().ident
    assert main is not None
    interrupted, finished = threading.Event(), threading.Event()

    def on_ctrl_c(signum: int, frame: Optional[FrameType]) -> None:
        if finished.is_set():
            return  # too late to mean anything: the test is over
        interrupted.set()
        raise KeyboardInterrupt

    def press() -> None:
        deadline = time.monotonic() + 10
        while not finished.is_set() and time.monotonic() < deadline:
            if arrived.is_set() and _waiting_in(caller, sys._current_frames().get(main)):
                signal.pthread_kill(main, signal.SIGINT)
                return
            time.sleep(0.005)

    previous = signal.signal(signal.SIGINT, on_ctrl_c)
    presser = threading.Thread(target=press, name="ctrl-c", daemon=True)
    presser.start()
    try:
        yield interrupted
    finally:
        finished.set()
        presser.join()
        signal.signal(signal.SIGINT, previous)


@pytest.mark.skipif(not hasattr(signal, "pthread_kill"), reason="needs signal.pthread_kill")
def test_verify_many_interrupted_while_its_list_is_made_names_it(caplog: pytest.LogCaptureFixture) -> None:
    emails = [f"p{n}@example.com" for n in range(1, 12)]
    arrived = threading.Event()
    with _ctrl_c_while_waiting_in("_finish_in_thread", arrived) as interrupted:

        def handler(request: httpx.Request) -> httpx.Response:
            if request.method != "POST":
                return httpx.Response(200, json=running_job(JOB_ID, "bulk"))
            arrived.set()
            interrupted.wait(10)  # the API is still making the list when Ctrl-C comes
            return httpx.Response(201, json={"id": JOB_ID, "status": "running", "total": 11})

        http = httpx.Client(transport=httpx.MockTransport(handler))
        with VerifyInboxes(KEY, base_url="https://api.example.com", http_client=http, timeout=5) as client:
            with pytest.raises(KeyboardInterrupt) as raised:
                client.verify_many(emails, timeout=5)  # a Ctrl-C that never comes fails in seconds
        http.close()
    assert interrupted.is_set()
    assert getattr(raised.value, "job_ids", None) == [JOB_ID]
    assert f"interrupted; still running: {JOB_ID}" in caplog.text


def _caller_interrupted() -> bool:
    """For a fake API's handler, which runs in the request's own context: waits until the SDK has taken in that the
    request's caller was interrupted, so that the answer comes after; False if it never does."""
    interrupted = verifyinboxes._base.CALLER_INTERRUPTED.get()
    return interrupted is not None and interrupted.wait(10)


@pytest.mark.skipif(not hasattr(signal, "pthread_kill"), reason="needs signal.pthread_kill")
def test_verify_many_interrupted_while_its_list_is_made_sends_it_no_more(caplog: pytest.LogCaptureFixture) -> None:
    emails = [f"p{n}@example.com" for n in range(1, 12)]
    arrived = threading.Event()
    posts: List[bool] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method != "POST":
            return httpx.Response(200, json=running_job(JOB_ID, "bulk"))
        if posts:
            return httpx.Response(201, json={"id": JOB_ID, "status": "running", "total": 11})  # a retry makes it
        arrived.set()
        posts.append(_caller_interrupted())
        return httpx.Response(503, json=error("unavailable"))

    with _ctrl_c_while_waiting_in("_finish_in_thread", arrived):
        http = httpx.Client(transport=httpx.MockTransport(handler))
        with VerifyInboxes(KEY, base_url="https://api.example.com", http_client=http, timeout=5) as client:
            with pytest.raises(KeyboardInterrupt) as raised:
                client.verify_many(emails, timeout=5)
        http.close()
    # The attempt on its way is waited for; once it fails, nothing more is sent: an interruption waits one attempt.
    assert posts == [True]
    assert getattr(raised.value, "job_ids", None) is None and "still running" not in caplog.text


@pytest.mark.skipif(not hasattr(signal, "pthread_kill"), reason="needs signal.pthread_kill")
def test_verify_many_interrupted_while_its_checks_are_sent_sends_them_no_more(caplog: pytest.LogCaptureFixture) -> None:
    arrived = threading.Event()
    posts: List[bool] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method != "POST":
            return httpx.Response(200, json=running_job(JOB_ID))
        if posts:
            return httpx.Response(202, json={"status": "pending", "job_id": JOB_ID})  # a retry makes the check
        arrived.set()
        posts.append(_caller_interrupted())
        return httpx.Response(503, json=error("unavailable"))

    with _ctrl_c_while_waiting_in("_many", arrived):
        http = httpx.Client(transport=httpx.MockTransport(handler))
        with VerifyInboxes(KEY, base_url="https://api.example.com", http_client=http, timeout=5) as client:
            with pytest.raises(KeyboardInterrupt) as raised:
                client.verify_many(["ann@example.com"], timeout=5)
        http.close()
    assert posts == [True]
    assert getattr(raised.value, "job_ids", None) is None and "still running" not in caplog.text


def test_results_stop_when_a_page_with_rows_repeats_the_cursor(serve: Serve) -> None:
    path = f"/v1/jobs/{JOB_ID}/results"
    page = {"data": [{**result_json("a@example.com"), "row": 1}], "next_cursor": "c1", "complete": True}
    api = serve([
        exchange("GET", path, answer(json=page), query={"limit": "500"}),
        exchange("GET", path, answer(json=page), query={"limit": "500", "cursor": "c1"}),
    ])
    seen: List[str] = []
    with sync_client(api) as client:
        with pytest.raises(VerifyInboxesError) as raised:
            for row in client.lists.results(JOB_ID):
                seen.append(row.email)
    assert type(raised.value) is VerifyInboxesError and "same cursor" in str(raised.value)
    assert seen == ["a@example.com", "a@example.com"] and len(api.requests) == 2


# --- secrets in logs ------------------------------------------------------------------------------------------------


def test_logs_no_secret(serve: Serve, caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG, logger="verifyinboxes")
    refused = answer(401, error("unauthorized", "missing, malformed or revoked API key"), request_id="019a7e00-0000-7000-8000-0000000000bb")
    api = serve([exchange("GET", "/v1/account", refused)])
    with sync_client(api) as client:
        with pytest.raises(AuthenticationError) as raised:
            client.account.get()
    assert "019a7e00-0000-7000-8000-0000000000bb" in caplog.text and "401" in caplog.text
    assert "bv_" not in caplog.text and "malformed" not in caplog.text
    assert "bv_" not in str(raised.value) and "bv_" not in repr(raised.value)
