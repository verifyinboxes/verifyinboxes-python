"""The two clients: `VerifyInboxes` (sync) and `AsyncVerifyInboxes` (async), with the same surface."""
from __future__ import annotations

import asyncio
import time
from types import TracebackType
from typing import Awaitable, Callable, List, Literal, Optional, Sequence, Type, Union, overload

import httpx

from ._base import (
    CHECK_TIMEOUT,
    DEFAULT_MAX_RETRIES,
    DEFAULT_TIMEOUT,
    MANY_TIMEOUT,
    Answer,
    BaseClient,
    Call,
    Destination,
    Sink,
    caller_interrupted,
    check_timeout,
    connection_error,
    error_from_answer,
    log_answer,
    log_no_answer,
    parse_body,
)
from ._exceptions import VerifyInboxesError
from ._models import PendingCheck, Result
from ._resources import (
    AccountResource,
    AsyncAccountResource,
    AsyncChecks,
    AsyncDomains,
    AsyncFeedback,
    AsyncIntegrations,
    AsyncLists,
    AsyncNotices,
    AsyncSuppressions,
    AsyncWebhooks,
    Checks,
    Domains,
    Feedback,
    Integrations,
    Lists,
    Notices,
    Suppressions,
    Webhooks,
)

__all__ = ["VerifyInboxes", "AsyncVerifyInboxes"]

class VerifyInboxes(BaseClient):
    """The VerifyInboxes API.

        client = VerifyInboxes()                    # reads VERIFYINBOXES_API_KEY
        result = client.verify("ann@example.com")
        print(result.verdict, result.safe_to_send)

    `api_key` is an API key (`bv_...`) or a personal access token (`vi_at_...`); default: the `VERIFYINBOXES_API_KEY`
    environment variable. `base_url` defaults to `VERIFYINBOXES_BASE_URL`, else `https://api.verifyinboxes.com`.
    `timeout` is the seconds one attempt of one request may take (uploads and downloads: at least 600).
    `max_retries` is how many times a request that failed in a way worth retrying is sent again. `user_agent` is your
    product's token, such as `my-app/1.2`, put in front of the library's own in the User-Agent (visible ASCII, no
    space). `http_client` is your own `httpx.Client` (a proxy, TLS settings); the library does not close a client it
    did not make.
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        *,
        base_url: Optional[str] = None,
        timeout: Optional[float] = DEFAULT_TIMEOUT,
        max_retries: int = DEFAULT_MAX_RETRIES,
        user_agent: Optional[str] = None,
        http_client: Optional[httpx.Client] = None,
    ) -> None:
        super().__init__(api_key=api_key, base_url=base_url, timeout=timeout, max_retries=max_retries,
                         user_agent=user_agent)
        self._owns_http = http_client is None
        self._http = http_client if http_client is not None else httpx.Client(follow_redirects=False)
        # The waits and the clock, replaced by the tests only.
        self._sleep: Callable[[float], None] = time.sleep
        self._clock: Callable[[], float] = time.monotonic
        self.account = AccountResource(self)
        self.checks = Checks(self)
        self.lists = Lists(self)
        self.domains = Domains(self)
        self.suppressions = Suppressions(self)
        self.webhooks = Webhooks(self)
        self.notices = Notices(self)
        self.feedback = Feedback(self)
        self.integrations = Integrations(self)

    # --- checks ---------------------------------------------------------------------------------------------------

    @overload
    def verify(self, email: str, *, force_recheck: bool = ..., wait: Literal[True] = ...,
               timeout: Optional[float] = ..., idempotency_key: Optional[str] = ...) -> Result: ...

    @overload
    def verify(self, email: str, *, force_recheck: bool = ..., wait: bool,
               timeout: Optional[float] = ..., idempotency_key: Optional[str] = ...) -> Union[Result, PendingCheck]: ...

    def verify(self, email: str, *, force_recheck: bool = False, wait: bool = True,
               timeout: Optional[float] = CHECK_TIMEOUT, idempotency_key: Optional[str] = None) -> Union[Result, PendingCheck]:
        """Check one address and return its result (`job_id` names the check's job).

        The API answers within about 5 seconds, or says the check is still running: then it is asked every 2 seconds
        until done. After `timeout` seconds (`None`: no limit) `WaitTimeoutError` is raised with the job's id in
        `job_ids`: the check carries on, and `client.checks.wait(job_id)` picks it up. `wait=False` returns a
        `PendingCheck` instead of waiting. `force_recheck` skips the cache.

        For many addresses use `verify_many` or a list, never a loop of `verify`.
        """
        timeout = check_timeout("timeout", timeout)
        deadline = None if timeout is None else self._clock() + timeout
        return self.checks._verify(email, force_recheck=force_recheck, wait=wait, deadline=deadline, timeout=timeout,
                                   idempotency_key=idempotency_key)

    def verify_many(self, emails: Sequence[str], *, timeout: Optional[float] = MANY_TIMEOUT) -> List[Result]:
        """Check several addresses and return their results in the same order.

        Up to 10 are checked one by one, together. More go as one list (one `POST /v1/jobs`), waited for and read
        back: each result's `job_id` is then the list's. A few hundred addresses take minutes, Microsoft-hosted ones
        longer; raise `timeout` (seconds, default 600) to match. An error names every job the call made in `job_ids`,
        so nothing is lost: wait for them with `checks.wait` or `lists.wait`.
        """
        return self.checks._many(emails, timeout)

    # --- transport ------------------------------------------------------------------------------------------------

    def _request(self, call: Call) -> Answer:
        attempt = 0
        unanswered = False
        while True:
            error: VerifyInboxesError
            never_sent = False
            closer = None
            try:
                extra = content = None
                if call.upload is not None:
                    extra, content, closer = call.upload.sync_body()
                response = self._http.request(
                    call.method, self._url(call.path), params=call.params,
                    json=call.json if call.upload is None else None, content=content,
                    headers=self._headers(call, extra), timeout=self._timeout(call),
                )
            except httpx.TransportError as exc:
                log_no_answer(call, attempt, exc)
                error, never_sent = connection_error(exc)
                error.__cause__ = exc
                unanswered = True
            else:
                request_id = response.headers.get("x-request-id")
                log_answer(call, attempt, response.status_code, request_id)
                if response.is_success:
                    return Answer(response.status_code, request_id,
                                  parse_body(response.status_code, response.headers, response.content))
                if call.gone_is_done and unanswered and response.status_code == 404:
                    return Answer(response.status_code, request_id, None)
                error = error_from_answer(response.status_code, response.headers, response.content)
            finally:
                if closer is not None:
                    closer.close()
            delay = self._retry_delay(call, error, attempt, never_sent=never_sent)
            if delay is None:
                raise error
            self._sleep(delay)
            if caller_interrupted():
                raise error  # interrupted during the pause: nothing more is sent
            attempt += 1

    def _download(self, call: Call, destination: Destination) -> int:
        sink = Sink(destination)
        attempt = 0
        while True:
            error: VerifyInboxesError
            never_sent = False
            try:
                with self._http.stream("GET", self._url(call.path), params=call.params, headers=self._headers(call),
                                       timeout=self._timeout(call)) as response:
                    log_answer(call, attempt, response.status_code, response.headers.get("x-request-id"))
                    if response.is_success:
                        sink.begin()
                        for chunk in response.iter_bytes():
                            sink.write(chunk)
                        return sink.commit()
                    error = error_from_answer(response.status_code, response.headers, response.read())
            except httpx.TransportError as exc:
                sink.abort()
                log_no_answer(call, attempt, exc)
                error, never_sent = connection_error(exc)
                error.__cause__ = exc
            except BaseException:
                sink.abort()
                raise
            if not sink.resendable:
                raise error
            delay = self._retry_delay(call, error, attempt, never_sent=never_sent)
            if delay is None:
                raise error
            self._sleep(delay)
            attempt += 1

    # --- lifetime -------------------------------------------------------------------------------------------------

    def close(self) -> None:
        """Close the connections. Not needed when the client is used as a context manager."""
        if self._owns_http:
            self._http.close()

    def __enter__(self) -> "VerifyInboxes":
        return self

    def __exit__(self, exc_type: Optional[Type[BaseException]], exc: Optional[BaseException],
                 tb: Optional[TracebackType]) -> None:
        self.close()


class AsyncVerifyInboxes(BaseClient):
    """The VerifyInboxes API, for `asyncio`: the same surface as `VerifyInboxes`, every call awaited.

        async with AsyncVerifyInboxes() as client:
            result = await client.verify("ann@example.com")
            async for row in client.lists.results(job_id):
                ...
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        *,
        base_url: Optional[str] = None,
        timeout: Optional[float] = DEFAULT_TIMEOUT,
        max_retries: int = DEFAULT_MAX_RETRIES,
        user_agent: Optional[str] = None,
        http_client: Optional[httpx.AsyncClient] = None,
    ) -> None:
        super().__init__(api_key=api_key, base_url=base_url, timeout=timeout, max_retries=max_retries,
                         user_agent=user_agent)
        self._owns_http = http_client is None
        self._http = http_client if http_client is not None else httpx.AsyncClient(follow_redirects=False)
        # The waits and the clock, replaced by the tests only.
        self._sleep: Callable[[float], Awaitable[None]] = asyncio.sleep
        self._clock: Callable[[], float] = time.monotonic
        self.account = AsyncAccountResource(self)
        self.checks = AsyncChecks(self)
        self.lists = AsyncLists(self)
        self.domains = AsyncDomains(self)
        self.suppressions = AsyncSuppressions(self)
        self.webhooks = AsyncWebhooks(self)
        self.notices = AsyncNotices(self)
        self.feedback = AsyncFeedback(self)
        self.integrations = AsyncIntegrations(self)

    @overload
    async def verify(self, email: str, *, force_recheck: bool = ..., wait: Literal[True] = ...,
                     timeout: Optional[float] = ..., idempotency_key: Optional[str] = ...) -> Result: ...

    @overload
    async def verify(self, email: str, *, force_recheck: bool = ..., wait: bool,
                     timeout: Optional[float] = ..., idempotency_key: Optional[str] = ...) -> Union[Result, PendingCheck]: ...

    async def verify(self, email: str, *, force_recheck: bool = False, wait: bool = True,
                     timeout: Optional[float] = CHECK_TIMEOUT,
                     idempotency_key: Optional[str] = None) -> Union[Result, PendingCheck]:
        """Check one address and return its result (see `VerifyInboxes.verify`)."""
        timeout = check_timeout("timeout", timeout)
        deadline = None if timeout is None else self._clock() + timeout
        return await self.checks._verify(email, force_recheck=force_recheck, wait=wait, deadline=deadline,
                                         timeout=timeout, idempotency_key=idempotency_key)

    async def verify_many(self, emails: Sequence[str], *, timeout: Optional[float] = MANY_TIMEOUT) -> List[Result]:
        """Check several addresses and return their results in the same order (see `VerifyInboxes.verify_many`); up
        to 10 are sent together with `asyncio.gather`."""
        return await self.checks._many(emails, timeout)

    async def _request(self, call: Call) -> Answer:
        attempt = 0
        unanswered = False
        while True:
            error: VerifyInboxesError
            never_sent = False
            closer = None
            try:
                extra = content = None
                if call.upload is not None:
                    extra, content, closer = call.upload.async_body()
                response = await self._http.request(
                    call.method, self._url(call.path), params=call.params,
                    json=call.json if call.upload is None else None, content=content,
                    headers=self._headers(call, extra), timeout=self._timeout(call),
                )
            except httpx.TransportError as exc:
                log_no_answer(call, attempt, exc)
                error, never_sent = connection_error(exc)
                error.__cause__ = exc
                unanswered = True
            else:
                request_id = response.headers.get("x-request-id")
                log_answer(call, attempt, response.status_code, request_id)
                if response.is_success:
                    return Answer(response.status_code, request_id,
                                  parse_body(response.status_code, response.headers, response.content))
                if call.gone_is_done and unanswered and response.status_code == 404:
                    return Answer(response.status_code, request_id, None)
                error = error_from_answer(response.status_code, response.headers, response.content)
            finally:
                if closer is not None:
                    closer.close()
            delay = self._retry_delay(call, error, attempt, never_sent=never_sent)
            if delay is None:
                raise error
            await self._sleep(delay)
            if caller_interrupted():
                raise error  # interrupted during the pause: nothing more is sent
            attempt += 1

    async def _download(self, call: Call, destination: Destination) -> int:
        sink = Sink(destination)
        attempt = 0
        while True:
            error: VerifyInboxesError
            never_sent = False
            try:
                async with self._http.stream("GET", self._url(call.path), params=call.params,
                                             headers=self._headers(call), timeout=self._timeout(call)) as response:
                    log_answer(call, attempt, response.status_code, response.headers.get("x-request-id"))
                    if response.is_success:
                        sink.begin()
                        async for chunk in response.aiter_bytes():
                            sink.write(chunk)
                        return sink.commit()
                    error = error_from_answer(response.status_code, response.headers, await response.aread())
            except httpx.TransportError as exc:
                sink.abort()
                log_no_answer(call, attempt, exc)
                error, never_sent = connection_error(exc)
                error.__cause__ = exc
            except BaseException:
                sink.abort()
                raise
            if not sink.resendable:
                raise error
            delay = self._retry_delay(call, error, attempt, never_sent=never_sent)
            if delay is None:
                raise error
            await self._sleep(delay)
            attempt += 1

    async def close(self) -> None:
        """Close the connections. Not needed when the client is used as a context manager."""
        if self._owns_http:
            await self._http.aclose()

    async def __aenter__(self) -> "AsyncVerifyInboxes":
        return self

    async def __aexit__(self, exc_type: Optional[Type[BaseException]], exc: Optional[BaseException],
                        tb: Optional[TracebackType]) -> None:
        await self.close()
