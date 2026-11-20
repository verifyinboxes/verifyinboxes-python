"""The API's resources: the account, checks, lists, domains, suppressions, webhooks, notices, feedback and integrations.
Each exists twice, sync and async, with the same methods."""
from __future__ import annotations

import asyncio
import dataclasses
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
    Optional,
    Sequence,
    TypeVar,
    Union,
)

from ._base import (
    ACTIVE,
    CALLER_INTERRUPTED,
    CHECK_TIMEOUT,
    LIST_POLL_INTERVALS,
    PAGE_SIZE,
    POLL_INTERVAL,
    SINGLES_APART,
    SINGLES_AT_MOST,
    UNSENT,
    Call,
    Destination,
    FileSource,
    Upload,
    check_email,
    check_emails,
    check_timeout,
    filter_params,
    note_interrupted,
    path_segment,
    results_params,
    wait_timeout_message,
)
from ._base import idempotency_key as make_key
from ._exceptions import VerifyInboxesError, WaitTimeoutError
from ._models import (
    Account,
    FeedbackEvent,
    FeedbackResult,
    Integration,
    IntegrationCreated,
    Job,
    JobCreated,
    NoticePage,
    PendingCheck,
    ResolveCreated,
    Result,
    ResultRow,
    ResultsPage,
    SuppressionList,
    Webhook,
    WebhookCreated,
    Domain,
)
from ._pagination import aiter_results, iter_results
from ._webhooks import DEFAULT_TOLERANCE, verify_signature

if TYPE_CHECKING:
    from ._client import AsyncVerifyInboxes, VerifyInboxes

__all__ = [
    "AccountResource", "Checks", "Lists", "Domains", "Suppressions", "Webhooks", "Notices", "Feedback", "Integrations",
    "AsyncAccountResource", "AsyncChecks", "AsyncLists", "AsyncDomains", "AsyncSuppressions", "AsyncWebhooks",
    "AsyncNotices", "AsyncFeedback", "AsyncIntegrations",
]

Verdicts = Optional[Sequence[str]]
T = TypeVar("T")


# --- shared by both -------------------------------------------------------------------------------------------------


def _deadline(now: float, timeout: Optional[float]) -> Optional[float]:
    return None if timeout is None else now + timeout


def _result_of(job: Job) -> Result:
    """A finished check's result, naming its job."""
    if job.result is None:
        raise VerifyInboxesError(f"The check {job.id} ended {job.status} without a result.", job_ids=[job.id])
    return dataclasses.replace(job.result, job_id=job.id)


def _started(status: int, data: Any) -> Union[Result, PendingCheck]:
    if status == 202:
        pending = PendingCheck.from_dict(data)
        if not isinstance(pending.job_id, str) or not pending.job_id:
            raise VerifyInboxesError("The API answered 202 to a check without its job_id.")
        return pending
    return Result.from_dict(data)


def _verify_body(email: str, force_recheck: bool) -> Dict[str, Any]:
    body: Dict[str, Any] = {"email": check_email(email)}
    if force_recheck:
        body["force_recheck"] = True
    return body


def _create_call(emails: Optional[Sequence[str]], file: Optional[FileSource], csv: Optional[str], email_column: str,
                 filename: Optional[str], key: Optional[str]) -> Call:
    if sum(source is not None for source in (emails, file, csv)) != 1:
        raise ValueError("give exactly one of emails, file or csv")
    idempotency_key = make_key(key)
    if emails is not None:
        if isinstance(emails, (str, bytes)):
            raise TypeError("emails must be a list of addresses")
        return Call("POST", "/v1/jobs", json={"emails": list(emails)}, idempotency_key=idempotency_key)
    if csv is not None:
        if not isinstance(csv, str):
            raise TypeError("csv must be the CSV's text (for bytes or a file, use file=)")
        source: FileSource = csv.encode("utf-8")
    else:
        assert file is not None
        source = file
    upload = Upload(source, email_column=email_column, filename=filename)
    return Call("POST", "/v1/jobs", upload=upload, idempotency_key=idempotency_key, long=True)


def _suppression_body(emails: Iterable[str], domains: Iterable[str]) -> Dict[str, List[str]]:
    if isinstance(emails, str) or isinstance(domains, str):
        raise TypeError("emails and domains are lists")
    body: Dict[str, List[str]] = {}
    if emails:
        body["emails"] = list(emails)
    if domains:
        body["domains"] = list(domains)
    if not body:
        raise ValueError("give emails, domains or both")
    return body


def _count(data: Any, name: str) -> int:
    value = data.get(name) if isinstance(data, Mapping) else None
    if isinstance(value, bool) or not isinstance(value, int):
        raise VerifyInboxesError(f"The API answered without {name!r}.")
    return value


def _feedback_body(events: Sequence[Union[FeedbackEvent, Mapping[str, Any]]]) -> Dict[str, Any]:
    if isinstance(events, (str, bytes, Mapping)) or not isinstance(events, Sequence):
        raise TypeError("events must be a list of FeedbackEvent or of dicts")
    return {"events": [e.to_dict() if isinstance(e, FeedbackEvent) else dict(e) for e in events]}


def _integration_body(provider: str, label: Optional[str], signing_secret: Optional[str]) -> Dict[str, Any]:
    body: Dict[str, Any] = {"provider": provider}
    if label is not None:
        body["label"] = label
    if signing_secret is not None:
        body["signing_secret"] = signing_secret
    return body


def _notices_params(after: Optional[str], limit: Optional[int]) -> Optional[Dict[str, str]]:
    params: Dict[str, str] = {}
    if after is not None:
        params["after"] = after
    if limit is not None:
        params["limit"] = str(int(limit))
    return params or None


def _job_path(job_id: str, what: str = "id", tail: str = "") -> str:
    return f"/v1/jobs/{path_segment(job_id, what)}{tail}"


def _list_of_results(emails: Sequence[str], job_id: str, rows: Mapping[int, ResultRow]) -> List[Result]:
    """The results of a list made from `emails`, in their order: row i is `emails[i - 1]`."""
    out: List[Result] = []
    for row_number, email in enumerate(emails, 1):
        row = rows.get(row_number)
        if row is None:
            raise VerifyInboxesError(f"The list {job_id} has no result for row {row_number} ({email}).", job_ids=[job_id])
        out.append(Result.from_dict({**row.to_dict(), "job_id": job_id}))
    return out


def _started_job(status: int, data: Any) -> Optional[str]:
    """The job a check's answer names, if it can be read."""
    try:
        job_id = _started(status, data).job_id
    except Exception:
        return None
    return job_id if isinstance(job_id, str) and job_id else None


def _sent_for(interrupted: threading.Event, send: Callable[..., T], *args: Any) -> T:
    """`send(*args)`, whose requests are sent for a caller that sets `interrupted` when it is interrupted: an attempt
    that fails then is not sent again."""
    token = CALLER_INTERRUPTED.set(interrupted)
    try:
        return send(*args)
    finally:
        CALLER_INTERRUPTED.reset(token)


def _finish_in_thread(send: Callable[[], T], job_of: Callable[[T], Optional[str]]) -> T:
    """`send` (a request that makes a job) run in a thread of its own, so that an interruption of the caller (Ctrl-C)
    does not cut it off once it may have reached the API: the job it made holds credits. The interruption waits for
    the attempt on its way (and sends no other), names the job on itself, and goes on; a second one stops waiting."""
    stop = threading.Event()
    pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="verifyinboxes")
    try:
        future = pool.submit(_sent_for, stop, send)
        try:
            return future.result()
        except Exception:
            raise  # the request's own failure: no job was made
        except BaseException as interrupted:
            stop.set()
            try:
                made = future.result()
            except BaseException:
                raise interrupted from None
            job_id = job_of(made)
            if job_id:
                note_interrupted(interrupted, [job_id])
            raise
    finally:
        pool.shutdown(wait=False)


async def _finish_when_cancelled(send: Awaitable[T], job_of: Callable[[T], Optional[str]],
                                 made: Optional[Callable[[str], None]] = None) -> T:
    """`send` (a request that makes a job) awaited so that a cancellation of the caller does not cut it off once it
    may have reached the API: the job it made holds credits. The cancellation waits for the attempt on its way (and
    sends no other), records the job (with `made`, else on the cancellation itself) and goes on; a second one stops
    waiting."""
    stop = threading.Event()
    token = CALLER_INTERRUPTED.set(stop)
    try:
        task = asyncio.ensure_future(send)  # the task's context is a copy of this one: `stop` reaches its requests
    finally:
        CALLER_INTERRUPTED.reset(token)
    try:
        # Waited for, not awaited: a cancellation stops this wait and leaves `task` running. Not `asyncio.shield`: on
        # Python 3.14 and later, once its caller is cancelled, it reports the task's error to the event loop as lost
        # (an ERROR log, a failure in a strict loop) although the error is taken in below.
        await asyncio.wait({task})
        return task.result()
    except asyncio.CancelledError as cancelled:
        stop.set()
        try:
            answer = await task
        except BaseException:
            raise cancelled from None
        job_id = job_of(answer)
        if job_id:
            if made is not None:
                made(job_id)
            else:
                note_interrupted(cancelled, [job_id])
        raise


# ======================================================================================================================
# sync
# ======================================================================================================================


class AccountResource:
    def __init__(self, client: "VerifyInboxes") -> None:
        self._client = client

    def get(self) -> Account:
        """`GET /v1/account`: the organisation's name, balance and switches."""
        return Account.from_dict(self._client._request(Call("GET", "/v1/account")).data)


class Checks:
    def __init__(self, client: "VerifyInboxes") -> None:
        self._client = client

    def get(self, job_id: str) -> Job:
        """`GET /v1/jobs/{id}`: a check's job as it is now (`result` once it is done)."""
        return Job.from_dict(self._client._request(Call("GET", _job_path(job_id, "job_id"))).data)

    def wait(self, job_id: str, *, timeout: Optional[float] = CHECK_TIMEOUT) -> Result:
        """Wait for a check that is still running (a `PendingCheck`, or one a `WaitTimeoutError` named) and return its
        result. Asks at once, then every 2 seconds; raises `WaitTimeoutError` after `timeout` seconds (`None`: no
        limit)."""
        timeout = check_timeout("timeout", timeout)
        return self._wait(job_id, _deadline(self._client._clock(), timeout), timeout, ask_first=True)

    def _wait(self, job_id: str, deadline: Optional[float], timeout: Optional[float], *, ask_first: bool,
              stop: Optional[threading.Event] = None, note: bool = True) -> Result:
        """`note`: an interruption (Ctrl-C) names the check on itself and in a warning; `verify_many` names all of its
        checks at once instead."""
        status = "pending"
        try:
            if ask_first:
                job = self.get(job_id)
                if job.status not in ACTIVE:
                    return _result_of(job)
                status = job.status
            while True:
                now = self._client._clock()
                if deadline is not None and now >= deadline:
                    raise WaitTimeoutError(wait_timeout_message("check", job_id, status, timeout, "client.checks.wait"),
                                           job_ids=[job_id], timeout=timeout)
                self._client._sleep(POLL_INTERVAL if deadline is None else min(POLL_INTERVAL, deadline - now))
                if stop is not None and stop.is_set():
                    raise VerifyInboxesError(f"Stopped waiting for the check {job_id}.", job_ids=[job_id])
                job = self.get(job_id)
                if job.status not in ACTIVE:
                    return _result_of(job)
                status = job.status
        except VerifyInboxesError as error:
            if not error.job_ids:
                error.job_ids = [job_id]
            raise
        except BaseException as error:
            if note:
                note_interrupted(error, [job_id])
            raise

    def _verify(self, email: str, *, force_recheck: bool, wait: bool, deadline: Optional[float],
                timeout: Optional[float], idempotency_key: Optional[str],
                made: Optional[Callable[[str], None]] = None, stop: Optional[threading.Event] = None,
                ) -> Union[Result, PendingCheck]:
        call = Call("POST", "/v1/verify", json=_verify_body(email, force_recheck), idempotency_key=make_key(idempotency_key))
        answer = self._client._request(call)
        started = _started(answer.status, answer.data)
        if made is not None and isinstance(started.job_id, str):
            made(started.job_id)
        if isinstance(started, Result) or not wait:
            return started
        return self._wait(started.job_id, deadline, timeout, ask_first=False, stop=stop, note=made is None)

    def _many(self, emails: Sequence[str], timeout: Optional[float]) -> List[Result]:
        emails = check_emails(emails)
        timeout = check_timeout("timeout", timeout)
        deadline = _deadline(self._client._clock(), timeout)
        if len(emails) > SINGLES_AT_MOST:
            return self._as_list(emails, deadline, timeout)
        made: List[Optional[str]] = [None] * len(emails)
        # Set when the caller is interrupted (Ctrl-C): the checks still waiting stop at their next poll, and a request
        # on its way that fails is not sent again.
        stop = threading.Event()

        def one(index: int) -> Result:
            def keep(job_id: str) -> None:
                made[index] = job_id

            remaining = None if deadline is None else max(0.0, deadline - self._client._clock())
            result = self._verify(emails[index], force_recheck=False, wait=True, deadline=deadline, timeout=remaining,
                                  idempotency_key=None, made=keep, stop=stop)
            assert isinstance(result, Result)
            return result

        interrupted: Optional[BaseException] = None
        with ThreadPoolExecutor(max_workers=SINGLES_AT_MOST, thread_name_prefix="verifyinboxes") as pool:
            futures = []
            outcomes: List[Union[Result, BaseException]] = []
            try:
                for index in range(len(emails)):
                    if index:
                        self._client._sleep(SINGLES_APART)
                    futures.append(pool.submit(_sent_for, stop, one, index))
                for future in futures:
                    try:
                        outcomes.append(future.result())
                    except Exception as error:  # every check finishes before any error is raised
                        outcomes.append(error)
            except BaseException as error:
                stop.set()
                interrupted = error
        # The checks started have all stopped here: every job id they made is known.
        job_ids = [job_id for job_id in made if job_id is not None]
        if interrupted is not None:
            note_interrupted(interrupted, job_ids)
            raise interrupted
        results: List[Result] = []
        for outcome in outcomes:
            if isinstance(outcome, BaseException):
                if isinstance(outcome, VerifyInboxesError):
                    outcome.job_ids = job_ids
                raise outcome
            results.append(outcome)
        return results

    def _as_list(self, emails: Sequence[str], deadline: Optional[float], timeout: Optional[float]) -> List[Result]:
        lists = self._client.lists
        created = _finish_in_thread(lambda: lists.create(emails=emails), lambda made: made.id)
        try:
            job = lists._wait(created.id, deadline, timeout)
            if job.status != "done":
                raise VerifyInboxesError(f"The list {created.id} ended {job.status}.")
            rows = {row.row: row for row in lists.results(created.id)}
            return _list_of_results(emails, created.id, rows)
        except VerifyInboxesError as error:
            error.job_ids = [created.id]
            raise
        except BaseException as error:
            note_interrupted(error, [created.id])
            raise


class Lists:
    def __init__(self, client: "VerifyInboxes") -> None:
        self._client = client

    def create(
        self,
        *,
        emails: Optional[Sequence[str]] = None,
        file: Optional[FileSource] = None,
        csv: Optional[str] = None,
        email_column: str = "email",
        filename: Optional[str] = None,
        idempotency_key: Optional[str] = None,
    ) -> JobCreated:
        """`POST /v1/jobs`: start a list from exactly one of `emails` (a list of addresses), `file` (a CSV: a path,
        bytes, or a file opened in binary mode) or `csv` (a CSV's text). `email_column` names the CSV's column of
        addresses; `filename` is the name the upload carries (default: the path's, else `list.csv`).

        Sent again, with the same `Idempotency-Key`, when the answer is lost: the API answers the retry with the same
        list, so it is made and charged once. Wait for it with `lists.wait`."""
        call = _create_call(emails, file, csv, email_column, filename, idempotency_key)
        return JobCreated.from_dict(self._client._request(call).data)

    def get(self, id: str) -> Job:
        """`GET /v1/jobs/{id}`: the list as it is now."""
        return Job.from_dict(self._client._request(Call("GET", _job_path(id))).data)

    def wait(self, id: str, *, timeout: Optional[float] = None) -> Job:
        """Wait for a list to finish and return it. Asks at once, then after 2, 4, 8, 16 and then every 30 seconds.
        Raises `WaitTimeoutError` after `timeout` seconds (`None`, the default: no limit). A few hundred addresses take
        minutes, Microsoft-hosted ones longer."""
        timeout = check_timeout("timeout", timeout)
        return self._wait(id, _deadline(self._client._clock(), timeout), timeout)

    def _wait(self, id: str, deadline: Optional[float], timeout: Optional[float]) -> Job:
        try:
            job = self.get(id)
            asked = 0
            while job.status in ACTIVE:
                now = self._client._clock()
                if deadline is not None and now >= deadline:
                    raise WaitTimeoutError(wait_timeout_message("list", id, job.status, timeout, "client.lists.wait"),
                                           job_ids=[id], timeout=timeout)
                pause = LIST_POLL_INTERVALS[min(asked, len(LIST_POLL_INTERVALS) - 1)]
                self._client._sleep(pause if deadline is None else min(pause, deadline - now))
                asked += 1
                job = self.get(id)
            return job
        except VerifyInboxesError as error:
            if not error.job_ids:
                error.job_ids = [id]
            raise

    def results(self, id: str, *, page_size: int = PAGE_SIZE, only_safe_to_send: bool = False,
                verdicts: Verdicts = None, partial: bool = False) -> Iterator[ResultRow]:
        """Every row of a finished list, in order, read a page (`page_size` rows, 1 to 1000) at a time as you iterate.

        `only_safe_to_send` keeps the rows safe to send to; `verdicts` the rows of those verdicts; `partial` reads a
        list still running, as it stands (read again from the start once it is done)."""
        path = _job_path(id, tail="/results")
        results_params(page_size, None, only_safe_to_send, verdicts, partial)

        def fetch(cursor: Optional[str]) -> ResultsPage:
            params = results_params(page_size, cursor, only_safe_to_send, verdicts, partial)
            return ResultsPage.from_dict(self._client._request(Call("GET", path, params=params)).data)

        return iter_results(id, fetch)

    def results_page(self, id: str, *, cursor: Optional[str] = None, page_size: int = PAGE_SIZE,
                     only_safe_to_send: bool = False, verdicts: Verdicts = None, partial: bool = False) -> ResultsPage:
        """One page of results, for keeping your own cursor: pass the page's `next_cursor` back as `cursor` until it
        is `None`. A page may hold fewer rows than asked, even none, before the last one."""
        params = results_params(page_size, cursor, only_safe_to_send, verdicts, partial)
        return ResultsPage.from_dict(self._client._request(Call("GET", _job_path(id, tail="/results"), params=params)).data)

    def download(self, id: str, destination: Destination, *, only_safe_to_send: bool = False, verdicts: Verdicts = None,
                 partial: bool = False) -> int:
        """`GET /v1/jobs/{id}/download`: the list's CSV with the results appended, written to `destination` (a path,
        or a file opened in binary mode for writing). Returns the bytes written.

        To a path, the file is written beside it and renamed over it once whole: a failed download leaves the file
        there was untouched."""
        call = Call("GET", _job_path(id, tail="/download"), params=filter_params(only_safe_to_send, verdicts, partial) or None,
                    long=True, accept="text/csv")
        return self._client._download(call, destination)

    def resolve(self, id: str, *, emails: Optional[Sequence[str]] = None,
                idempotency_key: Optional[str] = None) -> ResolveCreated:
        """`POST /v1/jobs/{id}/resolve`: send a finished job's `catch_all` and `risky` addresses (all of them, or those
        in `emails`) to be settled by a test email. Its own job: wait for it with `lists.wait`."""
        if isinstance(emails, str):
            raise TypeError("emails must be a list of addresses")
        body = {"emails": list(emails)} if emails is not None else None
        call = Call("POST", _job_path(id, tail="/resolve"), json=body, idempotency_key=make_key(idempotency_key))
        return ResolveCreated.from_dict(self._client._request(call).data)


class Domains:
    def __init__(self, client: "VerifyInboxes") -> None:
        self._client = client

    def get(self, domain: str) -> Domain:
        """`GET /v1/domains/{domain}`: its provider, mail servers, catch-all and address pattern (1 credit)."""
        return Domain.from_dict(self._client._request(Call("GET", f"/v1/domains/{path_segment(domain, 'domain')}")).data)


class Suppressions:
    def __init__(self, client: "VerifyInboxes") -> None:
        self._client = client

    def list(self) -> SuppressionList:
        """`GET /v1/suppressions`: the domains on the list, and how many addresses."""
        return SuppressionList.from_dict(self._client._request(Call("GET", "/v1/suppressions")).data)

    def add(self, *, emails: Iterable[str] = (), domains: Iterable[str] = ()) -> int:
        """`POST /v1/suppressions`: never check these addresses and domains again. Returns how many were added."""
        call = Call("POST", "/v1/suppressions", json=_suppression_body(emails, domains), idempotency_key=make_key(None))
        return _count(self._client._request(call).data, "added")

    def remove(self, *, emails: Iterable[str] = (), domains: Iterable[str] = ()) -> int:
        """`DELETE /v1/suppressions`. Returns how many were removed."""
        call = Call("DELETE", "/v1/suppressions", json=_suppression_body(emails, domains))
        return _count(self._client._request(call).data, "removed")


class Webhooks:
    def __init__(self, client: "VerifyInboxes") -> None:
        self._client = client

    def create(self, url: str, *, events: Optional[Sequence[str]] = None) -> WebhookCreated:
        """`POST /v1/webhooks`: `url` gets the events (`job.completed`, `address.contradicted`; default both). The
        answer's `secret` is shown only once: store it. Not sent again once it may have arrived (a second webhook would
        be made): on an `APIConnectionError`, look at `webhooks.list()` first."""
        body: Dict[str, Any] = {"url": url}
        if events is not None:
            body["events"] = list(events)
        call = Call("POST", "/v1/webhooks", json=body, idempotency_key=make_key(None), retry=UNSENT)
        return WebhookCreated.from_dict(self._client._request(call).data)

    def list(self) -> List[Webhook]:
        """`GET /v1/webhooks` (never the secrets)."""
        data = self._client._request(Call("GET", "/v1/webhooks")).data
        return [Webhook.from_dict(item) for item in data] if isinstance(data, list) else []

    def delete(self, id: str) -> None:
        """`DELETE /v1/webhooks/{id}`."""
        self._client._request(Call("DELETE", f"/v1/webhooks/{path_segment(id)}", gone_is_done=True))

    @staticmethod
    def verify_signature(raw_body: Union[bytes, str], header: str, secret: str, *,
                         tolerance: float = DEFAULT_TOLERANCE, now: Optional[float] = None) -> bool:
        """`verifyinboxes.verify_signature`."""
        return verify_signature(raw_body, header, secret, tolerance=tolerance, now=now)


class Notices:
    def __init__(self, client: "VerifyInboxes") -> None:
        self._client = client

    def list(self, *, after: Optional[str] = None, limit: Optional[int] = None) -> NoticePage:
        """`GET /v1/notices`: contradiction notices, oldest first. Keep the page's `next_after` and send it as `after`
        next time; a page shorter than `limit` (1 to 500, default 100) means you have caught up."""
        call = Call("GET", "/v1/notices", params=_notices_params(after, limit))
        return NoticePage.from_dict(self._client._request(call).data)


class Feedback:
    def __init__(self, client: "VerifyInboxes") -> None:
        self._client = client

    def report(self, events: Sequence[Union[FeedbackEvent, Mapping[str, Any]]], *,
               idempotency_key: Optional[str] = None) -> FeedbackResult:
        """`POST /v1/feedback`: what became of mail you sent (bounces, deliveries, replies)."""
        call = Call("POST", "/v1/feedback", json=_feedback_body(events), idempotency_key=make_key(idempotency_key))
        return FeedbackResult.from_dict(self._client._request(call).data)


class Integrations:
    def __init__(self, client: "VerifyInboxes") -> None:
        self._client = client

    def create(self, provider: str, *, label: Optional[str] = None, signing_secret: Optional[str] = None) -> IntegrationCreated:
        """`POST /v1/integrations`: an address (`url`, shown only once) to paste into Instantly or Smartlead, which
        then reports what became of your mail. Not sent again once it may have arrived."""
        call = Call("POST", "/v1/integrations", json=_integration_body(provider, label, signing_secret),
                    idempotency_key=make_key(None), retry=UNSENT)
        return IntegrationCreated.from_dict(self._client._request(call).data)

    def list(self) -> List[Integration]:
        """`GET /v1/integrations`."""
        data = self._client._request(Call("GET", "/v1/integrations")).data
        return [Integration.from_dict(item) for item in data] if isinstance(data, list) else []

    def delete(self, id: str) -> None:
        """`DELETE /v1/integrations/{id}`."""
        self._client._request(Call("DELETE", f"/v1/integrations/{path_segment(id)}", gone_is_done=True))


# ======================================================================================================================
# async
# ======================================================================================================================


class AsyncAccountResource:
    def __init__(self, client: "AsyncVerifyInboxes") -> None:
        self._client = client

    async def get(self) -> Account:
        """`GET /v1/account`: the organisation's name, balance and switches."""
        return Account.from_dict((await self._client._request(Call("GET", "/v1/account"))).data)


class AsyncChecks:
    def __init__(self, client: "AsyncVerifyInboxes") -> None:
        self._client = client

    async def get(self, job_id: str) -> Job:
        """`GET /v1/jobs/{id}`: a check's job as it is now (`result` once it is done)."""
        return Job.from_dict((await self._client._request(Call("GET", _job_path(job_id, "job_id")))).data)

    async def wait(self, job_id: str, *, timeout: Optional[float] = CHECK_TIMEOUT) -> Result:
        """Wait for a check that is still running and return its result (see `Checks.wait`)."""
        timeout = check_timeout("timeout", timeout)
        return await self._wait(job_id, _deadline(self._client._clock(), timeout), timeout, ask_first=True)

    async def _wait(self, job_id: str, deadline: Optional[float], timeout: Optional[float], *, ask_first: bool,
                    note: bool = True) -> Result:
        """`note`: a cancellation names the check on itself and in a warning; `verify_many` names all of its checks at
        once instead."""
        status = "pending"
        try:
            if ask_first:
                job = await self.get(job_id)
                if job.status not in ACTIVE:
                    return _result_of(job)
                status = job.status
            while True:
                now = self._client._clock()
                if deadline is not None and now >= deadline:
                    raise WaitTimeoutError(wait_timeout_message("check", job_id, status, timeout, "client.checks.wait"),
                                           job_ids=[job_id], timeout=timeout)
                await self._client._sleep(POLL_INTERVAL if deadline is None else min(POLL_INTERVAL, deadline - now))
                job = await self.get(job_id)
                if job.status not in ACTIVE:
                    return _result_of(job)
                status = job.status
        except VerifyInboxesError as error:
            if not error.job_ids:
                error.job_ids = [job_id]
            raise
        except BaseException as error:
            if note:
                note_interrupted(error, [job_id])
            raise

    async def _verify(self, email: str, *, force_recheck: bool, wait: bool, deadline: Optional[float],
                      timeout: Optional[float], idempotency_key: Optional[str],
                      made: Optional[Callable[[str], None]] = None) -> Union[Result, PendingCheck]:
        call = Call("POST", "/v1/verify", json=_verify_body(email, force_recheck), idempotency_key=make_key(idempotency_key))
        # Once sent, the check may hold a credit: a cancellation lets its answer come, to name it.
        answer = await _finish_when_cancelled(self._client._request(call),
                                              lambda answer: _started_job(answer.status, answer.data), made)
        started = _started(answer.status, answer.data)
        if made is not None and isinstance(started.job_id, str):
            made(started.job_id)
        if isinstance(started, Result) or not wait:
            return started
        return await self._wait(started.job_id, deadline, timeout, ask_first=False, note=made is None)

    async def _many(self, emails: Sequence[str], timeout: Optional[float]) -> List[Result]:
        emails = check_emails(emails)
        timeout = check_timeout("timeout", timeout)
        deadline = _deadline(self._client._clock(), timeout)
        if len(emails) > SINGLES_AT_MOST:
            return await self._as_list(emails, deadline, timeout)
        made: List[Optional[str]] = [None] * len(emails)

        async def one(index: int) -> Result:
            def keep(job_id: str) -> None:
                made[index] = job_id

            remaining = None if deadline is None else max(0.0, deadline - self._client._clock())
            result = await self._verify(emails[index], force_recheck=False, wait=True, deadline=deadline,
                                        timeout=remaining, idempotency_key=None, made=keep)
            assert isinstance(result, Result)
            return result

        tasks: List["asyncio.Future[Result]"] = []
        try:
            for index in range(len(emails)):
                if index:
                    await self._client._sleep(SINGLES_APART)
                tasks.append(asyncio.ensure_future(one(index)))
            # Shielded: a cancellation of the call reaches each check once, below, and not through gather as well.
            outcomes = await asyncio.shield(asyncio.gather(*tasks, return_exceptions=True))
        except BaseException as error:
            # Every check stops, and is waited for: one whose request was already sent lets its answer come, so
            # that every job made (and holding credits) is known before the interruption goes on.
            for task in tasks:
                task.cancel()
            if tasks:
                try:
                    await asyncio.gather(*tasks, return_exceptions=True)
                except BaseException:
                    pass  # interrupted again: what is known is named
            note_interrupted(error, [job_id for job_id in made if job_id is not None])
            raise
        job_ids = [job_id for job_id in made if job_id is not None]
        results: List[Result] = []
        for outcome in outcomes:
            if isinstance(outcome, BaseException):
                if isinstance(outcome, VerifyInboxesError):
                    outcome.job_ids = job_ids
                raise outcome
            results.append(outcome)
        return results

    async def _as_list(self, emails: Sequence[str], deadline: Optional[float], timeout: Optional[float]) -> List[Result]:
        lists = self._client.lists
        created = await _finish_when_cancelled(lists.create(emails=emails), lambda made: made.id)
        try:
            job = await lists._wait(created.id, deadline, timeout)
            if job.status != "done":
                raise VerifyInboxesError(f"The list {created.id} ended {job.status}.")
            rows = {row.row: row async for row in lists.results(created.id)}
            return _list_of_results(emails, created.id, rows)
        except VerifyInboxesError as error:
            error.job_ids = [created.id]
            raise
        except BaseException as error:
            note_interrupted(error, [created.id])
            raise


class AsyncLists:
    def __init__(self, client: "AsyncVerifyInboxes") -> None:
        self._client = client

    async def create(
        self,
        *,
        emails: Optional[Sequence[str]] = None,
        file: Optional[FileSource] = None,
        csv: Optional[str] = None,
        email_column: str = "email",
        filename: Optional[str] = None,
        idempotency_key: Optional[str] = None,
    ) -> JobCreated:
        """`POST /v1/jobs`: start a list (see `Lists.create`)."""
        call = _create_call(emails, file, csv, email_column, filename, idempotency_key)
        return JobCreated.from_dict((await self._client._request(call)).data)

    async def get(self, id: str) -> Job:
        """`GET /v1/jobs/{id}`: the list as it is now."""
        return Job.from_dict((await self._client._request(Call("GET", _job_path(id)))).data)

    async def wait(self, id: str, *, timeout: Optional[float] = None) -> Job:
        """Wait for a list to finish and return it (see `Lists.wait`)."""
        timeout = check_timeout("timeout", timeout)
        return await self._wait(id, _deadline(self._client._clock(), timeout), timeout)

    async def _wait(self, id: str, deadline: Optional[float], timeout: Optional[float]) -> Job:
        try:
            job = await self.get(id)
            asked = 0
            while job.status in ACTIVE:
                now = self._client._clock()
                if deadline is not None and now >= deadline:
                    raise WaitTimeoutError(wait_timeout_message("list", id, job.status, timeout, "client.lists.wait"),
                                           job_ids=[id], timeout=timeout)
                pause = LIST_POLL_INTERVALS[min(asked, len(LIST_POLL_INTERVALS) - 1)]
                await self._client._sleep(pause if deadline is None else min(pause, deadline - now))
                asked += 1
                job = await self.get(id)
            return job
        except VerifyInboxesError as error:
            if not error.job_ids:
                error.job_ids = [id]
            raise

    def results(self, id: str, *, page_size: int = PAGE_SIZE, only_safe_to_send: bool = False,
                verdicts: Verdicts = None, partial: bool = False) -> AsyncIterator[ResultRow]:
        """Every row of a finished list, in order, for `async for` (see `Lists.results`)."""
        path = _job_path(id, tail="/results")
        results_params(page_size, None, only_safe_to_send, verdicts, partial)

        async def fetch(cursor: Optional[str]) -> ResultsPage:
            params = results_params(page_size, cursor, only_safe_to_send, verdicts, partial)
            return ResultsPage.from_dict((await self._client._request(Call("GET", path, params=params))).data)

        return aiter_results(id, fetch)

    async def results_page(self, id: str, *, cursor: Optional[str] = None, page_size: int = PAGE_SIZE,
                           only_safe_to_send: bool = False, verdicts: Verdicts = None,
                           partial: bool = False) -> ResultsPage:
        """One page of results (see `Lists.results_page`)."""
        params = results_params(page_size, cursor, only_safe_to_send, verdicts, partial)
        call = Call("GET", _job_path(id, tail="/results"), params=params)
        return ResultsPage.from_dict((await self._client._request(call)).data)

    async def download(self, id: str, destination: Destination, *, only_safe_to_send: bool = False,
                       verdicts: Verdicts = None, partial: bool = False) -> int:
        """The list's CSV with the results appended, written to `destination` (see `Lists.download`)."""
        call = Call("GET", _job_path(id, tail="/download"), params=filter_params(only_safe_to_send, verdicts, partial) or None,
                    long=True, accept="text/csv")
        return await self._client._download(call, destination)

    async def resolve(self, id: str, *, emails: Optional[Sequence[str]] = None,
                      idempotency_key: Optional[str] = None) -> ResolveCreated:
        """`POST /v1/jobs/{id}/resolve` (see `Lists.resolve`)."""
        if isinstance(emails, str):
            raise TypeError("emails must be a list of addresses")
        body = {"emails": list(emails)} if emails is not None else None
        call = Call("POST", _job_path(id, tail="/resolve"), json=body, idempotency_key=make_key(idempotency_key))
        return ResolveCreated.from_dict((await self._client._request(call)).data)


class AsyncDomains:
    def __init__(self, client: "AsyncVerifyInboxes") -> None:
        self._client = client

    async def get(self, domain: str) -> Domain:
        """`GET /v1/domains/{domain}` (1 credit)."""
        call = Call("GET", f"/v1/domains/{path_segment(domain, 'domain')}")
        return Domain.from_dict((await self._client._request(call)).data)


class AsyncSuppressions:
    def __init__(self, client: "AsyncVerifyInboxes") -> None:
        self._client = client

    async def list(self) -> SuppressionList:
        """`GET /v1/suppressions`."""
        return SuppressionList.from_dict((await self._client._request(Call("GET", "/v1/suppressions"))).data)

    async def add(self, *, emails: Iterable[str] = (), domains: Iterable[str] = ()) -> int:
        """`POST /v1/suppressions`. Returns how many were added."""
        call = Call("POST", "/v1/suppressions", json=_suppression_body(emails, domains), idempotency_key=make_key(None))
        return _count((await self._client._request(call)).data, "added")

    async def remove(self, *, emails: Iterable[str] = (), domains: Iterable[str] = ()) -> int:
        """`DELETE /v1/suppressions`. Returns how many were removed."""
        call = Call("DELETE", "/v1/suppressions", json=_suppression_body(emails, domains))
        return _count((await self._client._request(call)).data, "removed")


class AsyncWebhooks:
    def __init__(self, client: "AsyncVerifyInboxes") -> None:
        self._client = client

    async def create(self, url: str, *, events: Optional[Sequence[str]] = None) -> WebhookCreated:
        """`POST /v1/webhooks` (see `Webhooks.create`)."""
        body: Dict[str, Any] = {"url": url}
        if events is not None:
            body["events"] = list(events)
        call = Call("POST", "/v1/webhooks", json=body, idempotency_key=make_key(None), retry=UNSENT)
        return WebhookCreated.from_dict((await self._client._request(call)).data)

    async def list(self) -> List[Webhook]:
        """`GET /v1/webhooks`."""
        data = (await self._client._request(Call("GET", "/v1/webhooks"))).data
        return [Webhook.from_dict(item) for item in data] if isinstance(data, list) else []

    async def delete(self, id: str) -> None:
        """`DELETE /v1/webhooks/{id}`."""
        await self._client._request(Call("DELETE", f"/v1/webhooks/{path_segment(id)}", gone_is_done=True))

    @staticmethod
    def verify_signature(raw_body: Union[bytes, str], header: str, secret: str, *,
                         tolerance: float = DEFAULT_TOLERANCE, now: Optional[float] = None) -> bool:
        """`verifyinboxes.verify_signature` (not awaited: it sends nothing)."""
        return verify_signature(raw_body, header, secret, tolerance=tolerance, now=now)


class AsyncNotices:
    def __init__(self, client: "AsyncVerifyInboxes") -> None:
        self._client = client

    async def list(self, *, after: Optional[str] = None, limit: Optional[int] = None) -> NoticePage:
        """`GET /v1/notices` (see `Notices.list`)."""
        call = Call("GET", "/v1/notices", params=_notices_params(after, limit))
        return NoticePage.from_dict((await self._client._request(call)).data)


class AsyncFeedback:
    def __init__(self, client: "AsyncVerifyInboxes") -> None:
        self._client = client

    async def report(self, events: Sequence[Union[FeedbackEvent, Mapping[str, Any]]], *,
                     idempotency_key: Optional[str] = None) -> FeedbackResult:
        """`POST /v1/feedback`."""
        call = Call("POST", "/v1/feedback", json=_feedback_body(events), idempotency_key=make_key(idempotency_key))
        return FeedbackResult.from_dict((await self._client._request(call)).data)


class AsyncIntegrations:
    def __init__(self, client: "AsyncVerifyInboxes") -> None:
        self._client = client

    async def create(self, provider: str, *, label: Optional[str] = None,
                     signing_secret: Optional[str] = None) -> IntegrationCreated:
        """`POST /v1/integrations` (see `Integrations.create`)."""
        call = Call("POST", "/v1/integrations", json=_integration_body(provider, label, signing_secret),
                    idempotency_key=make_key(None), retry=UNSENT)
        return IntegrationCreated.from_dict((await self._client._request(call)).data)

    async def list(self) -> List[Integration]:
        """`GET /v1/integrations`."""
        data = (await self._client._request(Call("GET", "/v1/integrations"))).data
        return [Integration.from_dict(item) for item in data] if isinstance(data, list) else []

    async def delete(self, id: str) -> None:
        """`DELETE /v1/integrations/{id}`."""
        await self._client._request(Call("DELETE", f"/v1/integrations/{path_segment(id)}", gone_is_done=True))
