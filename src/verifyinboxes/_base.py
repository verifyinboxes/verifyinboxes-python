"""What the sync and the async client share: configuration, headers, the retry decision, backoff, the error an answer
makes, uploads and downloads, and the checks of arguments. Nothing here sends a request."""
from __future__ import annotations

import contextvars
import email.utils
import io
import json
import logging
import os
import random
import re
import threading
import time
import uuid
from dataclasses import dataclass
from typing import IO, Any, AsyncIterator, Dict, Iterator, List, Mapping, Optional, Sequence, Tuple, Type, Union
from urllib.parse import quote

import httpx

from ._exceptions import (
    APIConnectionError,
    APIError,
    APITimeoutError,
    AuthenticationError,
    ConflictError,
    InsufficientCreditsError,
    InvalidRequestError,
    MissingAPIKeyError,
    NotFoundError,
    PermissionDeniedError,
    RateLimitError,
    ServerError,
    VerifyInboxesError,
)
from ._version import __version__

log = logging.getLogger("verifyinboxes")

DEFAULT_BASE_URL = "https://api.verifyinboxes.com"
API_KEY_ENV = "VERIFYINBOXES_API_KEY"
BASE_URL_ENV = "VERIFYINBOXES_BASE_URL"
USER_AGENT = f"verifyinboxes-python/{__version__}"

DEFAULT_TIMEOUT = 60.0             # one attempt of one request
LONG_TIMEOUT = 600.0               # an upload or a download: the API's own limit for a whole request
DEFAULT_MAX_RETRIES = 2
MAX_RETRY_AFTER = 60.0             # a longer Retry-After is not waited for: the error is raised
INITIAL_RETRY_DELAY = 0.5
MAX_RETRY_DELAY = 8.0

CHECK_TIMEOUT = 60.0               # verify and checks.wait
MANY_TIMEOUT = 600.0               # verify_many
POLL_INTERVAL = 2.0                # a check's job is asked every 2 seconds
LIST_POLL_INTERVALS = (2.0, 4.0, 8.0, 16.0, 30.0)   # a list's: then every 30 seconds
SINGLES_AT_MOST = 10               # verify_many: up to this many addresses are checked one by one, together
SINGLES_APART = 0.1                # ... started 100 ms apart
PAGE_SIZE = 500
ACTIVE = frozenset({"queued", "running"})

FileSource = Union[str, "os.PathLike[str]", bytes, bytearray, memoryview, IO[bytes]]
Destination = Union[str, "os.PathLike[str]", IO[bytes]]

CALLER_INTERRUPTED: "contextvars.ContextVar[Optional[threading.Event]]" = contextvars.ContextVar(
    "verifyinboxes_caller_interrupted", default=None)
"""Around a request that makes a job (a check, `verify_many`'s list): the event its caller sets when it is
interrupted (Ctrl-C, a cancelled task) and waits for the request's answer, to know the job. Once it is set, an attempt
that fails is not sent again: the caller waits for the attempt on its way (or the rest of a pause before a retry), and
no longer."""


def caller_interrupted() -> bool:
    """The caller of the request being sent was interrupted: an attempt that fails is not sent again."""
    interrupted = CALLER_INTERRUPTED.get()
    return interrupted is not None and interrupted.is_set()


# --- configuration ----------------------------------------------------------------------------------------------------


def resolve_api_key(api_key: Optional[str]) -> str:
    value = api_key if api_key is not None else os.environ.get(API_KEY_ENV)
    if value is not None and not isinstance(value, str):
        raise TypeError("api_key must be a string")
    value = (value or "").strip()
    if not value:
        raise MissingAPIKeyError(
            f"No API key. Pass api_key=... or set the {API_KEY_ENV} environment variable. An organisation admin "
            "makes keys in the VerifyInboxes app under Developers -> API keys."
        )
    if not re.fullmatch(r"[!-~]+", value):
        # A key and a token are visible ASCII: anything else (a space, a control or zero-width character, a letter
        # with an accent) is half of a copy and paste, and would fail on the first request instead.
        raise ValueError("The API key holds a space, a control character or a character that is not ASCII: "
                         "copy it again, whole.")
    return value


def resolve_base_url(base_url: Optional[str]) -> str:
    value = base_url or os.environ.get(BASE_URL_ENV) or DEFAULT_BASE_URL
    if not isinstance(value, str):
        raise TypeError("base_url must be a string")
    value = value.strip()
    if value.endswith("/"):
        value = value[:-1]
    if not re.match(r"^https?://[^/\s]+", value, re.IGNORECASE):
        raise ValueError("base_url must be an http:// or https:// address")
    return value


def check_timeout(name: str, value: Optional[float], *, allow_none: bool = True) -> Optional[float]:
    if value is None and allow_none:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value < 0:
        raise ValueError(f"{name} must be a number of seconds, 0 or more")
    return float(value)


def check_retries(value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError("max_retries must be a whole number, 0 or more")
    return value


def resolve_user_agent(product: Optional[str]) -> str:
    """The User-Agent: the caller's product token, when given, in front of the library's own."""
    if product is None or product == "":
        return USER_AGENT
    if not isinstance(product, str) or not re.fullmatch(r"[!-~]+", product):
        raise ValueError("user_agent must be visible ASCII with no space, such as my-app/1.2")
    return f"{product} {USER_AGENT}"


# --- requests ---------------------------------------------------------------------------------------------------------


def path_segment(value: str, what: str = "id") -> str:
    """A value as one segment of a path: an id with a `/` or a `?` in it never reaches another address."""
    if not isinstance(value, str) or not value:
        raise ValueError(f"{what} must be a non-empty string")
    return quote(value, safe="")


def new_idempotency_key() -> str:
    return str(uuid.uuid4())


def idempotency_key(value: Optional[str]) -> str:
    """The caller's own key, checked, or a new one."""
    if value is None:
        return new_idempotency_key()
    if not isinstance(value, str) or not re.fullmatch(r"[!-~]{1,255}", value):
        raise ValueError("idempotency_key must be 1 to 255 visible ASCII characters")
    return value


# How a request may be sent again after a failed attempt.
ALWAYS = "always"
"""GET and DELETE, and the POSTs the API answers again from the same `Idempotency-Key`."""
UNSENT = "unsent"
"""A POST the API never replays (its answer holds a secret shown once): again only when it surely did not arrive (the
connection was never made) or was refused before it ran (429)."""


@dataclass
class Call:
    """One request, as the resources describe it to the client's transport."""

    method: str
    path: str
    params: Optional[Dict[str, str]] = None
    json: Any = None
    upload: Optional["Upload"] = None
    idempotency_key: Optional[str] = None
    retry: str = ALWAYS
    long: bool = False
    """An upload or a download: the longer timeout."""
    gone_is_done: bool = False
    """A DELETE whose earlier attempt got no answer: a 404 on a retry means the first one did it."""
    accept: str = "application/json"


@dataclass
class Answer:
    status: int
    request_id: Optional[str]
    data: Any


def results_params(page_size: int, cursor: Optional[str], only_safe_to_send: bool, verdicts: Optional[Sequence[str]],
                   partial: bool) -> Dict[str, str]:
    if isinstance(page_size, bool) or not isinstance(page_size, int) or not 1 <= page_size <= 1000:
        raise ValueError("page_size must be a whole number from 1 to 1000")
    params = {"limit": str(page_size)}
    if cursor is not None:
        params["cursor"] = cursor
    params.update(filter_params(only_safe_to_send, verdicts, partial))
    return params


def filter_params(only_safe_to_send: bool, verdicts: Optional[Sequence[str]], partial: bool) -> Dict[str, str]:
    params: Dict[str, str] = {}
    if only_safe_to_send and verdicts:
        raise ValueError("give only_safe_to_send or verdicts, not both")
    if only_safe_to_send:
        params["only"] = "safe_to_send"
    if verdicts:
        if isinstance(verdicts, str):
            verdicts = [verdicts]
        params["verdicts"] = ",".join(verdicts)
    if partial:
        params["partial"] = "1"
    return params


def check_emails(emails: Sequence[str]) -> List[str]:
    if isinstance(emails, (str, bytes)) or not isinstance(emails, Sequence):
        raise TypeError("emails must be a list of addresses")
    out = list(emails)
    if not out:
        raise ValueError("emails is empty: give one address or more")
    for index, value in enumerate(out):
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"emails[{index}] is not an address")
    return out


def check_email(email: str) -> str:
    if not isinstance(email, str) or not email.strip():
        raise ValueError("email must be an address")
    return email


# --- answers ----------------------------------------------------------------------------------------------------------

_BY_STATUS: Dict[int, Type[APIError]] = {
    400: InvalidRequestError, 413: InvalidRequestError, 414: InvalidRequestError, 415: InvalidRequestError,
    422: InvalidRequestError, 401: AuthenticationError, 402: InsufficientCreditsError, 403: PermissionDeniedError,
    404: NotFoundError, 410: NotFoundError, 409: ConflictError, 429: RateLimitError,
}


def parse_retry_after(value: Optional[str]) -> Optional[float]:
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        pass
    try:
        when = email.utils.parsedate_to_datetime(value)
    except (TypeError, ValueError, IndexError):
        return None
    return max(0.0, when.timestamp() - time.time())


def error_from_answer(status: int, headers: httpx.Headers, content: bytes) -> APIError:
    """The typed error of an answer of 4xx or 5xx. The API's errors are `{"error": {"code", "message"}}`; a body that
    is not one (a proxy's HTML page) keeps its status, with no code."""
    body: Any
    try:
        body = json.loads(content.decode("utf-8")) if content else None
    except ValueError:
        body = content.decode("utf-8", "replace")
    code = message = None
    error = body.get("error") if isinstance(body, Mapping) else None
    if isinstance(error, Mapping):
        if isinstance(error.get("code"), str):
            code = error["code"]
        if isinstance(error.get("message"), str):
            message = error["message"]
    cls = _BY_STATUS.get(status) or (ServerError if status >= 500 else APIError)
    return cls(
        message or f"The API answered {status}",
        status=status,
        code=code,
        request_id=headers.get("x-request-id"),
        retry_after=parse_retry_after(headers.get("retry-after")),
        body=body,
    )


def parse_body(status: int, headers: httpx.Headers, content: bytes) -> Any:
    if not content:
        return None
    try:
        return json.loads(content.decode("utf-8"))
    except ValueError:
        request_id = headers.get("x-request-id")
        raise VerifyInboxesError(
            f"The API answered {status} with something that is not JSON"
            + (f" (request id {request_id})" if request_id else "")
        ) from None


def connection_error(exc: httpx.TransportError) -> Tuple[APIConnectionError, bool]:
    """The error of a request that got no answer, and whether it surely never reached the API."""
    never_sent = isinstance(exc, (httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout))
    if isinstance(exc, httpx.TimeoutException):
        return APITimeoutError(), never_sent
    detail = str(exc).strip()
    text = "Could not reach the VerifyInboxes API" + (f": {detail}" if detail else ".")
    return APIConnectionError(text), never_sent


# --- uploads ----------------------------------------------------------------------------------------------------------

_CHUNK = 64 * 1024


def _form_quote(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', "%22").replace("\r", "%0D").replace("\n", "%0A")


class Upload:
    """A CSV sent as `multipart/form-data`: the field `email_column`, then the file. It is built again for every
    attempt: a path is opened again, a seekable stream rewound to where it was; a stream that can be read only once
    is sent once."""

    def __init__(self, source: FileSource, *, email_column: str, filename: Optional[str]) -> None:
        if not isinstance(email_column, str) or not email_column:
            raise ValueError("email_column must be a column's name")
        self._path: Optional[str] = None
        self._data: Optional[bytes] = None
        self._stream: Optional[IO[bytes]] = None
        self._start: Optional[int] = None
        if isinstance(source, (str, os.PathLike)):
            self._path = os.fspath(source)
            if not os.path.isfile(self._path):
                raise FileNotFoundError(f"no such file: {self._path}")
            name = filename or os.path.basename(self._path) or "list.csv"
        elif isinstance(source, (bytes, bytearray, memoryview)):
            self._data = bytes(source)
            name = filename or "list.csv"
        elif hasattr(source, "read"):
            if isinstance(source, io.TextIOBase) or "b" not in str(getattr(source, "mode", "b")):
                raise TypeError("file is open in text mode: open it in binary mode: open(path, 'rb')")
            self._stream = source
            self._start = _position(source)
            name = filename or "list.csv"
        else:
            raise TypeError("file must be a path, bytes, or a file opened in binary mode")
        self.boundary = uuid.uuid4().hex
        self._head = (
            f'--{self.boundary}\r\nContent-Disposition: form-data; name="email_column"\r\n\r\n'
            f"{email_column}\r\n"
            f'--{self.boundary}\r\nContent-Disposition: form-data; name="file"; filename="{_form_quote(name)}"\r\n'
            "Content-Type: text/csv\r\n\r\n"
        ).encode("utf-8")
        self._tail = f"\r\n--{self.boundary}--\r\n".encode("ascii")
        self.started = False
        """Whether a one-way stream has begun to be read: it cannot be sent again."""

    @property
    def resendable(self) -> bool:
        return self._stream is None or self._start is not None or not self.started

    def _headers(self, size: Optional[int]) -> Dict[str, str]:
        headers = {"Content-Type": f"multipart/form-data; boundary={self.boundary}"}
        if size is not None:
            headers["Content-Length"] = str(len(self._head) + size + len(self._tail))
        return headers

    def _open(self) -> Tuple[Optional[IO[bytes]], Optional[int]]:
        """The file for this attempt and its size (`None`: unknown, sent chunked)."""
        if self._path is not None:
            handle: IO[bytes] = open(self._path, "rb")
            return handle, os.fstat(handle.fileno()).st_size
        if self._stream is not None:
            if self._start is None:
                return self._stream, None
            end = self._stream.seek(0, io.SEEK_END)
            self._stream.seek(self._start)
            return self._stream, max(0, end - self._start)
        return None, None

    def sync_body(self) -> Tuple[Dict[str, str], Union[bytes, Iterator[bytes]], "_Closer"]:
        if self._data is not None:
            return self._headers(len(self._data)), self._head + self._data + self._tail, _Closer(None, False)
        handle, size = self._open()
        assert handle is not None
        closer = _Closer(handle, self._path is not None)

        def chunks() -> Iterator[bytes]:
            self.started = True
            yield self._head
            while True:
                chunk = handle.read(_CHUNK)
                if not chunk:
                    break
                yield chunk
            yield self._tail

        return self._headers(size), chunks(), closer

    def async_body(self) -> Tuple[Dict[str, str], Union[bytes, AsyncIterator[bytes]], "_Closer"]:
        if self._data is not None:
            return self._headers(len(self._data)), self._head + self._data + self._tail, _Closer(None, False)
        handle, size = self._open()
        assert handle is not None
        closer = _Closer(handle, self._path is not None)

        async def chunks() -> AsyncIterator[bytes]:
            self.started = True
            yield self._head
            while True:
                chunk = handle.read(_CHUNK)
                if not chunk:
                    break
                yield chunk
            yield self._tail

        return self._headers(size), chunks(), closer


class _Closer:
    def __init__(self, handle: Optional[IO[bytes]], owned: bool) -> None:
        self._handle = handle
        self._owned = owned

    def close(self) -> None:
        if self._handle is not None and self._owned:
            self._handle.close()


def _position(stream: Any) -> Optional[int]:
    """Where a seekable stream is (to rewind it to on a retry); `None` for a stream read only once."""
    try:
        if callable(getattr(stream, "seekable", None)) and not stream.seekable():
            return None
        position = stream.tell()
        return position if isinstance(position, int) else None
    except (OSError, ValueError, AttributeError):
        return None


# --- downloads --------------------------------------------------------------------------------------------------------


class Sink:
    """Where a download goes. To a path: a part file beside it, renamed over it once the whole body arrived, removed on
    any failure, so the file there was is untouched. To a stream: written as it comes."""

    def __init__(self, destination: Destination) -> None:
        self._path: Optional[str] = None
        self._stream: Optional[IO[bytes]] = None
        if isinstance(destination, (str, os.PathLike)):
            self._path = os.path.abspath(os.fspath(destination))
        elif hasattr(destination, "write"):
            self._stream = destination
        else:
            raise TypeError("destination must be a path or a file opened in binary mode for writing")
        self._part: Optional[str] = None
        self._file: Optional[IO[bytes]] = None
        self.written = 0
        """Bytes written by this attempt."""

    @property
    def resendable(self) -> bool:
        """Whether a failed attempt can be made again from the start: always to a path, to a stream before its first
        byte."""
        return self._path is not None or self.written == 0

    def begin(self) -> None:
        self.written = 0
        if self._path is not None:
            directory, name = os.path.split(self._path)
            for _ in range(10):
                part = os.path.join(directory, f"{name}.part-{uuid.uuid4().hex[:12]}")
                try:
                    self._file = open(part, "xb")
                except FileExistsError:
                    continue
                self._part = part
                return
            raise VerifyInboxesError(f"could not make a part file beside {self._path}")

    def write(self, chunk: bytes) -> None:
        if not chunk:
            return
        target = self._file if self._path is not None else self._stream
        assert target is not None
        target.write(chunk)
        self.written += len(chunk)

    def commit(self) -> int:
        if self._path is not None and self._file is not None and self._part is not None:
            self._file.flush()
            os.fsync(self._file.fileno())
            self._file.close()
            self._file = None
            os.replace(self._part, self._path)
            self._part = None
        return self.written

    def abort(self) -> None:
        if self._file is not None:
            self._file.close()
            self._file = None
        if self._part is not None:
            try:
                os.unlink(self._part)
            except FileNotFoundError:
                pass
            self._part = None


# --- the client's shared part -----------------------------------------------------------------------------------------


class BaseClient:
    """Configuration and policy. It sends nothing: `VerifyInboxes` and `AsyncVerifyInboxes` do."""

    def __init__(self, *, api_key: Optional[str], base_url: Optional[str], timeout: Optional[float],
                 max_retries: int, user_agent: Optional[str] = None) -> None:
        self.api_key = resolve_api_key(api_key)
        self.base_url = resolve_base_url(base_url)
        self.timeout = check_timeout("timeout", timeout)
        self.max_retries = check_retries(max_retries)
        self._user_agent = resolve_user_agent(user_agent)

    def __repr__(self) -> str:
        return f"{type(self).__name__}(base_url={self.base_url!r}, api_key='{self.api_key[:6]}...')"

    def _headers(self, call: Call, extra: Optional[Mapping[str, str]] = None) -> Dict[str, str]:
        headers = {"Authorization": f"Bearer {self.api_key}", "Accept": call.accept, "User-Agent": self._user_agent}
        if call.idempotency_key is not None:
            headers["Idempotency-Key"] = call.idempotency_key
        if extra:
            headers.update(extra)
        return headers

    def _url(self, path: str) -> str:
        return f"{self.base_url}{path}"

    def _timeout(self, call: Call) -> Optional[float]:
        if self.timeout is None:
            return None
        return max(self.timeout, LONG_TIMEOUT) if call.long else self.timeout

    def _backoff(self, attempt: int) -> float:
        """The n-th retry's wait (n from 0): uniform between half of `min(8, 0.5 × 2^n)` seconds and all of it."""
        ceiling = min(MAX_RETRY_DELAY, INITIAL_RETRY_DELAY * float(2 ** attempt))
        return random.uniform(ceiling / 2, ceiling)

    def _retry_delay(self, call: Call, error: VerifyInboxesError, attempt: int, *, never_sent: bool = False) -> Optional[float]:
        """Seconds to wait before sending `call` again, or `None` to raise `error`."""
        if attempt >= self.max_retries or caller_interrupted():
            return None
        if call.upload is not None and not call.upload.resendable:
            return None
        if isinstance(error, APIConnectionError):
            if call.retry == UNSENT and not never_sent:
                return None
            return self._backoff(attempt)
        if not isinstance(error, APIError):
            return None
        in_progress = error.status == 409 and error.code == "idempotency_in_progress"
        if not (error.status == 429 or error.status >= 500 or in_progress):
            return None
        if call.retry == UNSENT and error.status != 429:
            return None
        if error.retry_after is not None:
            return error.retry_after if error.retry_after <= MAX_RETRY_AFTER else None
        return self._backoff(attempt)


def log_answer(call: Call, attempt: int, status: int, request_id: Optional[str]) -> None:
    log.debug("%s %s: %s, request id %s (attempt %d)", call.method, call.path, status, request_id, attempt + 1)


def log_no_answer(call: Call, attempt: int, exc: BaseException) -> None:
    log.debug("%s %s: no answer, %s (attempt %d)", call.method, call.path, type(exc).__name__, attempt + 1)


def note_interrupted(error: BaseException, job_ids: Sequence[str]) -> None:
    """A call that made jobs was interrupted (Ctrl-C, a cancelled task, a timeout around it): the jobs carry on and
    hold credits, so name them on the exception (where it takes attributes, and as a note on Python 3.11+) and in a
    warning, before it is raised again."""
    if not job_ids or isinstance(error, VerifyInboxesError):
        return
    ids = list(job_ids)
    text = (f"interrupted; still running: {', '.join(ids)} "
            "(wait for them again with client.checks.wait or client.lists.wait)")
    try:
        setattr(error, "job_ids", ids)
    except (AttributeError, TypeError):
        pass
    add_note = getattr(error, "add_note", None)
    if callable(add_note):
        add_note(f"verifyinboxes: {text}")
    log.warning("%s", text)


def wait_timeout_message(what: str, job_id: str, status: str, timeout: Optional[float], resume: str) -> str:
    after = f" after {timeout:g} seconds" if timeout is not None else ""
    return (f"The {what} {job_id} was still {status}{after}. It carries on, and its credits are held: "
            f"wait for it again with {resume}({job_id!r}).")
