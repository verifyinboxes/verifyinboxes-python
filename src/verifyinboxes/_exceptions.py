"""Everything this library raises.

    VerifyInboxesError                    the base of all; also "the API broke its own rules"
    ├── MissingAPIKeyError                no key given and VERIFYINBOXES_API_KEY not set
    ├── APIConnectionError                the request got no answer
    │   └── APITimeoutError               ... in time
    ├── APIError                          the API answered with an error (any other 4xx)
    │   ├── InvalidRequestError           400, 413, 414, 415, 422
    │   ├── AuthenticationError           401
    │   ├── InsufficientCreditsError      402
    │   ├── PermissionDeniedError         403
    │   ├── NotFoundError                 404, 410
    │   ├── ConflictError                 409
    │   ├── RateLimitError                429
    │   └── ServerError                   5xx
    └── WaitTimeoutError                  a check or a list was still running when the time to wait ran out

Every error has `job_ids`: the jobs the call had made when it failed (a check that timed out, a list being waited
for), so the credits they hold are never lost: wait for them again with `checks.wait` or `lists.wait`.
"""
from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Type, TypeVar

__all__ = [
    "VerifyInboxesError",
    "MissingAPIKeyError",
    "APIConnectionError",
    "APITimeoutError",
    "APIError",
    "InvalidRequestError",
    "AuthenticationError",
    "InsufficientCreditsError",
    "PermissionDeniedError",
    "NotFoundError",
    "ConflictError",
    "RateLimitError",
    "ServerError",
    "WaitTimeoutError",
]


class VerifyInboxesError(Exception):
    """The base of every error this library raises. Raised as itself when the API answered something it should not
    have (a page that repeats its cursor, a finished check with no result)."""

    def __init__(self, message: str, *, job_ids: Optional[Sequence[str]] = None) -> None:
        super().__init__(message)
        self.message = message
        self.job_ids: List[str] = list(job_ids or [])
        """The jobs the call had made when it failed, in the order of its input."""

    def __reduce__(self) -> Tuple[Callable[..., Any], Tuple[Any, ...]]:
        # Rebuilt without calling __init__ (whose keyword-only arguments pickle cannot give), with every attribute
        # back: an error that crosses a process (a ProcessPoolExecutor, a task queue) or is copied keeps its job ids
        # and request id.
        return _rebuild, (type(self), self.args, dict(self.__dict__))


E = TypeVar("E", bound=VerifyInboxesError)


def _rebuild(cls: Type[E], args: Tuple[Any, ...], state: Dict[str, Any]) -> E:
    """An error as `pickle` and `copy` make it again: through `__new__`, then its attributes."""
    error = cls.__new__(cls, *args)
    error.__dict__.update(state)
    return error


class MissingAPIKeyError(VerifyInboxesError):
    """No API key was given and `VERIFYINBOXES_API_KEY` is not set."""


class APIConnectionError(VerifyInboxesError):
    """The request got no answer: the connection could not be made, or it was closed or reset before the whole answer
    came. A request the API may have run is only sent again when that is safe (see the README's "Retries")."""

    def __init__(self, message: str = "Could not reach the VerifyInboxes API.", *,
                 job_ids: Optional[Sequence[str]] = None) -> None:
        super().__init__(message, job_ids=job_ids)


class APITimeoutError(APIConnectionError):
    """The request got no answer in time."""

    def __init__(self, message: str = "The VerifyInboxes API did not answer in time.", *,
                 job_ids: Optional[Sequence[str]] = None) -> None:
        super().__init__(message, job_ids=job_ids)


class APIError(VerifyInboxesError):
    """The API answered with an error status.

    `code` is the API's stable error code (`unauthorized`, `not_ready`, `consumer_list`, ...): match on it, and treat
    one you do not know by its `status`. `message` is a sentence for a person. `request_id` is the answer's
    `X-Request-Id`: quote it when you write to support. `retry_after` is the answer's `Retry-After` in seconds.
    """

    def __init__(
        self,
        message: str,
        *,
        status: int,
        code: Optional[str] = None,
        request_id: Optional[str] = None,
        retry_after: Optional[float] = None,
        body: Any = None,
        job_ids: Optional[Sequence[str]] = None,
    ) -> None:
        super().__init__(message, job_ids=job_ids)
        self.status = status
        self.code = code
        self.request_id = request_id
        self.retry_after = retry_after
        self.body = body
        """The answer's JSON body (its text when it is not JSON)."""

    def __str__(self) -> str:
        head = f"{self.status} {self.code}" if self.code else f"{self.status}"
        text = f"{head}: {self.message}"
        if self.request_id:
            text += f" (request id {self.request_id})"
        return text


class InvalidRequestError(APIError):
    """400, 413, 414, 415 or 422: the request does not fit (`invalid_request`, `consumer_list`, `empty_list`, ...)."""


class AuthenticationError(APIError):
    """401: the key or token is missing, mistyped, revoked, or (a personal access token) past its hour."""


class InsufficientCreditsError(APIError):
    """402 `insufficient_credits`: the balance does not cover the request. Nothing was charged."""


class PermissionDeniedError(APIError):
    """403: allowed for nobody with this credential (`forbidden`, `review_required`, `account_suspended`, ...)."""


class NotFoundError(APIError):
    """404 `not_found`, or 410 `gone` (a list whose data was deleted)."""


class ConflictError(APIError):
    """409: `not_ready` (the list has not finished), `idempotency_conflict` (the key was used for another request)."""


class RateLimitError(APIError):
    """429: too many requests or lists at once. Retried on its own, unless `retry_after` is over 60 seconds."""


class ServerError(APIError):
    """5xx: something went wrong on the API's side. Retried on its own; this is the last answer's."""


class WaitTimeoutError(VerifyInboxesError):
    """A check or a list was still running when the time given to wait ran out. It carries on, and its credits are
    held: wait for it again by its id (`job_ids`) with `checks.wait` or `lists.wait`."""

    def __init__(self, message: str, *, job_ids: Sequence[str], timeout: Optional[float]) -> None:
        super().__init__(message, job_ids=job_ids)
        self.timeout = timeout
