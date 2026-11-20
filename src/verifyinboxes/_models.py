"""What the API returns, as frozen dataclasses named after the API description's schemas.

Field names are the API's. `Model.from_dict` reads the fields this version knows and ignores the others (the API
adds fields without a new version); a value it does not know, such as a new verdict or reason, is kept as the plain
string it is. `to_dict()` gives the fields back as the API names them. Timestamps stay ISO 8601 strings. A field the
API left out is `None`.
"""
from __future__ import annotations

import dataclasses
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Mapping, Optional, Type, TypeVar

__all__ = [
    "Model",
    "Account",
    "Result",
    "ResultRow",
    "ResultsPage",
    "PendingCheck",
    "Job",
    "JobCreated",
    "VerdictCounts",
    "ResolveCreated",
    "ResolveSkipped",
    "Domain",
    "MxHost",
    "SuppressionList",
    "Webhook",
    "WebhookCreated",
    "Notice",
    "NoticePage",
    "FeedbackEvent",
    "FeedbackResult",
    "FeedbackRejection",
    "Integration",
    "IntegrationCreated",
]

M = TypeVar("M", bound="Model")


def _f(convert: Optional[Callable[[Any], Any]] = None, *, optional: bool = False, secret: bool = False) -> Any:
    """A field: `convert` builds it from JSON; `optional` leaves it out of `to_dict()` when it is `None` (the API may
    leave it out); `secret` keeps it out of `repr()`."""
    return field(default=None, repr=not secret, metadata={"convert": convert, "optional": optional})


def _plain(value: Any) -> Any:
    if isinstance(value, Model):
        return value.to_dict()
    if isinstance(value, list):
        return [_plain(item) for item in value]
    if isinstance(value, dict):
        return {key: _plain(item) for key, item in value.items()}
    return value


def _list_of(convert: Callable[[Any], Any]) -> Callable[[Any], List[Any]]:
    def run(value: Any) -> List[Any]:
        return [convert(item) for item in value] if isinstance(value, list) else []

    return run


def _copy(value: Any) -> Any:
    if isinstance(value, list):
        return [_copy(item) for item in value]
    if isinstance(value, Mapping):
        return {key: _copy(item) for key, item in value.items()}
    return value


@dataclass(frozen=True)
class Model:
    """The base of every model."""

    @classmethod
    def from_dict(cls: Type[M], data: Mapping[str, Any]) -> M:
        """The model of a JSON object: the fields it knows, the others ignored."""
        source: Mapping[str, Any] = data if isinstance(data, Mapping) else {}
        values: Dict[str, Any] = {}
        for item in dataclasses.fields(cls):
            value = source.get(item.name)
            convert = item.metadata.get("convert")
            if value is not None:
                value = convert(value) if convert is not None else _copy(value)
            values[item.name] = value
        return cls(**values)

    def to_dict(self) -> Dict[str, Any]:
        """The model as a JSON object, named as the API names it."""
        out: Dict[str, Any] = {}
        for item in dataclasses.fields(self):
            value = getattr(self, item.name)
            if value is None and item.metadata.get("optional"):
                continue
            out[item.name] = _plain(value)
        return out


def _model(cls: Type[M]) -> Callable[[Any], Optional[M]]:
    def run(value: Any) -> Optional[M]:
        return cls.from_dict(value) if isinstance(value, Mapping) else None

    return run


# --- the account ------------------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class Account(Model):
    """`GET /v1/account`."""

    name: str = _f()
    balance: int = _f()
    """Credits left; below zero after a refund of credits already spent."""
    reviewed: bool = _f()
    """Whether what the account reports as feedback counts toward everyone's results."""
    test_emails: bool = _f()
    """Whether the account may resolve catch-all addresses (`lists.resolve`)."""
    safe_to_send_threshold: float = _f()


# --- checks -----------------------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class Result(Model):
    """One address's result. `job_id` names the job that holds it: the single check of `verify`, or the list of
    `verify_many` (11 or more addresses)."""

    email: str = _f()
    verdict: str = _f()
    """`valid`, `invalid`, `catch_all`, `risky` or `unknown`; keep any other value you meet as it is."""
    score: Optional[float] = _f()
    safe_to_send: bool = _f()
    flags: List[str] = _f()
    provider: Optional[str] = _f()
    catch_all_domain: Optional[bool] = _f()
    verified_at: str = _f()
    source: str = _f()
    reason: str = _f()
    did_you_mean: Optional[str] = _f(optional=True)
    job_id: Optional[str] = _f(optional=True)


@dataclass(frozen=True)
class ResultRow(Result):
    """One row of a list's results: the result of its address, and `row`, its number in the CSV (1 for the first row
    under the header)."""

    row: int = _f()


@dataclass(frozen=True)
class PendingCheck(Model):
    """What `verify(..., wait=False)` returns for a check that is still running: wait for it with `checks.wait`."""

    status: str = _f()
    """`pending`."""
    job_id: str = _f()


@dataclass(frozen=True)
class VerdictCounts(Model):
    """How many rows have each verdict."""

    valid: Optional[int] = _f(optional=True)
    invalid: Optional[int] = _f(optional=True)
    catch_all: Optional[int] = _f(optional=True)
    risky: Optional[int] = _f(optional=True)
    unknown: Optional[int] = _f(optional=True)


@dataclass(frozen=True)
class Job(Model):
    """A check, a list or a catch-all resolution (`kind` `single`, `bulk` or `resolve`), as it is now."""

    id: str = _f()
    kind: str = _f()
    status: str = _f()
    """`running` until every address has a result, then `done`."""
    total: int = _f()
    done: int = _f()
    created_at: str = _f()
    finished_at: Optional[str] = _f()
    counts: Optional[VerdictCounts] = _f(_model(VerdictCounts))
    purged: bool = _f()
    result: Optional[Result] = _f(_model(Result), optional=True)
    """A single check's result, once it is done."""
    parent_job_id: Optional[str] = _f(optional=True)
    resolve_job_ids: Optional[List[str]] = _f(optional=True)
    download_url: Optional[str] = _f(optional=True)


@dataclass(frozen=True)
class JobCreated(Model):
    """A list, just made."""

    id: str = _f()
    status: str = _f()
    total: int = _f()
    """The number of addresses in it."""


@dataclass(frozen=True)
class ResultsPage(Model):
    """One page of a list's results."""

    data: List[ResultRow] = _f(_list_of(ResultRow.from_dict))
    next_cursor: Optional[str] = _f()
    """The next page's cursor; `None` on the last page. A page may hold fewer rows than asked, even none, before it."""
    complete: bool = _f()


# --- catch-all resolution ---------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class ResolveSkipped(Model):
    ineligible: int = _f()
    excluded: int = _f()
    already_requested: int = _f()
    duplicate: int = _f()


@dataclass(frozen=True)
class ResolveCreated(Model):
    """A resolution, just made: a job of its own (`id`)."""

    id: str = _f()
    status: str = _f()
    total: int = _f()
    skipped: ResolveSkipped = _f(_model(ResolveSkipped))


# --- domains, suppressions --------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class MxHost(Model):
    host: str = _f()
    preference: int = _f()


@dataclass(frozen=True)
class Domain(Model):
    """What is known about a domain."""

    domain: str = _f()
    provider: Optional[str] = _f()
    gateway: bool = _f()
    accepts_mail: bool = _f()
    mx_hosts: List[MxHost] = _f(_list_of(MxHost.from_dict))
    catch_all: Optional[bool] = _f()
    catch_all_checked_at: Optional[str] = _f()
    email_pattern: Optional[str] = _f()
    pattern_confidence: Optional[float] = _f()
    registered_at: Optional[str] = _f()


@dataclass(frozen=True)
class SuppressionList(Model):
    """The account's suppression list. Addresses are kept only as a hash, so only their number is told."""

    domains: List[str] = _f()
    address_count: int = _f()


# --- webhooks, notices ------------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class Webhook(Model):
    id: str = _f()
    url: str = _f()
    events: List[str] = _f()
    created_at: str = _f()


@dataclass(frozen=True)
class WebhookCreated(Model):
    """A webhook, just made. `secret` signs its deliveries and is shown only here: store it. It is left out of
    `repr()`, and kept in `to_dict()`."""

    id: str = _f()
    url: str = _f()
    events: List[str] = _f()
    secret: str = _f(secret=True)
    created_at: str = _f()


@dataclass(frozen=True)
class Notice(Model):
    """A verdict you bought, overturned by what became of mail sent to the address. `previous`, `current` and
    `evidence` are the API's objects."""

    notice_id: str = _f()
    email: str = _f()
    job_ids: List[str] = _f()
    previous: Dict[str, Any] = _f()
    current: Dict[str, Any] = _f()
    evidence: Dict[str, Any] = _f()
    message: str = _f()
    created_at: str = _f()


@dataclass(frozen=True)
class NoticePage(Model):
    notices: List[Notice] = _f(_list_of(Notice.from_dict))
    next_after: Optional[str] = _f()
    """Send it as `after` next time."""


# --- feedback, integrations -------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class FeedbackEvent(Model):
    """What became of mail you sent to an address: `FeedbackEvent(email=..., type="hard_bounce", observed_at=...)`."""

    email: str = _f()
    type: str = _f()
    """`sent`, `delivered`, `reply`, `auto_reply`, `hard_bounce`, `soft_bounce` or `bounce`."""
    observed_at: str = _f()
    """When it happened, ISO 8601."""
    event_id: Optional[str] = _f(optional=True)
    """Your own id of the event: the same id sent again counts once."""
    dsn_code: Optional[str] = _f(optional=True)
    diagnostic: Optional[str] = _f(optional=True)


@dataclass(frozen=True)
class FeedbackRejection(Model):
    index: int = _f()
    """The event's position in what you sent, from 0."""
    error: str = _f()


@dataclass(frozen=True)
class FeedbackResult(Model):
    accepted: int = _f()
    duplicates: int = _f()
    ignored: int = _f()
    rejected: List[FeedbackRejection] = _f(_list_of(FeedbackRejection.from_dict))


@dataclass(frozen=True)
class Integration(Model):
    id: str = _f()
    provider: str = _f()
    label: Optional[str] = _f()
    token_prefix: str = _f()
    created_at: str = _f()
    last_event_at: Optional[str] = _f()
    last_bounce_at: Optional[str] = _f()
    last_error: Optional[str] = _f()
    last_error_at: Optional[str] = _f()
    signed: bool = _f()


@dataclass(frozen=True)
class IntegrationCreated(Model):
    """An integration, just made. `url` holds its token and is shown only here: paste it into the provider. It is left
    out of `repr()`, and kept in `to_dict()`."""

    id: str = _f()
    provider: str = _f()
    label: Optional[str] = _f()
    url: str = _f(secret=True)
    created_at: str = _f()
