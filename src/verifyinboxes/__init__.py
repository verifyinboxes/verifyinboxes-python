"""The official Python library for the VerifyInboxes API.

    from verifyinboxes import VerifyInboxes

    client = VerifyInboxes()                        # reads VERIFYINBOXES_API_KEY
    result = client.verify("ann@example.com")
    print(result.verdict, result.safe_to_send)
"""
from ._client import AsyncVerifyInboxes, VerifyInboxes
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
    WaitTimeoutError,
)
from ._models import (
    Account,
    Domain,
    FeedbackEvent,
    FeedbackRejection,
    FeedbackResult,
    Integration,
    IntegrationCreated,
    Job,
    JobCreated,
    Model,
    MxHost,
    Notice,
    NoticePage,
    PendingCheck,
    ResolveCreated,
    ResolveSkipped,
    Result,
    ResultRow,
    ResultsPage,
    SuppressionList,
    VerdictCounts,
    Webhook,
    WebhookCreated,
)
from ._version import __version__
from ._webhooks import DELIVERY_HEADER, SIGNATURE_HEADER, verify_signature

__all__ = [
    "__version__",
    # clients
    "VerifyInboxes",
    "AsyncVerifyInboxes",
    # webhooks
    "verify_signature",
    "SIGNATURE_HEADER",
    "DELIVERY_HEADER",
    # errors
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
    # models
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
