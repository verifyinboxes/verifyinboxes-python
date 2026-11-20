# VerifyInboxes Python library

[![CI](https://github.com/verifyinboxes/verifyinboxes-python/actions/workflows/ci.yml/badge.svg)](https://github.com/verifyinboxes/verifyinboxes-python/actions/workflows/ci.yml)

The official Python library for the [VerifyInboxes](https://verifyinboxes.com) API: check whether email addresses
can receive mail, one at a time or as lists of any size, and read the results.

- Sync and async clients with the same surface
- Typed models and typed errors; ships `py.typed`
- Retries that never charge twice, waiting for checks and lists, results read page by page
- Webhook signature checks
- One runtime dependency: [`httpx`](https://www.python-httpx.org/)
- Python 3.9+

## Install

Not published yet: until the first release is on PyPI, install it from this repository:

```bash
pip install git+https://github.com/verifyinboxes/verifyinboxes-python
```

Once released:

```bash
pip install verifyinboxes
```

## Quickstart

An organisation admin makes an API key in the VerifyInboxes app under **Developers** → **API keys**. Put it in the
environment:

```bash
export VERIFYINBOXES_API_KEY="bv_..."
```

```python
from verifyinboxes import VerifyInboxes

client = VerifyInboxes()  # reads VERIFYINBOXES_API_KEY

result = client.verify("ann@example.com")
print(result.verdict)        # "valid", "invalid", "catch_all", "risky" or "unknown"
print(result.safe_to_send)   # True or False
print(result.reason)         # why, e.g. "mailbox_not_found"
print(result.job_id)         # the check's job

for result in client.verify_many(["bob@example.com", "cat@example.com"]):
    print(result.email, result.verdict)
```

## The client

```python
from verifyinboxes import VerifyInboxes

client = VerifyInboxes(
    api_key="bv_...",                         # default: the VERIFYINBOXES_API_KEY environment variable
    base_url="https://api.verifyinboxes.com", # default: VERIFYINBOXES_BASE_URL, else this
    timeout=60.0,                             # seconds one attempt of one request may take
    max_retries=2,                            # see "Retries"
    user_agent="my-app/1.2",                  # put in front of verifyinboxes-python/<version>
)
```

Without a key, the constructor raises `MissingAPIKeyError`; a key with a space or a control character in it (half
of a copy and paste) raises `ValueError`. Keep the key out of your code: the environment variable is the place for
it. The client never logs it, and `repr(client)` shows only its first 6 characters.

The client holds a connection pool. Use it as a context manager, or call `client.close()` when you are done:

```python
with VerifyInboxes() as client:
    print(client.account.get().balance)
```

To bring your own `httpx.Client` (a proxy, TLS settings), pass `http_client=...`. The library does not close a
client it did not make.

### Personal access tokens

Where an API key works, so does a personal access token (`vi_at_...`): what an app gets when a person signs in to it
with their VerifyInboxes account. It acts as that person, with their role in the organisation they chose, and lives
an hour; after that the API answers `401` (`AuthenticationError`) and the app signs in again or uses its refresh
token. Pass it as the key:

```python
client = VerifyInboxes(access_token)
```

## Checking addresses

### One address

```python
result = client.verify("ann@example.com")
result = client.verify("ann@example.com", force_recheck=True)   # skip the cache
```

The API answers within about 5 seconds, or says the check is still running; then the client asks every 2 seconds
until it is done, for up to `timeout` seconds (default 60). Every result names its job (`result.job_id`): a
`catch_all` or `risky` one can later be sent to `client.lists.resolve(result.job_id)`.

| `Result` field | |
| --- | --- |
| `email` | The address, as checked. |
| `verdict` | `valid`, `invalid`, `catch_all`, `risky` or `unknown`. |
| `safe_to_send` | Whether to send to it: `valid`, or a `catch_all` whose score reaches your organisation's threshold. |
| `score` | For a `catch_all` verdict, the probability that the mailbox exists (0 to 1), else `None`. |
| `reason` | Why: `mailbox_exists`, `mailbox_not_found`, `no_mail_server`, `accept_all`, `syntax`, `typo`, ... |
| `did_you_mean` | For a `typo`: the address it probably is. |
| `flags` | `disposable`, `role`, `free_provider`, `gateway`. |
| `provider`, `catch_all_domain`, `source`, `verified_at` | The domain's mail provider, whether it accepts every address, where the answer came from, and when. |
| `job_id` | The job that holds the result. |

The API adds verdicts, reasons and flags over time: a value you do not know is kept as the string it is, never an
error. Show it, and act on `safe_to_send`.

### Waiting, and picking a check up again

A check that has not finished when `timeout` runs out raises `WaitTimeoutError`. The check carries on, and the error
names its job in `job_ids`, so nothing is lost:

```python
from verifyinboxes import WaitTimeoutError

try:
    result = client.verify("ann@example.com", timeout=10)
except WaitTimeoutError as error:
    job_id = error.job_ids[0]
    ...
    result = client.checks.wait(job_id)        # later: picks it up where it was
```

`wait=False` does not wait at all: you get the result, or a `PendingCheck` with its `job_id`.

```python
from verifyinboxes import PendingCheck

started = client.verify("ann@example.com", wait=False)
if isinstance(started, PendingCheck):
    result = client.checks.wait(started.job_id, timeout=120)

job = client.checks.get(job_id)                # the job as it is now; job.result once it is done
```

### Many addresses

**Never check many addresses with a loop of `verify`.** Checks go ahead of lists only up to 10 running at once:
past that, your checks wait behind other work, and a loop gains nothing over the ways below.

`verify_many` returns the results in the order of its input:

```python
results = client.verify_many(addresses, timeout=600)
```

Up to 10 addresses are checked one by one, together. More go as one list (below), which the call waits for and
reads back; every result's `job_id` is then the list's. For a large or long-running batch, use a list yourself.

### Lists

```python
created = client.lists.create(file="prospects.csv", email_column="email")   # a path, bytes or a binary file
created = client.lists.create(csv="email\nann@example.com\n")              # a CSV's text
created = client.lists.create(emails=["ann@example.com", "bob@example.com"])

job = client.lists.wait(created.id)            # asks at once, then after 2, 4, 8, 16 and every 30 seconds
print(job.status, job.counts)                  # "done", VerdictCounts(valid=..., invalid=..., ...)
```

**How long a list takes.** A few hundred addresses take minutes, and Microsoft-hosted ones longer. `lists.wait` has no
time limit unless you give one; `lists.wait(id, timeout=...)` raises `WaitTimeoutError` (the list carries on), and
`lists.wait(id)` picks it up again at any time. Instead of waiting, you can be told: see "Webhooks" below.

The results, every row once and in order, read a page at a time as you iterate:

```python
for row in client.lists.results(created.id):
    print(row.row, row.email, row.verdict, row.safe_to_send)

client.lists.results(created.id, only_safe_to_send=True)
client.lists.results(created.id, verdicts=["valid", "catch_all"])
client.lists.results(created.id, partial=True)        # a list still running, as it stands
```

To keep your own cursor (to resume elsewhere), read one page at a time:

```python
page = client.lists.results_page(created.id, page_size=1000)
while True:
    save(page.data)
    if page.next_cursor is None:
        break
    page = client.lists.results_page(created.id, cursor=page.next_cursor, page_size=1000)
```

A page may hold fewer rows than asked, even none, before the last one: follow `next_cursor` until it is `None`.

The CSV you sent, with the results appended:

```python
written = client.lists.download(created.id, "results.csv")      # bytes written
client.lists.download(created.id, "safe.csv", only_safe_to_send=True)
with open("results.csv", "wb") as out:
    client.lists.download(created.id, out)
```

A download to a path is written beside it and renamed over it once whole: if it fails, the file that was there is
untouched.

`client.lists.get(id)` is the list as it is now.

### Catch-all resolution

For an organisation allowed to send test emails, the `catch_all` and `risky` addresses of a finished list or check
can be settled by a test email. Most are settled within about 3 days of sending, some take up to 7 days. One request
takes at most 500 addresses (more is refused with `too_many_to_resolve`): for a list with more, name them in parts of
at most 500:

```python
resolution = client.lists.resolve(created.id)                               # all of them, if at most 500
resolution = client.lists.resolve(created.id, emails=["bob@example.com"])  # some (at most 500)
job = client.lists.wait(resolution.id)
```

## Everything else

```python
account = client.account.get()                 # name, balance, reviewed, test_emails, safe_to_send_threshold

domain = client.domains.get("example.com")     # provider, mx_hosts, catch_all, email_pattern, ... (1 credit)

client.suppressions.add(emails=["ann@example.com"], domains=["competitor.example"])   # -> how many were added
client.suppressions.remove(emails=["ann@example.com"])                                # -> how many were removed
client.suppressions.list()                     # domains, and the number of addresses (kept only as a hash)

created = client.webhooks.create("https://example.com/webhooks/verifyinboxes", events=["job.completed"])
created.secret                                 # shown only now: store it
client.webhooks.list()
client.webhooks.delete(created.id)

page = client.notices.list(after=last_seen, limit=100)    # verdicts you bought, overturned since
last_seen = page.next_after

from verifyinboxes import FeedbackEvent
client.feedback.report([FeedbackEvent(email="ann@example.com", type="hard_bounce",
                                      observed_at="2026-11-04T10:00:00Z", dsn_code="5.1.1", event_id="b-1")])

hook = client.integrations.create("instantly", label="main workspace")
hook.url                                       # shown only now: paste it into the provider
client.integrations.list()
client.integrations.delete(hook.id)
```

## Errors

```text
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
```

An `APIError` carries what the API said, and the request id support asks for:

```python
import verifyinboxes

try:
    client.lists.create(file="prospects.csv")
except verifyinboxes.InsufficientCreditsError:
    print("top up first")
except verifyinboxes.APIError as error:
    print(error.status)        # 422
    print(error.code)          # "consumer_list"  <- match on this
    print(error.message)       # a sentence for a person
    print(error.request_id)    # the answer's X-Request-Id: quote it when you write to support
    print(error)               # "422 consumer_list: ... (request id 019a...)"
```

| Attribute | |
| --- | --- |
| `status` | The HTTP status. |
| `code` | The API's error code, or `None` when the answer was not the API's (a proxy's error page). Match on it; treat one you do not know by its status. |
| `message` | A sentence for a person. Show it or log it; do not parse it. |
| `request_id` | The answer's `X-Request-Id`. After retries, the last answer's. |
| `retry_after` | The answer's `Retry-After`, in seconds, when it had one. |
| `body` | The answer's JSON body. |
| `job_ids` | On every error: the jobs the call had made when it failed. |

**`job_ids`.** Every error raised while waiting, and by `verify_many`, names every job the call made, in the order
of its input. Those jobs carry on, and the credits they hold are spent on them: pick them up with
`client.checks.wait(job_id)` or `client.lists.wait(job_id)` instead of starting again.

When `verify` or `verify_many` is interrupted instead (Ctrl-C, a cancelled task, a timeout around it), it names the
jobs it made in a warning on the `verifyinboxes` logger ("interrupted; still running: ...") and on the exception, as
`job_ids` (and as a note on Python 3.11 and later), before the exception goes on. The exception you catch does not
always carry them: on Python 3.9 and 3.10 the `CancelledError` a cancelled task raises to whoever awaits it is a new
one, without `job_ids`, and a timeout around the call (`asyncio.wait_for`) raises a `TimeoutError` without them on
every version. The warning names them in every case.

A request that starts a check or `verify_many`'s list and may have reached the API is let finish first, so that its
job is known: a cancelled task waits for the attempt on its way (at most the client's `timeout`, or the rest of a
pause before a retry), and so does Ctrl-C during `verify_many`, whose requests run in threads of their own. Nothing
is sent after that attempt: if it fails, it is not retried, and a job it may have made is not named. A second
interruption stops waiting. Ctrl-C during the request of a plain `verify` (sync) cuts it off: if it had reached the
API, the check exists but is not named.

Errors can be pickled and copied with everything on them, so they cross a process pool or a task queue whole.

## Retries

A request that fails in a way worth retrying is sent again, up to `max_retries` times (default 2):

| Failure | Retried |
| --- | --- |
| No answer: the connection failed, was reset or closed, or timed out | Yes |
| `429` | Yes, after `Retry-After` |
| `409 idempotency_in_progress` | Yes, after `Retry-After` |
| `5xx` | Yes |
| Any other `4xx` | No: the same request would get the same answer |

The wait is the answer's `Retry-After` when it has one. A `Retry-After` over 60 seconds is not waited for: the error
is raised at once, with the wait on `.retry_after`. Otherwise it backs off exponentially, with jitter: up to half a
second before the first retry, up to a second before the second, doubling to at most eight. Once its call is
interrupted (Ctrl-C, a cancelled task), a request is not retried: see **`job_ids`** above.

**Nothing is charged twice.** Every `POST` carries an `Idempotency-Key`, the same on every attempt of one call.
When an answer is lost after the API ran the request (a check, a list, an upload, a resolution, feedback, a
suppression), the retry gets the first answer again: one job, charged once.

A `409 idempotency_in_progress` (`ConflictError`) left after the retries means the first request with that key is
still running on the API. Repeat the call later with your own `idempotency_key`, the same one: the API then answers
with what the first request answered, and runs nothing twice. (Without a key of your own, each call makes a new one,
which cannot be sent again: pass your own where a repeat matters.)

Your own key makes one request safe to send again after your program restarts. A key names **one** request, and the
API keeps its answer for 24 hours: in that time the same key gets that answer again, whatever you send with it. An
upload is known by its key alone, never by its file, so another file under a used key gets the first file's list;
and an error is kept like any answer, so a `402` stays a `402` under its key. So make the key from the request
itself, and give a new file, or a retry after you topped up, a new key:

```python
import hashlib

client.verify("ann@example.com", idempotency_key=f"order-{order_id}")    # one key per order

with open("prospects.csv", "rb") as f:
    digest = hashlib.sha256(f.read()).hexdigest()
client.lists.create(file="prospects.csv", idempotency_key=f"import-{digest}")  # one key per file
```

A file is sent again whole: a path is opened again, a seekable file rewound to where it was. A stream that can be
read only once is not sent again once it began to be read.

Two requests are not sent again once they may have arrived: `webhooks.create` and `integrations.create`, whose
answers hold a secret shown once. They are retried only when the connection was never made, or after a `429`; after
an `APIConnectionError`, look at `webhooks.list()` or `integrations.list()` before making another. A `DELETE` whose
answer was lost is sent again; if the retry finds it gone, the first one did it.

```python
client = VerifyInboxes(max_retries=0)   # never retry
```

## Webhooks

A webhook gets a `POST` when a list finishes (`job.completed`) and when a verdict you bought is overturned
(`address.contradicted`). Check its signature before you read anything in it, against the raw body as it arrived:

```python
from verifyinboxes import verify_signature

if not verify_signature(raw_body, headers["X-Boxverifier-Signature"], secret):
    return 400   # not from VerifyInboxes, changed on the way, or older than 5 minutes
event = json.loads(raw_body)
```

`secret` is the 64 characters `webhooks.create` returned. A signature more than `tolerance` seconds (default 300)
from your clock is refused, so an old delivery cannot be sent again. It never raises: anything it cannot read is
`False`. It needs no client and no key, and `client.webhooks.verify_signature` is the same function. Deliveries come
at least once: `X-Boxverifier-Delivery` is the same on every retry of one, to drop duplicates. The two header names
are `verifyinboxes.SIGNATURE_HEADER` and `verifyinboxes.DELIVERY_HEADER`.

## Async

`AsyncVerifyInboxes` has the same resources, methods and arguments; every call is awaited, and results are read
with `async for`. Up to 10 addresses of `verify_many` are sent together with `asyncio.gather`.

```python
import asyncio
from verifyinboxes import AsyncVerifyInboxes


async def main() -> None:
    async with AsyncVerifyInboxes() as client:
        result = await client.verify("ann@example.com")
        results = await client.verify_many(["ann@example.com", "bob@example.com"])

        created = await client.lists.create(file="prospects.csv")
        await client.lists.wait(created.id)
        async for row in client.lists.results(created.id):
            print(row.email, row.verdict)


asyncio.run(main())
```

## Models

Answers are frozen dataclasses named after the API's schemas (`Result`, `Job`, `JobCreated`, `ResultRow`,
`Domain`, ...), with the API's field names. Timestamps are ISO 8601 strings. `to_dict()` gives the fields back as
the API names them; a field this version does not know is left out, never an error. `WebhookCreated.secret` and
`IntegrationCreated.url` are left out of `repr()`, and kept in `to_dict()`.

## Logging

The library logs each request to the `verifyinboxes` logger at `DEBUG`: the method, the path, the status, the
request id and the attempt. Never a header, a body, or a download's query. At `WARNING` it logs only the jobs an
interrupted `verify_many` leaves running.

```python
import logging
logging.basicConfig()
logging.getLogger("verifyinboxes").setLevel(logging.DEBUG)
```

## Examples

- [`examples/quickstart.py`](https://github.com/verifyinboxes/verifyinboxes-python/blob/main/examples/quickstart.py): one check, then several with `verify_many`.
- [`examples/list_from_csv.py`](https://github.com/verifyinboxes/verifyinboxes-python/blob/main/examples/list_from_csv.py): a list from a CSV: start it, wait, read the results,
  download them.
- [`examples/webhook_receiver.py`](https://github.com/verifyinboxes/verifyinboxes-python/blob/main/examples/webhook_receiver.py): a webhook receiver on the standard library's HTTP
  server, checking the signature before anything else.

## Versioning

The library follows [semantic versioning](https://semver.org). It is `0.x` until the API's public launch, when it
becomes `1.0.0`; until then a minor version may change the library's surface, and the changes are listed in its
release notes. The API itself changes without breaking you: it adds fields, values and error codes, which this
library ignores or keeps as they are.

## Development

```bash
uv sync
uv run pytest       # the tests, against a fake API on 127.0.0.1 (no network)
uv run mypy         # the strict type check, of the package and the examples
uv build            # the wheel and the source distribution
```

`tests/conformance/scenarios.json` is shared by every official VerifyInboxes library: what each must send and do on
the wire (retries with the same `Idempotency-Key`, job ids kept on a timeout, request ids on errors, results through
empty pages, the webhook signature cases). `tests/test_conformance.py` replays it. `tests/test_smoke.py` runs against
the live API when `VERIFYINBOXES_SMOKE_API_KEY` holds a key (it spends a few credits).

## Licence

MIT. See [LICENSE](https://github.com/verifyinboxes/verifyinboxes-python/blob/main/LICENSE). To report a security problem, see [SECURITY.md](https://github.com/verifyinboxes/verifyinboxes-python/blob/main/SECURITY.md).
