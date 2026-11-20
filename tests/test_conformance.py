"""The conformance scenarios: what every official VerifyInboxes SDK must send and do, replayed here against a fake API.

`tests/conformance/scenarios.json` is shared, byte for byte, by the Python, JavaScript, Go and Java SDKs. Each
scenario names a call (`call.op` and its `args`, by this library's names), the exchanges the server expects and
answers (`exchanges`, in order unless `unordered`), the seconds the client must sleep (`sleeps`; absent: not
checked) and the outcome: `returns` (compared as a subset: an object's keys must be there with those values, an
array has exactly those elements) or `raises` (the error's kind, and its status, code, request id, `retry_after`
and `job_ids` when given). A request is checked for its method, path (as escaped on the wire), query, body
(`json` or the ordered `multipart` parts), the `headers` named, its `Idempotency-Key` (`"new"`: present and unseen,
`"same"`: the one of the exchange listed just before it, `{"equals": …}`, absent: none) and, always, the bearer
credential and the `User-Agent`. An answer can be dropped (`"drop": true`: the connection closes with no answer) or
cut short (`"cut_after"`). Each scenario runs with `VerifyInboxes` and with `AsyncVerifyInboxes`, on a clock that
moves only when the client sleeps.

Each signature vector says whether `verify_signature` must accept a webhook signature.
"""
from __future__ import annotations

import inspect
import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest

import verifyinboxes
from conftest import PaperClock, async_client, sync_client
from fakeapi import FakeAPI, strict_equal
from verifyinboxes import (
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
    verify_signature,
)
from verifyinboxes._models import Model

DATA = json.loads((Path(__file__).parent / "conformance" / "scenarios.json").read_text("utf-8"))
SCENARIOS: List[Dict[str, Any]] = DATA["scenarios"]
VECTORS: List[Dict[str, Any]] = DATA["signatures"]

# The error kinds of the scenario file, and the class each one is here (exactly that class, not a subclass).
KINDS = {
    "invalid_request": InvalidRequestError,
    "authentication": AuthenticationError,
    "insufficient_credits": InsufficientCreditsError,
    "permission": PermissionDeniedError,
    "not_found": NotFoundError,
    "conflict": ConflictError,
    "rate_limit": RateLimitError,
    "server": ServerError,
    "api": APIError,
    "connection": APIConnectionError,
    "timeout": APITimeoutError,
    "wait_timeout": WaitTimeoutError,
    "missing_api_key": MissingAPIKeyError,
    "sdk": VerifyInboxesError,
}

VISIBLE_ASCII = re.compile(r"[!-~]{1,255}")


# --- running a call -----------------------------------------------------------------------------------------------


def _method(client: Any, op: str) -> Any:
    target = client
    for name in op.split("."):
        target = getattr(target, name)
    return target


def _wire(value: Any) -> Any:
    """A result as the wire names it: models as their JSON objects."""
    if isinstance(value, Model):
        return value.to_dict()
    if isinstance(value, list):
        return [_wire(item) for item in value]
    return value


def _download_args(args: Dict[str, Any], tmp_path: Path) -> Dict[str, Any]:
    destination = tmp_path / "results.csv"
    destination.write_text(args["before"], encoding="utf-8")
    rest = {k: v for k, v in args.items() if k not in ("id", "before")}
    return {"id": args["id"], "destination": str(destination), **rest}


def run_sync(client: Any, op: str, args: Dict[str, Any], tmp_path: Path) -> Any:
    if op == "lists.results":
        return {"rows": [row.email for row in client.lists.results(**args)]}
    if op == "lists.download":
        client.lists.download(**_download_args(args, tmp_path))
        return {"file": (tmp_path / "results.csv").read_text("utf-8")}
    return _wire(_method(client, op)(**args))


async def run_async(client: Any, op: str, args: Dict[str, Any], tmp_path: Path) -> Any:
    if op == "lists.results":
        return {"rows": [row.email async for row in client.lists.results(**args)]}
    if op == "lists.download":
        await client.lists.download(**_download_args(args, tmp_path))
        return {"file": (tmp_path / "results.csv").read_text("utf-8")}
    returned = _method(client, op)(**args)
    assert inspect.isawaitable(returned), f"{op} of the async client must be awaited"
    return _wire(await returned)


# --- checking what happened ---------------------------------------------------------------------------------------


def assert_subset(got: Any, want: Any, where: str = "returns") -> None:
    if isinstance(want, dict):
        assert isinstance(got, dict), f"{where}: {got!r} is not an object"
        for key, value in want.items():
            assert key in got, f"{where}: no {key!r} in {sorted(got)}"
            assert_subset(got[key], value, f"{where}.{key}")
    elif isinstance(want, list):
        assert isinstance(got, list), f"{where}: {got!r} is not an array"
        assert len(got) == len(want), f"{where}: {len(got)} elements, not {len(want)}"
        for index, (item, wanted) in enumerate(zip(got, want)):
            assert_subset(item, wanted, f"{where}[{index}]")
    else:
        assert strict_equal(got, want), f"{where}: {got!r}, not {want!r}"


def check_requests(scenario: Dict[str, Any], api: FakeAPI) -> None:
    exchanges = scenario["exchanges"]
    unexpected = [f"{r.method} {r.path}?{r.raw_query} {r.body[:200]!r}" for r in api.unexpected]
    assert not unexpected, f"requests the scenario does not have: {unexpected}"
    matched = api.matched
    key_of: List[Optional[str]] = []
    credential = (scenario.get("client") or {}).get("api_key", DATA["api_key"])
    for index, exchange in enumerate(exchanges):
        want = exchange["request"]
        where = f"exchange {index} ({want['method']} {want['path']})"
        got = matched[index]
        assert got is not None, f"{where} was never requested"
        assert got.method == want["method"], where
        assert got.path == want["path"], f"{where}: path {got.path}"
        if "query" in want:
            assert strict_equal(got.query, want["query"]), f"{where}: query {got.query}"
        else:
            assert got.raw_query == "", f"{where}: a query {got.raw_query!r}"
        if "json" in want:
            assert strict_equal(got.json, want["json"]), f"{where}: body {got.body[:500]!r}"
        elif "multipart" in want:
            assert strict_equal(got.multipart, want["multipart"]), f"{where}: parts {got.multipart}"
        else:
            assert got.body == b"", f"{where}: a body {got.body[:200]!r}"
        for name, value in (want.get("headers") or {}).items():
            assert got.headers.get(name) == value, f"{where}: {name}: {got.headers.get(name)!r}"
        assert got.headers.get("authorization") == f"Bearer {credential}", f"{where}: the credential"
        assert got.headers.get("user-agent", "").startswith(f"verifyinboxes-python/{verifyinboxes.__version__}"), where
        key = got.headers.get("idempotency-key")
        rule = want.get("idempotency_key")
        if rule is None:
            assert key is None, f"{where}: an Idempotency-Key"
        elif rule == "new":
            assert key is not None and VISIBLE_ASCII.fullmatch(key), f"{where}: Idempotency-Key {key!r}"
            assert key not in key_of, f"{where}: an Idempotency-Key used earlier"
        elif rule == "same":
            assert index > 0 and key is not None and key == key_of[index - 1], f"{where}: not the same Idempotency-Key"
        else:
            assert key == rule["equals"], f"{where}: Idempotency-Key {key!r}"
        key_of.append(key)


def check_sleeps(scenario: Dict[str, Any], clock: PaperClock) -> None:
    if "sleeps" not in scenario:
        return
    want = scenario["sleeps"]
    assert len(clock.slept) == len(want), f"slept {clock.slept}, not {want}"
    for got, wanted in zip(clock.slept, want):
        if isinstance(wanted, dict):
            assert wanted["min"] <= got <= wanted["max"], f"slept {clock.slept}, not {want}"
        else:
            assert got == pytest.approx(wanted), f"slept {clock.slept}, not {want}"


def check_raised(raises: Dict[str, Any], error: BaseException) -> None:
    kind = KINDS[raises["kind"]]
    assert type(error) is kind, f"raised {type(error).__name__}: {error}, not {kind.__name__}"
    if "status" in raises:
        assert getattr(error, "status") == raises["status"]
    if "code" in raises:
        assert getattr(error, "code") == raises["code"]
    if "request_id" in raises:
        assert getattr(error, "request_id") == raises["request_id"]
    if "retry_after" in raises:
        assert getattr(error, "retry_after") == raises["retry_after"]
    if "job_ids" in raises:
        assert getattr(error, "job_ids") == raises["job_ids"]
    if raises.get("message_has_request_id"):
        assert str(error).endswith(f"(request id {raises['request_id']})"), str(error)


def check_file(outcome: Dict[str, Any], scenario: Dict[str, Any], tmp_path: Path) -> None:
    if outcome.get("file") != "unchanged":
        return
    assert (tmp_path / "results.csv").read_text("utf-8") == scenario["call"]["args"]["before"]
    assert sorted(p.name for p in tmp_path.iterdir()) == ["results.csv"], "a part file was left behind"


# --- the tests ----------------------------------------------------------------------------------------------------


def _options(scenario: Dict[str, Any]) -> Dict[str, Any]:
    client = scenario.get("client") or {}
    options: Dict[str, Any] = {"api_key": client.get("api_key", DATA["api_key"])}
    if "max_retries" in client:
        options["max_retries"] = client["max_retries"]
    return options


@pytest.mark.parametrize("scenario", SCENARIOS, ids=[s["name"] for s in SCENARIOS])
def test_scenario(scenario: Dict[str, Any], clock: PaperClock, tmp_path: Path) -> None:
    call = scenario["call"]
    outcome = scenario["outcome"]
    with FakeAPI(scenario["exchanges"], unordered=bool(scenario.get("unordered"))) as api:
        with sync_client(api, clock, **_options(scenario)) as client:
            if "raises" in outcome:
                with pytest.raises(VerifyInboxesError) as raised:
                    run_sync(client, call["op"], dict(call.get("args") or {}), tmp_path)
                check_raised(outcome["raises"], raised.value)
                check_file(outcome, scenario, tmp_path)
            else:
                got = run_sync(client, call["op"], dict(call.get("args") or {}), tmp_path)
                assert_subset(got, outcome["returns"])
    check_requests(scenario, api)
    check_sleeps(scenario, clock)


@pytest.mark.anyio
@pytest.mark.parametrize("scenario", SCENARIOS, ids=[s["name"] for s in SCENARIOS])
async def test_scenario_async(scenario: Dict[str, Any], clock: PaperClock, tmp_path: Path) -> None:
    call = scenario["call"]
    outcome = scenario["outcome"]
    with FakeAPI(scenario["exchanges"], unordered=bool(scenario.get("unordered"))) as api:
        async with async_client(api, clock, **_options(scenario)) as client:
            if "raises" in outcome:
                with pytest.raises(VerifyInboxesError) as raised:
                    await run_async(client, call["op"], dict(call.get("args") or {}), tmp_path)
                check_raised(outcome["raises"], raised.value)
                check_file(outcome, scenario, tmp_path)
            else:
                got = await run_async(client, call["op"], dict(call.get("args") or {}), tmp_path)
                assert_subset(got, outcome["returns"])
    check_requests(scenario, api)
    check_sleeps(scenario, clock)


@pytest.mark.parametrize("vector", VECTORS, ids=[v["name"] for v in VECTORS])
def test_signature_vector(vector: Dict[str, Any]) -> None:
    valid = verify_signature(vector["body"], vector["header"], vector["secret"], tolerance=vector["tolerance"],
                             now=vector["now"])
    assert valid is vector["valid"]
    as_bytes = verify_signature(vector["body"].encode("utf-8"), vector["header"], vector["secret"],
                                tolerance=vector["tolerance"], now=vector["now"])
    assert as_bytes is vector["valid"]


def test_every_scenario_and_vector_is_run() -> None:
    assert len(SCENARIOS) >= 50 and len(VECTORS) >= 20
    assert len({s["name"] for s in SCENARIOS}) == len(SCENARIOS)


def test_the_runner_tells_true_from_1_and_1_from_1_0() -> None:
    from fakeapi import Recorded

    assert strict_equal({"a": [1, {"b": "x"}], "c": None}, {"a": [1, {"b": "x"}], "c": None})
    for got, want in ((1, True), (True, 1), (1, 1.0), (0, False), ({"a": [1]}, {"a": [True]}), ([1.0], [1]),
                      ({"a": 1}, {"a": 1, "b": 2}), ("1", 1)):
        assert not strict_equal(got, want), (got, want)
    sent = Recorded(method="POST", path="/v1/verify", raw_query="", query={}, headers={}, body=b"{}",
                    json={"email": "ann@example.com", "force_recheck": 1})
    assert not sent.same_request({"method": "POST", "path": "/v1/verify",
                                  "json": {"email": "ann@example.com", "force_recheck": True}})
