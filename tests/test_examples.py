"""The examples, run against the fake API: what they print is what a reader copies."""
from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType

import pytest

from conftest import KEY, REQUEST_ID, Serve, answer, error, exchange, result_json, single_job_id

EXAMPLES = Path(__file__).resolve().parent.parent / "examples"


def load(name: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, EXAMPLES / f"{name}.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_quickstart_checks_each_address_once_and_names_the_jobs_an_error_leaves(
        serve: Serve, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    api = serve([
        exchange("POST", "/v1/verify", answer(json={**result_json("ann@example.com"), "job_id": single_job_id(1)}),
                 json={"email": "ann@example.com"}),
        exchange("POST", "/v1/verify", answer(json={**result_json("bob@example.com"), "job_id": single_job_id(2)}),
                 json={"email": "bob@example.com"}),
        exchange("POST", "/v1/verify", answer(402, error("insufficient_credits", "the balance is 0")),
                 json={"email": "cat@example.com"}),
    ], unordered=True)
    monkeypatch.setenv("VERIFYINBOXES_API_KEY", KEY)
    monkeypatch.setenv("VERIFYINBOXES_BASE_URL", api.url)

    code = load("quickstart").main(["ann@example.com", "bob@example.com", "cat@example.com"])

    assert code == 1
    # ann is checked once (by verify), not again by verify_many.
    assert [r.json["email"] for r in api.requests] == ["ann@example.com", "bob@example.com", "cat@example.com"]
    out, err = capsys.readouterr()
    assert out.startswith("ann@example.com: invalid")
    assert f"402 insufficient_credits: the balance is 0 (request id {REQUEST_ID})" in err
    # bob's check carries on, its credit held: the error names it.
    assert f"jobs made: {single_job_id(2)}" in err
