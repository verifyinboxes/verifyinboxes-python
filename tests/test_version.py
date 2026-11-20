from __future__ import annotations

import re
from pathlib import Path

import verifyinboxes
from conftest import Serve, answer, exchange, sync_client


def test_version_is_the_packages() -> None:
    pyproject = (Path(__file__).parent.parent / "pyproject.toml").read_text("utf-8")
    found = re.search(r'^version = "([^"]+)"$', pyproject, re.MULTILINE)
    assert found is not None
    assert verifyinboxes.__version__ == "0.1.0" == found.group(1)


def test_user_agent_names_the_library_and_its_version(serve: Serve) -> None:
    account = {"name": "acme", "balance": 1, "reviewed": False, "test_emails": False, "safe_to_send_threshold": 0.8}
    api = serve([exchange("GET", "/v1/account", answer(json=account))])
    with sync_client(api) as client:
        client.account.get()
    assert api.requests[0].headers["user-agent"] == "verifyinboxes-python/0.1.0"
