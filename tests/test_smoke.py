"""A smoke test against the real API, run weekly by `.github/workflows/smoke.yml`.

Skipped unless `VERIFYINBOXES_SMOKE_API_KEY` holds a key (it spends a few credits). `VERIFYINBOXES_BASE_URL` points
it elsewhere than production.
"""
from __future__ import annotations

import os

import pytest

from verifyinboxes import VerifyInboxes

KEY = os.environ.get("VERIFYINBOXES_SMOKE_API_KEY", "")

pytestmark = pytest.mark.skipif(not KEY, reason="VERIFYINBOXES_SMOKE_API_KEY is not set")

ADDRESSES = ["verify@verifyinboxes.com", "support@verifyinboxes.com"]


def test_against_the_real_api() -> None:
    with VerifyInboxes(KEY) as client:
        account = client.account.get()
        assert account.name

        result = client.verify("verify@verifyinboxes.com")
        assert result.verdict != "invalid", result
        assert result.job_id

        created = client.lists.create(emails=ADDRESSES)
        job = client.lists.wait(created.id, timeout=15 * 60)
        assert job.status == "done"
        rows = list(client.lists.results(created.id))
        assert sorted(row.email for row in rows) == sorted(ADDRESSES)
