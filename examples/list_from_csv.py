"""Verify a CSV of addresses as a list: start it, wait for it, read the results, download the CSV with them.

    export VERIFYINBOXES_API_KEY="bv_..."
    python examples/list_from_csv.py prospects.csv email
"""
from __future__ import annotations

import sys
from collections import Counter
from typing import List

from verifyinboxes import VerifyInboxes


def main(args: List[str]) -> int:
    if not args:
        print("usage: python examples/list_from_csv.py FILE.csv [EMAIL_COLUMN]", file=sys.stderr)
        return 2
    path = args[0]
    column = args[1] if len(args) > 1 else "email"

    with VerifyInboxes() as client:
        created = client.lists.create(file=path, email_column=column)
        print(f"list {created.id}: {created.total} addresses")

        # A few hundred addresses take minutes, Microsoft-hosted ones longer. If this stops, the list carries on:
        # wait for it again with client.lists.wait(created.id).
        job = client.lists.wait(created.id)
        print(f"{job.status}: {job.counts.to_dict() if job.counts else {}}")

        verdicts: Counter[str] = Counter()
        safe: List[str] = []
        for row in client.lists.results(created.id):
            verdicts[row.verdict] += 1
            if row.safe_to_send:
                safe.append(row.email)
        print(f"{len(safe)} safe to send; by verdict: {dict(verdicts)}")

        written = client.lists.download(created.id, "results.csv")
        print(f"saved results.csv ({written} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
