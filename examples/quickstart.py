"""Check one address, then several at once.

    export VERIFYINBOXES_API_KEY="bv_..."
    python examples/quickstart.py ann@example.com bob@example.com
"""
from __future__ import annotations

import sys
from typing import List

from verifyinboxes import VerifyInboxes, VerifyInboxesError


def main(addresses: List[str]) -> int:
    if not addresses:
        print("usage: python examples/quickstart.py ADDRESS [ADDRESS ...]", file=sys.stderr)
        return 2
    first, rest = addresses[0], addresses[1:]
    try:
        with VerifyInboxes() as client:  # reads VERIFYINBOXES_API_KEY
            result = client.verify(first)
            print(f"{result.email}: {result.verdict} ({result.reason}), safe to send: {result.safe_to_send}")

            # Several addresses: verify_many, never a loop of verify. Up to 10 are checked together; more go as one list.
            if rest:
                for each in client.verify_many(rest):
                    print(f"{each.email}: {each.verdict}")
    except VerifyInboxesError as error:
        print(error, file=sys.stderr)  # an API error's message ends with its request id
        if error.job_ids:
            # Each job carries on and its credits are held: wait for it again with client.checks.wait(job_id)
            # (a list's with client.lists.wait).
            print(f"jobs made: {', '.join(error.job_ids)}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
