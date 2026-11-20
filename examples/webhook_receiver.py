"""Receive VerifyInboxes webhooks: check the signature before anything else, then read the event.

    export VERIFYINBOXES_WEBHOOK_SECRET="..."      # the secret webhooks.create returned
    python examples/webhook_receiver.py 8000

Put it behind HTTPS (the API calls only https:// addresses), and register its address with
client.webhooks.create("https://example.com/webhooks/verifyinboxes").
"""
from __future__ import annotations

import json
import os
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer

from verifyinboxes import SIGNATURE_HEADER, verify_signature

SECRET = os.environ.get("VERIFYINBOXES_WEBHOOK_SECRET", "")


class Receiver(BaseHTTPRequestHandler):
    def do_POST(self) -> None:
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length)  # the raw bytes, exactly as they came
        if not verify_signature(body, self.headers.get(SIGNATURE_HEADER, ""), SECRET):
            self.send_response(400)
            self.end_headers()
            return
        event = json.loads(body)
        if event.get("event") == "job.completed":
            print(f"list {event['job_id']} is {event['status']}: {event.get('counts')}")
        elif event.get("event") == "address.contradicted":
            print(f"{event['email']}: {event['message']}")
        # Deliveries come at least once: X-Boxverifier-Delivery is the same on every retry of one.
        self.send_response(204)
        self.end_headers()


def main() -> int:
    if not SECRET:
        print("set VERIFYINBOXES_WEBHOOK_SECRET to the webhook's secret", file=sys.stderr)
        return 2
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8000
    HTTPServer(("", port), Receiver).serve_forever()
    return 0


if __name__ == "__main__":
    sys.exit(main())
