"""A scripted fake of the VerifyInboxes API on 127.0.0.1, for the tests.

It serves the exchanges of one conformance scenario (the format is described in `tests/test_conformance.py`): in order, or,
when `unordered`, each request matched to the first unused exchange with the same method, path and body. It records
every request, and can drop a connection (`"drop": true`) or cut an answer short (`"cut_after": <bytes>`).
"""
from __future__ import annotations

import json
import re
import threading
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs

__all__ = ["FakeAPI", "Recorded", "parse_multipart", "strict_equal"]

# What the server answers to a request the scenario does not have: a status no API gives.
UNEXPECTED_STATUS = 599


def strict_equal(got: Any, want: Any) -> bool:
    """JSON equality that tells `true` from `1` and `1` from `1.0` (Python's `==` does not)."""
    if type(got) is not type(want):
        return False
    if isinstance(got, dict):
        return got.keys() == want.keys() and all(strict_equal(got[key], want[key]) for key in got)
    if isinstance(got, list):
        return len(got) == len(want) and all(strict_equal(a, b) for a, b in zip(got, want))
    return bool(got == want)


@dataclass
class Recorded:
    """One request as the fake server read it."""

    method: str
    path: str
    """The path as it was sent, still escaped, without the query."""
    raw_query: str
    query: Dict[str, Any]
    """The decoded query: a string per name, or a list when a name came more than once."""
    headers: Dict[str, str]
    """Names in lower case."""
    body: bytes
    json: Any = None
    multipart: Optional[List[Dict[str, str]]] = None

    def same_request(self, want: Dict[str, Any]) -> bool:
        """Whether this is the request `want` describes: its method, path and body (how `unordered` matches)."""
        if self.method != want["method"] or self.path != want["path"]:
            return False
        if "json" in want:
            return strict_equal(self.json, want["json"])
        if "multipart" in want:
            return strict_equal(self.multipart, want["multipart"])
        return self.body == b""


@dataclass
class _State:
    exchanges: List[Dict[str, Any]]
    unordered: bool
    hold_until: int
    requests: List[Recorded] = field(default_factory=list)
    matched: List[Optional[Recorded]] = field(default_factory=list)
    unexpected: List[Recorded] = field(default_factory=list)
    in_flight: int = 0
    max_in_flight: int = 0
    held_out: bool = False
    lock: threading.Condition = field(default_factory=threading.Condition)


class FakeAPI:
    """`with FakeAPI(exchanges) as api:` serves them on `api.url` until the block ends.

    `hold_until=n` holds every answer until `n` requests are in flight at once (or 10 seconds pass), to show that a
    client sends them together.
    """

    def __init__(self, exchanges: List[Dict[str, Any]], *, unordered: bool = False, hold_until: int = 0) -> None:
        self._state = _State(exchanges=exchanges, unordered=unordered, hold_until=hold_until)
        self._state.matched = [None] * len(exchanges)
        state = self._state

        class Handler(_Handler):
            pass

        Handler.state = state
        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self._server.daemon_threads = True
        self._thread = threading.Thread(target=self._server.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True)

    @property
    def url(self) -> str:
        host, port = self._server.server_address[:2]
        return f"http://{host!s}:{port}"

    @property
    def requests(self) -> List[Recorded]:
        """Every request, in the order it arrived."""
        return list(self._state.requests)

    @property
    def matched(self) -> List[Optional[Recorded]]:
        """The request each exchange was used for (`None`: never)."""
        return list(self._state.matched)

    @property
    def unexpected(self) -> List[Recorded]:
        """The requests no exchange was left for; each was answered 599."""
        return list(self._state.unexpected)

    @property
    def max_in_flight(self) -> int:
        return self._state.max_in_flight

    @property
    def held_out(self) -> bool:
        """Whether `hold_until` gave up waiting (fewer requests came at once than it asked for)."""
        return self._state.held_out

    def __enter__(self) -> "FakeAPI":
        self._thread.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5)


def parse_multipart(body: bytes, content_type: str) -> Optional[List[Dict[str, str]]]:
    """The parts of a `multipart/form-data` body, in order: `{name, value}`, or `{name, filename, content_type, value}`
    for a file. `None` when the body is not multipart."""
    found = re.search(r'boundary="?([^";]+)"?', content_type)
    if not content_type.lower().startswith("multipart/form-data") or found is None:
        return None
    boundary = b"--" + found.group(1).encode("ascii")
    parts: List[Dict[str, str]] = []
    for segment in body.split(boundary)[1:]:
        if segment.startswith(b"--"):
            break
        if segment.startswith(b"\r\n"):
            segment = segment[2:]
        head, _, content = segment.partition(b"\r\n\r\n")
        if content.endswith(b"\r\n"):
            content = content[:-2]
        headers: Dict[str, str] = {}
        for line in head.decode("utf-8").split("\r\n"):
            name, _, value = line.partition(":")
            headers[name.strip().lower()] = value.strip()
        disposition = headers.get("content-disposition", "")
        part_name = re.search(r'\bname="([^"]*)"', disposition)
        filename = re.search(r'\bfilename="([^"]*)"', disposition)
        part: Dict[str, str] = {"name": part_name.group(1) if part_name else ""}
        if filename is not None:
            part["filename"] = filename.group(1)
            part["content_type"] = headers.get("content-type", "")
        part["value"] = content.decode("utf-8")
        parts.append(part)
    return parts


class _Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    state: _State

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A002 - the name is BaseHTTPRequestHandler's
        pass

    def do_GET(self) -> None:
        self._serve()

    def do_POST(self) -> None:
        self._serve()

    def do_DELETE(self) -> None:
        self._serve()

    def do_PUT(self) -> None:
        self._serve()

    def do_PATCH(self) -> None:
        self._serve()

    # --- reading ------------------------------------------------------------------------------------------------------

    def _read_body(self) -> bytes:
        if "chunked" in self.headers.get("Transfer-Encoding", "").lower():
            chunks = []
            while True:
                size = int(self.rfile.readline().split(b";")[0].strip() or b"0", 16)
                if size == 0:
                    while self.rfile.readline() not in (b"\r\n", b"\n", b""):
                        pass
                    break
                chunks.append(self.rfile.read(size))
                self.rfile.readline()
            return b"".join(chunks)
        length = int(self.headers.get("Content-Length") or 0)
        return self.rfile.read(length) if length else b""

    def _record(self) -> Recorded:
        path, _, raw_query = self.path.partition("?")
        query: Dict[str, Any] = {}
        for name, values in parse_qs(raw_query, keep_blank_values=True).items():
            query[name] = values[0] if len(values) == 1 else values
        headers = {name.lower(): value for name, value in self.headers.items()}
        body = self._read_body()
        content_type = headers.get("content-type", "")
        parsed: Any = None
        if body and content_type.lower().startswith("application/json"):
            try:
                parsed = json.loads(body)
            except ValueError:
                parsed = None
        return Recorded(
            method=self.command, path=path, raw_query=raw_query, query=query, headers=headers, body=body,
            json=parsed, multipart=parse_multipart(body, content_type),
        )

    def _take(self, request: Recorded) -> Optional[Dict[str, Any]]:
        state = self.state
        with state.lock:
            state.requests.append(request)
            index: Optional[int] = None
            if state.unordered:
                for i, exchange in enumerate(state.exchanges):
                    if state.matched[i] is None and request.same_request(exchange["request"]):
                        index = i
                        break
            else:
                used = sum(1 for m in state.matched if m is not None)
                if used < len(state.exchanges):
                    index = used
            if index is None:
                state.unexpected.append(request)
                return None
            state.matched[index] = request
            return state.exchanges[index]

    def _hold(self) -> None:
        state = self.state
        with state.lock:
            state.in_flight += 1
            state.max_in_flight = max(state.max_in_flight, state.in_flight)
            state.lock.notify_all()
            if state.hold_until and not state.held_out:
                if not state.lock.wait_for(lambda: state.max_in_flight >= state.hold_until, timeout=10):
                    state.held_out = True

    def _done(self) -> None:
        with self.state.lock:
            self.state.in_flight -= 1

    # --- answering ----------------------------------------------------------------------------------------------------

    def _serve(self) -> None:
        request = self._record()
        exchange = self._take(request)
        self._hold()
        try:
            if exchange is None:
                self._answer(UNEXPECTED_STATUS, {"content-type": "application/json"},
                             json.dumps({"error": {"code": "unexpected_request", "message": "not in the scenario"}}).encode())
                return
            response: Dict[str, Any] = exchange["response"]
            if response.get("drop"):
                # The request was read whole; the connection closes with no answer.
                self.close_connection = True
                return
            headers = dict(response.get("headers") or {})
            if "json" in response:
                data = json.dumps(response["json"]).encode("utf-8")
                headers.setdefault("content-type", "application/json; charset=utf-8")
            elif "text" in response:
                data = str(response["text"]).encode("utf-8")
            else:
                data = b""
            if "cut_after" in response:
                self.send_response(int(response["status"]))
                for name, value in headers.items():
                    self.send_header(name, value)
                self.send_header("content-length", str(len(data)))
                self.end_headers()
                self.wfile.write(data[: int(response["cut_after"])])
                self.wfile.flush()
                self.close_connection = True
                return
            self._answer(int(response["status"]), headers, data)
        finally:
            self._done()

    def _answer(self, status: int, headers: Dict[str, str], data: bytes) -> None:
        self.send_response(status)
        for name, value in headers.items():
            self.send_header(name, value)
        if status != 204 and status != 304:
            self.send_header("content-length", str(len(data)))
        self.end_headers()
        if data and status != 204:
            self.wfile.write(data)
