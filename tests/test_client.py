import base64
import json
import socket
import threading
import time
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from relay.client import RelayClient, RelayError, verify_signature
from relay.delivery import sign_headers


@contextmanager
def receiver(callback):
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            raw = self.rfile.read(int(self.headers["Content-Length"]))
            callback(self, raw)

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_client_retries_ambiguous_commit_with_identical_request(monkeypatch):
    requests = []

    def accept(handler, body):
        requests.append((body, handler.headers["Idempotency-Key"], handler.path))
        if len(requests) == 1:
            # Relay may commit and then lose the connection before confirming it.
            handler.connection.shutdown(socket.SHUT_RDWR)
            handler.connection.close()
            return
        data = json.dumps({"created": False, "event": {"id": "evt_saved"}}).encode()
        handler.send_response(200)
        handler.send_header("Content-Length", str(len(data)))
        handler.end_headers()
        handler.wfile.write(data)

    monkeypatch.setattr("relay.client.time.sleep", lambda _: None)
    with receiver(accept) as url:
        receipt = RelayClient(url, "project_key").emit(
            "order.created",
            {"order_id": "order_1"},
            idempotency_key="order_1:created",
        )
    assert receipt == {"created": False, "event": {"id": "evt_saved"}}
    assert len(requests) == 2
    assert requests[0] == requests[1]
    assert requests[0][1:] == ("order_1:created", "/v1/events")


def test_client_deadline_stops_slow_response_headers():
    def drip_headers(handler, body):
        try:
            # Each byte arrives inside the socket timeout; only an absolute
            # response deadline can terminate this response promptly.
            for byte in b"HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\n\r\n":
                handler.connection.sendall(bytes([byte]))
                time.sleep(0.025)
        except OSError:
            pass

    with receiver(drip_headers) as url:
        started = time.monotonic()
        with pytest.raises(RelayError) as caught:
            RelayClient(url, "project_key", timeout=0.12, max_retries=0).emit(
                "order.created",
                {},
                idempotency_key="stable",
            )
        assert time.monotonic() - started < 1
    assert caught.value.retryable


@pytest.mark.parametrize("status", [400, 401, 403, 409, 422, 307])
def test_client_does_not_retry_permanent_errors_or_follow_redirects(status, monkeypatch):
    client = RelayClient("http://127.0.0.1:8787", "project_key")
    calls = []
    monkeypatch.setattr(
        client, "_request", lambda *args: calls.append(args) or (status, b"{}", None)
    )
    with pytest.raises(RelayError) as caught:
        client.emit("order.created", {}, idempotency_key="one")
    assert caught.value.status == status
    assert caught.value.idempotency_key == "one"
    assert not caught.value.retryable
    assert len(calls) == 1
    assert "project_key" not in str(caught.value)


@pytest.mark.parametrize("status", [408, 429, 500, 503])
def test_client_retries_transient_status_with_bounded_retry_after(status, monkeypatch):
    client = RelayClient("http://127.0.0.1:8787", "project_key", max_retries=1)
    calls, delays = [], []
    monkeypatch.setattr(
        client, "_request", lambda *args: calls.append(args) or (status, b"{}", "9" * 10000)
    )
    monkeypatch.setattr("relay.client.time.sleep", delays.append)
    with pytest.raises(RelayError) as caught:
        client.emit("order.created", {}, idempotency_key="one")
    assert caught.value.retryable
    assert calls[0] == calls[1]
    assert delays == [5]


def test_client_invalid_success_receipt_is_ambiguous(monkeypatch):
    client = RelayClient("http://127.0.0.1:8787", "project_key", max_retries=0)
    monkeypatch.setattr(client, "_request", lambda *_: (202, b'{"created":true}', None))
    with pytest.raises(RelayError) as caught:
        client.emit("order.created", {}, idempotency_key="stable")
    assert caught.value.retryable
    assert caught.value.status == 202
    assert caught.value.idempotency_key == "stable"


@pytest.mark.parametrize(
    "payload", [{"amount": float("nan")}, {"a": object()}, {"a": "\ud800"}, []]
)
def test_client_rejects_invalid_payload_before_network(payload, monkeypatch):
    client = RelayClient("http://127.0.0.1:8787", "project_key")
    monkeypatch.setattr(client, "_request", lambda *_: pytest.fail("Unexpected network request"))
    with pytest.raises((ValueError, TypeError)):
        client.emit("order.created", payload, idempotency_key="one")


@pytest.mark.parametrize("key", ["", "space key", "a\nkey", "a" * 201])
def test_client_requires_valid_explicit_idempotency_key(key):
    with pytest.raises(ValueError):
        RelayClient("http://127.0.0.1:8787", "project_key").emit(
            "order.created", {}, idempotency_key=key
        )


def test_exported_verifier_checks_exact_body_timestamp_and_rotation():
    secret = "whsec_" + base64.b64encode(b"x" * 32).decode()
    next_secret = "whsec_" + base64.b64encode(b"y" * 32).decode()
    body = b'{"id":"evt_1","data":{"count":1}}'
    headers = sign_headers(secret, "evt_1", body, timestamp=1000)
    headers["webhook-signature"] += (
        " " + sign_headers(next_secret, "evt_1", body, timestamp=1000)["webhook-signature"]
    )
    assert verify_signature(secret, body, headers, now=1000)
    assert verify_signature(next_secret, body, headers, now=1000)
    assert not verify_signature(secret, body + b" ", headers, now=1000)
    assert not verify_signature(secret, body, headers, now=1301)
    assert not verify_signature(secret, body, headers, now=699)
