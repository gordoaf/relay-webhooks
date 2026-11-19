import base64
import http.client
import json
import socket
import ssl
import threading
import time
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from relay.client import RelayClient, RelayError, RelayEventExpired, verify_signature
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


def test_client_retains_expired_acceptance_id_without_retry():
    calls = []

    def expired(handler, body):
        calls.append(handler.headers["Idempotency-Key"])
        data = b'{"event_id":"evt_original","detail":"Previously accepted"}'
        handler.send_response(410)
        handler.send_header("Content-Length", str(len(data)))
        handler.end_headers()
        handler.wfile.write(data)

    with receiver(expired) as url, pytest.raises(RelayEventExpired) as caught:
        RelayClient(url, "project_key").emit("order.created", {}, idempotency_key="stable")
    error = caught.value
    assert error.accepted and error.event_id == "evt_original"
    assert error.status == 410 and not error.retryable
    assert error.idempotency_key == "stable" and calls == ["stable"]
    assert "do not resubmit" in str(error)
    assert "delivered" not in str(error)


@pytest.mark.parametrize("body", [b"{}", b'{"event_id":null}', b'{"event_id":""}', b"not-json"])
def test_client_malformed_expired_receipt_does_not_invent_acceptance(body, monkeypatch):
    client = RelayClient("http://localhost", "project_key")
    monkeypatch.setattr(client, "_request", lambda *_: (410, body, None))
    with pytest.raises(RelayError) as caught:
        client.emit("order.created", {}, idempotency_key="stable")
    assert not isinstance(caught.value, RelayEventExpired)
    assert "retain the original" in str(caught.value)
    assert not caught.value.retryable


def test_client_certificate_failure_is_not_retried_and_preserves_cause(monkeypatch):
    client = RelayClient("https://relay.example", "project_key")
    original = ssl.SSLCertVerificationError(1, "certificate verify failed")
    calls = []

    def fail(*_):
        calls.append(1)
        raise original

    monkeypatch.setattr(client, "_request", fail)
    with pytest.raises(RelayError) as caught:
        client.emit("order.created", {}, idempotency_key="stable")
    assert calls == [1]
    assert not caught.value.retryable
    assert caught.value.cause is original and caught.value.__cause__ is original
    assert "certificate verification failed" in str(caught.value)


@pytest.mark.parametrize(
    "url,host,port",
    [
        ("http://[::1]", "::1", 80),
        ("https://[2001:db8::2]", "2001:db8::2", 443),
        ("http://[::1]:8787", "::1", 8787),
        ("https://münich.example", "xn--mnich-kva.example", 443),
        ("http://relay_api:8787", "relay_api", 8787),
        ("https://RELAY_API.internal.", "relay_api.internal.", 443),
        ("http://_relay_.internal", "_relay_.internal", 80),
        ("http://" + "_" * 63, "_" * 63, 80),
        (
            "http://" + ".".join(["a" * 63] * 3 + ["b" * 61]),
            ".".join(["a" * 63] * 3 + ["b" * 61]),
            80,
        ),
    ],
)
def test_client_normalizes_hostname_and_default_ports(url, host, port):
    client = RelayClient(url, "project_key")
    connection = http.client.HTTPConnection(client._host, client._port)
    assert (connection.host, connection.port) == (host, port)


def test_client_reaches_underscore_service_name_with_original_host_header(monkeypatch):
    hosts = []

    def accept(handler, body):
        hosts.append(handler.headers["Host"])
        data = b'{"created":true,"event":{"id":"evt_private_dns"}}'
        handler.send_response(202)
        handler.send_header("Content-Length", str(len(data)))
        handler.end_headers()
        handler.wfile.write(data)

    resolve = socket.getaddrinfo

    def private_dns(host, port, *args, **kwargs):
        assert host == "relay_api"
        return resolve("127.0.0.1", port, *args, **kwargs)

    with receiver(accept) as url:
        monkeypatch.setattr(socket, "getaddrinfo", private_dns)
        receipt = RelayClient(url.replace("127.0.0.1", "relay_api"), "project_key").emit(
            "order.created", {}, idempotency_key="private_dns"
        )
        assert hosts == [url.removeprefix("http://").replace("127.0.0.1", "relay_api")]
        assert receipt["event"]["id"] == "evt_private_dns"


@pytest.mark.parametrize(
    "url",
    [
        "http://" + "a" * 64 + ".example",
        "http://" + "_" * 64,
        "http://" + ".".join(["a" * 63] * 3 + ["b" * 62]),
        "http://bad\ud800.example",
        "http://relay_api..internal",
        "http://-relay_api.internal",
        "http://relay_api-.internal",
        "http://relay_api%2Finternal",
        "http://relay_api\\internal",
        "http://project_key@relay_api",
        "http://relay_api/path",
        "http://relay_api?token=value",
        "http://relay_api#fragment",
        "http://relay_api\x00.internal",
        "http://relay_api\x7f.internal",
        "http://[fe80::1%25eth0]",
        "http://example.com:0",
        "http://example.com:65536",
        "http://good.example\n.evil.example",
    ],
)
def test_client_invalid_hostname_fails_at_configuration(url):
    with pytest.raises(ValueError):
        RelayClient(url, "project_key")


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
