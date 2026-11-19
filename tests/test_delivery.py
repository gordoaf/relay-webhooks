import base64
import hashlib
import hmac
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import socket
import threading
import time
import pytest
from relay.delivery import deliver, sign_headers, verify_signature, validate_endpoint

SECRET = "whsec_" + base64.b64encode(b"x" * 32).decode()


def test_signature_matches_independent_hmac():
    body = b'{"amount":2400}'
    headers = sign_headers(SECRET, "evt_123", body, timestamp=1000)
    expected = base64.b64encode(
        hmac.new(b"x" * 32, b"evt_123.1000." + body, hashlib.sha256).digest()
    ).decode()
    assert headers["webhook-signature"] == "v1," + expected
    assert verify_signature(SECRET, body, headers, now=1001)
    assert not verify_signature(SECRET, b'{"amount":2401}', headers, now=1001)
    assert not verify_signature(SECRET, body, headers, now=1400)
    headers["webhook-signature"] = "v1,invalid v2,other " + headers["webhook-signature"]
    assert verify_signature(SECRET, body, headers, now=1001)


@pytest.mark.parametrize(
    "url",
    [
        "http://127.0.0.1",
        "http://169.254.169.254/latest/meta-data",
        "http://[::1]",
        "http://[::ffff:127.0.0.1]",
        "http://10.0.0.1",
        "ftp://example.com",
        "http://user:password@example.com",
        "http://example.com/#fragment",
        "http://example.com/\r\nHost: localhost",
        "http://example.com:0",
        "http://[fe80::1%25en0]",
        "http://example.com\\@localhost",
    ],
)
def test_unsafe_destinations_rejected(url):
    with pytest.raises(ValueError):
        validate_endpoint(url)


def test_all_dns_answers_are_checked(monkeypatch):
    monkeypatch.setattr(
        socket,
        "getaddrinfo",
        lambda *a, **kw: [
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 443)),
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", 443)),
        ],
    )
    with pytest.raises(ValueError, match="nonpublic"):
        validate_endpoint("https://example.com")


@pytest.fixture
def receiver():
    class Handler(BaseHTTPRequestHandler):
        calls = []

        def log_message(self, *_):
            pass

        def do_POST(self):
            self.calls.append(
                (self.path, self.headers, self.rfile.read(int(self.headers["Content-Length"])))
            )
            if self.path == "/redirect":
                self.send_response(302)
                self.send_header("Location", "/target")
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            if self.path == "/slow":
                self.send_response(200)
                self.send_header("Content-Length", "100")
                self.end_headers()
                try:
                    for _ in range(100):
                        self.wfile.write(b"x")
                        self.wfile.flush()
                        time.sleep(0.03)
                except OSError:
                    pass
                return
            body = b"x" * 10000 if self.path == "/large" else b"ok"
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_port}", Handler.calls
    server.shutdown()
    server.server_close()
    thread.join()


def test_real_http_delivery_and_redirects(receiver):
    url, calls = receiver
    result = deliver(
        url + "/ok", b"{}", sign_headers(SECRET, "evt_test", b"{}"), allow_private=True
    )
    assert result.status_code == 200
    assert verify_signature(SECRET, calls[0][2], dict(calls[0][1]))
    redirect = deliver(url + "/redirect", b"{}", {}, allow_private=True)
    assert redirect.status_code == 302
    assert len(calls) == 2


def test_response_size_and_absolute_deadline(receiver):
    url, _ = receiver
    result = deliver(url + "/large", b"{}", {}, allow_private=True)
    assert len(result.response_excerpt) == 4096
    started = time.monotonic()
    slow = deliver(url + "/slow", b"{}", {}, timeout=0.2, allow_private=True)
    assert time.monotonic() - started < 1
    assert slow.status_code is None and "deadline" in slow.error


def test_destination_is_pinned_after_dns(receiver, monkeypatch):
    url, calls = receiver
    port = int(url.rsplit(":", 1)[1])
    count = []

    def lookup(*args, **kw):
        count.append(args)
        if len(count) > 1:
            raise AssertionError("Second lookup could permit DNS rebinding")
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", port))]

    monkeypatch.setattr(socket, "getaddrinfo", lookup)
    result = deliver(f"http://test.example:{port}/ok", b"{}", {}, allow_private=True)
    assert result.status_code == 200 and len(count) == 1
    assert calls[-1][1]["Host"] == f"test.example:{port}"
