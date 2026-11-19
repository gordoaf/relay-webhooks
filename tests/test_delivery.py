import base64
import hashlib
import hmac
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import ipaddress
import socket
import threading
import time
import pytest
from relay import delivery as delivery_module
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


def test_signature_verifier_rejects_oversized_timestamp_without_raising():
    headers = sign_headers(SECRET, "evt_test", b"{}", timestamp=1000)
    headers["webhook-timestamp"] = "9" * 400
    assert not verify_signature(SECRET, b"{}", headers, now=1000)


@pytest.mark.parametrize(
    "options",
    [
        {"now": float("nan")},
        {"now": float("inf")},
        {"now": 10**400},
        {"tolerance": float("nan")},
        {"tolerance": float("inf")},
        {"tolerance": 10**400},
        {"tolerance": True},
    ],
)
def test_signature_verifier_rejects_invalid_time_configuration(options):
    headers = sign_headers(SECRET, "evt_test", b"{}", timestamp=1000)
    assert not verify_signature(SECRET, b"{}", headers, **options)


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
            if self.path == "/stall":
                time.sleep(0.5)
                return
            if self.path.startswith("/retry-after-"):
                self.send_response(429)
                value = "9" * 5000 if self.path.endswith("huge") else "0" * 5000 + "12"
                self.send_header("Retry-After", value)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            if self.path == "/bad-chunks":
                self.send_response(503)
                self.send_header("Transfer-Encoding", "chunked")
                self.end_headers()
                self.wfile.write(b"not-a-chunk-size\r\n")
                self.close_connection = True
                return
            if self.path == "/slow-error":
                self.send_response(503)
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
            self.send_response(503 if self.path == "/large" else 200)
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
    slow = deliver(url + "/stall", b"{}", {}, timeout=0.1, allow_private=True)
    assert time.monotonic() - started < 1
    assert slow.status_code is None and "deadline" in slow.error
    assert slow.timed_out and not slow.permanent


def test_successful_headers_do_not_wait_for_or_retry_partial_body(receiver):
    url, _ = receiver
    started = time.monotonic()
    result = deliver(url + "/slow", b"{}", {}, timeout=0.2, allow_private=True)
    assert result.status_code == 200 and result.error is None
    assert result.response_excerpt == ""
    assert not result.timed_out
    assert time.monotonic() - started < 1


@pytest.mark.parametrize("path, delay", [("huge", 3600), ("zeroes", 12)])
def test_huge_retry_after_header_keeps_http_status(receiver, path, delay):
    url, _ = receiver
    result = deliver(url + "/retry-after-" + path, b"{}", {}, allow_private=True)
    assert result.status_code == 429
    assert result.retry_after_seconds == delay
    assert not result.permanent and not result.timed_out


@pytest.mark.parametrize("path", ["/bad-chunks", "/slow-error"])
def test_diagnostic_body_errors_preserve_received_status(receiver, path):
    url, _ = receiver
    started = time.monotonic()
    result = deliver(url + path, b"{}", {}, allow_private=True)
    assert time.monotonic() - started < 1
    assert result.status_code == 503 and result.error == "HTTP 503"
    assert not result.timed_out


def test_tls_handshake_has_shorter_connect_budget(monkeypatch):
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen()
    listener.settimeout(2)
    release = threading.Event()

    def accept_without_tls():
        with listener.accept()[0]:
            release.wait(2)

    thread = threading.Thread(target=accept_without_tls, daemon=True)
    thread.start()
    monkeypatch.setattr(delivery_module, "_CONNECT_SECONDS", 0.1)
    try:
        started = time.monotonic()
        result = deliver(
            f"https://127.0.0.1:{listener.getsockname()[1]}/",
            b"{}",
            {},
            timeout=2,
            allow_private=True,
        )
        assert time.monotonic() - started < 1
        assert result.status_code is None and result.timed_out
    finally:
        release.set()
        listener.close()
        thread.join(2)


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


@pytest.mark.parametrize(
    "address",
    [
        "fec0::1",
        "feff:ffff::1",
        "::ffff:127.0.0.1",
        "::127.0.0.1",
        "64:ff9b::7f00:1",
        "64:ff9b:1::a00:1",
        "2002:a00:1::1",
        "2001::1",
        "2001:db8::1",
        "3fff::1",
        "192.0.0.8",
        "192.88.99.1",
    ],
)
def test_special_ranges_rejected_independently_of_python_classifications(address, monkeypatch):
    parsed = ipaddress.ip_address(address)
    cls = type(parsed)
    monkeypatch.setattr(cls, "is_global", property(lambda _: True))
    monkeypatch.setattr(cls, "is_reserved", property(lambda _: False))
    family = socket.AF_INET6 if parsed.version == 6 else socket.AF_INET
    sockaddr = (address, 443, 0, 0) if parsed.version == 6 else (address, 443)
    monkeypatch.setattr(
        socket,
        "getaddrinfo",
        lambda *a, **kw: [
            (family, socket.SOCK_STREAM, 6, "", sockaddr),
        ],
    )
    result = deliver("https://unsafe.example/", b"{}", {})
    assert result.status_code is None and result.permanent
    assert "nonpublic" in result.error
    assert not result.timed_out
    validate_endpoint("https://unsafe.example/", allow_private=True)


@pytest.mark.parametrize("address", ["8.8.8.8", "2001:4860:4860::8888"])
def test_public_addresses_still_allowed(address, monkeypatch):
    parsed = ipaddress.ip_address(address)
    family = socket.AF_INET6 if parsed.version == 6 else socket.AF_INET
    sockaddr = (address, 443, 0, 0) if parsed.version == 6 else (address, 443)
    monkeypatch.setattr(
        socket,
        "getaddrinfo",
        lambda *a, **kw: [
            (family, socket.SOCK_STREAM, 6, "", sockaddr),
        ],
    )
    validate_endpoint("https://public.example/")


@pytest.fixture
def isolated_dns(monkeypatch):
    monkeypatch.setattr(delivery_module, "_DNS_SLOTS", threading.BoundedSemaphore(8))
    monkeypatch.setattr(delivery_module, "_DNS_LOCK", threading.Lock())
    monkeypatch.setattr(
        delivery_module, "_DNS_WAKE", threading.Condition(delivery_module._DNS_LOCK)
    )
    monkeypatch.setattr(delivery_module, "_DNS_INFLIGHT", {})
    monkeypatch.setattr(delivery_module, "_DNS_HOSTS", {})
    monkeypatch.setattr(delivery_module, "_DNS_PROJECT_COUNTS", {})
    monkeypatch.setattr(delivery_module, "_DNS_FAILURES", OrderedDict())
    monkeypatch.setattr(delivery_module, "_DNS_ADMISSION", OrderedDict())
    yield
    deadline = time.monotonic() + 2
    while delivery_module._DNS_INFLIGHT and time.monotonic() < deadline:
        time.sleep(0.01)
    assert not delivery_module._DNS_INFLIGHT
    assert not delivery_module._DNS_HOSTS
    assert not delivery_module._DNS_PROJECT_COUNTS


def test_dns_coalesces_waiters_without_delaying_healthy_host(monkeypatch, isolated_dns):
    entered, release = threading.Event(), threading.Event()
    calls = []

    def lookup(host, port, **_):
        calls.append((host, port))
        if host == "slow.example":
            entered.set()
            assert release.wait(2)
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("8.8.8.8", port))]

    monkeypatch.setattr(socket, "getaddrinfo", lookup)
    with ThreadPoolExecutor(max_workers=3) as pool:
        first = pool.submit(delivery_module._resolve, "SLOW.example.", 443, time.monotonic() + 2)
        assert entered.wait(1)
        state = delivery_module._DNS_INFLIGHT[("slow.example", 443)]
        waiting = threading.Event()
        wait = state.ready.wait

        def observe_wait(timeout):
            waiting.set()
            return wait(timeout)

        monkeypatch.setattr(state.ready, "wait", observe_wait)
        second = pool.submit(delivery_module._resolve, "slow.example", 443, time.monotonic() + 2)
        try:
            assert waiting.wait(1)
            healthy = pool.submit(
                delivery_module._resolve, "healthy.example", 443, time.monotonic() + 1
            )
            assert healthy.result(timeout=1)[0][-1][0] == "8.8.8.8"
            assert calls.count(("slow.example", 443)) == 1
        finally:
            release.set()
        assert first.result(timeout=1) == second.result(timeout=1)
        assert calls.count(("slow.example", 443)) == 1


def test_timed_out_hostname_cannot_consume_more_slots_using_other_ports(monkeypatch, isolated_dns):
    release = threading.Event()
    calls = []

    def lookup(host, port, **_):
        calls.append((host, port))
        if host == "slow.example":
            assert release.wait(2)
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("8.8.8.8", port))]

    monkeypatch.setattr(socket, "getaddrinfo", lookup)
    try:
        with pytest.raises(TimeoutError):
            delivery_module._resolve("slow.example", 443, time.monotonic() + 0.03)
        for port in range(440, 450):
            with pytest.raises(TimeoutError):
                delivery_module._resolve("SLOW.example.", port, time.monotonic() + 1)
        assert len(calls) == 1
        assert len(delivery_module._DNS_INFLIGHT) == 1
        healthy = delivery_module._resolve("healthy.example", 443, time.monotonic() + 1)
        assert healthy[0][-1][0] == "8.8.8.8"
    finally:
        release.set()


def test_dns_slots_are_retained_until_os_calls_finish(monkeypatch, isolated_dns):
    release = threading.Event()
    calls = []

    def lookup(host, port, **_):
        calls.append(host)
        assert release.wait(2)
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("8.8.8.8", port))]

    monkeypatch.setattr(socket, "getaddrinfo", lookup)
    try:
        for number in range(8):
            with pytest.raises(TimeoutError):
                delivery_module._resolve(f"host{number}.example", 443, time.monotonic() + 0.02)
        assert len(delivery_module._DNS_INFLIGHT) == 8
        assert not delivery_module._DNS_SLOTS.acquire(blocking=False)
        with pytest.raises(delivery_module.DeliveryCapacityUnavailable, match="capacity"):
            delivery_module._resolve("ninth.example", 443, time.monotonic() + 1)
        assert len(calls) == 8
    finally:
        release.set()
    deadline = time.monotonic() + 1
    while delivery_module._DNS_INFLIGHT and time.monotonic() < deadline:
        time.sleep(0.01)
    assert not delivery_module._DNS_INFLIGHT
    assert all(delivery_module._DNS_SLOTS.acquire(blocking=False) for _ in range(8))
    for _ in range(8):
        delivery_module._DNS_SLOTS.release()


def test_dns_negative_cache_is_bounded_expires_and_is_transient(monkeypatch, isolated_dns):
    calls = []

    def lookup(host, *_, **__):
        calls.append(host)
        raise socket.gaierror("private resolver diagnostic")

    monkeypatch.setattr(socket, "getaddrinfo", lookup)
    monkeypatch.setattr(delivery_module, "_DNS_CACHE_LIMIT", 3)
    for number in range(4):
        result = deliver(f"https://host{number}.example/", b"{}", {})
        assert result.status_code is None and not result.permanent
        assert result.error == "Endpoint hostname could not be resolved"
    assert len(delivery_module._DNS_FAILURES) == 3
    assert not delivery_module._DNS_INFLIGHT
    deliver("https://HOST3.example./", b"{}", {})
    assert calls.count("host3.example") == 1
    delivery_module._DNS_FAILURES["host3.example"] = (time.monotonic() - 1, False)
    deliver("https://host3.example/", b"{}", {})
    assert calls.count("host3.example") == 2


def test_project_dns_allowance_survives_caller_timeout_and_protects_other_projects(
    monkeypatch, isolated_dns
):
    release = threading.Event()
    calls = []

    def lookup(host, port, **_):
        calls.append(host)
        if host.startswith("slow"):
            assert release.wait(3)
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("8.8.8.8", port))]

    monkeypatch.setattr(socket, "getaddrinfo", lookup)
    try:
        for index in range(2):
            with pytest.raises(delivery_module.DNSResolutionTimeout):
                delivery_module._resolve(
                    f"slow{index}.example", 443, time.monotonic() + 0.02, "prj_a"
                )
        assert delivery_module._DNS_PROJECT_COUNTS == {"prj_a": 2}
        result = deliver("https://slow3.example/", b"{}", {}, resolver_scope="prj_a")
        assert result.deferred_reason == "dns_project_capacity"
        assert not result.timed_out and not result.permanent
        assert "slow3.example" not in calls
        assert delivery_module._resolve("healthy.example", 443, time.monotonic() + 1, "prj_b")
        validate_endpoint("https://operator.example/")
        assert delivery_module._DNS_PROJECT_COUNTS == {"prj_a": 2}
    finally:
        release.set()


def test_delivery_dns_limit_reserves_control_capacity(monkeypatch, isolated_dns):
    release = threading.Event()
    calls = []

    def lookup(host, port, **_):
        calls.append(host)
        if host.startswith("slow"):
            assert release.wait(3)
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("8.8.8.8", port))]

    monkeypatch.setattr(socket, "getaddrinfo", lookup)
    try:
        for index in range(6):
            with pytest.raises(delivery_module.DNSResolutionTimeout):
                delivery_module._resolve(
                    f"slow{index}.example", 443, time.monotonic() + 0.02, f"prj_{index // 2}"
                )
        result = deliver("https://another.example/", b"{}", {}, resolver_scope="prj_new")
        assert result.deferred_reason == "dns_global_capacity"
        assert "another.example" not in calls
        validate_endpoint("https://operator.example/")
        assert "operator.example" in calls
        assert sum(delivery_module._DNS_PROJECT_COUNTS.values()) == 6
        for index in range(2):
            with pytest.raises(delivery_module.DNSResolutionTimeout):
                delivery_module._resolve(
                    f"slow-control{index}.example", 443, time.monotonic() + 0.02
                )
        with pytest.raises(delivery_module.DeliveryCapacityUnavailable):
            validate_endpoint("https://operator-new.example/")
        assert len(delivery_module._DNS_INFLIGHT) == 8
    finally:
        release.set()


def test_dns_start_failure_returns_project_and_global_capacity(monkeypatch, isolated_dns):
    def failed_start(_):
        raise RuntimeError("Unable to start a thread")

    monkeypatch.setattr(threading.Thread, "start", failed_start)
    with pytest.raises(delivery_module.DNSResolutionError):
        delivery_module._resolve("receiver.example", 443, time.monotonic() + 1, "prj_a")
    assert not delivery_module._DNS_PROJECT_COUNTS
    assert not delivery_module._DNS_INFLIGHT
    assert all(delivery_module._DNS_SLOTS.acquire(blocking=False) for _ in range(8))
    for _ in range(8):
        delivery_module._DNS_SLOTS.release()


def test_dns_admission_backoff_is_scoped_bounded_and_preserves_reusable_answers(
    monkeypatch, isolated_dns
):
    clock = [100.0]
    monkeypatch.setattr(delivery_module.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(delivery_module.random, "uniform", lambda low, high: (low + high) / 2)
    for expected in (1, 2, 4, 5, 5):
        with delivery_module._DNS_LOCK:
            delivery_module._capacity_unavailable("dns_project_capacity", "blocked")
        until, _ = delivery_module._DNS_ADMISSION["blocked"]
        assert until - clock[0] == expected
        allowed = delivery_module.dns_admission_filter()
        assert not allowed("https://new.example/", "blocked")
        assert allowed("https://new.example/", "other")
        assert allowed("http://8.8.8.8/", "blocked")
        clock[0] = until + 0.01
        assert delivery_module.dns_admission_filter() is None
    with delivery_module._DNS_LOCK:
        delivery_module._capacity_unavailable()
        delivery_module._DNS_FAILURES["cached.example"] = (clock[0] + 20, False)
        delivery_module._DNS_HOSTS["coalesced.example"] = delivery_module._DNSLookup()
    allowed = delivery_module.dns_admission_filter()
    assert allowed("https://cached.example/", "other")
    assert allowed("https://coalesced.example/", "other")
    assert allowed("http://[2606:4700:4700::1111]/", "other")
    assert not allowed("https://fresh.example/", "other")
    delivery_module._DNS_HOSTS.clear()
    with delivery_module._DNS_LOCK:
        for index in range(100):
            delivery_module._capacity_unavailable("dns_project_capacity", f"project-{index}")
    assert len(delivery_module._DNS_ADMISSION) == delivery_module._DNS_ADMISSION_LIMIT


def test_control_lookup_release_cannot_reopen_saturated_delivery_gate(isolated_dns):
    for _ in range(7):
        assert delivery_module._DNS_SLOTS.acquire(blocking=False)
    delivery_module._DNS_PROJECT_COUNTS.update({"p1": 2, "p2": 2, "p3": 2})
    with delivery_module._DNS_LOCK:
        delivery_module._capacity_unavailable()
        delivery_module._release_dns_slot(None)
    assert sum(delivery_module._DNS_PROJECT_COUNTS.values()) == 6
    assert not delivery_module.dns_admission_filter()("https://new.example/", "new")
    with delivery_module._DNS_LOCK:
        delivery_module._release_dns_slot("p1")
    assert delivery_module.dns_admission_filter() is None
    with delivery_module._DNS_LOCK:
        for project, count in list(delivery_module._DNS_PROJECT_COUNTS.items()):
            for _ in range(count):
                delivery_module._release_dns_slot(project)


def test_literal_addresses_bypass_exhausted_dns_but_keep_network_policy(monkeypatch, isolated_dns):
    def forbidden_lookup(*args, **kwargs):
        raise AssertionError("Literal addresses must not enter DNS")

    monkeypatch.setattr(socket, "getaddrinfo", forbidden_lookup)
    monkeypatch.setattr(delivery_module, "_DNS_SLOTS", threading.BoundedSemaphore(0))
    validate_endpoint("https://8.8.8.8/")
    validate_endpoint("https://[2606:4700:4700::1111]/")
    for url in ("http://127.0.0.1/", "http://169.254.169.254/", "http://[::1]/"):
        with pytest.raises(delivery_module.NetworkPolicyError, match="nonpublic"):
            validate_endpoint(url)
    target = delivery_module._target("http://127.0.0.1:8790/", True, time.monotonic() + 1)
    assert target.addresses[0][-1] == ("127.0.0.1", 8790)
