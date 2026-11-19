"""Signed, deadline-bounded HTTP delivery with pinned destination addresses."""

import base64
import hashlib
import hmac
import http.client
import ipaddress
import queue
import socket
import ssl
import threading
import time
from dataclasses import dataclass
from datetime import timezone
from email.utils import parsedate_to_datetime
from urllib.parse import quote, urlsplit

_RESPONSE_LIMIT = 4096
_DNS_SLOTS = threading.BoundedSemaphore(8)


@dataclass(frozen=True)
class DeliveryResult:
    status_code: int | None
    error: str | None
    duration_ms: float
    response_excerpt: str
    retry_after_seconds: float | None = None


@dataclass(frozen=True)
class _Target:
    host: str
    port: int
    secure: bool
    path: str
    addresses: tuple


def _resolve(host, port, deadline):
    remaining = deadline - time.monotonic()
    if remaining <= 0 or not _DNS_SLOTS.acquire(timeout=remaining):
        raise TimeoutError("DNS resolution timed out")
    result = queue.Queue(maxsize=1)

    def lookup():
        try:
            result.put((True, socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)))
        except Exception as exc:
            result.put((False, exc))
        finally:
            _DNS_SLOTS.release()

    try:
        threading.Thread(target=lookup, daemon=True, name="relay-dns").start()
    except Exception:
        _DNS_SLOTS.release()
        raise
    try:
        ok, value = result.get(timeout=max(0, deadline - time.monotonic()))
    except queue.Empty as exc:
        raise TimeoutError("DNS resolution timed out") from exc
    if not ok:
        raise value
    return value


def _target(url, allow_private, deadline):
    if (
        not isinstance(url, str)
        or len(url) > 8192
        or any(ord(c) <= 32 or ord(c) == 127 for c in url)
        or "\\" in url
        or "#" in url
    ):
        raise ValueError("Endpoint URL contains whitespace, a fragment, or invalid characters")
    try:
        parts = urlsplit(url)
        host, port = parts.hostname, parts.port
    except ValueError as exc:
        raise ValueError("Invalid endpoint URL") from exc
    if parts.scheme not in ("http", "https") or not host:
        raise ValueError("Endpoint must use HTTP or HTTPS")
    if parts.username is not None or parts.password is not None or "%" in host:
        raise ValueError("Endpoint credentials and scoped addresses are forbidden")
    if port is not None and not 1 <= port <= 65535:
        raise ValueError("Invalid endpoint port")
    host = host.rstrip(".").encode("idna").decode("ascii")
    if not host:
        raise ValueError("Invalid hostname")
    secure = parts.scheme == "https"
    port = port or (443 if secure else 80)
    addresses = _resolve(host, port, deadline)
    if not addresses:
        raise ValueError("Endpoint hostname has no addresses")
    for family, _, _, _, sockaddr in addresses:
        if family not in (socket.AF_INET, socket.AF_INET6):
            raise ValueError("Unsupported address family")
        address = ipaddress.ip_address(sockaddr[0])
        public = (
            address.is_global
            and not address.is_multicast
            and not address.is_reserved
            and getattr(address, "ipv4_mapped", None) is None
        )
        if not allow_private and not public:
            raise ValueError("Endpoint resolves to a nonpublic address")
    path = quote(parts.path or "/", safe="/%:@!$&'()*+,;=-._~")
    if parts.query:
        path += "?" + quote(parts.query, safe="/?%:@!$&'()*+,;=-._~")
    return _Target(host, port, secure, path, tuple(addresses))


def validate_endpoint(url, allow_private=False):
    try:
        _target(url, allow_private, time.monotonic() + 5)
    except (OSError, UnicodeError) as exc:
        raise ValueError("Endpoint hostname could not be resolved") from exc


def _secret_bytes(secret):
    if not isinstance(secret, str) or not secret.startswith("whsec_"):
        raise ValueError("Signing secret must start with whsec_")
    decoded = base64.b64decode(secret[6:], validate=True)
    if len(decoded) < 16:
        raise ValueError("Signing secret is too short")
    return decoded


def sign_headers(secret, event_id, payload, timestamp=None):
    stamp = str(int(time.time() if timestamp is None else timestamp))
    message = event_id.encode() + b"." + stamp.encode() + b"." + payload
    digest = hmac.new(_secret_bytes(secret), message, hashlib.sha256).digest()
    return {
        "webhook-id": event_id,
        "webhook-timestamp": stamp,
        "webhook-signature": "v1," + base64.b64encode(digest).decode(),
    }


def verify_signature(secret, payload, headers, tolerance=300, now=None):
    try:
        values = {k.lower(): v for k, v in headers.items()}
        event_id, stamp = values["webhook-id"], values["webhook-timestamp"]
        if not event_id or not stamp.isascii() or not stamp.isdigit():
            return False
        if tolerance < 0 or abs((time.time() if now is None else now) - int(stamp)) > tolerance:
            return False
        message = event_id.encode() + b"." + stamp.encode() + b"." + payload
        expected = hmac.new(_secret_bytes(secret), message, hashlib.sha256).digest()
        matched = False
        for candidate in values["webhook-signature"].split():
            version, sep, encoded = candidate.partition(",")
            if version != "v1" or not sep:
                continue
            try:
                supplied = base64.b64decode(encoded, validate=True)
            except (ValueError, UnicodeError):
                continue
            matched |= hmac.compare_digest(expected, supplied)
        return matched
    except (KeyError, TypeError, ValueError, AttributeError):
        return False


class _PinnedConnection(http.client.HTTPConnection):
    def __init__(self, target, deadline):
        super().__init__(target.host, target.port)
        self.target, self.deadline = target, deadline
        self.expired = threading.Event()
        self.active_socket = None

    def remaining(self):
        remaining = self.deadline - time.monotonic()
        if self.expired.is_set() or remaining <= 0:
            self.abort()
            raise TimeoutError("Delivery deadline exceeded")
        return remaining

    def abort(self):
        self.expired.set()
        active = self.active_socket
        if active is not None:
            try:
                active.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            active.close()

    def connect(self):
        last_error = None
        for family, socktype, protocol, _, sockaddr in self.target.addresses:
            self.remaining()
            active = socket.socket(family, socktype, protocol)
            self.sock = self.active_socket = active
            try:
                active.settimeout(self.remaining())
                active.connect(sockaddr)
                if self.target.secure:
                    active = ssl.create_default_context().wrap_socket(
                        active, server_hostname=self.target.host, do_handshake_on_connect=False
                    )
                    self.sock = self.active_socket = active
                    active.settimeout(self.remaining())
                    active.do_handshake()
                self.remaining()
                return
            except OSError as exc:
                last_error = exc
                active.close()
                self.sock = self.active_socket = None
        raise last_error or OSError("No endpoint address was reachable")


def _retry_after(value):
    if not value:
        return None
    try:
        seconds = float(int(value.strip()))
    except ValueError:
        try:
            date = parsedate_to_datetime(value)
            if date.tzinfo is None:
                date = date.replace(tzinfo=timezone.utc)
            seconds = date.timestamp() - time.time()
        except (TypeError, ValueError, OverflowError):
            return None
    return max(0, min(seconds, 3600))


def deliver(url, body, headers, timeout=10, allow_private=False):
    started = time.monotonic()
    connection = watchdog = response = None
    try:
        if timeout <= 0:
            raise ValueError("Timeout must be positive")
        target = _target(url, allow_private, started + timeout)
        connection = _PinnedConnection(target, started + timeout)
        watchdog = threading.Timer(connection.remaining(), connection.abort)
        watchdog.daemon = True
        watchdog.start()
        outgoing = {"Content-Type": "application/json", "User-Agent": "Relay/0.1"}
        reserved = {"host", "content-length", "transfer-encoding", "connection"}
        outgoing.update({k: v for k, v in headers.items() if k.lower() not in reserved})
        outgoing["Connection"] = "close"
        connection.request("POST", target.path, body=body, headers=outgoing)
        response = connection.getresponse()
        retry_after = _retry_after(response.getheader("Retry-After"))
        excerpt = bytearray()
        while len(excerpt) < _RESPONSE_LIMIT:
            connection.remaining()
            chunk = response.read1(min(1024, _RESPONSE_LIMIT - len(excerpt)))
            if not chunk:
                break
            excerpt.extend(chunk)
        connection.remaining()
        status = response.status
        return DeliveryResult(
            status,
            None if 200 <= status < 300 else f"HTTP {status}",
            (time.monotonic() - started) * 1000,
            excerpt.decode("utf-8", errors="replace"),
            retry_after,
        )
    except (OSError, ValueError, http.client.HTTPException) as exc:
        timed_out = isinstance(exc, TimeoutError) or (connection and connection.expired.is_set())
        error = (
            "Delivery deadline exceeded" if timed_out else f"Delivery failed ({type(exc).__name__})"
        )
        return DeliveryResult(None, error, (time.monotonic() - started) * 1000, "")
    finally:
        if watchdog:
            watchdog.cancel()
        if response:
            response.close()
        if connection:
            connection.close()
