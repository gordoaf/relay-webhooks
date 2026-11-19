"""Signed, deadline-bounded HTTP delivery with pinned destination addresses."""

import base64
import hashlib
import hmac
import http.client
import ipaddress
import math
import random
from collections import OrderedDict
import socket
import ssl
import threading
import time
from dataclasses import dataclass, field
from datetime import timezone
from email.utils import parsedate_to_datetime
from urllib.parse import quote, urlsplit

_RESPONSE_LIMIT = 4096
_RESPONSE_SECONDS = 0.25
_CONNECT_SECONDS = 3.0
_DNS_SLOTS = threading.BoundedSemaphore(8)
_DNS_LOCK = threading.Lock()
_DNS_WAKE = threading.Condition(_DNS_LOCK)
_DNS_INFLIGHT = {}
_DNS_HOSTS = {}
_DNS_PROJECT_COUNTS = {}
_DNS_PROJECT_LIMIT = 2
_DNS_DELIVERY_LIMIT = 6  # Two of the eight OS slots remain available to operator requests.
_DNS_FAILURES = OrderedDict()
_DNS_CACHE_LIMIT = 256
_DNS_FAILURE_SECONDS = 5.0
_DNS_ADMISSION = OrderedDict()
_DNS_ADMISSION_LIMIT = 64
_DNS_ADMISSION_MAX_SECONDS = 5.0

# Keep these exclusions explicit: ipaddress classifications differ between
# supported Python patch versions. Transition mechanisms can route embedded
# IPv4 destinations and are deliberately unsupported by the public policy.
_IPV4_EXCLUDED = tuple(
    ipaddress.ip_network(value)
    for value in (
        "0.0.0.0/8",
        "10.0.0.0/8",
        "100.64.0.0/10",
        "127.0.0.0/8",
        "169.254.0.0/16",
        "172.16.0.0/12",
        "192.0.0.0/24",
        "192.0.2.0/24",
        "192.88.99.0/24",
        "192.168.0.0/16",
        "198.18.0.0/15",
        "198.51.100.0/24",
        "203.0.113.0/24",
        "224.0.0.0/4",
        "240.0.0.0/4",
    )
)
_IPV6_UNICAST = ipaddress.ip_network("2000::/3")
_IPV6_EXCLUDED = tuple(
    ipaddress.ip_network(value)
    for value in ("2001::/23", "2001:db8::/32", "2002::/16", "3fff::/20")
)


class NetworkPolicyError(ValueError):
    """A destination that retries cannot make acceptable to the network policy."""


class DNSResolutionError(OSError):
    """A transient resolver failure without exposing resolver-provided text."""


class DNSResolutionTimeout(TimeoutError):
    """The destination's own resolver lookup did not complete in time."""


class DeliveryCapacityUnavailable(OSError):
    """Local resolver admission pressure; no destination attempt was made."""

    def __init__(self, reason="dns_global_capacity"):
        self.reason = reason
        message = (
            "Project DNS lookup capacity is temporarily exhausted"
            if reason == "dns_project_capacity"
            else "DNS lookup capacity is temporarily exhausted"
        )
        super().__init__(message)


@dataclass
class _DNSLookup:
    ready: threading.Event = field(default_factory=threading.Event)
    addresses: tuple = ()
    error: Exception | None = None
    timed_out: bool = False


@dataclass(frozen=True)
class DeliveryResult:
    status_code: int | None
    error: str | None
    duration_ms: float
    response_excerpt: str
    retry_after_seconds: float | None = None
    permanent: bool = False
    timed_out: bool = False
    deferred_reason: str | None = None


@dataclass(frozen=True)
class _Target:
    host: str
    port: int
    secure: bool
    path: str
    addresses: tuple


def _remember_dns_failure(host, timed_out):
    # Called under _DNS_LOCK. There is no positive cache: each new delivery
    # validates the addresses it will actually use, once per connection.
    _DNS_FAILURES[host] = (time.monotonic() + _DNS_FAILURE_SECONDS, timed_out)
    _DNS_FAILURES.move_to_end(host)
    while len(_DNS_FAILURES) > _DNS_CACHE_LIMIT:
        _DNS_FAILURES.popitem(last=False)


def _release_dns_slot(project_id):
    # Called under _DNS_LOCK; caller timeouts never return OS lookup capacity.
    if project_id is not None:
        remaining = _DNS_PROJECT_COUNTS[project_id] - 1
        if remaining:
            _DNS_PROJECT_COUNTS[project_id] = remaining
        else:
            del _DNS_PROJECT_COUNTS[project_id]
    _DNS_SLOTS.release()
    changed = False
    if sum(_DNS_PROJECT_COUNTS.values()) < _DNS_DELIVERY_LIMIT:
        changed = _DNS_ADMISSION.pop(None, None) is not None
    if project_id is not None:
        changed = _DNS_ADMISSION.pop(project_id, None) is not None or changed
    if changed:
        _DNS_WAKE.notify_all()


def _capacity_unavailable(reason="dns_global_capacity", project_id=None):
    # Called under _DNS_LOCK. Gates are process-local because OS resolver slots
    # are process-local too. Never park another process's available resolver.
    key = project_id if reason == "dns_project_capacity" else None
    now = time.monotonic()
    previous = _DNS_ADMISSION.get(key)
    delay = (
        min(_DNS_ADMISSION_MAX_SECONDS, previous[1] * 2)
        if previous and now - previous[0] < 30
        else 1.0
    )
    until = now + min(_DNS_ADMISSION_MAX_SECONDS, random.uniform(delay * 0.75, delay * 1.25))
    _DNS_ADMISSION[key] = (until, delay)
    _DNS_ADMISSION.move_to_end(key)
    while len(_DNS_ADMISSION) > _DNS_ADMISSION_LIMIT:
        _DNS_ADMISSION.popitem(last=False)
    return DeliveryCapacityUnavailable(reason)


def dns_admission_filter():
    """Snapshot which endpoints can progress during local DNS backoff.

    Literal IPs need no resolver; coalesced lookups and negative-cache results
    need no new OS slot. A stale snapshot is harmless: _resolve still enforces
    capacity and revalidates the addresses actually used for the connection.
    """
    with _DNS_LOCK:
        now = time.monotonic()
        for key, (until, _) in list(_DNS_ADMISSION.items()):
            if now - until >= 30:
                del _DNS_ADMISSION[key]
        blocked = {key for key, (until, _) in _DNS_ADMISSION.items() if until > now}
        if not blocked:
            return None
        reusable = set(_DNS_HOSTS) | {
            host for host, (until, _) in _DNS_FAILURES.items() if until > now
        }

    def allowed(url, project_id):
        if None not in blocked and project_id not in blocked:
            return True
        try:
            host = (urlsplit(url).hostname or "").rstrip(".").encode("idna").decode("ascii").lower()
        except (ValueError, UnicodeError):
            # Let delivery record a permanent validation failure, rather than
            # leaving malformed legacy configuration parked indefinitely.
            return True
        if host in reusable:
            return True
        try:
            ipaddress.ip_address(host)
            return True
        except ValueError:
            return False

    return allowed


def wait_for_dns_capacity(stopping, timeout):
    with _DNS_WAKE:
        if not stopping.is_set():
            _DNS_WAKE.wait(timeout)


def wake_dns_waiters():
    with _DNS_WAKE:
        _DNS_WAKE.notify_all()


def _resolve(host, port, deadline, resolver_scope=None):
    host = host.rstrip(".").lower()
    key = (host, port)
    while True:
        if deadline <= time.monotonic():
            raise DNSResolutionTimeout("DNS resolution timed out")
        with _DNS_LOCK:
            now = time.monotonic()
            for cached_host, (expires, _) in list(_DNS_FAILURES.items()):
                if expires <= now:
                    del _DNS_FAILURES[cached_host]
            failure = _DNS_FAILURES.get(host)
            if failure:
                if failure[1]:
                    raise DNSResolutionTimeout("DNS resolution is temporarily unavailable")
                raise DNSResolutionError("Endpoint hostname could not be resolved")
            lookup = _DNS_INFLIGHT.get(key)
            same_key = lookup is not None
            if lookup is None:
                lookup = _DNS_HOSTS.get(host)
            if lookup is not None:
                if lookup.timed_out:
                    raise DNSResolutionTimeout("DNS resolution is still in progress")
                start_lookup = False
            else:
                # No queued resolver threads. The slot stays occupied until
                # the OS call ends, even if every caller has timed out.
                if resolver_scope is not None:
                    if _DNS_PROJECT_COUNTS.get(resolver_scope, 0) >= _DNS_PROJECT_LIMIT:
                        raise _capacity_unavailable("dns_project_capacity", resolver_scope)
                    if sum(_DNS_PROJECT_COUNTS.values()) >= _DNS_DELIVERY_LIMIT:
                        raise _capacity_unavailable()
                if not _DNS_SLOTS.acquire(blocking=False):
                    raise _capacity_unavailable()
                if resolver_scope is not None:
                    _DNS_PROJECT_COUNTS[resolver_scope] = (
                        _DNS_PROJECT_COUNTS.get(resolver_scope, 0) + 1
                    )
                lookup = _DNSLookup()
                _DNS_INFLIGHT[key] = _DNS_HOSTS[host] = lookup
                start_lookup = same_key = True

        if start_lookup:

            def run_lookup(state=lookup, project_id=resolver_scope):
                try:
                    state.addresses = tuple(socket.getaddrinfo(host, port, type=socket.SOCK_STREAM))
                    if not state.addresses:
                        raise DNSResolutionError("Endpoint hostname has no addresses")
                except Exception as exc:
                    state.error = exc
                finally:
                    with _DNS_LOCK:
                        if state.error is not None:
                            _remember_dns_failure(host, isinstance(state.error, TimeoutError))
                        else:
                            _DNS_FAILURES.pop(host, None)
                        _DNS_INFLIGHT.pop(key, None)
                        _DNS_HOSTS.pop(host, None)
                        _release_dns_slot(project_id)
                        state.ready.set()

            try:
                threading.Thread(target=run_lookup, daemon=True, name="relay-dns").start()
            except Exception as exc:
                with _DNS_LOCK:
                    lookup.error = exc
                    _DNS_INFLIGHT.pop(key, None)
                    _DNS_HOSTS.pop(host, None)
                    _remember_dns_failure(host, False)
                    _release_dns_slot(resolver_scope)
                    lookup.ready.set()
                raise DNSResolutionError("DNS lookup could not be started") from exc
        if not lookup.ready.wait(max(0, deadline - time.monotonic())):
            with _DNS_LOCK:
                if not lookup.ready.is_set():
                    lookup.timed_out = True
                    _remember_dns_failure(host, True)
            raise DNSResolutionTimeout("DNS resolution timed out")
        if lookup.error is not None:
            if isinstance(lookup.error, TimeoutError):
                raise DNSResolutionTimeout("DNS resolution timed out") from lookup.error
            raise DNSResolutionError("Endpoint hostname could not be resolved") from lookup.error
        if same_key:
            return lookup.addresses
        # A different port for this host shared its in-flight wait, but needs
        # its own port-specific answer. Never use two OS slots for one host.


def _public_address(address):
    if address.version == 4:
        excluded = any(address in network for network in _IPV4_EXCLUDED)
    else:
        excluded = address not in _IPV6_UNICAST or any(
            address in network for network in _IPV6_EXCLUDED
        )
    return (
        not excluded
        and address.is_global
        and not address.is_multicast
        and not address.is_reserved
        and getattr(address, "ipv4_mapped", None) is None
    )


def _target(url, allow_private, deadline, resolver_scope=None):
    if (
        not isinstance(url, str)
        or len(url) > 8192
        or any(ord(c) <= 32 or ord(c) == 127 for c in url)
        or "\\" in url
        or "#" in url
    ):
        raise NetworkPolicyError(
            "Endpoint URL contains whitespace, a fragment, or invalid characters"
        )
    try:
        parts = urlsplit(url)
        host, port = parts.hostname, parts.port
    except ValueError as exc:
        raise NetworkPolicyError("Invalid endpoint URL") from exc
    if parts.scheme not in ("http", "https") or not host:
        raise NetworkPolicyError("Endpoint must use HTTP or HTTPS")
    if parts.username is not None or parts.password is not None or "%" in host:
        raise NetworkPolicyError("Endpoint credentials and scoped addresses are forbidden")
    if port is not None and not 1 <= port <= 65535:
        raise NetworkPolicyError("Invalid endpoint port")
    try:
        host = host.rstrip(".").encode("idna").decode("ascii").lower()
    except UnicodeError as exc:
        raise NetworkPolicyError("Invalid endpoint hostname") from exc
    if not host:
        raise NetworkPolicyError("Invalid hostname")
    secure = parts.scheme == "https"
    port = port or (443 if secure else 80)
    try:
        literal = ipaddress.ip_address(host)
    except ValueError:
        addresses = _resolve(host, port, deadline, resolver_scope)
    else:
        family = socket.AF_INET if literal.version == 4 else socket.AF_INET6
        sockaddr = (str(literal), port) if literal.version == 4 else (str(literal), port, 0, 0)
        addresses = ((family, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", sockaddr),)
    if not addresses:
        raise DNSResolutionError("Endpoint hostname has no addresses")
    for family, _, _, _, sockaddr in addresses:
        if family not in (socket.AF_INET, socket.AF_INET6):
            raise NetworkPolicyError("Unsupported address family")
        address = ipaddress.ip_address(sockaddr[0])
        if not allow_private and not _public_address(address):
            raise NetworkPolicyError(
                "Endpoint resolves to a nonpublic or unsupported transition address"
            )
    path = quote(parts.path or "/", safe="/%:@!$&'()*+,;=-._~")
    if parts.query:
        path += "?" + quote(parts.query, safe="/?%:@!$&'()*+,;=-._~")
    return _Target(host, port, secure, path, tuple(addresses))


def validate_endpoint(url, allow_private=False):
    try:
        _target(url, allow_private, time.monotonic() + 5)
    except DeliveryCapacityUnavailable:
        raise
    except (OSError, UnicodeError) as exc:
        raise ValueError("Endpoint hostname could not be resolved") from exc


def _secret_bytes(secret):
    if not isinstance(secret, str) or not secret.startswith("whsec_"):
        raise ValueError("Signing secret must start with whsec_")
    decoded = base64.b64decode(secret[6:], validate=True)
    if len(decoded) < 16:
        raise ValueError("Signing secret is too short")
    return decoded


def sign_headers(secret, event_id, payload, timestamp=None, *, previous_secret=None):
    stamp = str(int(time.time() if timestamp is None else timestamp))
    message = event_id.encode() + b"." + stamp.encode() + b"." + payload
    signatures = []
    for signing_secret in (secret, previous_secret):
        if signing_secret is not None:
            digest = hmac.new(_secret_bytes(signing_secret), message, hashlib.sha256).digest()
            signatures.append("v1," + base64.b64encode(digest).decode())
    return {
        "webhook-id": event_id,
        "webhook-timestamp": stamp,
        "webhook-signature": " ".join(signatures),
    }


def verify_signature(secret, payload, headers, tolerance=300, now=None):
    try:
        values = {k.lower(): v for k, v in headers.items()}
        event_id, stamp = values["webhook-id"], values["webhook-timestamp"]
        if not event_id or len(stamp) > 20 or not stamp.isascii() or not stamp.isdigit():
            return False
        current_time = time.time() if now is None else now
        if (
            type(tolerance) not in (int, float)
            or not math.isfinite(tolerance)
            or tolerance < 0
            or type(current_time) not in (int, float)
            or not math.isfinite(current_time)
            or abs(current_time - int(stamp)) > tolerance
        ):
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
    except (KeyError, TypeError, ValueError, AttributeError, OverflowError):
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
        connect_deadline = min(self.deadline, time.monotonic() + _CONNECT_SECONDS)

        def remaining():
            self.remaining()
            budget = connect_deadline - time.monotonic()
            if budget <= 0:
                raise TimeoutError("Connection establishment timed out")
            return budget

        for family, socktype, protocol, _, sockaddr in self.target.addresses:
            remaining()
            active = socket.socket(family, socktype, protocol)
            self.sock = self.active_socket = active
            try:
                active.settimeout(remaining())
                active.connect(sockaddr)
                if self.target.secure:
                    active = ssl.create_default_context().wrap_socket(
                        active, server_hostname=self.target.host, do_handshake_on_connect=False
                    )
                    self.sock = self.active_socket = active
                    active.settimeout(remaining())
                    active.do_handshake()
                remaining()
                active.settimeout(self.remaining())
                return
            except OSError as exc:
                last_error = exc
                active.close()
                self.sock = self.active_socket = None
        raise last_error or OSError("No endpoint address was reachable")


def _retry_after(value):
    if not value:
        return None
    value = value.strip()
    # HTTP delay-seconds are ASCII digits. Clamp by length before int() so a
    # malicious header cannot hit Python's integer-string conversion limit.
    if value.isascii() and value.isdigit():
        digits = value.lstrip("0") or "0"
        return 3600 if len(digits) > 4 else min(int(digits), 3600)
    try:
        date = parsedate_to_datetime(value)
        if date.tzinfo is None:
            date = date.replace(tzinfo=timezone.utc)
        seconds = date.timestamp() - time.time()
    except (TypeError, ValueError, OverflowError):
        return None
    return max(0, min(seconds, 3600))


def deliver(url, body, headers, timeout=10, allow_private=False, *, resolver_scope=None):
    started = time.monotonic()
    connection = watchdog = excerpt_watchdog = response = None
    try:
        if timeout <= 0:
            raise ValueError("Timeout must be positive")
        target = _target(url, allow_private, started + timeout, resolver_scope)
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
        status = response.status
        retry_after = _retry_after(response.getheader("Retry-After"))
        excerpt = bytearray()
        # A final HTTP status is authoritative. Excerpts are only diagnostic;
        # an incomplete or slow response body must not cause a duplicate retry
        # after a successful receiver acknowledgement.
        if not 200 <= status < 300:
            excerpt_seconds = min(_RESPONSE_SECONDS, max(0, started + timeout - time.monotonic()))
            if excerpt_seconds > 0:
                excerpt_watchdog = threading.Timer(excerpt_seconds, connection.abort)
                excerpt_watchdog.daemon = True
                excerpt_watchdog.start()
                try:
                    while len(excerpt) < _RESPONSE_LIMIT:
                        connection.remaining()
                        chunk = response.read1(min(1024, _RESPONSE_LIMIT - len(excerpt)))
                        if not chunk:
                            break
                        excerpt.extend(chunk)
                except http.client.IncompleteRead as exc:
                    excerpt.extend(exc.partial[: _RESPONSE_LIMIT - len(excerpt)])
                except (OSError, ValueError, http.client.HTTPException):
                    pass
        return DeliveryResult(
            status,
            None if 200 <= status < 300 else f"HTTP {status}",
            (time.monotonic() - started) * 1000,
            excerpt.decode("utf-8", errors="replace"),
            retry_after,
        )
    except DeliveryCapacityUnavailable as exc:
        return DeliveryResult(
            None,
            str(exc),
            (time.monotonic() - started) * 1000,
            "",
            retry_after_seconds=1,
            deferred_reason=exc.reason,
        )
    except NetworkPolicyError as exc:
        return DeliveryResult(
            None, str(exc), (time.monotonic() - started) * 1000, "", permanent=True
        )
    except (DNSResolutionError, DNSResolutionTimeout) as exc:
        return DeliveryResult(
            None,
            str(exc),
            (time.monotonic() - started) * 1000,
            "",
            timed_out=isinstance(exc, TimeoutError),
        )
    except (OSError, ValueError, http.client.HTTPException) as exc:
        timed_out = isinstance(exc, TimeoutError) or bool(
            connection and connection.expired.is_set()
        )
        error = (
            "Delivery deadline exceeded" if timed_out else f"Delivery failed ({type(exc).__name__})"
        )
        return DeliveryResult(
            None, error, (time.monotonic() - started) * 1000, "", timed_out=timed_out
        )
    finally:
        if excerpt_watchdog:
            excerpt_watchdog.cancel()
        if watchdog:
            watchdog.cancel()
        if response:
            response.close()
        if connection:
            connection.close()
