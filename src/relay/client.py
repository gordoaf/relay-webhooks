"""Small synchronous ingestion client and raw-body signature verification helper.

Each logical event must have a persistent idempotency key. Reuse that key and
the same payload after an ambiguous network failure, including after restart.
"""

import http.client
import ipaddress
import json
import math
import re
import socket
import ssl
import threading
import time
from typing import Any, TypedDict
from urllib.parse import urlsplit

from .delivery import verify_signature

__all__ = ["EventReceipt", "RelayClient", "RelayError", "RelayEventExpired", "verify_signature"]


class EventReceipt(TypedDict):
    event: dict[str, Any]
    created: bool


class RelayError(Exception):
    """An ingestion failure; an absent status may mean the event was committed."""

    def __init__(self, message, *, idempotency_key, status=None, retryable=False, cause=None):
        super().__init__(message)
        self.idempotency_key = idempotency_key
        self.status = status
        self.retryable = retryable
        self.cause = cause


class RelayEventExpired(RelayError):
    """The event was accepted earlier; its retained details are no longer available.

    Acceptance does not establish successful delivery to any receiver.
    """

    accepted = True

    def __init__(self, *, idempotency_key, event_id):
        super().__init__(
            "Relay previously accepted this event, but its retained details expired; "
            "do not resubmit under a new idempotency key",
            idempotency_key=idempotency_key,
            status=410,
        )
        self.event_id = event_id


class RelayClient:
    """Use a project API key, never an administrator token.

    ``timeout`` bounds connect and individual socket reads. Response reading
    also observes a total deadline; an operating-system DNS lookup can exceed
    the socket timeout. At most ``max_retries + 1`` requests are made.
    """

    def __init__(self, base_url, api_key, *, timeout=10.0, max_retries=2):
        if not isinstance(base_url, str) or any(
            char.isspace() or ord(char) < 32 or ord(char) == 127 or char == "\\"
            for char in base_url
        ):
            raise ValueError("base_url must be a valid HTTP(S) origin")
        parts = urlsplit(base_url)
        if (
            parts.scheme not in {"http", "https"}
            or not parts.hostname
            or parts.username is not None
            or parts.password is not None
            or parts.query
            or parts.fragment
            or parts.path not in {"", "/"}
        ):
            raise ValueError("base_url must be an HTTP(S) origin without credentials or a path")
        try:
            port = (
                parts.port if parts.port is not None else (443 if parts.scheme == "https" else 80)
            )
            if not 1 <= port <= 65535:
                raise ValueError()
            if ":" in parts.hostname:
                host = str(ipaddress.IPv6Address(parts.hostname))
                if "%" in host:
                    raise ValueError()
            else:
                host = parts.hostname.encode("idna").decode("ascii").lower()
                # Private DNS and Compose service names can contain underscores.
                # Keep label lengths and origin boundaries strict after IDNA.
                if len(host.rstrip(".")) > 253 or not all(
                    re.fullmatch(r"[a-z0-9_](?:[a-z0-9_-]{0,61}[a-z0-9_])?", label)
                    for label in host.removesuffix(".").split(".")
                ):
                    raise ValueError()
        except (ValueError, UnicodeError):
            raise ValueError("base_url has an invalid hostname or port") from None
        if (
            not isinstance(api_key, str)
            or not api_key
            or any(ord(char) < 33 or ord(char) > 126 for char in api_key)
        ):
            raise ValueError("api_key must be a nonempty ASCII token")
        if (
            type(timeout) not in {int, float}
            or not math.isfinite(timeout)
            or not 0 < timeout <= 120
        ):
            raise ValueError("timeout must be between 0 and 120 seconds")
        if type(max_retries) is not int or not 0 <= max_retries <= 5:
            raise ValueError("max_retries must be an integer between 0 and 5")
        self._host, self._port, self._secure = host, port, parts.scheme == "https"
        self._api_key, self.timeout, self.max_retries = api_key, timeout, max_retries

    def emit(self, event_type, payload, *, idempotency_key) -> EventReceipt:
        """Submit one event. Keys must identify the business operation, not each try."""
        if not isinstance(event_type, str) or not re.fullmatch(
            r"[A-Za-z][A-Za-z0-9_.:-]{0,127}", event_type
        ):
            raise ValueError("Invalid event type")
        if not isinstance(payload, dict):
            raise TypeError("payload must be a JSON object")
        if (
            not isinstance(idempotency_key, str)
            or not 1 <= len(idempotency_key) <= 200
            or any(ord(char) < 33 or ord(char) > 126 for char in idempotency_key)
        ):
            raise ValueError("idempotency_key must contain 1-200 visible ASCII characters")
        try:
            body = json.dumps(
                {"type": event_type, "payload": payload},
                ensure_ascii=False,
                allow_nan=False,
                separators=(",", ":"),
            ).encode("utf-8")
        except (TypeError, ValueError, UnicodeError, RecursionError):
            raise ValueError("payload must contain valid JSON with finite numbers") from None
        if len(body) > 256 * 1024:
            raise ValueError("Event request exceeds 256 KiB")
        headers = {
            "Authorization": "Bearer " + self._api_key,
            "Content-Type": "application/json",
            "Idempotency-Key": idempotency_key,
            "User-Agent": "relay-python-client/0.2",
        }
        for attempt in range(self.max_retries + 1):
            retry_after = None
            try:
                status, raw, retry_after = self._request(body, headers)
                if status in {200, 202}:
                    try:
                        receipt = json.loads(raw)
                        if (
                            not isinstance(receipt, dict)
                            or type(receipt.get("created")) is not bool
                            or not isinstance(receipt.get("event"), dict)
                            or not isinstance(receipt["event"].get("id"), str)
                        ):
                            raise ValueError("Invalid receipt")
                        return receipt
                    except (ValueError, TypeError, UnicodeError):
                        error = RelayError(
                            "Relay returned an invalid receipt; reuse the same idempotency key",
                            status=status,
                            idempotency_key=idempotency_key,
                            retryable=True,
                        )
                elif status == 410:
                    try:
                        expired = json.loads(raw)
                        event_id = expired.get("event_id") if isinstance(expired, dict) else None
                        if not isinstance(event_id, str) or not re.fullmatch(
                            r"[\x21-\x7e]{1,200}", event_id
                        ):
                            raise ValueError("Invalid expired-event receipt")
                        error = RelayEventExpired(
                            idempotency_key=idempotency_key, event_id=event_id
                        )
                    except (ValueError, TypeError, UnicodeError):
                        error = RelayError(
                            "Relay returned an invalid expired-event receipt; retain the original idempotency key",
                            idempotency_key=idempotency_key,
                            status=status,
                        )
                else:
                    error = RelayError(
                        f"Relay rejected the event (HTTP {status})",
                        status=status,
                        idempotency_key=idempotency_key,
                        retryable=status in {408, 429} or 500 <= status <= 599,
                    )
            except ssl.SSLCertVerificationError as exc:
                error = RelayError(
                    "Relay TLS certificate verification failed; check the certificate and hostname",
                    idempotency_key=idempotency_key,
                    cause=exc,
                )
            except UnicodeError as exc:
                error = RelayError(
                    "Relay hostname encoding failed; check base_url",
                    idempotency_key=idempotency_key,
                    cause=exc,
                )
            except (OSError, http.client.HTTPException) as exc:
                error = RelayError(
                    "Relay could not confirm acceptance; reuse the same idempotency key",
                    idempotency_key=idempotency_key,
                    retryable=True,
                    cause=exc,
                )
            if not error.retryable or attempt == self.max_retries:
                raise error from error.cause
            delay = min(5.0, 0.25 * 2**attempt)
            if retry_after and retry_after.isascii() and retry_after.isdigit():
                delay = min(5.0, max(delay, float(retry_after[:8])))
            time.sleep(delay)
        raise AssertionError("Unreachable")

    def _request(self, body, headers):
        connection_type = (
            http.client.HTTPSConnection if self._secure else http.client.HTTPConnection
        )
        connection = connection_type(self._host, self._port, timeout=self.timeout)
        deadline = time.monotonic() + self.timeout
        timer = None
        try:
            # Keep the original socket: HTTP/1.0 responses may detach it from
            # the connection while the response body is still being read.
            connection.connect()
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("Connection deadline exceeded")
            active_socket = connection.sock

            def abort():
                try:
                    active_socket.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass

            timer = threading.Timer(remaining, abort)
            timer.daemon = True
            timer.start()
            active_socket.settimeout(remaining)
            connection.request("POST", "/v1/events", body=body, headers=headers)
            response = connection.getresponse()
            chunks, size = [], 0
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("Response deadline exceeded")
                if connection.sock is not None:
                    connection.sock.settimeout(remaining)
                chunk = response.read1(min(8192, 1024 * 1024 + 1 - size))
                if not chunk:
                    break
                chunks.append(chunk)
                size += len(chunk)
                if size > 1024 * 1024:
                    raise http.client.HTTPException("Receipt exceeds 1 MiB")
            return response.status, b"".join(chunks), response.getheader("Retry-After")
        finally:
            if timer:
                timer.cancel()
            connection.close()
