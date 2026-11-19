"""Local signed receiver with bounded connections and durable deduplication.

Run: RELAY_SIGNING_SECRET=whsec_... python examples/receiver.py
This example deliberately binds only to loopback.
It admits at most eight connections and closes each request after ten seconds,
including time spent waiting for headers. Use --port and --db to isolate runs.
"""

import argparse
import base64
import json
import os
import socket
import sqlite3
import threading
from contextlib import closing
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from relay.client import verify_signature


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        if self.path != "/webhook":
            self.send_error(404)
            return
        try:
            lengths = self.headers.get_all("Content-Length", [])
            if len(lengths) != 1 or self.headers.get("Transfer-Encoding"):
                self.send_error(400, "Expected one Content-Length")
                return
            size = int(lengths[0])
            if not 0 < size <= 270000:
                self.send_error(413)
                return
            body = self.rfile.read(size)
            if len(body) != size or not verify_signature(
                self.server.secret, body, dict(self.headers)
            ):
                self.send_error(401)
                return
            event = json.loads(body)
            if not isinstance(event, dict) or event.get("id") != self.headers.get("webhook-id"):
                raise ValueError("Invalid event envelope")
            data = json.dumps(event["data"], ensure_ascii=False, allow_nan=False)
            data.encode("utf-8")
            # Each handler owns a connection. Deduplication and the example
            # business effect commit in the same transaction.
            with closing(sqlite3.connect(self.server.db_path, timeout=3)) as db, db:
                db.execute(
                    "INSERT OR IGNORE INTO orders(event_id,data) VALUES(?,?)",
                    (event["id"], data),
                )
            self.send_response(204)
            self.end_headers()
        except (ValueError, KeyError, TypeError, OSError):
            self.send_error(400)
        except sqlite3.OperationalError:
            self.send_error(503, "Receiver database is temporarily unavailable")


class ReceiverServer(ThreadingHTTPServer):
    """At most eight handlers, with deadlines that start before HTTP parsing."""

    daemon_threads = True

    def __init__(self, address, secret, db_path, *, max_connections=8, request_timeout=10):
        if type(max_connections) is not int or not 1 <= max_connections <= 64:
            raise ValueError("max_connections must be between 1 and 64")
        if not 0 < request_timeout <= 60:
            raise ValueError("request_timeout must be between 0 and 60 seconds")
        if not isinstance(secret, str) or not secret.startswith("whsec_"):
            raise ValueError("Set RELAY_SIGNING_SECRET to the endpoint signing secret")
        if len(base64.b64decode(secret[6:], validate=True)) < 16:
            raise ValueError("Signing secret is too short")
        self.secret, self.db_path = secret, Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        fd = os.open(self.db_path, os.O_CREAT | os.O_RDWR, 0o600)
        os.close(fd)
        with closing(sqlite3.connect(self.db_path)) as db, db:
            db.execute(
                "CREATE TABLE IF NOT EXISTS orders(event_id TEXT PRIMARY KEY, data TEXT NOT NULL)"
            )
        self.request_timeout = request_timeout
        self._slots = threading.BoundedSemaphore(max_connections)
        self._connections = {}
        self._connection_lock = threading.Lock()
        super().__init__(address, Handler)

    @staticmethod
    def _expire(connection):
        try:
            connection.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass

    def process_request(self, request, client_address):
        if not self._slots.acquire(blocking=False):
            self.shutdown_request(request)
            return
        request.settimeout(self.request_timeout)
        deadline = threading.Timer(self.request_timeout, self._expire, (request,))
        deadline.daemon = True
        with self._connection_lock:
            self._connections[request] = deadline
        deadline.start()
        try:
            super().process_request(request, client_address)
        except Exception:
            self._release(request)
            raise

    def _release(self, request):
        with self._connection_lock:
            deadline = self._connections.pop(request, None)
        if deadline is not None:
            deadline.cancel()
            self._slots.release()

    def process_request_thread(self, request, client_address):
        try:
            super().process_request_thread(request, client_address)
        finally:
            self._release(request)

    def server_close(self):
        with self._connection_lock:
            pending = list(self._connections)
        for connection in pending:
            self._expire(connection)
        super().server_close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=9000)
    parser.add_argument("--db", default="receiver.db")
    args = parser.parse_args()
    if not 1 <= args.port <= 65535:
        parser.error("Port must be between 1 and 65535")
    try:
        server = ReceiverServer(
            ("127.0.0.1", args.port), os.environ.get("RELAY_SIGNING_SECRET"), args.db
        )
    except (ValueError, OSError) as exc:
        parser.error(str(exc))
    print(f"Receiver: http://127.0.0.1:{args.port}/webhook")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
