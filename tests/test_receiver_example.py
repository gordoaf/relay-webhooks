import base64
import http.client
import importlib.util
import json
import socket
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing, contextmanager
from pathlib import Path

from relay.delivery import sign_headers

spec = importlib.util.spec_from_file_location(
    "receiver_example",
    Path(__file__).parents[1] / "examples" / "receiver.py",
)
example = importlib.util.module_from_spec(spec)
spec.loader.exec_module(example)
SECRET = "whsec_" + base64.b64encode(b"r" * 32).decode()


def wait_for(check, timeout=3):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if check():
            return
        time.sleep(0.01)
    raise AssertionError("Receiver did not reach expected state")


@contextmanager
def running_receiver(tmp_path, **options):
    server = example.ReceiverServer(("127.0.0.1", 0), SECRET, tmp_path / "receiver.db", **options)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)
        wait_for(lambda: not server._connections)


def deliver(server, event_id="evt_duplicate"):
    body = json.dumps({"id": event_id, "data": {"order_id": "ord_one"}}).encode()
    headers = sign_headers(SECRET, event_id, body)
    with closing(http.client.HTTPConnection(*server.server_address, timeout=3)) as client:
        client.request("POST", "/webhook", body=body, headers=headers)
        response = client.getresponse()
        response.read()
        return response.status


def test_idle_connection_does_not_block_healthy_delivery_and_expires(tmp_path):
    with running_receiver(tmp_path, request_timeout=0.5) as server:
        with socket.create_connection(server.server_address, timeout=3) as idle:
            wait_for(lambda: len(server._connections) == 1)
            assert deliver(server) == 204
            assert idle.recv(1) == b""
        assert deliver(server, "evt_after_idle") == 204


def test_receiver_connection_concurrency_is_bounded(tmp_path):
    with running_receiver(tmp_path, max_connections=2, request_timeout=5) as server:
        sockets = [socket.create_connection(server.server_address, timeout=2) for _ in range(2)]
        try:
            wait_for(lambda: len(server._connections) == 2)
            with socket.create_connection(server.server_address, timeout=2) as extra:
                assert extra.recv(1) == b""
            assert len(server._connections) == 2
        finally:
            for connection in sockets:
                connection.close()
        wait_for(lambda: not server._connections)
        assert deliver(server) == 204


def test_receiver_deadline_stops_headers_that_keep_trickling(tmp_path):
    with (
        running_receiver(tmp_path, request_timeout=0.2) as server,
        socket.create_connection(server.server_address, timeout=2) as connection,
    ):
        wait_for(lambda: len(server._connections) == 1)
        start = time.monotonic()
        try:
            for byte in b"POST /webhook HTTP/1.1\r\nHost: localhost\r\n":
                connection.sendall(bytes([byte]))
                time.sleep(0.025)
        except OSError:
            pass
        wait_for(lambda: not server._connections)
        assert time.monotonic() - start < 1.5


def test_receiver_concurrent_duplicate_requests_have_one_business_effect(tmp_path):
    with running_receiver(tmp_path) as server, ThreadPoolExecutor(max_workers=4) as pool:
        assert list(pool.map(lambda _: deliver(server), range(24))) == [204] * 24
    with closing(sqlite3.connect(tmp_path / "receiver.db")) as db:
        assert db.execute("SELECT count(*) FROM orders").fetchone()[0] == 1
