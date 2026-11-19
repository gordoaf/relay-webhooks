import time
import json
import threading
import pytest
import relay.worker as module
from relay.delivery import DeliveryResult, verify_signature
import relay.delivery as delivery_module
from relay.worker import Worker


@pytest.mark.parametrize(
    "status,retry",
    [
        (204, False),
        (400, False),
        (401, False),
        (302, False),
        (408, True),
        (429, True),
        (500, True),
        (503, True),
        (None, True),
    ],
)
def test_retry_policy(store, project, endpoint, monkeypatch, status, retry):
    store.create_event(project["id"], "test", {})
    monkeypatch.setattr(module, "deliver", lambda *a, **kw: DeliveryResult(status, None, 1, ""))
    worker = Worker(store)
    assert worker.run_once()
    row = store.list_deliveries(project["id"])["items"][0]
    assert (row["status"] == "retrying") == retry
    if status == 204:
        assert row["status"] == "delivered"


def test_retry_exhaustion_and_retry_after(store, project, endpoint, monkeypatch):
    store.create_event(project["id"], "test", {})
    monkeypatch.setattr(
        module, "deliver", lambda *a, **kw: DeliveryResult(429, "HTTP 429", 1, "", 60)
    )
    worker = Worker(store, max_attempts=2, retry_base=0)
    worker.run_once()
    row = store.list_deliveries(project["id"])["items"][0]
    assert row["next_attempt_at"] >= time.time() + 59
    with store._db(True) as db:
        db.execute("UPDATE deliveries SET next_attempt_at=0")
        db.execute("UPDATE endpoints SET next_available_at=0")
    worker.run_once()
    row = store.list_deliveries(project["id"])["items"][0]
    assert row["status"] == "failed" and row["attempt_count"] == 2


def test_idle_worker_stops_promptly(store):
    worker = Worker(store, poll_interval=30)
    worker.start()
    started = time.monotonic()
    worker.stop()
    assert time.monotonic() - started < 1


def test_worker_signs_the_complete_event_envelope(store, project, endpoint, monkeypatch):
    event, _ = store.create_event(project["id"], "order.created", {"label": "café"})
    captured = {}

    def receiver(url, body, headers, **kwargs):
        captured.update(body=body, headers=headers)
        return DeliveryResult(204, None, 1, "")

    monkeypatch.setattr(module, "deliver", receiver)
    Worker(store).run_once()
    body = captured["body"]
    assert json.loads(body) == {
        "id": event["id"],
        "type": "order.created",
        "created_at": event["created_at"],
        "data": {"label": "café"},
    }
    assert verify_signature(endpoint["signing_secret"], body, captured["headers"])
    assert not verify_signature(
        endpoint["signing_secret"],
        body.replace(b"order.created", b"order.deleted"),
        captured["headers"],
    )


def test_unexpected_delivery_exception_consumes_retry_budget(
    store, project, endpoint, monkeypatch, caplog
):
    store.create_event(project["id"], "test", {})

    def broken_receiver(*args, **kwargs):
        raise OverflowError("unexpected receiver parsing failure")

    monkeypatch.setattr(module, "deliver", broken_receiver)
    worker = Worker(store, max_attempts=2, retry_base=0)
    assert worker.run_once()
    first = store.list_deliveries(project["id"])["items"][0]
    assert first["status"] == "retrying" and first["attempt_count"] == 1
    with store._db(True) as db:
        db.execute("UPDATE endpoints SET next_available_at=0")
    assert worker.run_once()
    last = store.delivery_detail(project["id"], first["id"])
    assert last["status"] == "failed" and len(last["attempts"]) == 2
    assert not worker.run_once()
    assert any(record.exc_info for record in caplog.records)


def test_policy_rejection_is_permanent(store, project, endpoint, monkeypatch):
    store.create_event(project["id"], "test", {})
    monkeypatch.setattr(
        module,
        "deliver",
        lambda *a, **kw: DeliveryResult(
            None, "Endpoint resolves to a nonpublic address", 1, "", permanent=True
        ),
    )
    Worker(store).run_once()
    row = store.list_deliveries(project["id"])["items"][0]
    assert row["status"] == "failed" and row["attempt_count"] == 1
    assert row["next_attempt_at"] is None
    assert "nonpublic" in row["last_error"]


def test_exhausted_dns_capacity_never_spends_attempts_or_cools_down_receiver(
    store, project, endpoint, monkeypatch
):
    store.create_event(project["id"], "test", {})
    monkeypatch.setattr(delivery_module, "_DNS_SLOTS", threading.BoundedSemaphore(0))
    monkeypatch.setattr(delivery_module, "_DNS_PROJECT_COUNTS", {})
    requests = []
    monkeypatch.setattr(
        delivery_module._PinnedConnection, "request", lambda *a, **kw: requests.append(True)
    )
    worker = Worker(store)
    for _ in range(12):
        with store._db(True) as db:
            db.execute("UPDATE deliveries SET next_attempt_at=0")
            db.execute("UPDATE endpoints SET next_available_at=0")
        assert worker.run_once()
        assert not worker.run_once()  # Deferred work has a delay, not a busy loop.
    row = store.list_deliveries(project["id"])["items"][0]
    detail = store.delivery_detail(project["id"], row["id"])
    assert row["status"] == "pending" and row["attempt_count"] == row["cycle_attempts"] == 0
    assert detail["attempts"] == [] and requests == []
    with store._db() as db:
        ep = db.execute("SELECT consecutive_timeouts,cooldown_until FROM endpoints").fetchone()
        assert tuple(ep) == (0, 0)
    assert store.deferral_stats()["dns_global_capacity"]["count"] == 12


def test_known_hung_endpoints_leave_delivery_capacity_for_healthy_work(store, monkeypatch):
    bad = []
    for index in range(4):
        owner = store.create_project(f"Hung {index}")
        store.create_endpoint(owner["id"], f"https://hung{index}.example/hook", rate_limit=1000)
        store.create_event(owner["id"], "test", {})
        bad.append(owner["id"])
    with store._db(True) as db:
        db.execute("UPDATE endpoints SET consecutive_timeouts=1")
    good = store.create_project("Healthy")
    store.create_endpoint(good["id"], "https://healthy.example/hook", rate_limit=1000)
    store.create_event(good["id"], "test", {})
    entered, release, healthy = threading.Event(), threading.Event(), threading.Event()
    hung_calls = []

    def receiver(url, *args, **kwargs):
        if "hung" in url:
            hung_calls.append(url)
            entered.set()
            assert release.wait(5)
            return DeliveryResult(None, "Timeout", 1, "", timed_out=True)
        healthy.set()
        return DeliveryResult(204, None, 1, "")

    monkeypatch.setattr(module, "deliver", receiver)
    worker = Worker(store, concurrency=4, poll_interval=0.01)
    worker.start()
    try:
        assert entered.wait(2)
        assert healthy.wait(2)
        assert len(hung_calls) == 1
    finally:
        release.set()
        worker.stop()


def test_timeouts_put_endpoint_into_persistent_cooldown(store, project, endpoint, monkeypatch):
    store.create_event(project["id"], "test", {})
    monkeypatch.setattr(
        module,
        "deliver",
        lambda *a, **kw: DeliveryResult(None, "Delivery deadline exceeded", 1, "", timed_out=True),
    )
    worker = Worker(store, retry_base=0)
    for _ in range(3):
        with store._db(True) as db:
            db.execute("UPDATE endpoints SET next_available_at=0")
        assert worker.run_once()
    assert not worker.run_once()
    with store._db() as db:
        row = db.execute(
            "SELECT cooldown_until FROM endpoints WHERE id=?", (endpoint["id"],)
        ).fetchone()
        assert row[0] > time.time() + 25


def test_unhealthy_project_does_not_occupy_every_worker(store, monkeypatch):
    bad = store.create_project("Unhealthy")
    good = store.create_project("Healthy")
    for i in range(3):
        store.create_endpoint(bad["id"], f"https://bad-{i}.example/hook", rate_limit=1000)
    store.create_endpoint(good["id"], "https://good.example/hook", rate_limit=1000)
    for _ in range(4):
        store.create_event(bad["id"], "test", {})
    release = threading.Event()
    busy = threading.Event()
    healthy_delivered = threading.Event()
    lock = threading.Lock()
    blocked = []

    def receiver(url, *args, **kwargs):
        if "bad-" in url:
            with lock:
                blocked.append(url)
                if len(blocked) == 2:
                    busy.set()
            release.wait(10)
        else:
            healthy_delivered.set()
        return DeliveryResult(204, None, 1, "")

    monkeypatch.setattr(module, "deliver", receiver)
    worker = Worker(store, concurrency=4, poll_interval=0.01)
    worker.start()
    try:
        assert busy.wait(5), "Two unhealthy destinations should be in flight"
        store.create_event(good["id"], "test", {})
        assert healthy_delivered.wait(5), (
            "Healthy work must progress while bad receivers remain blocked"
        )
        with lock:
            assert len(blocked) == 2
            assert len(set(blocked)) == 2
    finally:
        release.set()
        worker.stop()
