import time
import json
import pytest
import relay.worker as module
from relay.delivery import DeliveryResult, verify_signature
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
