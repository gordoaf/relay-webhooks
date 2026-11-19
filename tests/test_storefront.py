import base64
import importlib.util
import json
import sqlite3
import threading
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from relay.client import RelayError, RelayEventExpired
from relay.delivery import sign_headers

spec = importlib.util.spec_from_file_location(
    "storefront_example",
    Path(__file__).parents[1] / "examples" / "storefront" / "app.py",
)
example = importlib.util.module_from_spec(spec)
spec.loader.exec_module(example)
SECRET = "whsec_" + base64.b64encode(b"s" * 32).decode()


@pytest.fixture
def storefront(tmp_path):
    store = example.Storefront(tmp_path / "storefront.db")
    store.configure(
        {
            "relay_url": "http://127.0.0.1:8787",
            "project_id": "prj_example",
            "api_key": "project_secret_never_in_html",
            "secrets": dict.fromkeys(example.SERVICES, SECRET),
            "port": 9000,
        }
    )
    return store


def test_order_and_outbox_are_one_transaction(storefront):
    with storefront.db() as db:
        db.execute("""CREATE TRIGGER fail_outbox BEFORE INSERT ON outbox
                      BEGIN SELECT RAISE(ABORT, 'simulated failure'); END""")
    with pytest.raises(sqlite3.IntegrityError):
        storefront.order("Alex", 2)
    assert storefront.state()["totals"]["orders"] == 0
    assert storefront.state()["outbox"] == []


def test_storefront_rejects_invalid_unicode_before_database_write(storefront):
    with pytest.raises(ValueError, match="Unicode"):
        storefront.order("\ud800", 1)
    with TestClient(
        example.create_app(storefront, run_sender=False), base_url="http://127.0.0.1:9000"
    ) as client:
        response = client.post(
            "/api/orders",
            content=b'{"customer":"\\ud800","quantity":1}',
            headers={"X-Storefront-Request": "1"},
        )
        assert response.status_code == 422
    assert storefront.state()["totals"]["orders"] == 0


def test_outbox_preserves_keys_and_business_operations_across_restart(storefront):
    order_id = storefront.order("Alex", 2)
    calls = []

    class Sender:
        fail = True

        def emit(self, event_type, payload, *, idempotency_key):
            calls.append((event_type, payload, idempotency_key))
            if self.fail:
                raise RelayError(
                    "Ambiguous connection failure", idempotency_key=idempotency_key, retryable=True
                )
            return {"created": False, "event": {"id": "evt_" + event_type}}

    sender = Sender()
    storefront.send_pending(sender, now=1000)
    assert len(storefront.state()["outbox"]) == 2
    reopened = example.Storefront(storefront.path)
    sender.fail = False
    reopened.send_pending(sender, now=1002)
    assert calls[:2] == calls[2:]
    assert {call[0] for call in calls} == {"order.created", "invoice.paid"}
    assert all(call[2].startswith(order_id + ":") for call in calls)
    assert reopened.state()["orders"][0]["pending_events"] == 0


def test_permanent_ingress_error_blocks_outbox_without_new_key(storefront):
    storefront.order("Alex", 1)

    class Conflict:
        def emit(self, _, payload, *, idempotency_key):
            raise RelayError("Conflict", idempotency_key=idempotency_key, status=409)

    storefront.send_pending(Conflict(), now=1000)
    assert all(entry["blocked"] for entry in storefront.state()["outbox"])


def test_expired_acceptance_completes_outbox_without_claiming_delivery(storefront):
    storefront.order("Already accepted", 1)

    class Expired:
        def emit(self, event_type, payload, *, idempotency_key):
            raise RelayEventExpired(idempotency_key=idempotency_key, event_id="evt_" + event_type)

    storefront.send_pending(Expired())
    state = storefront.state()
    assert state["outbox"] == [] and state["blocked_count"] == 0
    assert state["orders"][0]["expired_events"] == 2
    assert state["orders"][0]["pending_events"] == 0
    assert not state["orders"][0]["warehouse"] and not state["orders"][0]["accounting"]
    with storefront.db() as db:
        assert {row[0] for row in db.execute("SELECT event_id FROM outbox")} == {
            "evt_order.created",
            "evt_invoice.paid",
        }


def test_credential_refresh_and_blocked_resume_keep_original_keys(storefront, monkeypatch):
    storefront.order("Rotated key", 1)
    attempts = []

    class Sender:
        def __init__(self, base_url, api_key, **kwargs):
            self.key = api_key

        def emit(self, event_type, payload, *, idempotency_key):
            attempts.append((self.key, idempotency_key))
            if self.key != "new_project_key":
                raise RelayError("HTTP 401", idempotency_key=idempotency_key, status=401)
            return {"created": False, "event": {"id": "evt_" + event_type}}

    monkeypatch.setattr(example, "RelayClient", Sender)
    storefront.send_pending()
    assert storefront.state()["blocked_count"] == 2
    assert storefront.update_credentials("new_project_key", resume_blocked=True) == 2
    storefront.send_pending()
    assert storefront.state()["outbox"] == []
    assert [key for _, key in attempts[:2]] == [key for _, key in attempts[2:]]
    assert all(key == "new_project_key" for key, _ in attempts[2:])


def test_old_inflight_auth_failure_cannot_reblock_after_credential_refresh(storefront, monkeypatch):
    storefront.order("Rotating", 1)

    class Sender:
        def __init__(self, base_url, api_key, **kwargs):
            self.key = api_key

        def emit(self, event_type, payload, *, idempotency_key):
            if self.key != "new_project_key":
                storefront.update_credentials("new_project_key", resume_blocked=True)
                raise RelayError(
                    "Old in-flight HTTP 401", idempotency_key=idempotency_key, status=401
                )
            return {"created": True, "event": {"id": "evt_" + event_type}}

    monkeypatch.setattr(example, "RelayClient", Sender)
    storefront.send_pending(now=1000)
    assert storefront.state()["blocked_count"] == 0
    storefront.send_pending(now=1002)
    assert storefront.state()["outbox"] == []


def test_resume_api_is_guarded_and_does_not_change_conflicting_keys(storefront):
    storefront.order("Conflict", 1)
    with storefront.db() as db:
        db.execute("UPDATE outbox SET blocked=1,last_error='HTTP 409'")
        keys = [row[0] for row in db.execute("SELECT idempotency_key FROM outbox ORDER BY id")]
    with TestClient(
        example.create_app(storefront, run_sender=False), base_url="http://127.0.0.1:9000"
    ) as client:
        assert client.post("/api/outbox/resume", json={}).status_code == 403
        result = client.post("/api/outbox/resume", json={}, headers={"X-Storefront-Request": "1"})
        assert result.json() == {"resumed": 2}
    with storefront.db() as db:
        assert [
            row[0] for row in db.execute("SELECT idempotency_key FROM outbox ORDER BY id")
        ] == keys
    assert storefront.state()["blocked_count"] == 0


def test_receiver_signing_credentials_refresh_without_restart(storefront):
    order_id = storefront.order("Rotated secret", 1)
    new_secret = "whsec_" + base64.b64encode(b"n" * 32).decode()
    raw = json.dumps(
        {"id": "evt_rotation", "type": "order.created", "data": {"order_id": order_id}}
    ).encode()
    with TestClient(
        example.create_app(storefront, run_sender=False), base_url="http://127.0.0.1:9000"
    ) as client:
        storefront.update_credentials(signing_secrets={"warehouse": new_secret})
        assert (
            client.post(
                "/hooks/warehouse", content=raw, headers=sign_headers(SECRET, "evt_rotation", raw)
            ).status_code
            == 401
        )
        assert (
            client.post(
                "/hooks/warehouse",
                content=raw,
                headers=sign_headers(new_secret, "evt_rotation", raw),
            ).status_code
            == 200
        )
        assert new_secret not in client.get("/api/state").text


def test_invalid_credential_update_is_atomic_and_does_not_resume(storefront):
    storefront.order("Blocked", 1)
    with storefront.db() as db:
        db.execute("UPDATE outbox SET blocked=1")
    original = storefront.config()
    with pytest.raises(ValueError, match="signing secret"):
        storefront.update_credentials("new_project_key", {"warehouse": "invalid"}, True)
    assert storefront.config() == original
    assert storefront.state()["blocked_count"] == 2


def test_slow_outage_cannot_starve_later_orders_and_stop_interrupts_batch(storefront, monkeypatch):
    for _ in range(10):
        storefront.order("First batch", 1)
    last_order = storefront.order("Later order", 1)
    now, calls, stop = [1000.0], [], threading.Event()
    monkeypatch.setattr(example.time, "time", lambda: now[0])

    class SlowOutage:
        def emit(self, event_type, payload, *, idempotency_key):
            calls.append(payload["order_id"])
            now[0] += 3
            raise RelayError("Timed out", idempotency_key=idempotency_key, retryable=True)

    storefront.send_pending(SlowOutage())
    assert len(calls) == 20
    with storefront.db() as db:
        # The final failed row retries after its failure, not after the batch started.
        row = db.execute("SELECT next_attempt_at FROM outbox WHERE id=20").fetchone()
        assert row[0] > now[0]
    storefront.send_pending(SlowOutage())
    assert calls[20:22] == [last_order, last_order]

    class StopAfterOne(SlowOutage):
        def emit(self, *args, **kwargs):
            stop.set()
            return super().emit(*args, **kwargs)

    now[0] += 100
    before = len(calls)
    storefront.send_pending(StopAfterOne(), stop=stop)
    assert len(calls) == before + 1


def test_signed_receivers_fail_independently_and_deduplicate(storefront):
    order_id = storefront.order("Alex", 1)
    app = example.create_app(storefront, run_sender=False)
    with TestClient(app, base_url="http://127.0.0.1:9000") as client:
        storefront.set_online("warehouse", False)
        for service, event_type in example.SERVICES.items():
            raw = json.dumps(
                {"id": f"evt_{service}", "type": event_type, "data": {"order_id": order_id}}
            ).encode()
            headers = sign_headers(SECRET, f"evt_{service}", raw)
            assert (
                client.post(f"/hooks/{service}", content=raw + b" ", headers=headers).status_code
                == 401
            )
            result = client.post(f"/hooks/{service}", content=raw, headers=headers)
            assert result.status_code == (503 if service == "warehouse" else 200)
            if service == "warehouse":
                storefront.set_online(service, True)
            assert client.post(f"/hooks/{service}", content=raw, headers=headers).status_code == 200
            assert client.post(f"/hooks/{service}", content=raw, headers=headers).status_code == 200
    with storefront.db() as db:
        assert db.execute("SELECT count(*) FROM receipts").fetchone()[0] == 2
        assert db.execute("SELECT count(*) FROM fulfillment").fetchone()[0] == 2


def test_storefront_browser_boundary_and_no_secrets(storefront):
    app = example.create_app(storefront, run_sender=False)
    with TestClient(app, base_url="http://127.0.0.1:9000") as client:
        data = {"customer": "<script>bad()</script>", "quantity": 1}
        assert client.post("/api/orders", json=data).status_code == 403
        assert (
            client.post(
                "/api/orders",
                json=data,
                headers={
                    "X-Storefront-Request": "1",
                    "Origin": "https://other.example",
                },
            ).status_code
            == 403
        )
        assert (
            client.post(
                "/api/orders",
                json=data,
                headers={
                    "X-Storefront-Request": "1",
                    "Origin": "http://127.0.0.1:9000",
                },
            ).status_code
            == 201
        )
        assert (
            client.get("/api/state", headers={"Host": "attacker.example:9000"}).status_code == 403
        )
        for path in ["/", "/app.js", "/api/state"]:
            result = client.get(path)
            assert result.status_code == 200
            assert SECRET not in result.text
            assert "project_secret_never_in_html" not in result.text
        assert "frame-ancestors 'none'" in client.get("/").headers["Content-Security-Policy"]


def test_receiver_rejects_signed_wrong_subscription(storefront):
    order_id = storefront.order("Alex", 1)
    with pytest.raises(ValueError, match="subscription"):
        storefront.receive(
            "warehouse", {"id": "evt_bad", "type": "invoice.paid", "data": {"order_id": order_id}}
        )
