"""Operator workflows exercised through the authenticated HTTP boundary."""

import time

import pytest
from fastapi.testclient import TestClient

from relay.app import create_app
from relay.config import Settings

TOKEN = "operator-test-token-with-at-least-32-characters"
ADMIN = {"Authorization": "Bearer " + TOKEN}


@pytest.fixture
def client(tmp_path):
    settings = Settings(
        db_path=str(tmp_path / "features.db"),
        admin_token=TOKEN,
        worker_enabled=False,
        maintenance_enabled=False,
        allow_private_networks=True,
        retention_days=30,
    )
    with TestClient(create_app(settings)) as result:
        yield result


def project(client):
    response = client.post("/api/projects", headers=ADMIN, json={"name": "Shop"})
    assert response.status_code == 201
    return response.json()


def endpoint(client, owner, subscriptions=None):
    response = client.post(
        f"/api/projects/{owner['id']}/endpoints",
        headers=ADMIN,
        json={"url": "http://127.0.0.1:9999/hooks", "subscriptions": subscriptions or ["*"]},
    )
    assert response.status_code == 201
    return response.json()


def emit(client, owner, event_type="order.created", key="order-1"):
    return client.post(
        "/v1/events",
        headers={"Authorization": "Bearer " + owner["api_key"], "Idempotency-Key": key},
        json={"type": event_type, "payload": {"order": 1}},
    )


def test_subscriptions_editing_and_secret_redaction(client):
    owner = project(client)
    ep = endpoint(client, owner, ["order.*"])
    path = f"/api/projects/{owner['id']}/endpoints/{ep['id']}"
    assert emit(client, owner).json()["event"]["delivery_count"] == 1
    assert emit(client, owner, "invoice.paid", "invoice-1").json()["event"]["delivery_count"] == 0
    changed = client.patch(
        path,
        headers=ADMIN,
        json={
            "url": "http://127.0.0.1:9998/next",
            "rate_limit": 2,
            "subscriptions": ["invoice.paid"],
            "description": "Accounting",
        },
    )
    assert changed.status_code == 200
    assert changed.json()["subscriptions"] == ["invoice.paid"]
    assert emit(client, owner, "invoice.paid", "invoice-2").json()["event"]["delivery_count"] == 1
    rotated = client.post(path + "/rotate-secret", headers=ADMIN, json={"overlap_seconds": 60})
    assert rotated.status_code == 200
    assert rotated.json()["previous_valid_until"] > time.time()
    listing = client.get(path.rsplit("/", 1)[0], headers=ADMIN).text
    assert ep["signing_secret"] not in listing
    assert rotated.json()["signing_secret"] not in listing
    for invalid in ({}, {"url": None}, {"subscriptions": ["order*bad"]}, {"rate_limit": 0}):
        assert client.patch(path, headers=ADMIN, json=invalid).status_code == 422
    assert (
        client.post(
            path + "/rotate-secret", headers=ADMIN, json={"overlap_seconds": -1}
        ).status_code
        == 422
    )


def test_project_key_overlap_can_be_revoked(client):
    owner = project(client)
    path = f"/api/projects/{owner['id']}/rotate-key"
    changed = client.post(path, headers=ADMIN, json={"overlap_seconds": 60}).json()
    assert emit(client, owner).status_code == 202
    new_owner = {**owner, "api_key": changed["api_key"]}
    assert emit(client, new_owner).status_code == 200
    client.post(path, headers=ADMIN, json={"overlap_seconds": 0})
    assert emit(client, owner).status_code == 401
    assert emit(client, new_owner).status_code == 401


def test_recovery_preview_progress_cancellation_and_scope(client):
    owner, other = project(client), project(client)
    endpoint(client, owner)
    for i in range(3):
        assert emit(client, owner, key=f"order-{i}").status_code == 202
    store = client.app.state.store
    with store._db(True) as db:
        db.execute("UPDATE deliveries SET status='failed',next_attempt_at=NULL")
    path = f"/api/projects/{owner['id']}/recoveries"
    preview = client.post(path + "/preview", headers=ADMIN, json={"limit": 2})
    assert preview.status_code == 200
    assert preview.json()["total"] == 3
    assert preview.json()["selected_count"] == 2
    body = {"preview_id": preview.json()["preview_id"], "pace_per_second": 1}
    assert (
        client.post(f"/api/projects/{other['id']}/recoveries", headers=ADMIN, json=body).status_code
        == 422
    )
    created = client.post(path, headers=ADMIN, json=body)
    assert created.status_code == 201
    job = created.json()
    assert client.post(path, headers=ADMIN, json=body).json()["id"] == job["id"]
    store.advance_recoveries()
    detail = client.get(path + "/" + job["id"], headers=ADMIN).json()
    assert detail["requeued"] == 1
    assert detail["queued"] == 1
    cancelled = client.post(path + "/" + job["id"] + "/cancel", headers=ADMIN).json()
    assert cancelled["cancelled"] == 1
    assert cancelled["status"] == "cancelled"
    assert (
        client.get(f"/api/projects/{other['id']}/recoveries/{job['id']}", headers=ADMIN).status_code
        == 404
    )
    assert (
        client.post(path + "/preview", headers=ADMIN, json={"since": 100, "until": 10}).status_code
        == 422
    )


def test_metrics_and_cleanup_are_admin_only_and_redacted(client):
    owner = project(client)
    for path in ["/metrics", "/api/operations", f"/api/projects/{owner['id']}/metrics"]:
        assert client.get(path).status_code == 401
        assert (
            client.get(path, headers={"Authorization": "Bearer " + owner["api_key"]}).status_code
            == 401
        )
        response = client.get(path, headers=ADMIN)
        assert response.status_code == 200
        assert response.headers["cache-control"] == "no-store"
        assert TOKEN not in response.text
        assert owner["api_key"] not in response.text
    metrics = client.get("/metrics", headers=ADMIN)
    assert "relay_queue_oldest_age_seconds" in metrics.text
    assert "text/plain" in metrics.headers["content-type"]
    assert client.post("/api/maintenance/cleanup", json={"dry_run": False}).status_code == 401
    snapshot = client.get(f"/api/projects/{owner['id']}/metrics", headers=ADMIN).json()
    assert snapshot["queue"]["depth"] == 0
    assert "alert_signing_secret" not in snapshot["policy"]


def test_expired_payload_keeps_idempotency_contract(client):
    owner = project(client)
    event = emit(client, owner).json()["event"]  # No destinations; safe to retain/purge.
    with client.app.state.store._db(True) as db:
        db.execute("UPDATE events SET created_at=?", (time.time() - 31 * 86400,))
    preview = client.post("/api/maintenance/cleanup", headers=ADMIN, json={"dry_run": True})
    assert preview.json()["eligible_events"] == 1
    assert emit(client, owner).status_code == 200
    result = client.post("/api/maintenance/cleanup", headers=ADMIN, json={"dry_run": False})
    assert result.json()["deleted_events"] == 1
    duplicate = emit(client, owner)
    assert duplicate.status_code == 410
    assert duplicate.json()["event_id"] == event["id"]
    assert emit(client, owner, event_type="order.changed").status_code == 409


def test_capacity_rejects_new_events_but_allows_idempotent_reads(client):
    from relay.operations import Operations, OperationsPolicy

    owner = project(client)
    assert emit(client, owner).status_code == 202
    Operations(client.app.state.store, OperationsPolicy(max_storage_bytes=1))
    assert emit(client, owner).status_code == 200
    response = emit(client, owner, key="new-order")
    assert response.status_code == 503
    assert response.headers["retry-after"] == "30"
    with client.app.state.store._db() as db:
        assert db.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 1


def test_edited_destinations_obey_network_policy(tmp_path):
    with TestClient(
        create_app(
            Settings(
                db_path=str(tmp_path / "public.db"),
                admin_token=TOKEN,
                worker_enabled=False,
                maintenance_enabled=False,
            )
        )
    ) as client:
        owner = project(client)
        ep = client.app.state.store.create_endpoint(owner["id"], "https://example.com/webhook")
        response = client.patch(
            f"/api/projects/{owner['id']}/endpoints/{ep['id']}",
            headers=ADMIN,
            json={"url": "http://127.0.0.1:9999/private"},
        )
        assert response.status_code == 422
        assert client.app.state.store.list_endpoints(owner["id"])[0]["url"] == ep["url"]
