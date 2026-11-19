"""Admission and monitoring regressions from adversarial review."""

import pytest
from fastapi.testclient import TestClient

from relay.app import create_app, make_worker
from relay.config import Settings
from relay.delivery import DeliveryCapacityUnavailable, DeliveryResult
from relay.events import serialize_event

ADMIN = "test-administrator-with-at-least-32-characters"
METRICS = "test-monitoring-token-with-at-least-32-characters"


def settings(tmp_path, **kwargs):
    return Settings(
        db_path=str(tmp_path / "relay.db"),
        admin_token=ADMIN,
        worker_enabled=False,
        maintenance_enabled=False,
        **kwargs,
    )


@pytest.mark.parametrize("console", [False, True])
def test_expanded_delivery_is_rejected_before_acceptance(tmp_path, console):
    with TestClient(create_app(settings(tmp_path))) as client:
        store = client.app.state.store
        project = store.create_project("Expansion")
        store.create_endpoint(project["id"], "https://example.com/hook")
        path = f"/api/projects/{project['id']}/events" if console else "/v1/events"
        raw = b'{"type":"t","payload":{"x":[' + b",".join([b"1e15"] * 52000) + b"]}}"
        assert len(raw) == 260030
        response = client.post(
            path,
            content=raw,
            headers={
                "Authorization": "Bearer " + (ADMIN if console else project["api_key"]),
                "Content-Type": "application/json",
                "Idempotency-Key": "oversized",
            },
        )
        assert response.status_code == 413
        assert "not accepted" in response.json()["detail"]
        with store._db() as db:
            assert db.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 0
            assert db.execute("SELECT COUNT(*) FROM deliveries").fetchone()[0] == 0
        response = client.post(
            "/v1/events",
            json={"type": "t", "payload": {}},
            headers={
                "Authorization": "Bearer " + project["api_key"],
                "Idempotency-Key": "oversized",
            },
        )
        assert response.status_code == 202


def test_size_boundary_matches_actual_signed_envelope(tmp_path, monkeypatch):
    monkeypatch.setattr("relay.store.time.time", lambda: 1700000000.0)
    config = settings(tmp_path, max_payload_bytes=2048)
    with TestClient(create_app(config)) as client:
        store = client.app.state.store
        owner = store.create_project("Boundary")
        store.create_endpoint(owner["id"], "https://example.com/hook")
        overhead = len(serialize_event("evt_" + "0" * 24, "test", 1700000000.0, {"text": ""}))
        body = {"type": "test", "payload": {"text": "x" * (2048 - overhead)}}
        headers = {"Authorization": "Bearer " + owner["api_key"]}
        assert client.post("/v1/events", json=body, headers=headers).status_code == 202
        sent = []

        def deliver(_url, raw, _headers, **_kwargs):
            sent.append(raw)
            return DeliveryResult(204, None, 1, "")

        monkeypatch.setattr("relay.worker.deliver", deliver)
        assert make_worker(store, config).run_once()
        assert len(sent[0]) == 2048
        body["payload"]["text"] += "x"
        assert client.post("/v1/events", json=body, headers=headers).status_code == 413


def test_existing_acceptance_survives_tighter_size_limit(tmp_path):
    with TestClient(create_app(settings(tmp_path, max_payload_bytes=2048))) as client:
        store = client.app.state.store
        owner = store.create_project("Existing events")
        store.max_delivery_bytes = 10000
        event, _ = store.create_event(owner["id"], "test", {"x": [1e15] * 200}, "existing")
        store.max_delivery_bytes = 2048
        raw = b'{"type":"test","payload":{"x":[' + b",".join([b"1e15"] * 200) + b"]}}"
        response = client.post(
            "/v1/events",
            content=raw,
            headers={
                "Authorization": "Bearer " + owner["api_key"],
                "Content-Type": "application/json",
                "Idempotency-Key": "existing",
            },
        )
        assert response.status_code == 200
        assert response.json()["event"]["id"] == event["id"]


def test_metrics_token_has_only_scrape_access_and_rotates(tmp_path):
    config = settings(tmp_path, metrics_token=METRICS)
    with TestClient(create_app(config)) as client:
        owner = client.app.state.store.create_project("Monitored")
        headers = {"Authorization": "Bearer " + METRICS}
        for method in (client.get, client.head):
            response = method("/metrics", headers=headers)
            assert response.status_code == 200
            assert response.headers["cache-control"] == "no-store"
        for path in ("/api/projects", "/api/session", "/api/operations", "/api/openapi.json"):
            assert client.get(path, headers=headers).status_code == 401
        assert (
            client.post("/api/projects", json={"name": "Denied"}, headers=headers).status_code
            == 401
        )
        assert client.post("/api/session", json={"token": METRICS}).status_code == 401
        assert (
            client.post(
                "/v1/events", json={"type": "test", "payload": {}}, headers=headers
            ).status_code
            == 401
        )
        assert (
            client.get(
                "/metrics", headers={"Authorization": "Bearer " + owner["api_key"]}
            ).status_code
            == 401
        )
        assert client.get("/metrics").status_code == 401
        assert (
            client.get("/metrics", headers={"Authorization": "Bearer " + ADMIN}).status_code == 200
        )
        summary = client.get("/api/operations", headers={"Authorization": "Bearer " + ADMIN})
        assert METRICS not in summary.text
    new_token = METRICS + "-rotated"
    with TestClient(create_app(settings(tmp_path, metrics_token=new_token))) as client:
        assert (
            client.get("/metrics", headers={"Authorization": "Bearer " + METRICS}).status_code
            == 401
        )
        assert (
            client.get("/metrics", headers={"Authorization": "Bearer " + new_token}).status_code
            == 200
        )


def test_worker_and_metrics_environment(monkeypatch, tmp_path):
    monkeypatch.setenv("RELAY_WORKER_CONCURRENCY", "8")
    monkeypatch.setenv("RELAY_METRICS_TOKEN", METRICS)
    config = Settings.from_env()
    assert config.worker_concurrency == 8 and config.metrics_token == METRICS
    assert make_worker(None, config).concurrency == 8
    for invalid in (0, 65, True, 1.5):
        with pytest.raises(ValueError, match="CONCURRENCY"):
            settings(tmp_path, worker_concurrency=invalid)
    for invalid in ("short", ADMIN, " " * 40):
        with pytest.raises(ValueError, match="METRICS_TOKEN"):
            settings(tmp_path, metrics_token=invalid)


def test_alert_rearm_configuration_reaches_operations_api(monkeypatch, tmp_path):
    monkeypatch.setenv("RELAY_ALERT_REARM_SECONDS", "600")
    config = Settings.from_env()
    assert config.alert_rearm_seconds == 600
    assert config.operations_policy().alert_rearm_seconds == 600
    with TestClient(create_app(settings(tmp_path, alert_rearm_seconds=600))) as client:
        response = client.get("/api/operations", headers={"Authorization": "Bearer " + ADMIN})
        assert response.status_code == 200
        assert response.json()["policy"]["alert_rearm_seconds"] == 600
    for invalid in (0, -1, True, float("inf"), float("nan")):
        with pytest.raises(ValueError, match="alert_rearm_seconds"):
            settings(tmp_path, alert_rearm_seconds=invalid)


def test_endpoint_validation_reports_temporary_resolver_pressure(tmp_path, monkeypatch):
    with TestClient(create_app(settings(tmp_path))) as client:
        store = client.app.state.store
        owner = store.create_project("Validation")
        endpoint = store.create_endpoint(owner["id"], "https://example.com/original")

        def unavailable(*_args):
            raise DeliveryCapacityUnavailable("dns_global_capacity")

        monkeypatch.setattr("relay.app.validate_endpoint", unavailable)
        path = f"/api/projects/{owner['id']}/endpoints"
        headers = {"Authorization": "Bearer " + ADMIN}
        response = client.post(path, headers=headers, json={"url": "https://example.com/new"})
        assert response.status_code == 503
        assert response.headers["retry-after"] == "1"
        response = client.patch(
            path + "/" + endpoint["id"],
            headers=headers,
            json={"url": "https://example.com/changed"},
        )
        assert response.status_code == 503
        assert store.list_endpoints(owner["id"])[0]["url"] == "https://example.com/original"
        assert len(store.list_endpoints(owner["id"])) == 1
