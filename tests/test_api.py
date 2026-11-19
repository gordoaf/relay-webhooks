import pytest
import json
from fastapi.testclient import TestClient
from relay.app import create_app
from relay.config import Settings

TOKEN = "test-admin-token-with-at-least-24-characters"


@pytest.fixture
def client(tmp_path):
    app = create_app(
        Settings(
            db_path=str(tmp_path / "api.db"),
            admin_token=TOKEN,
            worker_enabled=False,
            allow_private_networks=True,
        )
    )
    with TestClient(app) as client:
        yield client


def admin_headers():
    return {"Authorization": "Bearer " + TOKEN}


def make_project(client, name="Test"):
    return client.post("/api/projects", headers=admin_headers(), json={"name": name}).json()


def test_admin_auth_and_csrf(client):
    assert client.get("/api/projects").status_code == 401
    assert client.post("/api/session", json={"token": "bad"}).status_code == 401
    response = client.post("/api/session", json={"token": TOKEN})
    assert response.status_code == 200
    assert (
        "HttpOnly" in response.headers["set-cookie"]
        and "SameSite=strict" in response.headers["set-cookie"]
    )
    assert TOKEN not in response.headers["set-cookie"]
    assert client.get("/api/projects").status_code == 200
    assert client.post("/api/projects", json={"name": "Unsafe"}).status_code == 403
    assert (
        client.post(
            "/api/projects",
            headers={"Origin": "https://evil.example", "X-Relay-Request": "1"},
            json={"name": "Unsafe"},
        ).status_code
        == 403
    )
    assert (
        client.post(
            "/api/projects", headers={"X-Relay-Request": "1"}, json={"name": "Valid"}
        ).status_code
        == 201
    )


def test_ingest_auth_idempotency_and_project_scope(client):
    p = make_project(client)
    q = make_project(client, "Other")
    ep = client.post(
        f"/api/projects/{p['id']}/endpoints",
        headers=admin_headers(),
        json={"url": "http://127.0.0.1:9999/hook"},
    ).json()
    assert "signing_secret" in ep
    headers = {"Authorization": "Bearer " + p["api_key"], "Idempotency-Key": "unique"}
    body = {"type": "order.created", "payload": {"id": 12}}
    assert client.post("/v1/events", json=body).status_code == 401
    response = client.post("/v1/events", headers=headers, json=body)
    assert response.status_code == 202 and response.json()["event"]["delivery_count"] == 1
    assert client.post("/v1/events", headers=headers, json=body).status_code == 200
    body["payload"]["id"] = 13
    assert client.post("/v1/events", headers=headers, json=body).status_code == 409
    rows = client.get(f"/api/projects/{p['id']}/deliveries", headers=admin_headers()).json()
    did = rows["items"][0]["id"]
    assert (
        client.get(f"/api/projects/{q['id']}/deliveries/{did}", headers=admin_headers()).status_code
        == 404
    )
    assert (
        client.get("/api/projects", headers={"Authorization": "Bearer " + p["api_key"]}).status_code
        == 401
    )
    assert (
        ep["signing_secret"]
        not in client.get(f"/api/projects/{p['id']}/endpoints", headers=admin_headers()).text
    )


def test_large_chunked_body_rejected(client):
    def chunks():
        for _ in range(50):
            yield b"x" * 10000

    assert client.post("/v1/events", content=chunks()).status_code == 413


def test_invalid_input_and_security_headers(client):
    p = make_project(client)
    assert (
        client.get(
            f"/api/projects/{p['id']}/deliveries?status=bogus", headers=admin_headers()
        ).status_code
        == 422
    )
    assert (
        client.post("/api/projects", headers=admin_headers(), json={"name": "   "}).status_code
        == 422
    )
    assert client.post("/api/demo/setup", headers=admin_headers()).status_code == 404
    response = client.get("/api/projects", headers=admin_headers())
    assert response.headers["cache-control"] == "no-store"
    assert response.headers["x-content-type-options"] == "nosniff"
    assert client.get("/").status_code == 200
    assert client.get("/assets/app.js").status_code == 200


def test_private_destination_blocked_by_default(tmp_path):
    with TestClient(
        create_app(
            Settings(db_path=str(tmp_path / "secure.db"), admin_token=TOKEN, worker_enabled=False)
        )
    ) as c:
        p = make_project(c)
        assert (
            c.post(
                f"/api/projects/{p['id']}/endpoints",
                headers=admin_headers(),
                json={"url": "http://127.0.0.1:8888/"},
            ).status_code
            == 422
        )


def test_key_rotation_revokes_old_key(client):
    p = make_project(client)
    changed = client.post(f"/api/projects/{p['id']}/rotate-key", headers=admin_headers()).json()
    payload = {"type": "test", "payload": {}}
    assert (
        client.post(
            "/v1/events", headers={"Authorization": "Bearer " + p["api_key"]}, json=payload
        ).status_code
        == 401
    )
    assert (
        client.post(
            "/v1/events", headers={"Authorization": "Bearer " + changed["api_key"]}, json=payload
        ).status_code
        == 202
    )


@pytest.mark.parametrize("invalid", ["\ud800", "\udfff", float("inf"), float("nan")])
def test_invalid_unicode_and_nonfinite_numbers_are_rejected(client, invalid):
    p = make_project(client)
    headers = {"Authorization": "Bearer " + p["api_key"], "Content-Type": "application/json"}
    response = client.post(
        "/v1/events",
        headers=headers,
        content=json.dumps({"type": "test", "payload": {"value": invalid}}),
    )
    assert response.status_code == 422
    assert client.app.state.store.stats(p["id"])["total"] == 0


def test_invalid_unicode_in_admin_fields_is_rejected(client):
    for path, body in [
        ("/api/projects", {"name": "\ud800"}),
        ("/api/session", {"token": "\ud800"}),
    ]:
        response = client.post(
            path,
            headers={**admin_headers(), "Content-Type": "application/json"},
            content=json.dumps(body),
        )
        assert response.status_code == 422
