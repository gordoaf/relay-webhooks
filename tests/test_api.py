import pytest
import json
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from fastapi.testclient import TestClient
from relay.app import COOKIE, create_app
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


@pytest.mark.parametrize(
    "media_type",
    ["Application/JSON", "application/json ", "APPLICATION/VND.RELAY+JSON; charset=utf-8", None],
)
@pytest.mark.parametrize(
    "path,body", [("/api/session", {"token": "\ud800"}), ("/api/projects", {"name": "\udfff"})]
)
def test_json_media_type_variants_cannot_bypass_unicode_validation(client, media_type, path, body):
    headers = admin_headers()
    if media_type:
        headers["Content-Type"] = media_type
    response = client.post(path, headers=headers, content=json.dumps(body))
    assert response.status_code == 422


@pytest.mark.parametrize("media_type", ["text/plain", "application/octet-stream"])
def test_validation_errors_do_not_echo_untrusted_input(client, media_type):
    response = client.post(
        "/api/session",
        content=b'{"token":"secret-token-should-not-be-echoed","value":"\\ud800"}',
        headers={"Content-Type": media_type},
    )
    assert response.status_code == 422
    assert "secret-token-should-not-be-echoed" not in response.text
    assert response.json()["detail"]


def test_huge_pagination_offset_rejected(client):
    project = make_project(client)
    path = f"/api/projects/{project['id']}/deliveries"
    assert client.get(path, params={"offset": 2**63}, headers=admin_headers()).status_code == 422
    assert (
        client.get(path, params={"offset": 2**63 - 1}, headers=admin_headers()).status_code == 200
    )


def test_payload_value_error_is_not_an_idempotency_conflict(client, monkeypatch):
    project = make_project(client)

    def invalid_payload(*_args):
        raise ValueError("Invalid payload")

    monkeypatch.setattr(client.app.state.store, "create_event", invalid_payload)
    response = client.post(
        "/v1/events",
        headers={"Authorization": "Bearer " + project["api_key"]},
        json={"type": "test", "payload": {}},
    )
    assert response.status_code == 422
    assert "Idempotency" not in response.text


def test_public_origin_and_secure_cookie_behind_proxy(tmp_path):
    settings = Settings(
        db_path=str(tmp_path / "proxy.db"),
        admin_token=TOKEN,
        worker_enabled=False,
        public_url="https://Relay.Example:443/",
    )
    assert settings.public_url == "https://relay.example"
    with TestClient(create_app(settings), base_url="http://backend.internal:8787") as client:
        response = client.post(
            "/api/session",
            json={"token": TOKEN},
            headers={"Origin": "https://relay.example", "X-Forwarded-Proto": "http"},
        )
        assert response.status_code == 200
        assert "Secure" in response.headers["set-cookie"]
        cookie = response.cookies.get(COOKIE)
        assert (
            client.post(
                "/api/projects",
                json={"name": "Proxy"},
                headers={"Origin": "https://relay.example", "Cookie": f"{COOKIE}={cookie}"},
            ).status_code
            == 201
        )
        for origin in ("http://backend.internal:8787", "https://evil.example", "null"):
            assert (
                client.post(
                    "/api/session",
                    json={"token": TOKEN},
                    headers={"Origin": origin},
                ).status_code
                == 403
            )


def test_forwarded_headers_do_not_override_unconfigured_origin(client):
    response = client.post(
        "/api/session",
        json={"token": TOKEN},
        headers={
            "Origin": "https://evil.example",
            "X-Forwarded-Proto": "https",
            "X-Forwarded-Host": "evil.example",
        },
    )
    assert response.status_code == 403


@pytest.mark.parametrize(
    "url",
    [
        "ftp://relay.example",
        "https://relay.example/path",
        "https://user:password@relay.example",
        "https://relay.example?x=1",
        "https://relay.example#fragment",
        "https://relay.example:65536",
        "https://relay.example:0",
        "https://relay.example\\evil",
        "https://relay.example\n",
        "\x00https://relay.example",
        "https://relay.example\x7f",
        "https://relay.example?",
        "https://relay.example#",
        "https://relay.example/<bad>",
        "https://<bad>.example",
        "https://[fe80::1%25eth0]",
    ],
)
def test_public_origin_must_be_a_valid_origin(url):
    with pytest.raises(ValueError, match="RELAY_PUBLIC_URL"):
        Settings(public_url=url)


def test_public_origin_environment_and_local_port(monkeypatch):
    monkeypatch.setenv("RELAY_PUBLIC_URL", "http://localhost:8787/")
    assert Settings.from_env().public_url == "http://localhost:8787"
    assert Settings(public_url="https://[2001:db8::1]:443").public_url == "https://[2001:db8::1]"


def test_successful_logins_do_not_exhaust_authentication_limit(client):
    for _ in range(12):
        assert client.post("/api/session", json={"token": TOKEN}).status_code == 200
    assert client.post("/api/session", json={"token": "wrong"}).status_code == 401


@pytest.mark.parametrize("first_path", ["bearer", "login"])
def test_failed_auth_limit_is_shared_and_cannot_lock_out_valid_credentials(client, first_path):
    for _ in range(10):
        if first_path == "bearer":
            response = client.get("/api/projects", headers={"Authorization": "Bearer wrong"})
        else:
            response = client.post("/api/session", json={"token": "wrong"})
        assert response.status_code == 401
    assert client.get("/api/projects", headers={"Authorization": "Bearer wrong"}).status_code == 429
    response = client.post("/api/session", json={"token": "wrong"})
    assert response.status_code == 429
    assert response.headers["retry-after"] == "60"
    assert client.get("/api/projects", headers=admin_headers()).status_code == 200
    assert client.post("/api/session", json={"token": TOKEN}).status_code == 200
    assert client.get("/api/projects").status_code == 200


@pytest.mark.parametrize("with_bearer", [False, True])
def test_logout_revokes_copied_cookie_across_instances(client, with_bearer):
    assert client.post("/api/session", json={"token": TOKEN}).status_code == 200
    cookie = client.cookies.get(COOKIE)
    copied = {"Cookie": f"{COOKIE}={cookie}"}
    with TestClient(create_app(client.app.state.settings)) as other:
        assert other.get("/api/projects", headers=copied).status_code == 200
        headers = {"X-Relay-Request": "1", **(admin_headers() if with_bearer else {})}
        assert client.delete("/api/session", headers=headers).status_code == 200
        assert other.get("/api/projects", headers=copied).status_code == 401


def test_admin_token_rotation_invalidates_existing_sessions(client):
    assert client.post("/api/session", json={"token": TOKEN}).status_code == 200
    cookie = client.cookies.get(COOKIE)
    settings = replace(
        client.app.state.settings, admin_token="replacement-admin-token-at-least-24-characters"
    )
    with TestClient(create_app(settings)) as other:
        assert (
            other.get("/api/projects", headers={"Cookie": f"{COOKIE}={cookie}"}).status_code == 401
        )


@pytest.mark.parametrize("operation", ["endpoint", "login"])
def test_contended_async_database_write_does_not_block_other_requests(
    client, monkeypatch, operation
):
    project = make_project(client)
    store = client.app.state.store
    method = "create_endpoint" if operation == "endpoint" else "create_session"
    original = getattr(store, method)
    entered = threading.Event()

    def contended_write(*args):
        entered.set()
        return original(*args)

    monkeypatch.setattr(store, method, contended_write)
    db = sqlite3.connect(store.path, isolation_level=None)
    db.execute("BEGIN IMMEDIATE")
    with ThreadPoolExecutor(max_workers=2) as pool:
        try:
            if operation == "endpoint":
                pending = pool.submit(
                    client.post,
                    f"/api/projects/{project['id']}/endpoints",
                    headers=admin_headers(),
                    json={"url": "http://127.0.0.1:9999/hook"},
                )
            else:
                pending = pool.submit(client.post, "/api/session", json={"token": TOKEN})
            assert entered.wait(timeout=2)
            health = pool.submit(client.get, "/healthz")
            assert health.result(timeout=2).status_code == 200
            assert not pending.done()
        finally:
            db.execute("ROLLBACK")
            db.close()
        assert pending.result(timeout=2).status_code in {200, 201}
