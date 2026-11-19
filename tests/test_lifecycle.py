import hashlib
import sqlite3
import time

import pytest

import relay.store as store_module
import relay.worker as worker_module
from relay.delivery import DeliveryResult, sign_headers, verify_signature
from relay.store import Store
from relay.worker import Worker


def complete(store, job, code=204):
    return store.complete_delivery(
        job["id"],
        job["lease_token"],
        status_code=code,
        error=None,
        duration_ms=1,
        response_excerpt="",
    )


def test_subscriptions_are_decided_once_and_namespace_matching_is_bounded(store, project):
    exact = store.create_endpoint(
        project["id"], "https://exact.example/", subscriptions=["order.created"]
    )
    namespace = store.create_endpoint(
        project["id"], "https://namespace.example/", subscriptions=["order.*"]
    )
    all_events = store.create_endpoint(project["id"], "https://all.example/")
    created, _ = store.create_event(project["id"], "order.created", {}, "stable")
    assert created["delivery_count"] == 3
    for event_type, count in [
        ("order.shipped", 2),
        ("ordering.created", 1),
        ("order", 1),
        ("order.", 1),
    ]:
        event, _ = store.create_event(project["id"], event_type, {})
        assert event["delivery_count"] == count
    store.update_endpoint(project["id"], exact["id"], subscriptions=["invoice.*"])
    store.update_endpoint(project["id"], namespace["id"], enabled=False)
    assert (
        store.create_event(project["id"], "order.created", {}, "stable")[0]["delivery_count"] == 3
    )
    assert store.create_event(project["id"], "order.created", {})[0]["delivery_count"] == 1
    assert len(store.list_deliveries(project["id"])["items"]) == 9
    assert all_events["subscriptions"] == ["*"]


@pytest.mark.parametrize(
    "patterns",
    [
        ["foo*"],
        ["*.created"],
        ["order.**"],
        "order.created",
        [None],
        ["bad space"],
        ["a"] * 101,
        ["a" * 129],
        ["1order.created"],
    ],
)
def test_invalid_subscriptions_are_rejected(store, project, patterns):
    with pytest.raises(ValueError, match="subscriptions"):
        store.create_endpoint(project["id"], "https://example.com/", subscriptions=patterns)


def test_empty_subscriptions_stop_new_fanout_but_preserve_queued_work(store, project, endpoint):
    original, _ = store.create_event(project["id"], "before", {})
    result = store.update_endpoint(project["id"], endpoint["id"], subscriptions=[])
    assert result["subscriptions"] == [] and result["enabled"]
    assert store.create_event(project["id"], "after", {})[0]["delivery_count"] == 0
    assert store.claim_delivery()["event_id"] == original["id"]
    store.update_endpoint(
        project["id"], endpoint["id"], subscriptions=["order:created", "invoice:paid.*"]
    )
    assert store.create_event(project["id"], "order:created", {})[0]["delivery_count"] == 1
    assert store.create_event(project["id"], "invoice:paid.usd", {})[0]["delivery_count"] == 1


def test_exact_subscriptions_accept_the_ingestion_event_type_grammar(store, project):
    endpoint = store.create_endpoint(
        project["id"], "https://receiver.example/", subscriptions=["a..b", "a."]
    )
    assert store.create_event(project["id"], "a..b", {})[0]["delivery_count"] == 1
    assert store.create_event(project["id"], "a.", {})[0]["delivery_count"] == 1
    assert store.create_event(project["id"], "a.c", {})[0]["delivery_count"] == 0
    store.update_endpoint(project["id"], endpoint["id"], subscriptions=["a..*"])
    assert store.create_event(project["id"], "a..b", {})[0]["delivery_count"] == 1
    store.update_endpoint(project["id"], endpoint["id"], subscriptions=["*"])
    assert store.create_event(project["id"], "anything", {})[0]["delivery_count"] == 1


def test_edits_preserve_attempt_destination_and_queue_uses_current_destination(
    store, project, endpoint
):
    store.create_event(project["id"], "test", {})
    claim = store.claim_delivery()
    changed = store.update_endpoint(
        project["id"],
        endpoint["id"],
        url="https://new.example/hook",
        description="Updated",
        rate_limit=100,
        subscriptions=["invoice.*"],
    )
    assert changed["url"] == "https://new.example/hook"
    assert complete(store, claim, 400)
    detail = store.delivery_detail(project["id"], claim["id"])
    assert detail["endpoint_url"] == endpoint["url"]
    assert detail["current_endpoint_url"] == changed["url"]
    assert detail["attempts"][0]["endpoint_url"] == endpoint["url"]
    assert store.replay_delivery(project["id"], claim["id"])
    with store._db(True) as db:
        db.execute("UPDATE endpoints SET next_available_at=0")
    retry = store.claim_delivery()
    assert retry["endpoint_url"] == changed["url"]
    assert complete(store, retry)
    detail = store.delivery_detail(project["id"], claim["id"])
    assert [a["endpoint_url"] for a in detail["attempts"]] == [endpoint["url"], changed["url"]]
    assert detail["attempt_count"] == 2


def test_worker_uses_both_signatures_until_overlap_expires(store, project, endpoint, monkeypatch):
    current_time = time.time()
    monkeypatch.setattr(store_module.time, "time", lambda: current_time)
    rotated = store.rotate_endpoint_secret(project["id"], endpoint["id"], 60)
    assert rotated["previous_valid_until"] == current_time + 60
    assert "signing_secret" not in store.list_endpoints(project["id"])[0]
    assert "previous_signing_secret" not in store.list_endpoints(project["id"])[0]
    calls = []

    def deliver(url, body, headers, **kwargs):
        calls.append((body, headers))
        return DeliveryResult(204, None, 1, "")

    monkeypatch.setattr(worker_module, "deliver", deliver)
    worker = Worker(store)
    store.create_event(project["id"], "first", {})
    worker.run_once()
    body, headers = calls[-1]
    assert len(headers["webhook-signature"].split()) == 2
    assert verify_signature(endpoint["signing_secret"], body, headers)
    assert verify_signature(rotated["signing_secret"], body, headers)
    current_time += 60
    store.create_event(project["id"], "second", {})
    worker.run_once()
    body, headers = calls[-1]
    assert len(headers["webhook-signature"].split()) == 1
    assert not verify_signature(endpoint["signing_secret"], body, headers)
    assert verify_signature(rotated["signing_secret"], body, headers)


def test_second_rotation_replaces_old_overlap_and_immediate_rotation_revokes(
    store, project, endpoint
):
    first = store.rotate_endpoint_secret(project["id"], endpoint["id"], 60)
    second = store.rotate_endpoint_secret(project["id"], endpoint["id"], 60)
    store.create_event(project["id"], "test", {})
    claim = store.claim_delivery()
    headers = sign_headers(
        claim["signing_secret"],
        claim["event_id"],
        b"{}",
        previous_secret=claim["previous_signing_secret"],
    )
    assert not verify_signature(endpoint["signing_secret"], b"{}", headers)
    assert verify_signature(first["signing_secret"], b"{}", headers)
    assert verify_signature(second["signing_secret"], b"{}", headers)
    assert (
        store.rotate_endpoint_secret(project["id"], endpoint["id"], 0)["previous_valid_until"]
        is None
    )


def test_project_key_overlap_expiry_and_default_immediate_revocation(store, project, monkeypatch):
    now = time.time()
    monkeypatch.setattr(store_module.time, "time", lambda: now)
    key = store.rotate_project_key(project["id"], 60)
    assert store.authenticate_project(project["api_key"])
    assert store.authenticate_project(key["api_key"])
    now += 60
    assert store.authenticate_project(project["api_key"]) is None
    current = store.rotate_project_key(project["id"])
    assert store.authenticate_project(key["api_key"]) is None
    assert store.authenticate_project(current["api_key"])
    with store._db() as db:
        values = str(dict(db.execute("SELECT * FROM projects").fetchone()))
        assert current["api_key"] not in values


@pytest.mark.parametrize("seconds", [-1, 86401, True, 1.5, "60"])
def test_overlap_is_bounded(store, project, endpoint, seconds):
    with pytest.raises(ValueError, match="overlap_seconds"):
        store.rotate_project_key(project["id"], seconds)
    with pytest.raises(ValueError, match="overlap_seconds"):
        store.rotate_endpoint_secret(project["id"], endpoint["id"], seconds)


def test_v2_migration_preserves_legacy_data_without_inventing_attempt_destinations(tmp_path):
    path = tmp_path / "old.db"
    with sqlite3.connect(path) as db:
        db.executescript(store_module.SCHEMA + store_module.SCHEDULER_SCHEMA)
        db.execute("ALTER TABLE projects ADD COLUMN last_claimed_at REAL NOT NULL DEFAULT 0")
        for column in [
            "last_claimed_at REAL NOT NULL DEFAULT 0",
            "consecutive_timeouts INTEGER NOT NULL DEFAULT 0",
            "cooldown_until REAL NOT NULL DEFAULT 0",
        ]:
            db.execute("ALTER TABLE endpoints ADD COLUMN " + column)
        db.execute(
            "INSERT INTO projects(id,name,api_key_hash,created_at) VALUES('prj_old','Old',?,1)",
            (hashlib.sha256(b"old-key").hexdigest(),),
        )
        db.execute("""INSERT INTO endpoints(id,project_id,url,rate_limit,signing_secret,created_at)
                   VALUES('ep_old','prj_old','https://old.example/',10,'whsec_placeholder',1)""")
        db.execute("INSERT INTO events VALUES('evt_old','prj_old','test','{}','unique',1)")
        db.execute("""INSERT INTO deliveries(id,project_id,event_id,endpoint_id,status,attempt_count,created_at,updated_at)
                   VALUES('dlv_old','prj_old','evt_old','ep_old','delivered',1,1,2)""")
        db.execute("""INSERT INTO attempts(delivery_id,attempt_number,cycle_attempt,status_code,duration_ms,response_excerpt,
                   started_at,finished_at,created_at) VALUES('dlv_old',1,1,204,1,'',1,2,2)""")
        db.execute("PRAGMA user_version=2")
    migrated = Store(path)
    migrated.initialize()
    migrated.initialize()
    assert migrated.authenticate_project("old-key")["id"] == "prj_old"
    assert migrated.list_endpoints("prj_old")[0]["subscriptions"] == ["*"]
    assert migrated.get_event("prj_old", "evt_old")["payload"] == {}
    assert migrated.delivery_detail("prj_old", "dlv_old")["attempts"][0]["endpoint_url"] is None
    assert migrated.create_event("prj_old", "test", {}, "unique")[1] is False
    with migrated._db() as db:
        assert db.execute("PRAGMA user_version").fetchone()[0] == 3
