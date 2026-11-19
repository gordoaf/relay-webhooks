from concurrent.futures import ThreadPoolExecutor
import sqlite3
import time
import pytest
from relay.store import Store


def submit(store, project):
    return store.create_event(project["id"], "order.created", {"order_id": 12}, "order-12")[0]


def finish(store, job, code=204, next_at=None):
    return store.complete_delivery(
        job["id"],
        job["lease_token"],
        status_code=code,
        error=None,
        duration_ms=12,
        response_excerpt="",
        next_attempt_at=next_at,
    )


def test_atomic_fanout_and_idempotency(store, project, endpoint):
    store.create_endpoint(project["id"], "https://example.net/hook")
    first = submit(store, project)
    repeated, created = store.create_event(
        project["id"], "order.created", {"order_id": 12}, "order-12"
    )
    assert first["id"] == repeated["id"] and not created
    assert first["delivery_count"] == 2
    assert store.stats(project["id"])["total"] == 2
    with pytest.raises(ValueError, match="different request"):
        store.create_event(project["id"], "order.created", {"order_id": 99}, "order-12")


def test_fanout_failure_rolls_back_event(store, project, endpoint):
    with store._db(True) as db:
        db.execute(
            "CREATE TRIGGER fail_delivery BEFORE INSERT ON deliveries BEGIN SELECT RAISE(ABORT,'test failure'); END"
        )
    with pytest.raises(sqlite3.IntegrityError):
        submit(store, project)
    with store._db() as db:
        assert db.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 0


def test_concurrent_submit_is_one_event(store, project, endpoint):
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda _: submit(store, project), range(16)))
    assert len({r["id"] for r in results}) == 1
    assert store.stats(project["id"])["total"] == 1


def test_only_one_worker_claims(store, project, endpoint):
    submit(store, project)
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda _: store.claim_delivery(), range(16)))
    assert sum(r is not None for r in results) == 1


def test_crash_reclaim_and_stale_completion(store, project, endpoint):
    submit(store, project)
    old = store.claim_delivery()
    with store._db(True) as db:
        db.execute("UPDATE deliveries SET lease_expires_at=0")
        db.execute("UPDATE endpoints SET next_available_at=0")
    recovered = Store(store.path).claim_delivery()
    assert old["id"] == recovered["id"]
    assert old["lease_token"] != recovered["lease_token"]
    assert not finish(store, old)
    assert finish(store, recovered)
    assert store.stats(project["id"])["delivered"] == 1


def test_replay_preserves_history_and_resets_budget(store, project, endpoint):
    submit(store, project)
    job = store.claim_delivery()
    assert not store.replay_delivery(project["id"], job["id"])
    finish(store, job, 400)
    assert store.replay_delivery(project["id"], job["id"])
    with store._db(True) as db:
        db.execute("UPDATE endpoints SET next_available_at=0")
    second = store.claim_delivery()
    assert second["cycle_attempts"] == 0 and second["attempt_count"] == 1
    finish(store, second)
    detail = store.delivery_detail(project["id"], job["id"])
    assert [a["status_code"] for a in detail["attempts"]] == [400, 204]


def test_pause_and_rate_limit(store, project, endpoint):
    store.create_event(project["id"], "one", {})
    store.create_event(project["id"], "two", {})
    store.set_endpoint_enabled(project["id"], endpoint["id"], False)
    assert store.claim_delivery() is None
    store.set_endpoint_enabled(project["id"], endpoint["id"], True)
    with store._db(True) as db:
        db.execute("UPDATE endpoints SET rate_limit=1")
    assert store.claim_delivery() is not None
    assert store.claim_delivery() is None


def test_disabled_endpoints_do_not_receive_new_events(store, project, endpoint):
    store.set_endpoint_enabled(project["id"], endpoint["id"], False)
    assert submit(store, project)["delivery_count"] == 0


def test_project_scope_and_secret_redaction(store, project, endpoint):
    event = submit(store, project)
    job = store.claim_delivery()
    other = store.create_project("Other")
    assert store.get_event(other["id"], event["id"]) is None
    assert store.delivery_detail(other["id"], job["id"]) is None
    assert not store.replay_delivery(other["id"], job["id"])
    assert store.list_deliveries(other["id"])["total"] == 0
    assert "api_key_hash" not in str(store.list_projects())
    assert endpoint["signing_secret"] not in str(store.list_endpoints(project["id"]))
    rotated = store.rotate_project_key(project["id"])
    assert store.authenticate_project(project["api_key"]) is None
    assert store.authenticate_project(rotated["api_key"])["id"] == project["id"]


def test_restart_preserves_pending_work(store, project, endpoint):
    submit(store, project)
    restarted = Store(store.path)
    restarted.initialize()
    assert restarted.claim_delivery()["project_id"] == project["id"]


def test_future_retry_not_claimed(store, project, endpoint):
    submit(store, project)
    finish(store, store.claim_delivery(), 503, time.time() + 120)
    assert store.claim_delivery() is None
    assert store.stats(project["id"])["retrying"] == 1
