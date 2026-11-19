import base64
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager

import pytest

import relay.operations as module
from relay.delivery import DeliveryResult, verify_signature
from relay.operations import CapacityExceeded, IdempotencyExpired, Operations, OperationsPolicy
from relay.store import IdempotencyConflict, Store


def finish_event(store, project, key="one", code=204):
    event, _ = store.create_event(project["id"], "order.created", {"order": 12}, key)
    job = store.claim_delivery()
    assert job is not None
    store.complete_delivery(
        job["id"],
        job["lease_token"],
        status_code=code,
        error=None,
        duration_ms=20,
        response_excerpt="response",
    )
    return event


def age_history(store, days=10):
    stamp = time.time() - days * 86400
    with store._db(True) as db:
        db.execute("UPDATE events SET created_at=?", (stamp,))
        db.execute(
            "UPDATE deliveries SET created_at=?,updated_at=?,cycle_started_at=?",
            (stamp, stamp, stamp),
        )
    return stamp


def alert_operations(store, **kwargs):
    secret = "whsec_" + base64.b64encode(b"a" * 32).decode()
    return Operations(
        store,
        OperationsPolicy(
            alert_webhook_url="https://alerts.example.invalid/hook?key=secret",
            alert_signing_secret=secret,
            **kwargs,
        ),
    )


def quiet_snapshot(ops):
    snapshot = ops.summary()
    snapshot["storage"]["disk_percent"] = 0
    snapshot["queue"]["runnable_oldest_age_seconds"] = 0
    snapshot["outcomes"]["attempts"] = 0
    snapshot["endpoints"]["cooling_down"] = 0
    return snapshot


def test_retention_is_opt_in(store, project, endpoint):
    event = finish_event(store, project)
    age_history(store)
    result = Operations(store).cleanup(dry_run=False)
    assert result["enabled"] is False
    assert store.get_event(project["id"], event["id"]) is not None


def test_cleanup_dry_run_and_payload_purge_preserve_idempotency(store, project, endpoint):
    event = finish_event(store, project, key="sensitive-client-key")
    age_history(store)
    ops = Operations(store, OperationsPolicy(retention_days=1, idempotency_days=2))
    preview = ops.cleanup()
    assert preview["eligible_events"] == 1
    assert preview["deleted_events"] == 0
    assert store.get_event(project["id"], event["id"]) is not None
    result = ops.cleanup(dry_run=False)
    assert [result[f"deleted_{table}"] for table in ("events", "deliveries", "attempts")] == [
        1,
        1,
        1,
    ]
    assert store.get_event(project["id"], event["id"]) is None
    with pytest.raises(IdempotencyExpired) as error:
        store.create_event(project["id"], "order.created", {"order": 12}, "sensitive-client-key")
    assert error.value.event_id == event["id"]
    with pytest.raises(IdempotencyConflict):
        store.create_event(project["id"], "order.created", {"order": 13}, "sensitive-client-key")
    with store._db() as db:
        row = dict(db.execute("SELECT * FROM idempotency_tombstones").fetchone())
        assert "sensitive-client-key" not in json.dumps(row)
        assert "order.created" not in json.dumps(row)
        assert row["expires_at"] > time.time() + 86400
    with store._db(True) as db:
        db.execute("UPDATE idempotency_tombstones SET expires_at=0")
    replacement, created = store.create_event(
        project["id"], "order.created", {"order": 12}, "sensitive-client-key"
    )
    assert created and replacement["id"] != event["id"]


def test_cleanup_protects_unfinished_and_recently_completed_fanout(store, project, endpoint):
    store.create_endpoint(project["id"], "https://example.net/hook", rate_limit=1000)
    event, _ = store.create_event(project["id"], "order.created", {}, "unfinished")
    job = store.claim_delivery()
    store.complete_delivery(
        job["id"],
        job["lease_token"],
        status_code=204,
        error=None,
        duration_ms=1,
        response_excerpt="",
    )
    age_history(store)
    ops = Operations(store, OperationsPolicy(retention_days=1))
    assert ops.cleanup(dry_run=False)["eligible_events"] == 0
    job = store.claim_delivery()
    store.complete_delivery(
        job["id"],
        job["lease_token"],
        status_code=204,
        error=None,
        duration_ms=1,
        response_excerpt="",
    )
    assert ops.cleanup(dry_run=False)["eligible_events"] == 0
    assert store.get_event(project["id"], event["id"])["delivery_count"] == 2


def test_cleanup_protects_queued_recovery_membership(store, project, endpoint):
    event = finish_event(store, project, code=400)
    age_history(store)
    preview = store.preview_recovery(project["id"])
    recovery = store.create_recovery(project["id"], preview["preview_id"])
    ops = Operations(store, OperationsPolicy(retention_days=1))
    assert ops.cleanup(dry_run=False)["eligible_events"] == 0
    store.cancel_recovery(project["id"], recovery["id"])
    assert ops.cleanup(dry_run=False)["deleted_events"] == 1
    assert store.get_event(project["id"], event["id"]) is None


def test_cleanup_batches_bound_attempts_deliveries_and_events(store, project, endpoint):
    event, _ = store.create_event(project["id"], "order.created", {}, "many-attempts")
    for attempt in range(5):
        with store._db(True) as db:
            db.execute("UPDATE endpoints SET next_available_at=0")
        job = store.claim_delivery()
        assert job is not None
        store.complete_delivery(
            job["id"],
            job["lease_token"],
            status_code=500 if attempt < 4 else 204,
            error=None,
            duration_ms=1,
            response_excerpt="",
            next_attempt_at=0 if attempt < 4 else None,
        )
    age_history(store)
    ops = Operations(store, OperationsPolicy(retention_days=1, cleanup_batch_size=2))
    first = ops.cleanup(dry_run=False)
    assert first["deleted_attempts"] == 2 and first["deleted_deliveries"] == 0 and first["has_more"]
    assert store.get_event(project["id"], event["id"]) is not None
    for _ in range(2):
        result = ops.cleanup(dry_run=False)
        assert all(
            result[f"deleted_{table}"] <= 2 for table in ("events", "deliveries", "attempts")
        )
    assert store.get_event(project["id"], event["id"]) is None


def test_no_endpoint_event_cleanup_and_bounded_expired_tombstones(store, project):
    for number in range(5):
        store.create_event(project["id"], "ignored.event", {}, str(number))
    age_history(store)
    ops = Operations(store, OperationsPolicy(retention_days=1, cleanup_batch_size=2))
    assert ops.cleanup()["has_more"]
    for _ in range(3):
        ops.cleanup(dry_run=False)
    with store._db(True) as db:
        assert db.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 0
        db.execute("UPDATE idempotency_tombstones SET expires_at=0")
    result = Operations(store, OperationsPolicy(cleanup_batch_size=2)).cleanup(dry_run=False)
    assert not result["enabled"] and result["deleted_tombstones"] == 2


def test_concurrent_cleanup_keeps_atomic_key_tombstone(store, project, endpoint):
    finish_event(store, project)
    age_history(store)
    one = Operations(store, OperationsPolicy(retention_days=1))
    two = Operations(Store(store.path), OperationsPolicy(retention_days=1))
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda ops: ops.cleanup(dry_run=False), (one, two)))
    assert sum(result["deleted_events"] for result in results) == 1
    with pytest.raises(IdempotencyExpired):
        store.create_event(project["id"], "order.created", {"order": 12}, "one")


def seed_parked_events(store, project, endpoint, count):
    with store._db(True) as db:
        db.execute(
            """WITH RECURSIVE n(i) AS (SELECT 0 UNION ALL SELECT i+1 FROM n WHERE i<?)
            INSERT INTO events SELECT printf('parked-%06d',i),?,'parked','{}',NULL,1 FROM n""",
            (count - 1, project["id"]),
        )
        db.execute(
            """INSERT INTO deliveries(id,project_id,event_id,endpoint_id,status,next_attempt_at,created_at,updated_at)
            SELECT 'dlv-'||id,project_id,id,?,'pending',0,1,1 FROM events WHERE event_type='parked'""",
            (endpoint["id"],),
        )


def test_retention_selection_is_bounded_with_200000_parked_events(
    store, project, endpoint, monkeypatch
):
    seed_parked_events(store, project, endpoint, 200000)
    ops = Operations(store, OperationsPolicy(retention_days=1))
    original_db = store._db
    progress = []

    @contextmanager
    def budgeted_db(write=False):
        with original_db(write) as db:

            def check_budget():
                progress.append(None)
                return int(len(progress) > 200)

            db.set_progress_handler(check_budget, 1000)
            yield db

    monkeypatch.setattr(store, "_db", budgeted_db)
    result = ops.cleanup(dry_run=False)
    assert result["scanned_events"] == 1000
    assert result["eligible_events"] == 0 and result["has_more"]


def test_retention_cursor_reaches_history_after_parked_prefix(store, project, endpoint):
    seed_parked_events(store, project, endpoint, 1001)
    with store._db(True) as db:
        db.execute(
            "INSERT INTO events VALUES('purge-me',?,'finished','{}',NULL,2)", (project["id"],)
        )
    ops = Operations(store, OperationsPolicy(retention_days=1))
    assert ops.cleanup(dry_run=False)["deleted_events"] == 0
    assert ops.cleanup(dry_run=False)["deleted_events"] == 1
    assert store.get_event(project["id"], "purge-me") is None
    with store._db() as db:
        assert db.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 1001


def test_retention_attempt_cleanup_is_bounded_with_large_terminal_fanout(
    store, project, endpoint, monkeypatch
):
    with store._db(True) as db:
        db.execute(
            "INSERT INTO events VALUES('large-fanout',?,'finished','{}',NULL,1)", (project["id"],)
        )
        db.execute(
            """WITH RECURSIVE n(i) AS (SELECT 0 UNION ALL SELECT i+1 FROM n WHERE i<19999)
            INSERT INTO deliveries(id,project_id,event_id,endpoint_id,status,created_at,updated_at)
            SELECT printf('done-%06d',i),?,'large-fanout',?,'delivered',1,1 FROM n""",
            (project["id"], endpoint["id"]),
        )
        db.execute(
            """INSERT INTO attempts(delivery_id,attempt_number,cycle_attempt,status_code,error,duration_ms,
            response_excerpt,started_at,finished_at,created_at)
            SELECT d.id,n.i,n.i,204,NULL,1,'',1,1,1 FROM deliveries d
            CROSS JOIN (SELECT 1 i UNION ALL SELECT 2 UNION ALL SELECT 3) n
            WHERE d.event_id='large-fanout'""",
        )
    ops = Operations(store, OperationsPolicy(retention_days=1))
    original_db = store._db
    progress = []

    @contextmanager
    def budgeted_db(write=False):
        with original_db(write) as db:

            def check_budget():
                progress.append(None)
                return int(len(progress) > 100)

            db.set_progress_handler(check_budget, 1000)
            yield db

    monkeypatch.setattr(store, "_db", budgeted_db)
    result = ops.cleanup(dry_run=False)
    assert result["deleted_attempts"] == 100
    assert result["deleted_deliveries"] == 33
    assert result["deleted_events"] == 0 and result["has_more"]


def test_capacity_rejects_new_events_but_preserves_duplicate_response(store, project, endpoint):
    original, _ = store.create_event(project["id"], "order.created", {}, "same")
    ops = Operations(store, OperationsPolicy(max_storage_bytes=1))
    repeated, created = store.create_event(project["id"], "order.created", {}, "same")
    assert repeated["id"] == original["id"] and not created
    with pytest.raises(CapacityExceeded):
        store.create_event(project["id"], "order.created", {}, "different")
    with store._db() as db:
        assert db.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 1
    snapshot = ops.storage()
    assert snapshot["total_bytes"] >= snapshot["used_bytes"] > 0
    assert snapshot["free_bytes"] >= 0


def test_free_space_reserve_and_pressure_warning(store, project, monkeypatch):
    ops = Operations(store, OperationsPolicy(min_free_bytes=100000))
    fake = {"free_bytes": 105000, "used_bytes": 1000, "disk_percent": 10}
    monkeypatch.setattr(ops, "storage", lambda db=None: fake)
    with pytest.raises(CapacityExceeded, match="reserve"):
        store.create_event(project["id"], "large.event", "a" * 10000)
    snapshot = quiet_snapshot(ops)
    ops.evaluate_alerts(snapshot)
    assert [alert["code"] for alert in ops.list_alerts()] == ["storage_pressure"]


def test_metrics_aggregate_scope_and_fixed_cardinality(store, project, endpoint):
    finish_event(store, project, code=400)
    other = store.create_project("Other")
    other_endpoint = store.create_endpoint(other["id"], "https://private-customer.example/hook")
    store.create_event(other["id"], "private.event", {"password": "keep-private"})
    age_history(store)
    store.set_endpoint_enabled(other["id"], other_endpoint["id"], False)
    ops = Operations(store)
    summary = ops.summary(project["id"])
    assert summary["queue"]["failed"] == 1 and summary["queue"]["depth"] == 0
    assert summary["outcomes"]["attempts"] == 1 and summary["outcomes"]["failure_percent"] == 100
    global_summary = ops.summary()
    assert global_summary["queue"]["depth"] == 1
    assert global_summary["queue"]["oldest_age_seconds"] > 86400
    assert global_summary["queue"]["runnable_oldest_age_seconds"] == 0
    metrics = ops.prometheus()
    assert 'relay_deliveries{status="failed"} 1' in metrics
    assert "relay_attempt_latency_mean_seconds 0.02" in metrics
    for sensitive in (
        project["id"],
        other["id"],
        "private.event",
        "private-customer",
        "keep-private",
    ):
        assert sensitive not in metrics


def test_replaying_old_history_starts_a_fresh_queue_age(store, project, endpoint):
    finish_event(store, project, code=400)
    age_history(store)
    delivery = store.list_deliveries(project["id"])["items"][0]
    store.replay_delivery(project["id"], delivery["id"])
    ops = Operations(store)
    snapshot = ops.summary()
    assert 0 <= snapshot["queue"]["oldest_age_seconds"] < 5
    ops.evaluate_alerts(snapshot)
    assert "queue_delay" not in [alert["code"] for alert in ops.list_alerts()]


def test_failure_alert_requires_enough_samples_and_recovers(store):
    ops = Operations(store, OperationsPolicy(failure_min_samples=10, failure_warning_percent=25))
    snapshot = quiet_snapshot(ops)
    snapshot["outcomes"].update(attempts=9, failure_percent=100)
    ops.evaluate_alerts(snapshot, now=1)
    assert not ops.list_alerts()
    snapshot["outcomes"].update(attempts=10, failure_percent=30)
    ops.evaluate_alerts(snapshot, now=2)
    assert ops.list_alerts()[0]["code"] == "delivery_failures"
    snapshot["outcomes"]["failure_percent"] = 20
    ops.evaluate_alerts(snapshot, now=3)
    assert not ops.list_alerts()[0]["active"]


def test_alert_transitions_deduplicate_recover_and_hide_secrets(store, monkeypatch):
    ops = alert_operations(store, alert_cooldown_seconds=60)
    snapshot = quiet_snapshot(ops)
    snapshot["queue"]["runnable_oldest_age_seconds"] = 500
    delivered = []

    def sender(url, body, headers, **kwargs):
        assert verify_signature(ops.policy.alert_signing_secret, body, headers)
        assert kwargs == {"timeout": 5, "allow_private": False}
        delivered.append(json.loads(body))
        return DeliveryResult(204, None, 1, "")

    monkeypatch.setattr(module, "deliver", sender)
    ops.evaluate_alerts(snapshot, now=100)
    ops.evaluate_alerts(snapshot, now=110)
    assert len(ops.list_alerts()) == 1 and ops.list_alerts()[0]["first_seen_at"] == 100
    assert ops.notify_once(now=110)
    assert not ops.notify_once(now=111)
    assert ops.notify_once(now=170)
    assert delivered[0]["id"] != delivered[1]["id"]
    snapshot["queue"]["runnable_oldest_age_seconds"] = 0
    ops.evaluate_alerts(snapshot, now=180)
    assert ops.notify_once(now=180)
    assert delivered[-1]["type"] == "relay.alert.resolved"
    assert not ops.notify_once(now=1000)
    assert ops.list_alerts()[0]["resolved_at"] == 180
    public = json.dumps(ops.summary()) + json.dumps(delivered)
    assert "?key=secret" not in public and ops.policy.alert_signing_secret not in public


def test_alert_notifications_retry_with_stable_id_and_bounded_cooldown(store, monkeypatch):
    ops = alert_operations(store, alert_cooldown_seconds=120)
    snapshot = quiet_snapshot(ops)
    snapshot["endpoints"]["cooling_down"] = 1
    attempts = []

    def fail(url, body, headers, **kwargs):
        attempts.append(headers["webhook-id"])
        return DeliveryResult(503, "unavailable", 1, "do not persist receiver secrets")

    monkeypatch.setattr(module, "deliver", fail)
    ops.evaluate_alerts(snapshot, now=100)
    for stamp in (100, 130, 190):
        assert ops.notify_once(now=stamp)
        assert not ops.notify_once(now=stamp + 1)
    assert len(set(attempts)) == 1
    assert not ops.notify_once(now=309)
    assert ops.notify_once(now=310)
    assert attempts[-1] != attempts[0]
    assert "receiver secrets" not in json.dumps(ops.list_alerts())


def test_notification_lease_fences_parallel_senders_and_old_transition(store, monkeypatch):
    ops = alert_operations(store)
    other = alert_operations(Store(store.path))
    snapshot = quiet_snapshot(ops)
    snapshot["queue"]["runnable_oldest_age_seconds"] = 500
    ops.evaluate_alerts(snapshot, now=100)
    entered, release = threading.Event(), threading.Event()

    def sender(*args, **kwargs):
        entered.set()
        assert release.wait(5)
        return DeliveryResult(204, None, 1, "")

    monkeypatch.setattr(module, "deliver", sender)
    with ThreadPoolExecutor(max_workers=1) as pool:
        first = pool.submit(ops.notify_once, now=100)
        assert entered.wait(5)
        assert not other.notify_once(now=101)
        snapshot["queue"]["runnable_oldest_age_seconds"] = 0
        other.evaluate_alerts(snapshot, now=102)
        release.set()
        assert first.result()
    alert = other.list_alerts()[0]
    assert not alert["active"] and alert["last_notified_at"] is None
    assert other.notify_once(now=103)
    assert other.list_alerts()[0]["last_notified_at"] == 103


def test_maintenance_lease_and_threads_do_not_take_delivery_slots(store):
    ops = Operations(store)
    with store._db(True) as db:
        db.execute("INSERT INTO operation_leases VALUES('maintenance','other',200)")
    assert ops.run_once(now=100) is False
    assert ops.run_once(now=201) is True
    ops.start()
    try:
        assert [thread.name for thread in ops._threads] == ["relay-maintenance"]
        ops.start()
        assert len(ops._threads) == 1
    finally:
        ops.stop()
    assert ops._threads == []


@pytest.mark.parametrize(
    "settings",
    [
        {"retention_days": -1},
        {"idempotency_days": 0},
        {"cleanup_batch_size": 1001},
        {"failure_min_samples": True},
        {"storage_warning_percent": float("nan")},
        {"queue_warning_age_seconds": float("inf")},
        {"interval_seconds": 0},
        {"alert_webhook_url": "https://example.com"},
        {
            "alert_webhook_url": "http://user:pass@example.com",
            "alert_signing_secret": "whsec_" + base64.b64encode(b"x" * 32).decode(),
        },
    ],
)
def test_invalid_operations_policy(settings):
    with pytest.raises(ValueError):
        OperationsPolicy(**settings)
