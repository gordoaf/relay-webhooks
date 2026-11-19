import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager

import pytest

import relay.store as module
import relay.worker as worker_module
from relay.delivery import DeliveryResult
from relay.store import Store
from relay.worker import Worker


def fail_events(store, project, count):
    result = []
    for index in range(count):
        event, _ = store.create_event(project["id"], "test", {"index": index})
        result.append(event["id"])
    with store._db(True) as db:
        db.execute(
            "UPDATE deliveries SET status='failed',next_attempt_at=NULL WHERE project_id=?",
            (project["id"],),
        )
    return result


def test_preview_is_frozen_scoped_and_job_survives_restart(store, project, endpoint):
    fail_events(store, project, 3)
    preview = store.preview_recovery(project["id"], endpoint["id"], limit=2)
    assert preview["total"] == 3 and preview["selected_count"] == 2
    fail_events(store, project, 1)
    other = store.create_project("Other")
    with pytest.raises(ValueError, match="unknown or expired"):
        store.create_recovery(other["id"], preview["preview_id"])
    job = store.create_recovery(project["id"], preview["preview_id"], 1)
    assert job["total"] == 2 and job["queued"] == 2
    assert store.create_recovery(project["id"], preview["preview_id"], 1)["id"] == job["id"]
    with pytest.raises(ValueError, match="different pace"):
        store.create_recovery(project["id"], preview["preview_id"], 2)
    reopened = Store(store.path)
    assert reopened.advance_recoveries() == 1
    progress = reopened.get_recovery(project["id"], job["id"])
    assert progress["requeued"] == 1 and progress["queued"] == 1
    assert reopened.get_recovery(other["id"], job["id"]) is None
    assert reopened.advance_recoveries() == 0


def test_pacing_is_atomic_across_workers_and_has_no_restart_burst(
    store, project, endpoint, monkeypatch
):
    now = time.time()
    monkeypatch.setattr(module.time, "time", lambda: now)
    fail_events(store, project, 5)
    preview = store.preview_recovery(project["id"])
    job = store.create_recovery(project["id"], preview["preview_id"], 2)
    with ThreadPoolExecutor(8) as pool:
        assert sum(pool.map(lambda _: Store(store.path).advance_recoveries(), range(8))) == 1
    now += 0.49
    assert store.advance_recoveries() == 0
    now += 100
    assert store.advance_recoveries() == 1
    assert store.advance_recoveries() == 0
    progress = store.get_recovery(project["id"], job["id"])
    assert progress["requeued"] == 2 and progress["queued"] == 3


def test_recovery_pacing_starts_after_waiting_for_writer_lock(
    store, project, endpoint, monkeypatch
):
    now = time.time()
    monkeypatch.setattr(module.time, "time", lambda: now)
    fail_events(store, project, 2)
    preview = store.preview_recovery(project["id"])
    job = store.create_recovery(project["id"], preview["preview_id"], 1)
    original_db = store._db

    @contextmanager
    def delayed_writer(write=False):
        nonlocal now
        with original_db(write) as db:
            if write:
                now += 10
            yield db

    monkeypatch.setattr(store, "_db", delayed_writer)
    assert store.advance_recoveries() == 1
    assert store.get_recovery(project["id"], job["id"])["next_release_at"] == now + 1


def test_changed_members_and_duplicate_jobs_are_skipped(store, project, endpoint):
    fail_events(store, project, 3)
    preview = store.preview_recovery(project["id"])
    first_id = preview["sample"][0]
    assert store.replay_delivery(project["id"], first_id)
    job = store.create_recovery(project["id"], preview["preview_id"])
    assert job["skipped"] == 1 and job["queued"] == 2
    second_preview = store.preview_recovery(project["id"])
    duplicate = store.create_recovery(project["id"], second_preview["preview_id"])
    assert duplicate["status"] == "completed" and duplicate["skipped"] == 2
    with store._db(True) as db:
        # A subsequent failure is a new version and must not be swept into an old job.
        db.execute(
            "UPDATE deliveries SET updated_at=updated_at+1 WHERE id=?", (preview["sample"][1],)
        )
    store.advance_recoveries()
    with store._db(True) as db:
        db.execute("UPDATE recovery_jobs SET next_release_at=0")
    store.advance_recoveries()
    progress = store.get_recovery(project["id"], job["id"])
    assert (
        progress["status"] == "completed" and progress["skipped"] == 2 and progress["requeued"] == 1
    )


def test_new_failure_in_same_clock_tick_does_not_match_preview(
    store, project, endpoint, monkeypatch
):
    now = time.time()
    monkeypatch.setattr(module.time, "time", lambda: now)
    fail_events(store, project, 1)
    preview = store.preview_recovery(project["id"])
    delivery_id = preview["sample"][0]
    assert store.replay_delivery(project["id"], delivery_id)
    claim = store.claim_delivery()
    assert store.complete_delivery(
        claim["id"],
        claim["lease_token"],
        status_code=400,
        error="Bad request",
        duration_ms=1,
        response_excerpt="",
    )
    result = store.create_recovery(project["id"], preview["preview_id"])
    assert result["status"] == "completed" and result["skipped"] == 1


def test_cancelling_stops_only_unreleased_members(store, project, endpoint):
    fail_events(store, project, 3)
    preview = store.preview_recovery(project["id"])
    job = store.create_recovery(project["id"], preview["preview_id"])
    store.advance_recoveries()
    cancelled = store.cancel_recovery(project["id"], job["id"])
    assert (
        cancelled["status"] == "cancelled"
        and cancelled["requeued"] == 1
        and cancelled["cancelled"] == 2
    )
    assert store.advance_recoveries() == 0
    assert store.stats(project["id"])["pending"] == 1
    assert store.cancel_recovery(project["id"], job["id"]) == cancelled
    assert store.cancel_recovery(project["id"], "unknown") is None


def test_preview_filters_and_expiry(store, project, endpoint, monkeypatch):
    now = time.time()
    monkeypatch.setattr(module.time, "time", lambda: now)
    fail_events(store, project, 1)
    older = now
    now += 10
    fail_events(store, project, 1)
    assert (
        store.preview_recovery(project["id"], endpoint["id"], since=older, until=older)["total"]
        == 1
    )
    preview = store.preview_recovery(project["id"], since=now)
    assert preview["total"] == 1
    now += 900
    with pytest.raises(ValueError, match="expired"):
        store.create_recovery(project["id"], preview["preview_id"])
    for kwargs in (
        {"since": float("nan")},
        {"until": True},
        {"since": 2, "until": 1},
        {"limit": 1001},
        {"endpoint_id": "missing"},
    ):
        with pytest.raises(ValueError):
            store.preview_recovery(project["id"], **kwargs)


def test_preview_capacity_is_bounded(store, project):
    for _ in range(10):
        store.preview_recovery(project["id"])
    with pytest.raises(ValueError, match="10 unused"):
        store.preview_recovery(project["id"])


def test_recovery_worker_keeps_event_identity_attempt_history_and_outcomes(
    store, project, endpoint, monkeypatch
):
    event, _ = store.create_event(project["id"], "test", {})
    monkeypatch.setattr(
        worker_module, "deliver", lambda *a, **kw: DeliveryResult(400, "HTTP 400", 1, "bad")
    )
    worker = Worker(store)
    assert worker.run_once()
    before = store.list_deliveries(project["id"])["items"][0]
    preview = store.preview_recovery(project["id"])
    job = store.create_recovery(project["id"], preview["preview_id"])
    captured = {}

    def deliver(url, body, headers, **kwargs):
        captured.update(headers)
        return DeliveryResult(204, None, 1, "")

    monkeypatch.setattr(worker_module, "deliver", deliver)
    with store._db(True) as db:
        db.execute("UPDATE endpoints SET next_available_at=0")
    assert worker.run_once()
    detail = store.delivery_detail(project["id"], before["id"])
    assert detail["event_id"] == event["id"] == captured["webhook-id"]
    assert detail["attempt_count"] == 2 and detail["cycle_attempts"] == 1
    assert [attempt["status_code"] for attempt in detail["attempts"]] == [400, 204]
    progress = store.get_recovery(project["id"], job["id"])
    assert progress["status"] == "completed" and progress["outcomes"]["delivered"] == 1


def test_finished_job_cleanup_does_not_delete_deliveries(store, project, endpoint, monkeypatch):
    now = time.time()
    monkeypatch.setattr(module.time, "time", lambda: now)
    fail_events(store, project, 1)
    preview = store.preview_recovery(project["id"])
    job = store.create_recovery(project["id"], preview["preview_id"])
    store.cancel_recovery(project["id"], job["id"])
    now += 31 * 86400
    reopened = Store(store.path)
    reopened.advance_recoveries()
    assert reopened.get_recovery(project["id"], job["id"]) is None
    assert store.stats(project["id"])["failed"] == 1
    with store._db() as db:
        assert db.execute("SELECT COUNT(*) FROM recovery_items").fetchone()[0] == 0
