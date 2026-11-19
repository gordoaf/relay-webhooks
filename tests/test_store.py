from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from contextlib import contextmanager
import hashlib
import multiprocessing
import os
import sqlite3
import stat
import time
import pytest
import relay.store as module
from relay.store import EXPIRED_DELIVERY, PENDING_DELIVERY, IdempotencyConflict, Store


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
    with pytest.raises(IdempotencyConflict, match="different request"):
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


def _claim_in_process(path):
    return Store(path).claim_delivery()


def test_capacity_is_shared_between_processes(store, project, endpoint):
    for number in range(3):
        store.create_endpoint(project["id"], f"https://other-{number}.example/", rate_limit=1000)
    for _ in range(4):
        store.create_event(project["id"], "queued", {})
    with ProcessPoolExecutor(
        max_workers=4, mp_context=multiprocessing.get_context("spawn")
    ) as pool:
        claimed = list(pool.map(_claim_in_process, [store.path] * 8))
    jobs = [job for job in claimed if job]
    assert len(jobs) == 2
    assert len({job["endpoint_id"] for job in jobs}) == 2
    assert Store(store.path).claim_delivery() is None


def test_endpoint_capacity_and_expired_lease_recovery(store, project, endpoint):
    for _ in range(4):
        store.create_event(project["id"], "queued", {})
    first = store.claim_delivery()
    with store._db(True) as db:
        db.execute("UPDATE endpoints SET next_available_at=0")
    assert Store(store.path).claim_delivery() is None
    with store._db(True) as db:
        db.execute("UPDATE deliveries SET lease_expires_at=0 WHERE id=?", (first["id"],))
    replacement = Store(store.path).claim_delivery()
    assert replacement["id"] == first["id"]
    assert replacement["lease_token"] != first["lease_token"]
    assert not finish(store, first)
    assert finish(store, replacement)


def test_project_and_endpoint_round_robin(store, project, endpoint):
    other_endpoint = store.create_endpoint(
        project["id"], "https://second.example/", rate_limit=1000
    )
    other_project = store.create_project("Other")
    other_project_endpoint = store.create_endpoint(
        other_project["id"], "https://other-project.example/", rate_limit=1000
    )
    for _ in range(4):
        store.create_event(project["id"], "backlog", {})
        store.create_event(other_project["id"], "healthy", {})
    claimed = []
    for _ in range(6):
        with store._db(True) as db:
            db.execute("UPDATE endpoints SET next_available_at=0")
        job = Store(store.path).claim_delivery()
        claimed.append((job["project_id"], job["endpoint_id"]))
        assert finish(store, job)
    assert [p for p, _ in claimed] == [project["id"], other_project["id"]] * 3
    assert [e for p, e in claimed if p == project["id"]] == [
        endpoint["id"],
        other_endpoint["id"],
        endpoint["id"],
    ]
    assert all(e == other_project_endpoint["id"] for p, e in claimed if p == other_project["id"])


def test_failing_probe_capacity_is_shared_between_processes(store):
    for index in range(4):
        owner = store.create_project(f"Unavailable {index}")
        store.create_endpoint(owner["id"], f"https://unavailable{index}.example/", rate_limit=1000)
        store.create_event(owner["id"], "test", {})
    with store._db(True) as db:
        db.execute("UPDATE endpoints SET consecutive_timeouts=1")
    with ProcessPoolExecutor(
        max_workers=2, mp_context=multiprocessing.get_context("spawn")
    ) as pool:
        claims = list(pool.map(_claim_in_process, [store.path] * 4))
    jobs = [job for job in claims if job]
    assert len(jobs) == 1 and jobs[0]["lease_is_probe"] == 1
    assert Store(store.path).claim_delivery() is None
    with store._db(True) as db:
        db.execute("UPDATE deliveries SET lease_expires_at=0 WHERE id=?", (jobs[0]["id"],))
    replacement = store.claim_delivery()
    assert replacement is not None and replacement["lease_is_probe"] == 1


def test_deferral_is_fenced_and_preserves_retry_history(store, project, endpoint):
    submit(store, project)
    first = store.claim_delivery()
    finish(store, first, 503, 0)
    with store._db(True) as db:
        db.execute("UPDATE endpoints SET next_available_at=0")
    stale = store.claim_delivery()
    with store._db(True) as db:
        db.execute("UPDATE deliveries SET lease_expires_at=0")
        db.execute("UPDATE endpoints SET next_available_at=0")
    assert not store.defer_delivery(stale["id"], stale["lease_token"], reason="dns_global_capacity")
    current = Store(store.path).claim_delivery()
    assert not store.defer_delivery(
        stale["id"], stale["lease_token"], reason="dns_project_capacity"
    )
    assert not store.deferral_stats()
    before = time.time()
    assert store.defer_delivery(
        current["id"], current["lease_token"], reason="dns_project_capacity", retry_after_seconds=2
    )
    row = store.delivery_detail(project["id"], current["id"])
    assert row["status"] == "retrying" and row["attempt_count"] == row["cycle_attempts"] == 1
    assert len(row["attempts"]) == 1 and row["last_status_code"] == 503
    assert row["next_attempt_at"] >= before + 2
    assert not finish(store, current)
    assert store.deferral_stats()["dns_project_capacity"]["count"] == 1


def test_endpoint_cooldown_grows_is_bounded_and_resets_after_response(
    store, project, endpoint, monkeypatch
):
    now = time.time()
    monkeypatch.setattr(module.time, "time", lambda: now)
    submit(store, project)
    for expected_delay in [0, 0, 30, 60, 120, 240, 480, 600, 600]:
        claim = store.claim_delivery()
        assert claim is not None
        assert store.complete_delivery(
            claim["id"],
            claim["lease_token"],
            status_code=None,
            error="Timeout",
            duration_ms=10000,
            response_excerpt="",
            next_attempt_at=now,
            max_attempts=20,
            timed_out=True,
        )
        with store._db() as db:
            until = db.execute("SELECT cooldown_until FROM endpoints").fetchone()[0]
        assert max(0, until - now) == expected_delay
        if expected_delay:
            assert store.claim_delivery() is None
        now += expected_delay + 1
    claim = store.claim_delivery()
    assert finish(store, claim, 204)
    with store._db() as db:
        assert tuple(
            db.execute("SELECT consecutive_timeouts,cooldown_until FROM endpoints").fetchone()
        ) == (0, 0)


def test_v3_migration_adds_probe_and_deferral_state_without_changing_history(
    store, project, endpoint
):
    event = submit(store, project)
    claim = store.claim_delivery()
    assert finish(store, claim)
    with store._db(True) as db:
        db.execute("DROP INDEX deliveries_processing_probes")
        db.execute("ALTER TABLE deliveries DROP COLUMN lease_is_probe")
        db.execute("DROP TABLE delivery_deferrals")
        db.execute("PRAGMA user_version=3")
    store.initialize()
    store.initialize()
    assert store.get_event(project["id"], event["id"])["payload"] == {"order_id": 12}
    assert (
        store.delivery_detail(project["id"], claim["id"])["attempts"][0]["endpoint_url"]
        == endpoint["url"]
    )
    assert store.authenticate_project(project["api_key"])
    assert store.deferral_stats() == {}
    with store._db() as db:
        assert db.execute("PRAGMA user_version").fetchone()[0] == module.SCHEMA_VERSION
        assert db.execute("SELECT lease_is_probe FROM deliveries").fetchone()[0] == 0


def test_unhealthy_project_cannot_occupy_all_capacity(store, project, endpoint):
    for number in range(6):
        store.create_endpoint(
            project["id"], f"https://unhealthy-{number}.example/", rate_limit=1000
        )
    store.create_event(project["id"], "slow", {})
    assert store.claim_delivery()
    assert store.claim_delivery()
    assert store.claim_delivery() is None
    healthy = store.create_project("Healthy")
    store.create_endpoint(healthy["id"], "https://healthy.example/", rate_limit=1000)
    store.create_event(healthy["id"], "fast", {})
    assert Store(store.path).claim_delivery()["project_id"] == healthy["id"]


def test_timeout_cooldown_persists_and_success_resets(store, project, endpoint):
    store.create_event(project["id"], "queued", {})
    for _ in range(3):
        with store._db(True) as db:
            db.execute("UPDATE endpoints SET next_available_at=0")
        job = store.claim_delivery()
        assert store.complete_delivery(
            job["id"],
            job["lease_token"],
            status_code=None,
            error="Delivery deadline exceeded",
            duration_ms=10000,
            response_excerpt="",
            next_attempt_at=0,
            timed_out=True,
        )
    endpoint_state = store.list_endpoints(project["id"])[0]
    assert endpoint_state["consecutive_timeouts"] == 3
    assert endpoint_state["cooldown_until"] >= time.time() + 29
    assert Store(store.path).claim_delivery() is None
    healthy = store.create_project("Healthy")
    store.create_endpoint(healthy["id"], "https://healthy.example/")
    store.create_event(healthy["id"], "healthy", {})
    assert finish(store, store.claim_delivery())
    with store._db(True) as db:
        db.execute("UPDATE endpoints SET cooldown_until=0,next_available_at=0")
    recovered = Store(store.path).claim_delivery()
    assert recovered["endpoint_id"] == endpoint["id"]
    assert finish(store, recovered)
    state = store.list_endpoints(project["id"])[0]
    assert state["consecutive_timeouts"] == state["cooldown_until"] == 0


@pytest.mark.parametrize("parked_state", ["paused", "rate_limited", "cooldown"])
def test_parked_backlog_does_not_scan_deliveries(
    store, project, endpoint, monkeypatch, parked_state
):
    event = submit(store, project)
    with store._db(True) as db:
        db.executemany(
            """INSERT INTO deliveries(id,project_id,event_id,endpoint_id,status,next_attempt_at,created_at,updated_at)
            VALUES(?,?,?,?,'pending',0,0,0)""",
            ((f"parked-{i}", project["id"], event["id"], endpoint["id"]) for i in range(200000)),
        )
        if parked_state == "paused":
            db.execute("UPDATE endpoints SET enabled=0")
        elif parked_state == "rate_limited":
            db.execute("UPDATE endpoints SET next_available_at=?", (time.time() + 3600,))
        else:
            db.execute("UPDATE endpoints SET cooldown_until=?", (time.time() + 3600,))
    healthy = store.create_project("Healthy")
    store.create_endpoint(healthy["id"], "https://healthy.example/")
    store.create_event(healthy["id"], "healthy", {})
    original_db = store._db
    progress_calls = []

    @contextmanager
    def budgeted_db(write=False):
        with original_db(write) as db:

            def check_budget():
                progress_calls.append(None)
                return int(len(progress_calls) > 100)

            # Abort if a claim executes more than 100k VM instructions. A scan of
            # 200k parked rows exceeds this; indexed selection is far below it.
            db.set_progress_handler(check_budget, 1000)
            yield db

    monkeypatch.setattr(store, "_db", budgeted_db)
    assert store.claim_delivery()["project_id"] == healthy["id"]
    with original_db() as db:
        for query, index in (
            (PENDING_DELIVERY, "deliveries_pending_endpoint"),
            (EXPIRED_DELIVERY, "deliveries_processing_endpoint"),
        ):
            plan = " ".join(
                row["detail"]
                for row in db.execute("EXPLAIN QUERY PLAN " + query, (endpoint["id"], time.time()))
            )
            assert "SEARCH" in plan and index in plan
            assert "TEMP B-TREE" not in plan


def _legacy_database(path):
    with sqlite3.connect(path) as db:
        db.executescript(module.SCHEMA + "PRAGMA user_version=1;")
    return Store(path)


def test_v1_migration_preserves_events_attempts_and_keys(tmp_path):
    legacy = _legacy_database(tmp_path / "legacy.db")
    project = legacy.create_project("Existing")
    # Populate the original schema directly: current Store methods intentionally
    # require initialization/migration before use.
    endpoint = {"id": "ep_legacy", "signing_secret": "whsec_legacy"}
    event = {"id": "evt_legacy"}
    delivery = {"id": "dlv_legacy"}
    with legacy._db(True) as db:
        db.execute(
            """INSERT INTO endpoints(id,project_id,url,rate_limit,signing_secret,created_at)
                   VALUES(?,?,'https://existing.example/',10,?,1)""",
            (endpoint["id"], project["id"], endpoint["signing_secret"]),
        )
        db.execute(
            "INSERT INTO events VALUES(?,?, 'order.created',?,NULL,1)",
            (event["id"], project["id"], '{"order_id":12}'),
        )
        db.execute(
            """INSERT INTO deliveries(id,project_id,event_id,endpoint_id,status,created_at,updated_at)
                   VALUES(?,?,?,?,'pending',1,1)""",
            (delivery["id"], project["id"], event["id"], endpoint["id"]),
        )
        db.execute("UPDATE deliveries SET status='delivered',attempt_count=1,last_status_code=204")
        db.execute(
            """INSERT INTO attempts(delivery_id,attempt_number,cycle_attempt,status_code,duration_ms,
            response_excerpt,started_at,finished_at,created_at) VALUES(?,1,1,204,1,'',1,2,2)""",
            (delivery["id"],),
        )
    legacy.initialize()
    legacy.initialize()
    assert legacy.authenticate_project(project["api_key"])["id"] == project["id"]
    assert legacy.get_event(project["id"], event["id"])["payload"] == {"order_id": 12}
    detail = legacy.delivery_detail(project["id"], delivery["id"])
    assert detail["status"] == "delivered" and detail["attempts"][0]["status_code"] == 204
    with legacy._db() as db:
        assert db.execute("PRAGMA user_version").fetchone()[0] == module.SCHEMA_VERSION
        assert (
            db.execute("SELECT signing_secret FROM endpoints").fetchone()[0]
            == endpoint["signing_secret"]
        )


def test_migration_rolls_back_on_failure(tmp_path, monkeypatch):
    legacy = _legacy_database(tmp_path / "legacy.db")
    with monkeypatch.context() as patch:
        patch.setattr(module, "SCHEDULER_SCHEMA", module.SCHEDULER_SCHEMA + "INVALID SQL;")
        with pytest.raises(sqlite3.OperationalError):
            legacy.initialize()
    with legacy._db() as db:
        assert db.execute("PRAGMA user_version").fetchone()[0] == 1
        assert "last_claimed_at" not in {
            row["name"] for row in db.execute("PRAGMA table_info(projects)")
        }
        assert not db.execute("SELECT name FROM sqlite_master WHERE name='sessions'").fetchone()
    legacy.initialize()


def test_newer_schema_is_rejected(store):
    with store._db() as db:
        db.execute(f"PRAGMA user_version={module.SCHEMA_VERSION + 1}")
    with pytest.raises(RuntimeError, match="newer"):
        store.initialize()
    with store._db() as db:
        assert db.execute("PRAGMA user_version").fetchone()[0] == module.SCHEMA_VERSION + 1


def test_sessions_persist_expire_and_revoke_across_instances(store):
    digest = hashlib.sha256(b"signed-session").hexdigest()
    store.create_session(digest, time.time() + 60)
    other = Store(store.path)
    assert other.valid_session(digest)
    other.revoke_session(digest)
    assert not store.valid_session(digest)
    store.create_session(digest, time.time() - 1)
    assert not other.valid_session(digest)
    assert not store.valid_session("not-a-hash")
    with pytest.raises(ValueError):
        store.create_session("raw-cookie", time.time() + 60)


def test_session_cleanup_is_bounded(store):
    with store._db(True) as db:
        db.executemany("INSERT INTO sessions VALUES(?,0)", ((f"{i:064x}",) for i in range(150)))
    store.create_session("f" * 64, time.time() + 60)
    with store._db() as db:
        assert db.execute("SELECT COUNT(*) FROM sessions").fetchone()[0] == 51


@pytest.mark.parametrize(
    "field", ["name", "url", "description", "event_type", "idempotency_key", "payload"]
)
def test_invalid_unicode_rejected_before_storage(store, project, field):
    invalid = "\ud800"
    with pytest.raises(ValueError, match="Unicode"):
        if field == "name":
            store.create_project(invalid)
        elif field in ("url", "description"):
            store.create_endpoint(
                project["id"],
                **{
                    field: invalid,
                    **({"url": "https://example.com"} if field == "description" else {}),
                },
            )
        else:
            arguments = {"event_type": "valid", "payload": {}}
            arguments[field] = {"nested": invalid} if field == "payload" else invalid
            store.create_event(project["id"], **arguments)


def test_pagination_boundaries_and_missing_error(store, project, endpoint):
    submit(store, project)
    assert store.list_deliveries(project["id"], offset=2**63 - 1)["items"] == []
    for offset in (-1, 2**63, 10**100):
        with pytest.raises(ValueError, match="offset"):
            store.list_deliveries(project["id"], offset=offset)
    job = store.claim_delivery()
    assert finish(store, job, code=None)
    assert store.delivery_detail(project["id"], job["id"])["last_error"] == "Delivery failed"


@pytest.mark.skipif(os.name != "posix", reason="POSIX modes do not describe Windows ACLs")
def test_database_and_new_directories_are_private(tmp_path):
    existing_mode = stat.S_IMODE(tmp_path.stat().st_mode)
    store = Store(tmp_path / "new" / "nested" / "relay.db")
    store.initialize()
    assert stat.S_IMODE((tmp_path / "new").stat().st_mode) == 0o700
    assert stat.S_IMODE((tmp_path / "new" / "nested").stat().st_mode) == 0o700
    assert stat.S_IMODE(os.stat(store.path).st_mode) == 0o600
    assert stat.S_IMODE(tmp_path.stat().st_mode) == existing_mode
