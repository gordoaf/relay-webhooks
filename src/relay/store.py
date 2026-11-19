"""Durable webhook storage, queue claims, and delivery history."""

import base64
from contextlib import contextmanager
import hashlib
import json
import math
import os
from pathlib import Path
import re
import secrets
import sqlite3
import time

from .events import PayloadTooLarge, serialize_event

SCHEMA = """
CREATE TABLE IF NOT EXISTS projects (
 id TEXT PRIMARY KEY, name TEXT NOT NULL, api_key_hash TEXT NOT NULL UNIQUE, created_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS endpoints (
 id TEXT PRIMARY KEY, project_id TEXT NOT NULL REFERENCES projects(id), url TEXT NOT NULL,
 description TEXT NOT NULL DEFAULT '', enabled INTEGER NOT NULL DEFAULT 1 CHECK(enabled IN (0,1)),
 rate_limit INTEGER NOT NULL CHECK(rate_limit > 0), signing_secret TEXT NOT NULL,
 next_available_at REAL NOT NULL DEFAULT 0, created_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS events (
 id TEXT PRIMARY KEY, project_id TEXT NOT NULL REFERENCES projects(id), event_type TEXT NOT NULL,
 payload_json TEXT NOT NULL, idempotency_key TEXT, created_at REAL NOT NULL,
 UNIQUE(project_id,idempotency_key)
);
CREATE TABLE IF NOT EXISTS deliveries (
 id TEXT PRIMARY KEY, project_id TEXT NOT NULL REFERENCES projects(id),
 event_id TEXT NOT NULL REFERENCES events(id), endpoint_id TEXT NOT NULL REFERENCES endpoints(id),
 status TEXT NOT NULL CHECK(status IN ('pending','processing','retrying','delivered','failed')),
 attempt_count INTEGER NOT NULL DEFAULT 0, cycle_attempts INTEGER NOT NULL DEFAULT 0,
 next_attempt_at REAL, lease_token TEXT, lease_expires_at REAL, lease_started_at REAL,
 last_status_code INTEGER, last_error TEXT, last_duration_ms REAL, last_response_excerpt TEXT,
 created_at REAL NOT NULL, updated_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS attempts (
 id INTEGER PRIMARY KEY AUTOINCREMENT, delivery_id TEXT NOT NULL REFERENCES deliveries(id),
 attempt_number INTEGER NOT NULL, cycle_attempt INTEGER NOT NULL, status_code INTEGER,
 error TEXT, duration_ms REAL NOT NULL, response_excerpt TEXT NOT NULL,
 started_at REAL NOT NULL, finished_at REAL NOT NULL, created_at REAL NOT NULL,
 UNIQUE(delivery_id,attempt_number)
);
CREATE INDEX IF NOT EXISTS deliveries_due ON deliveries(status,next_attempt_at,lease_expires_at);
CREATE INDEX IF NOT EXISTS deliveries_project ON deliveries(project_id,created_at);
CREATE INDEX IF NOT EXISTS deliveries_event ON deliveries(event_id);
CREATE INDEX IF NOT EXISTS endpoints_project ON endpoints(project_id);
CREATE INDEX IF NOT EXISTS attempts_delivery ON attempts(delivery_id,attempt_number);
"""
SCHEMA_VERSION = 5
SLOW_RESPONSE_MS = 5000
RESPONSIVE_SECONDS = 900
AVAILABILITY_SCHEMA = """
CREATE INDEX IF NOT EXISTS deliveries_processing_probes ON deliveries(lease_expires_at)
 WHERE status='processing' AND lease_is_probe=1;
CREATE INDEX IF NOT EXISTS deliveries_processing_risky ON deliveries(lease_expires_at)
 WHERE status='processing' AND lease_is_risky=1;
CREATE INDEX IF NOT EXISTS deliveries_processing_risky_project ON deliveries(project_id,lease_expires_at)
 WHERE status='processing' AND lease_is_risky=1;
CREATE TABLE IF NOT EXISTS delivery_deferrals (
 reason TEXT PRIMARY KEY CHECK(reason IN ('dns_global_capacity','dns_project_capacity')),
 count INTEGER NOT NULL, last_seen_at REAL NOT NULL
);
"""
LIFECYCLE_SCHEMA = """
CREATE INDEX IF NOT EXISTS deliveries_failed_project
 ON deliveries(project_id,updated_at,id) WHERE status='failed';
CREATE TABLE IF NOT EXISTS recovery_previews (
 id TEXT PRIMARY KEY, project_id TEXT NOT NULL REFERENCES projects(id),
 filters_json TEXT NOT NULL, membership_json TEXT NOT NULL, total INTEGER NOT NULL,
 expires_at REAL NOT NULL, created_at REAL NOT NULL, used_job_id TEXT
);
CREATE INDEX IF NOT EXISTS recovery_previews_project ON recovery_previews(project_id,expires_at);
CREATE TABLE IF NOT EXISTS recovery_jobs (
 id TEXT PRIMARY KEY, project_id TEXT NOT NULL REFERENCES projects(id),
 preview_id TEXT NOT NULL UNIQUE, status TEXT NOT NULL CHECK(status IN ('running','completed','cancelled')),
 pace_per_second INTEGER NOT NULL, next_release_at REAL NOT NULL,
 created_at REAL NOT NULL, updated_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS recovery_jobs_due ON recovery_jobs(status,next_release_at);
CREATE INDEX IF NOT EXISTS recovery_jobs_project ON recovery_jobs(project_id,created_at);
CREATE INDEX IF NOT EXISTS recovery_jobs_finished ON recovery_jobs(updated_at) WHERE status!='running';
CREATE TABLE IF NOT EXISTS recovery_items (
 job_id TEXT NOT NULL REFERENCES recovery_jobs(id) ON DELETE CASCADE,
 delivery_id TEXT NOT NULL, event_id TEXT NOT NULL,
 snapshot_updated_at REAL NOT NULL, snapshot_attempt_count INTEGER NOT NULL,
 state TEXT NOT NULL CHECK(state IN ('queued','requeued','skipped','cancelled')),
 PRIMARY KEY(job_id,delivery_id)
);
CREATE INDEX IF NOT EXISTS recovery_items_queued ON recovery_items(job_id,state,delivery_id);
CREATE INDEX IF NOT EXISTS recovery_items_delivery ON recovery_items(delivery_id,state);
CREATE INDEX IF NOT EXISTS recovery_items_event_waiting ON recovery_items(event_id,job_id) WHERE state='queued';
"""
SCHEDULER_SCHEMA = """
CREATE INDEX IF NOT EXISTS deliveries_pending_endpoint
 ON deliveries(endpoint_id,next_attempt_at,created_at,id)
 WHERE status IN ('pending','retrying');
CREATE INDEX IF NOT EXISTS deliveries_processing_endpoint
 ON deliveries(endpoint_id,lease_expires_at,created_at,id) WHERE status='processing';
CREATE INDEX IF NOT EXISTS deliveries_processing_project
 ON deliveries(project_id,lease_expires_at) WHERE status='processing';
CREATE TABLE IF NOT EXISTS sessions (
 session_hash TEXT PRIMARY KEY, expires_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS sessions_expiry ON sessions(expires_at);
"""
PENDING_DELIVERY = """SELECT * FROM deliveries INDEXED BY deliveries_pending_endpoint
 WHERE endpoint_id=? AND status IN ('pending','retrying') AND next_attempt_at<=?
 ORDER BY next_attempt_at,created_at,id LIMIT 1"""
EXPIRED_DELIVERY = """SELECT * FROM deliveries INDEXED BY deliveries_processing_endpoint
 WHERE endpoint_id=? AND status='processing' AND lease_expires_at<=?
 ORDER BY lease_expires_at,created_at,id LIMIT 1"""
EVENT_SELECT = """SELECT ev.*, (SELECT COUNT(*) FROM deliveries d WHERE d.event_id=ev.id)
 AS delivery_count FROM events ev"""
DELIVERY_SELECT = """SELECT d.id,d.project_id,d.event_id,d.endpoint_id,ev.event_type,
 COALESCE(d.lease_endpoint_url,d.last_endpoint_url,ep.url) AS endpoint_url,
 ep.url AS current_endpoint_url,ep.enabled AS endpoint_enabled,d.status,d.attempt_count,d.cycle_attempts,
 d.next_attempt_at,d.last_status_code,d.last_error,d.last_duration_ms,d.last_response_excerpt,
 d.created_at,d.updated_at FROM deliveries d JOIN events ev ON ev.id=d.event_id
 JOIN endpoints ep ON ep.id=d.endpoint_id"""
STATUSES = {"pending", "processing", "retrying", "delivered", "failed"}


class IdempotencyConflict(ValueError):
    """An existing idempotency key identifies a different event."""


def _utf8(value, field):
    try:
        value.encode("utf-8")
    except UnicodeError as exc:
        raise ValueError(f"{field} must contain valid Unicode") from exc
    return value


def _id(prefix):
    return prefix + "_" + secrets.token_hex(12)


def _text(value, field, limit=200):
    if not isinstance(value, str) or not value.strip() or len(value) > limit:
        raise ValueError(f"{field} must be a nonempty string of at most {limit} characters")
    return _utf8(value.strip(), field)


def _event(row):
    if row is None:
        return None
    result = dict(row)
    result["payload"] = json.loads(result.pop("payload_json"))
    return result


def _endpoint(row):
    if row is None:
        return None
    result = dict(row)
    result.pop("signing_secret", None)
    result.pop("previous_signing_secret", None)
    result.pop("next_available_at", None)
    result.pop("last_claimed_at", None)
    result["enabled"] = bool(result["enabled"])
    result["subscriptions"] = json.loads(result.pop("subscriptions_json", '["*"]'))
    return result


def _subscriptions(value):
    if value is None:
        return ["*"]
    if not isinstance(value, list) or len(value) > 100:
        raise ValueError("subscriptions must contain at most 100 event patterns")
    result = []
    for pattern in value:
        if (
            not isinstance(pattern, str)
            or len(pattern) > 128
            or not (
                pattern == "*"
                or re.fullmatch(r"[A-Za-z][A-Za-z0-9_.:-]{0,127}", pattern)
                or (
                    pattern.endswith(".*")
                    and re.fullmatch(r"[A-Za-z][A-Za-z0-9_.:-]{0,127}", pattern[:-2])
                )
            )
        ):
            raise ValueError("subscriptions accept event names, namespace.*, or *")
        if pattern not in result:
            result.append(pattern)
    return ["*"] if "*" in result else result


def _subscribed(patterns, event_type):
    return any(
        pattern == "*"
        or pattern == event_type
        or (
            pattern.endswith(".*")
            and event_type.startswith(pattern[:-1])
            and len(event_type) > len(pattern) - 1
        )
        for pattern in patterns
    )


def _overlap(value):
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= 86400:
        raise ValueError("overlap_seconds must be between 0 and 86400")
    return value


class Store:
    """File-backed SQLite queue. Mutations take a short exclusive writer reservation."""

    def __init__(self, path, max_delivery_bytes=262144):
        if str(path) == ":memory:":
            raise ValueError("Use a file-backed database so deliveries survive process restarts")
        self.path = str(Path(path).expanduser().resolve())
        self.max_delivery_bytes = max_delivery_bytes
        self.capacity_guard = None
        self._next_recovery_cleanup = 0

    @contextmanager
    def _db(self, write=False):
        db = sqlite3.connect(self.path, timeout=10, isolation_level=None)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys=ON")
        db.execute("PRAGMA busy_timeout=10000")
        db.execute("PRAGMA synchronous=FULL")
        try:
            if write:
                db.execute("BEGIN IMMEDIATE")
            yield db
            if write:
                db.commit()
        except BaseException:
            if db.in_transaction:
                db.rollback()
            raise
        finally:
            db.close()

    def initialize(self):
        parent = Path(self.path).parent
        missing = []
        while not parent.exists():
            missing.append(parent)
            parent = parent.parent
        for directory in reversed(missing):
            directory.mkdir(mode=0o700, exist_ok=True)
        try:
            descriptor = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except FileExistsError:
            pass
        else:
            os.close(descriptor)
        if os.name == "posix":
            os.chmod(self.path, 0o600)
            for suffix in ("-wal", "-shm"):
                try:
                    os.chmod(self.path + suffix, 0o600)
                except FileNotFoundError:
                    pass
        with self._db() as db:
            version = db.execute("PRAGMA user_version").fetchone()[0]
            if version > SCHEMA_VERSION:
                raise RuntimeError("Database schema is newer than this version of Relay")
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("BEGIN IMMEDIATE")
            # Recheck after taking the writer lock: another process may have migrated.
            version = db.execute("PRAGMA user_version").fetchone()[0]
            if version > SCHEMA_VERSION:
                raise RuntimeError("Database schema is newer than this version of Relay")
            for statement in SCHEMA.split(";"):
                if statement.strip():
                    db.execute(statement)
            if version < 2:
                additions = {
                    "projects": {"last_claimed_at": "REAL NOT NULL DEFAULT 0"},
                    "endpoints": {
                        "last_claimed_at": "REAL NOT NULL DEFAULT 0",
                        "consecutive_timeouts": "INTEGER NOT NULL DEFAULT 0",
                        "cooldown_until": "REAL NOT NULL DEFAULT 0",
                    },
                }
                for table, columns in additions.items():
                    existing = {row["name"] for row in db.execute(f"PRAGMA table_info({table})")}
                    for name, definition in columns.items():
                        if name not in existing:
                            db.execute(f"ALTER TABLE {table} ADD COLUMN {name} {definition}")
            for statement in SCHEDULER_SCHEMA.split(";"):
                if statement.strip():
                    db.execute(statement)
            if version < 3:
                additions = {
                    "projects": {
                        "previous_api_key_hash": "TEXT",
                        "previous_key_expires_at": "REAL",
                    },
                    "endpoints": {
                        "subscriptions_json": "TEXT NOT NULL DEFAULT '[\"*\"]'",
                        "previous_signing_secret": "TEXT",
                        "previous_secret_expires_at": "REAL",
                    },
                    "deliveries": {
                        "lease_endpoint_url": "TEXT",
                        "last_endpoint_url": "TEXT",
                        "cycle_started_at": "REAL",
                    },
                    "attempts": {"endpoint_url": "TEXT"},
                }
                for table, columns in additions.items():
                    existing = {row["name"] for row in db.execute(f"PRAGMA table_info({table})")}
                    for name, definition in columns.items():
                        if name not in existing:
                            db.execute(f"ALTER TABLE {table} ADD COLUMN {name} {definition}")
                db.execute(
                    "UPDATE deliveries SET cycle_started_at=created_at WHERE cycle_started_at IS NULL"
                )
            for statement in LIFECYCLE_SCHEMA.split(";"):
                if statement.strip():
                    db.execute(statement)
            if version < 4:
                existing = {row["name"] for row in db.execute("PRAGMA table_info(deliveries)")}
                if "lease_is_probe" not in existing:
                    db.execute(
                        "ALTER TABLE deliveries ADD COLUMN lease_is_probe INTEGER NOT NULL DEFAULT 0"
                    )
            if version < 5:
                additions = {
                    "endpoints": {
                        "response_class": "TEXT NOT NULL DEFAULT 'unknown'",
                        "response_classified_at": "REAL NOT NULL DEFAULT 0",
                    },
                    "deliveries": {"lease_is_risky": "INTEGER NOT NULL DEFAULT 0"},
                }
                for table, columns in additions.items():
                    existing = {row["name"] for row in db.execute(f"PRAGMA table_info({table})")}
                    for name, definition in columns.items():
                        if name not in existing:
                            db.execute(f"ALTER TABLE {table} ADD COLUMN {name} {definition}")
            for statement in AVAILABILITY_SCHEMA.split(";"):
                if statement.strip():
                    db.execute(statement)
            from .operations import initialize_operations

            initialize_operations(db)
            db.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
            db.commit()

    @staticmethod
    def _session_hash(value):
        return (
            isinstance(value, str)
            and len(value) == 64
            and all(character in "0123456789abcdef" for character in value)
        )

    def create_session(self, session_hash, expires_at):
        if not self._session_hash(session_hash):
            raise ValueError("session_hash must be a SHA-256 hexadecimal digest")
        if not math.isfinite(expires_at):
            raise ValueError("Session expiry must be finite")
        with self._db(True) as db:
            db.execute(
                """DELETE FROM sessions WHERE session_hash IN
                (SELECT session_hash FROM sessions WHERE expires_at<=? ORDER BY expires_at LIMIT 100)""",
                (time.time(),),
            )
            db.execute(
                """INSERT INTO sessions(session_hash,expires_at) VALUES(?,?)
                ON CONFLICT(session_hash) DO UPDATE SET expires_at=excluded.expires_at""",
                (session_hash, expires_at),
            )

    def valid_session(self, session_hash):
        if not self._session_hash(session_hash):
            return False
        with self._db() as db:
            return (
                db.execute(
                    "SELECT 1 FROM sessions WHERE session_hash=? AND expires_at>?",
                    (session_hash, time.time()),
                ).fetchone()
                is not None
            )

    def revoke_session(self, session_hash):
        with self._db(True) as db:
            db.execute("DELETE FROM sessions WHERE session_hash=?", (session_hash,))

    @staticmethod
    def _require_project(db, project_id):
        if not db.execute("SELECT 1 FROM projects WHERE id=?", (project_id,)).fetchone():
            raise ValueError("Unknown project")

    def create_project(self, name):
        name = _text(name, "name")
        key = "relay_" + secrets.token_urlsafe(32)
        result = {"id": _id("prj"), "name": name, "created_at": time.time()}
        with self._db(True) as db:
            db.execute(
                "INSERT INTO projects(id,name,api_key_hash,created_at) VALUES(?,?,?,?)",
                (
                    result["id"],
                    name,
                    hashlib.sha256(key.encode()).hexdigest(),
                    result["created_at"],
                ),
            )
        return {**result, "api_key": key}

    def list_projects(self):
        with self._db() as db:
            return [
                dict(r)
                for r in db.execute(
                    "SELECT id,name,created_at FROM projects ORDER BY created_at,id"
                )
            ]

    def authenticate_project(self, raw_key):
        if not isinstance(raw_key, str) or not raw_key:
            return None
        with self._db() as db:
            row = db.execute(
                """SELECT id,name,created_at FROM projects WHERE api_key_hash=?
                OR (previous_api_key_hash=? AND previous_key_expires_at>?)""",
                (
                    hashlib.sha256(raw_key.encode()).hexdigest(),
                    hashlib.sha256(raw_key.encode()).hexdigest(),
                    time.time(),
                ),
            ).fetchone()
            return dict(row) if row else None

    def rotate_project_key(self, project_id, overlap_seconds=0):
        overlap_seconds = _overlap(overlap_seconds)
        key = "relay_" + secrets.token_urlsafe(32)
        expires = time.time() + overlap_seconds if overlap_seconds else None
        with self._db(True) as db:
            self._require_project(db, project_id)
            db.execute(
                """UPDATE projects SET previous_api_key_hash=CASE WHEN ? IS NOT NULL THEN api_key_hash END,
                previous_key_expires_at=?,api_key_hash=? WHERE id=?""",
                (expires, expires, hashlib.sha256(key.encode()).hexdigest(), project_id),
            )
        return {"api_key": key, "previous_valid_until": expires}

    def create_endpoint(self, project_id, url, description="", rate_limit=10, subscriptions=None):
        url = _text(url, "url", 4096)
        if not isinstance(description, str) or len(description) > 2000:
            raise ValueError("Invalid description")
        _utf8(description, "description")
        subscriptions = _subscriptions(subscriptions)
        if (
            isinstance(rate_limit, bool)
            or not isinstance(rate_limit, int)
            or not 1 <= rate_limit <= 1000
        ):
            raise ValueError("rate_limit must be between 1 and 1000")
        endpoint_id = _id("ep")
        secret = "whsec_" + base64.b64encode(secrets.token_bytes(32)).decode()
        with self._db(True) as db:
            self._require_project(db, project_id)
            db.execute(
                """INSERT INTO endpoints(id,project_id,url,description,rate_limit,signing_secret,created_at,subscriptions_json)
                          VALUES(?,?,?,?,?,?,?,?)""",
                (
                    endpoint_id,
                    project_id,
                    url,
                    description,
                    rate_limit,
                    secret,
                    time.time(),
                    json.dumps(subscriptions),
                ),
            )
            result = _endpoint(
                db.execute("SELECT * FROM endpoints WHERE id=?", (endpoint_id,)).fetchone()
            )
        return {**result, "signing_secret": secret}

    def list_endpoints(self, project_id):
        with self._db() as db:
            return [
                _endpoint(r)
                for r in db.execute(
                    "SELECT * FROM endpoints WHERE project_id=? ORDER BY created_at,id",
                    (project_id,),
                )
            ]

    def set_endpoint_enabled(self, project_id, endpoint_id, enabled):
        return self.update_endpoint(project_id, endpoint_id, enabled=enabled)

    def update_endpoint(self, project_id, endpoint_id, **changes):
        if not changes or set(changes) - {
            "url",
            "description",
            "rate_limit",
            "enabled",
            "subscriptions",
        }:
            raise ValueError("Provide supported endpoint fields to update")
        values = {}
        for name, value in changes.items():
            if name == "url":
                value = _text(value, "url", 4096)
            elif name == "description":
                if not isinstance(value, str) or len(value) > 2000:
                    raise ValueError("Invalid description")
                _utf8(value, "description")
            elif name == "rate_limit":
                if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 1000:
                    raise ValueError("rate_limit must be between 1 and 1000")
            elif name == "enabled":
                if not isinstance(value, bool):
                    raise ValueError("enabled must be a boolean")
                value = int(value)
            elif name == "subscriptions":
                if value is None:
                    raise ValueError("subscriptions must be a list")
                name, value = "subscriptions_json", json.dumps(_subscriptions(value))
            values[name] = value
        with self._db(True) as db:
            if "url" in values:
                previous = db.execute(
                    "SELECT url FROM endpoints WHERE id=? AND project_id=?",
                    (endpoint_id, project_id),
                ).fetchone()
                if previous is not None and previous["url"] != values["url"]:
                    values.update(
                        response_class="unknown",
                        response_classified_at=0,
                        consecutive_timeouts=0,
                        cooldown_until=0,
                    )
            db.execute(
                "UPDATE endpoints SET "
                + ",".join(f"{name}=?" for name in values)
                + " WHERE id=? AND project_id=?",
                (*values.values(), endpoint_id, project_id),
            )
            return _endpoint(
                db.execute(
                    "SELECT * FROM endpoints WHERE id=? AND project_id=?", (endpoint_id, project_id)
                ).fetchone()
            )

    def rotate_endpoint_secret(self, project_id, endpoint_id, overlap_seconds=3600):
        overlap_seconds = _overlap(overlap_seconds)
        secret = "whsec_" + base64.b64encode(secrets.token_bytes(32)).decode()
        expires = time.time() + overlap_seconds if overlap_seconds else None
        with self._db(True) as db:
            changed = db.execute(
                """UPDATE endpoints SET
                previous_signing_secret=CASE WHEN ? IS NOT NULL THEN signing_secret END,
                previous_secret_expires_at=?,signing_secret=? WHERE id=? AND project_id=?""",
                (expires, expires, secret, endpoint_id, project_id),
            ).rowcount
            if not changed:
                return None
        return {"signing_secret": secret, "previous_valid_until": expires}

    def create_event(self, project_id, event_type, payload, idempotency_key=None):
        event_type = _text(event_type, "event_type")
        if idempotency_key is not None:
            _text(idempotency_key, "idempotency_key")
        raw = json.dumps(
            payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
        )
        _utf8(raw, "payload")
        with self._db(True) as db:
            self._require_project(db, project_id)
            if idempotency_key is not None:
                previous = db.execute(
                    EVENT_SELECT + " WHERE ev.project_id=? AND ev.idempotency_key=?",
                    (project_id, idempotency_key),
                ).fetchone()
                if previous is not None:
                    if previous["event_type"] != event_type or previous["payload_json"] != raw:
                        raise IdempotencyConflict(
                            "Idempotency key was already used for a different request"
                        )
                    return _event(previous), False
            event_id, now = _id("evt"), time.time()
            from .operations import check_tombstone

            check_tombstone(db, project_id, idempotency_key, event_type, raw, now=now)
            if len(serialize_event(event_id, event_type, now, payload)) > self.max_delivery_bytes:
                raise PayloadTooLarge(
                    "Serialized delivery body is too large; event was not accepted"
                )
            endpoints = [
                ep
                for ep in db.execute(
                    "SELECT id,subscriptions_json FROM endpoints WHERE project_id=? AND enabled=1",
                    (project_id,),
                )
                if _subscribed(json.loads(ep["subscriptions_json"]), event_type)
            ]
            if self.capacity_guard is not None:
                self.capacity_guard(
                    db, incoming_bytes=len(raw.encode("utf-8")), endpoint_count=len(endpoints)
                )
            db.execute(
                "INSERT INTO events VALUES(?,?,?,?,?,?)",
                (event_id, project_id, event_type, raw, idempotency_key, now),
            )
            db.executemany(
                """INSERT INTO deliveries(id,project_id,event_id,endpoint_id,status,next_attempt_at,created_at,updated_at,cycle_started_at)
                              VALUES(?,?,?,?,'pending',?,?,?,?)""",
                [
                    (_id("dlv"), project_id, event_id, ep["id"], now, now, now, now)
                    for ep in endpoints
                ],
            )
            return _event(db.execute(EVENT_SELECT + " WHERE ev.id=?", (event_id,)).fetchone()), True

    def get_event(self, project_id, event_id):
        with self._db() as db:
            return _event(
                db.execute(
                    EVENT_SELECT + " WHERE ev.id=? AND ev.project_id=?", (event_id, project_id)
                ).fetchone()
            )

    def list_deliveries(self, project_id, status=None, search=None, limit=50, offset=0):
        if isinstance(offset, bool) or not isinstance(offset, int) or not 0 <= offset <= 2**63 - 1:
            raise ValueError("offset must be between 0 and 9223372036854775807")
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 200:
            raise ValueError("limit must be between 1 and 200")
        clauses, args = ["d.project_id=?"], [project_id]
        if status:
            if status not in STATUSES:
                raise ValueError("Unknown delivery status")
            clauses.append("d.status=?")
            args.append(status)
        if search:
            clauses.append(
                "(d.id LIKE ? OR d.event_id LIKE ? OR ev.event_type LIKE ? OR ep.url LIKE ? OR d.last_error LIKE ?)"
            )
            args.extend([f"%{search}%"] * 5)
        query = DELIVERY_SELECT + " WHERE " + " AND ".join(clauses)
        with self._db() as db:
            db.execute("BEGIN")
            total = db.execute("SELECT COUNT(*) FROM (" + query + ")", args).fetchone()[0]
            rows = db.execute(
                query + " ORDER BY d.created_at DESC,d.id DESC LIMIT ? OFFSET ?",
                [*args, limit, offset],
            )
            return {"items": [dict(r) for r in rows], "total": total}

    def delivery_detail(self, project_id, delivery_id):
        with self._db() as db:
            db.execute("BEGIN")
            row = db.execute(
                DELIVERY_SELECT + " WHERE d.project_id=? AND d.id=?", (project_id, delivery_id)
            ).fetchone()
            if row is None:
                return None
            result = dict(row)
            result["payload"] = json.loads(
                db.execute(
                    "SELECT payload_json FROM events WHERE id=?", (row["event_id"],)
                ).fetchone()[0]
            )
            result["attempts"] = [
                dict(a)
                for a in db.execute(
                    "SELECT * FROM attempts WHERE delivery_id=? ORDER BY attempt_number",
                    (delivery_id,),
                )
            ]
            return result

    def stats(self, project_id):
        result = {status: 0 for status in STATUSES}
        with self._db() as db:
            db.execute("BEGIN")
            for r in db.execute(
                "SELECT status,COUNT(*) AS count FROM deliveries WHERE project_id=? GROUP BY status",
                (project_id,),
            ):
                result[r["status"]] = r["count"]
            average = db.execute(
                "SELECT AVG(last_duration_ms) FROM deliveries WHERE project_id=? AND status='delivered'",
                (project_id,),
            ).fetchone()[0]
        result["total"] = sum(result.values())
        result["success_rate"] = (
            round(100 * result["delivered"] / result["total"], 2) if result["total"] else 0.0
        )
        result["avg_latency_ms"] = round(average, 2) if average is not None else 0.0
        return result

    @staticmethod
    def _next_delivery(
        db,
        now,
        endpoint_concurrency,
        project_concurrency,
        max_failing_probes,
        worker_concurrency,
        endpoint_filter,
    ):
        probe_available = (
            db.execute(
                """SELECT COUNT(*) FROM deliveries INDEXED BY deliveries_processing_probes
            WHERE status='processing' AND lease_is_probe=1 AND lease_expires_at>?""",
                (now,),
            ).fetchone()[0]
            < max_failing_probes
        )
        risky_available = db.execute(
            """SELECT COUNT(*) FROM deliveries INDEXED BY deliveries_processing_risky
            WHERE status='processing' AND lease_is_risky=1 AND lease_expires_at>?""",
            (now,),
        ).fetchone()[0] < max(1, worker_concurrency - 1)
        projects = db.execute(
            """SELECT p.id FROM projects p WHERE
            (SELECT COUNT(*) FROM deliveries d INDEXED BY deliveries_processing_project
             WHERE d.project_id=p.id AND d.status='processing' AND d.lease_expires_at>?)<?
            ORDER BY p.last_claimed_at,p.created_at,p.id""",
            (now, project_concurrency),
        )
        for project in projects:
            project_risky_available = risky_available and db.execute(
                """SELECT COUNT(*) FROM deliveries INDEXED BY deliveries_processing_risky_project
                WHERE project_id=? AND status='processing' AND lease_is_risky=1 AND lease_expires_at>?""",
                (project["id"], now),
            ).fetchone()[0] < max(1, project_concurrency - 1)
            endpoints = db.execute(
                """SELECT ep.* FROM endpoints ep
                WHERE ep.project_id=? AND ep.enabled=1 AND ep.next_available_at<=?
                  AND ep.cooldown_until<=? AND (ep.consecutive_timeouts=0 OR ?) AND
                (SELECT COUNT(*) FROM deliveries d INDEXED BY deliveries_processing_endpoint
                 WHERE d.endpoint_id=ep.id AND d.status='processing' AND d.lease_expires_at>?)<?
                ORDER BY ep.last_claimed_at,ep.created_at,ep.id""",
                (project["id"], now, now, probe_available, now, endpoint_concurrency),
            )
            for endpoint in endpoints:
                risky = (
                    endpoint["consecutive_timeouts"] > 0
                    or endpoint["response_class"] != "responsive"
                    or endpoint["response_classified_at"] <= now - RESPONSIVE_SECONDS
                )
                if risky and not project_risky_available:
                    continue
                if endpoint_filter is not None and not endpoint_filter(
                    endpoint["url"], project["id"]
                ):
                    continue
                # Each lookup reads at most one indexed candidate. Disabled/rate-limited
                # endpoints never scan their parked deliveries while holding the writer lock.
                candidates = [
                    candidate
                    for candidate in (
                        db.execute(PENDING_DELIVERY, (endpoint["id"], now)).fetchone(),
                        db.execute(EXPIRED_DELIVERY, (endpoint["id"], now)).fetchone(),
                    )
                    if candidate is not None
                ]
                if candidates:
                    row = min(
                        candidates,
                        key=lambda candidate: (
                            candidate["lease_expires_at"]
                            if candidate["status"] == "processing"
                            else candidate["next_attempt_at"],
                            candidate["created_at"],
                            candidate["id"],
                        ),
                    )
                    event = db.execute(
                        "SELECT event_type,payload_json FROM events WHERE id=?", (row["event_id"],)
                    ).fetchone()
                    return {
                        **dict(row),
                        **dict(event),
                        "endpoint_url": endpoint["url"],
                        "signing_secret": endpoint["signing_secret"],
                        "previous_signing_secret": (
                            endpoint["previous_signing_secret"]
                            if (endpoint["previous_secret_expires_at"] or 0) > now
                            else None
                        ),
                        "rate_limit": endpoint["rate_limit"],
                        "lease_is_probe": int(endpoint["consecutive_timeouts"] > 0),
                        "lease_is_risky": int(risky),
                    }
        return None

    def claim_delivery(
        self,
        lease_seconds=30,
        *,
        endpoint_concurrency=1,
        project_concurrency=2,
        max_failing_probes=1,
        worker_concurrency=4,
        endpoint_filter=None,
    ):
        if not math.isfinite(lease_seconds) or lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive and finite")
        for name, value in (
            ("endpoint_concurrency", endpoint_concurrency),
            ("project_concurrency", project_concurrency),
            ("max_failing_probes", max_failing_probes),
            ("worker_concurrency", worker_concurrency),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValueError(f"{name} must be a positive integer")
        if endpoint_filter is not None and not callable(endpoint_filter):
            raise TypeError("endpoint_filter must be callable")
        with self._db(True) as db:
            now = time.time()
            row = self._next_delivery(
                db,
                now,
                endpoint_concurrency,
                project_concurrency,
                max_failing_probes,
                worker_concurrency,
                endpoint_filter,
            )
            if row is None:
                return None
            token = secrets.token_urlsafe(24)
            db.execute(
                """UPDATE deliveries SET status='processing',lease_token=?,lease_expires_at=?,
                lease_started_at=?,lease_endpoint_url=?,lease_is_probe=?,lease_is_risky=?,updated_at=? WHERE id=?""",
                (
                    token,
                    now + lease_seconds,
                    now,
                    row["endpoint_url"],
                    row["lease_is_probe"],
                    row["lease_is_risky"],
                    now,
                    row["id"],
                ),
            )
            db.execute(
                "UPDATE endpoints SET next_available_at=?,last_claimed_at=? WHERE id=?",
                (now + 1 / row["rate_limit"], now, row["endpoint_id"]),
            )
            db.execute("UPDATE projects SET last_claimed_at=? WHERE id=?", (now, row["project_id"]))
            return {
                **dict(row),
                "delivery_id": row["id"],
                "lease_token": token,
                "lease_expires_at": now + lease_seconds,
                "status": "processing",
            }

    def complete_delivery(
        self,
        delivery_id,
        lease_token,
        *,
        status_code,
        error,
        duration_ms,
        response_excerpt,
        next_attempt_at=None,
        max_attempts=6,
        timed_out=False,
    ):
        if not math.isfinite(duration_ms) or duration_ms < 0:
            raise ValueError("duration_ms must be nonnegative and finite")
        if next_attempt_at is not None and not math.isfinite(next_attempt_at):
            raise ValueError("next_attempt_at must be finite")
        if not isinstance(timed_out, bool):
            raise ValueError("timed_out must be a boolean")
        with self._db(True) as db:
            now = time.time()
            row = db.execute(
                "SELECT * FROM deliveries WHERE id=? AND status='processing' AND lease_token=? AND lease_expires_at>?",
                (delivery_id, lease_token, now),
            ).fetchone()
            if row is None:
                return False
            number, cycle = row["attempt_count"] + 1, row["cycle_attempts"] + 1
            if status_code is not None and 200 <= status_code < 300:
                status, next_attempt_at, error = "delivered", None, None
            else:
                fallback = f"HTTP {status_code}" if status_code is not None else "Delivery failed"
                error = str(error or fallback)[:2000]
                status = (
                    "retrying" if next_attempt_at is not None and cycle < max_attempts else "failed"
                )
                if status == "failed":
                    next_attempt_at = None
            excerpt = str(response_excerpt or "")[:4096]
            db.execute(
                """INSERT INTO attempts(delivery_id,attempt_number,cycle_attempt,status_code,error,duration_ms,response_excerpt,started_at,finished_at,created_at,endpoint_url)
                          VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    delivery_id,
                    number,
                    cycle,
                    status_code,
                    error,
                    duration_ms,
                    excerpt,
                    row["lease_started_at"],
                    now,
                    now,
                    row["lease_endpoint_url"],
                ),
            )
            db.execute(
                """UPDATE deliveries SET status=?,attempt_count=?,cycle_attempts=?,next_attempt_at=?,last_status_code=?,last_error=?,last_duration_ms=?,
                          last_response_excerpt=?,updated_at=?,last_endpoint_url=lease_endpoint_url,
                          lease_endpoint_url=NULL,lease_token=NULL,lease_expires_at=NULL,lease_started_at=NULL,
                          lease_is_probe=0,lease_is_risky=0 WHERE id=?""",
                (
                    status,
                    number,
                    cycle,
                    next_attempt_at,
                    status_code,
                    error,
                    duration_ms,
                    excerpt,
                    now,
                    delivery_id,
                ),
            )
            previous = db.execute(
                "SELECT consecutive_timeouts FROM endpoints WHERE id=? AND url=?",
                (row["endpoint_id"], row["lease_endpoint_url"]),
            ).fetchone()
            # A completed request to an old URL belongs in history, but says
            # nothing about the endpoint's replacement destination.
            if previous is None:
                return True
            if timed_out and status != "delivered":
                previous = previous["consecutive_timeouts"]
                streak = min(previous + 1, 32)
                cooldown = min(600, 30 * 2 ** min(streak - 3, 5)) if streak >= 3 else 0
                db.execute(
                    """UPDATE endpoints SET
                    cooldown_until=CASE WHEN ?>=3 THEN ? ELSE cooldown_until END,
                    consecutive_timeouts=? WHERE id=?""",
                    (streak, now + cooldown, streak, row["endpoint_id"]),
                )
            else:
                db.execute(
                    "UPDATE endpoints SET consecutive_timeouts=0,cooldown_until=0 WHERE id=?",
                    (row["endpoint_id"],),
                )
            if status_code is not None or timed_out or duration_ms >= SLOW_RESPONSE_MS:
                response_class = (
                    "slow" if timed_out or duration_ms >= SLOW_RESPONSE_MS else "responsive"
                )
                db.execute(
                    "UPDATE endpoints SET response_class=?,response_classified_at=? WHERE id=?",
                    (response_class, now, row["endpoint_id"]),
                )
            return True

    def defer_delivery(self, delivery_id, lease_token, *, reason, retry_after_seconds=1):
        """Return an owned claim to the queue without blaming its destination."""
        if reason not in {"dns_global_capacity", "dns_project_capacity"}:
            raise ValueError("Unknown delivery deferral reason")
        if (
            isinstance(retry_after_seconds, bool)
            or not isinstance(retry_after_seconds, (int, float))
            or not math.isfinite(retry_after_seconds)
            or not 0.1 <= retry_after_seconds <= 60
        ):
            raise ValueError("Deferral delay must be between 0.1 and 60 seconds")
        with self._db(True) as db:
            now = time.time()
            changed = db.execute(
                """UPDATE deliveries SET status=CASE WHEN cycle_attempts=0 THEN 'pending' ELSE 'retrying' END,
                next_attempt_at=?,updated_at=?,lease_token=NULL,lease_expires_at=NULL,lease_started_at=NULL,
                lease_endpoint_url=NULL,lease_is_probe=0,lease_is_risky=0
                WHERE id=? AND status='processing' AND lease_token=? AND lease_expires_at>?""",
                (now + retry_after_seconds, now, delivery_id, lease_token, now),
            ).rowcount
            if changed:
                db.execute(
                    """INSERT INTO delivery_deferrals(reason,count,last_seen_at) VALUES(?,1,?)
                           ON CONFLICT(reason) DO UPDATE SET count=MIN(count+1,9223372036854775807),
                           last_seen_at=excluded.last_seen_at""",
                    (reason, now),
                )
            return bool(changed)

    def deferral_stats(self):
        with self._db() as db:
            return {
                row["reason"]: {"count": row["count"], "last_seen_at": row["last_seen_at"]}
                for row in db.execute("SELECT * FROM delivery_deferrals")
            }

    def replay_delivery(self, project_id, delivery_id):
        with self._db(True) as db:
            now = time.time()
            return (
                db.execute(
                    """UPDATE deliveries SET status='pending',cycle_attempts=0,next_attempt_at=?,updated_at=?,cycle_started_at=?,lease_token=NULL,lease_expires_at=NULL,lease_started_at=NULL,lease_endpoint_url=NULL,
                last_status_code=NULL,last_error=NULL,last_duration_ms=NULL,last_response_excerpt=NULL WHERE id=? AND project_id=?
                AND (status!='processing' OR lease_expires_at<=?)""",
                    (now, now, now, delivery_id, project_id, now),
                ).rowcount
                == 1
            )

    @staticmethod
    def _cleanup_recoveries(db, now):
        # A completed job remains inspectable for 30 days. Preview snapshots are
        # deliberately short lived and never expand when more failures arrive.
        db.execute(
            """DELETE FROM recovery_previews WHERE id IN
                   (SELECT id FROM recovery_previews WHERE expires_at<=? LIMIT 100)""",
            (now,),
        )
        db.execute(
            """DELETE FROM recovery_jobs WHERE id IN
                   (SELECT id FROM recovery_jobs WHERE status!='running' AND updated_at<?
                    ORDER BY updated_at LIMIT 10)""",
            (now - 30 * 86400,),
        )
        db.execute(
            """UPDATE endpoints SET previous_signing_secret=NULL,
                   previous_secret_expires_at=NULL WHERE previous_secret_expires_at<=?""",
            (now,),
        )
        db.execute(
            """UPDATE projects SET previous_api_key_hash=NULL,
                   previous_key_expires_at=NULL WHERE previous_key_expires_at<=?""",
            (now,),
        )

    def preview_recovery(self, project_id, endpoint_id=None, since=None, until=None, limit=1000):
        """Freeze failed delivery IDs and versions; time filters use last failure time."""
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 1000:
            raise ValueError("limit must be between 1 and 1000")
        for name, value in (("since", since), ("until", until)):
            if value is not None and (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(value)
                or not 0 <= value <= 253402300799
            ):
                raise ValueError(f"{name} must be a finite Unix timestamp")
        if since is not None and until is not None and since > until:
            raise ValueError("since must be before or equal to until")
        clauses, args = ["project_id=?", "status='failed'"], [project_id]
        if endpoint_id is not None:
            if not isinstance(endpoint_id, str):
                raise ValueError("endpoint_id must be a string")
            clauses.append("endpoint_id=?")
            args.append(endpoint_id)
        if since is not None:
            clauses.append("updated_at>=?")
            args.append(since)
        if until is not None:
            clauses.append("updated_at<=?")
            args.append(until)
        filters = {"endpoint_id": endpoint_id, "since": since, "until": until, "limit": limit}
        now, preview_id = time.time(), _id("rpv")
        with self._db(True) as db:
            self._require_project(db, project_id)
            self._cleanup_recoveries(db, now)
            if (
                endpoint_id is not None
                and not db.execute(
                    "SELECT 1 FROM endpoints WHERE id=? AND project_id=?", (endpoint_id, project_id)
                ).fetchone()
            ):
                raise ValueError("Unknown endpoint")
            if (
                db.execute(
                    """SELECT COUNT(*) FROM recovery_previews WHERE project_id=?
                          AND used_job_id IS NULL AND expires_at>?""",
                    (project_id, now),
                ).fetchone()[0]
                >= 10
            ):
                raise ValueError(
                    "At most 10 unused recovery previews may exist per project; wait for expiry"
                )
            if db.execute("SELECT COUNT(*) FROM recovery_previews").fetchone()[0] >= 1000:
                raise ValueError("Recovery preview capacity reached; wait for expiry")
            where = " AND ".join(clauses)
            total = db.execute("SELECT COUNT(*) FROM deliveries WHERE " + where, args).fetchone()[0]
            members = [
                dict(row)
                for row in db.execute(
                    "SELECT id,event_id,updated_at,attempt_count FROM deliveries WHERE "
                    + where
                    + " ORDER BY updated_at,id LIMIT ?",
                    (*args, limit),
                )
            ]
            db.execute(
                """INSERT INTO recovery_previews
                       (id,project_id,filters_json,membership_json,total,expires_at,created_at)
                       VALUES(?,?,?,?,?,?,?)""",
                (
                    preview_id,
                    project_id,
                    json.dumps(filters),
                    json.dumps(members),
                    total,
                    now + 900,
                    now,
                ),
            )
        return {
            "preview_id": preview_id,
            "total": total,
            "selected_count": len(members),
            "expires_at": now + 900,
            "sample": [member["id"] for member in members[:20]],
            "filters": filters,
        }

    @staticmethod
    def _recovery(db, row):
        if row is None:
            return None
        result = dict(row)
        counts = {"queued": 0, "requeued": 0, "skipped": 0, "cancelled": 0}
        for item in db.execute(
            "SELECT state,COUNT(*) AS n FROM recovery_items WHERE job_id=? GROUP BY state",
            (row["id"],),
        ):
            counts[item["state"]] = item["n"]
        outcomes = {status: 0 for status in STATUSES}
        outcomes["removed"] = 0
        for item in db.execute(
            """SELECT d.status,COUNT(*) AS n FROM recovery_items i
                               LEFT JOIN deliveries d ON d.id=i.delivery_id
                               WHERE i.job_id=? AND i.state='requeued' GROUP BY d.status""",
            (row["id"],),
        ):
            outcomes[item["status"] or "removed"] = item["n"]
        return {**result, **counts, "total": sum(counts.values()), "outcomes": outcomes}

    def create_recovery(self, project_id, preview_id, pace_per_second=10):
        if (
            isinstance(pace_per_second, bool)
            or not isinstance(pace_per_second, int)
            or not 1 <= pace_per_second <= 1000
        ):
            raise ValueError("pace_per_second must be between 1 and 1000")
        if not isinstance(preview_id, str):
            raise ValueError("preview_id must be a string")
        with self._db(True) as db:
            self._require_project(db, project_id)
            now = time.time()
            existing = db.execute(
                "SELECT * FROM recovery_jobs WHERE preview_id=? AND project_id=?",
                (preview_id, project_id),
            ).fetchone()
            if existing:
                if existing["pace_per_second"] != pace_per_second:
                    raise ValueError("Preview was already started with a different pace")
                return self._recovery(db, existing)
            preview = db.execute(
                "SELECT * FROM recovery_previews WHERE id=? AND project_id=? AND expires_at>?",
                (preview_id, project_id, now),
            ).fetchone()
            if preview is None:
                raise ValueError("Recovery preview is unknown or expired; create another preview")
            if (
                db.execute(
                    "SELECT COUNT(*) FROM recovery_jobs WHERE project_id=?", (project_id,)
                ).fetchone()[0]
                >= 1000
            ):
                raise ValueError(
                    "Recovery history capacity reached; completed jobs expire after 30 days"
                )
            if (
                db.execute(
                    "SELECT COUNT(*) FROM recovery_jobs WHERE project_id=? AND status='running'",
                    (project_id,),
                ).fetchone()[0]
                >= 10
            ):
                raise ValueError("At most 10 recovery jobs may run per project")
            if (
                db.execute("SELECT COUNT(*) FROM recovery_jobs WHERE status='running'").fetchone()[
                    0
                ]
                >= 100
            ):
                raise ValueError("At most 100 recovery jobs may run at once")
            members = json.loads(preview["membership_json"])
            job_id = _id("rcv")
            db.execute(
                """INSERT INTO recovery_jobs
                       (id,project_id,preview_id,status,pace_per_second,next_release_at,created_at,updated_at)
                       VALUES(?,?,?,?,?,?,?,?)""",
                (
                    job_id,
                    project_id,
                    preview_id,
                    "running" if members else "completed",
                    pace_per_second,
                    now,
                    now,
                    now,
                ),
            )
            for member in members:
                eligible = db.execute(
                    """SELECT 1 FROM deliveries d WHERE d.id=? AND d.project_id=?
                    AND d.status='failed' AND d.updated_at=? AND d.attempt_count=? AND NOT EXISTS
                    (SELECT 1 FROM recovery_items i JOIN recovery_jobs j ON j.id=i.job_id
                     WHERE i.delivery_id=d.id AND i.state='queued' AND j.status='running')""",
                    (member["id"], project_id, member["updated_at"], member["attempt_count"]),
                ).fetchone()
                db.execute(
                    "INSERT INTO recovery_items VALUES(?,?,?,?,?,?)",
                    (
                        job_id,
                        member["id"],
                        member["event_id"],
                        member["updated_at"],
                        member["attempt_count"],
                        "queued" if eligible else "skipped",
                    ),
                )
            db.execute(
                "UPDATE recovery_previews SET used_job_id=? WHERE id=?", (job_id, preview_id)
            )
            db.execute(
                """UPDATE recovery_jobs SET status='completed' WHERE id=? AND NOT EXISTS
                       (SELECT 1 FROM recovery_items WHERE job_id=? AND state='queued')""",
                (job_id, job_id),
            )
            return self._recovery(
                db, db.execute("SELECT * FROM recovery_jobs WHERE id=?", (job_id,)).fetchone()
            )

    def get_recovery(self, project_id, job_id):
        with self._db() as db:
            db.execute("BEGIN")
            return self._recovery(
                db,
                db.execute(
                    "SELECT * FROM recovery_jobs WHERE id=? AND project_id=?", (job_id, project_id)
                ).fetchone(),
            )

    def list_recoveries(self, project_id, limit=50):
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 100:
            raise ValueError("limit must be between 1 and 100")
        with self._db() as db:
            db.execute("BEGIN")
            return [
                self._recovery(db, row)
                for row in db.execute(
                    "SELECT * FROM recovery_jobs WHERE project_id=? ORDER BY created_at DESC,id LIMIT ?",
                    (project_id, limit),
                )
            ]

    def cancel_recovery(self, project_id, job_id):
        with self._db(True) as db:
            row = db.execute(
                "SELECT * FROM recovery_jobs WHERE id=? AND project_id=?", (job_id, project_id)
            ).fetchone()
            if row is None:
                return None
            if row["status"] == "running":
                db.execute(
                    "UPDATE recovery_items SET state='cancelled' WHERE job_id=? AND state='queued'",
                    (job_id,),
                )
                db.execute(
                    "UPDATE recovery_jobs SET status='cancelled',updated_at=? WHERE id=?",
                    (time.time(), job_id),
                )
            return self._recovery(
                db, db.execute("SELECT * FROM recovery_jobs WHERE id=?", (job_id,)).fetchone()
            )

    def advance_recoveries(self):
        """Release at most one member from each of ten due jobs, without catch-up bursts."""
        now = time.time()
        clean = time.monotonic() >= self._next_recovery_cleanup
        if not clean:
            with self._db() as db:
                if not db.execute(
                    "SELECT 1 FROM recovery_jobs WHERE status='running' AND next_release_at<=? LIMIT 1",
                    (now,),
                ).fetchone():
                    return 0
        with self._db(True) as db:
            # Pace from the instant the writer reservation is acquired. Waiting
            # behind another writer must not accumulate overdue release slots.
            now = time.time()
            if clean:
                self._cleanup_recoveries(db, now)
                self._next_recovery_cleanup = time.monotonic() + 60
            jobs = db.execute(
                """SELECT * FROM recovery_jobs WHERE status='running' AND next_release_at<=?
                              ORDER BY next_release_at,created_at,id LIMIT 10""",
                (now,),
            ).fetchall()
            released = 0
            for job in jobs:
                item = db.execute(
                    "SELECT * FROM recovery_items WHERE job_id=? AND state='queued' ORDER BY delivery_id LIMIT 1",
                    (job["id"],),
                ).fetchone()
                if item:
                    changed = db.execute(
                        """UPDATE deliveries SET status='pending',cycle_attempts=0,
                        next_attempt_at=?,updated_at=?,cycle_started_at=?,lease_token=NULL,lease_expires_at=NULL,
                        lease_started_at=NULL,lease_endpoint_url=NULL,last_status_code=NULL,
                        last_error=NULL,last_duration_ms=NULL,last_response_excerpt=NULL
                        WHERE id=? AND project_id=? AND status='failed' AND updated_at=? AND attempt_count=?""",
                        (
                            now,
                            now,
                            now,
                            item["delivery_id"],
                            job["project_id"],
                            item["snapshot_updated_at"],
                            item["snapshot_attempt_count"],
                        ),
                    ).rowcount
                    db.execute(
                        "UPDATE recovery_items SET state=? WHERE job_id=? AND delivery_id=?",
                        ("requeued" if changed else "skipped", job["id"], item["delivery_id"]),
                    )
                    released += changed
                remaining = db.execute(
                    "SELECT 1 FROM recovery_items WHERE job_id=? AND state='queued' LIMIT 1",
                    (job["id"],),
                ).fetchone()
                db.execute(
                    "UPDATE recovery_jobs SET status=?,next_release_at=?,updated_at=? WHERE id=?",
                    (
                        "running" if remaining else "completed",
                        now + 1 / job["pace_per_second"],
                        now,
                        job["id"],
                    ),
                )
            return released
