"""Durable webhook storage, queue claims, and delivery history."""

import base64
from contextlib import contextmanager
import hashlib
import json
import math
import os
from pathlib import Path
import secrets
import sqlite3
import time

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
SCHEMA_VERSION = 2
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
 ep.url AS endpoint_url,ep.enabled AS endpoint_enabled,d.status,d.attempt_count,d.cycle_attempts,
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
    result.pop("next_available_at", None)
    result.pop("last_claimed_at", None)
    result["enabled"] = bool(result["enabled"])
    return result


class Store:
    """File-backed SQLite queue. Mutations take a short exclusive writer reservation."""

    def __init__(self, path):
        if str(path) == ":memory:":
            raise ValueError("Use a file-backed database so deliveries survive process restarts")
        self.path = str(Path(path).expanduser().resolve())

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
                "SELECT id,name,created_at FROM projects WHERE api_key_hash=?",
                (hashlib.sha256(raw_key.encode()).hexdigest(),),
            ).fetchone()
            return dict(row) if row else None

    def rotate_project_key(self, project_id):
        key = "relay_" + secrets.token_urlsafe(32)
        with self._db(True) as db:
            self._require_project(db, project_id)
            db.execute(
                "UPDATE projects SET api_key_hash=? WHERE id=?",
                (hashlib.sha256(key.encode()).hexdigest(), project_id),
            )
        return {"api_key": key}

    def create_endpoint(self, project_id, url, description="", rate_limit=10):
        url = _text(url, "url", 4096)
        if not isinstance(description, str) or len(description) > 2000:
            raise ValueError("Invalid description")
        _utf8(description, "description")
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
                """INSERT INTO endpoints(id,project_id,url,description,rate_limit,signing_secret,created_at)
                          VALUES(?,?,?,?,?,?,?)""",
                (endpoint_id, project_id, url, description, rate_limit, secret, time.time()),
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
        if not isinstance(enabled, bool):
            raise ValueError("enabled must be a boolean")
        with self._db(True) as db:
            db.execute(
                "UPDATE endpoints SET enabled=? WHERE id=? AND project_id=?",
                (int(enabled), endpoint_id, project_id),
            )
            return _endpoint(
                db.execute(
                    "SELECT * FROM endpoints WHERE id=? AND project_id=?", (endpoint_id, project_id)
                ).fetchone()
            )

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
            db.execute(
                "INSERT INTO events VALUES(?,?,?,?,?,?)",
                (event_id, project_id, event_type, raw, idempotency_key, now),
            )
            endpoints = db.execute(
                "SELECT id FROM endpoints WHERE project_id=? AND enabled=1", (project_id,)
            ).fetchall()
            db.executemany(
                """INSERT INTO deliveries(id,project_id,event_id,endpoint_id,status,next_attempt_at,created_at,updated_at)
                              VALUES(?,?,?,?,'pending',?,?,?)""",
                [(_id("dlv"), project_id, event_id, ep["id"], now, now, now) for ep in endpoints],
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
    def _next_delivery(db, now, endpoint_concurrency, project_concurrency):
        projects = db.execute(
            """SELECT p.id FROM projects p WHERE
            (SELECT COUNT(*) FROM deliveries d INDEXED BY deliveries_processing_project
             WHERE d.project_id=p.id AND d.status='processing' AND d.lease_expires_at>?)<?
            ORDER BY p.last_claimed_at,p.created_at,p.id""",
            (now, project_concurrency),
        )
        for project in projects:
            endpoints = db.execute(
                """SELECT ep.* FROM endpoints ep
                WHERE ep.project_id=? AND ep.enabled=1 AND ep.next_available_at<=?
                  AND ep.cooldown_until<=? AND
                (SELECT COUNT(*) FROM deliveries d INDEXED BY deliveries_processing_endpoint
                 WHERE d.endpoint_id=ep.id AND d.status='processing' AND d.lease_expires_at>?)<?
                ORDER BY ep.last_claimed_at,ep.created_at,ep.id""",
                (project["id"], now, now, now, endpoint_concurrency),
            )
            for endpoint in endpoints:
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
                        "rate_limit": endpoint["rate_limit"],
                    }
        return None

    def claim_delivery(self, lease_seconds=30, *, endpoint_concurrency=1, project_concurrency=2):
        if not math.isfinite(lease_seconds) or lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive and finite")
        for name, value in (
            ("endpoint_concurrency", endpoint_concurrency),
            ("project_concurrency", project_concurrency),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValueError(f"{name} must be a positive integer")
        with self._db(True) as db:
            now = time.time()
            row = self._next_delivery(db, now, endpoint_concurrency, project_concurrency)
            if row is None:
                return None
            token = secrets.token_urlsafe(24)
            db.execute(
                "UPDATE deliveries SET status='processing',lease_token=?,lease_expires_at=?,lease_started_at=?,updated_at=? WHERE id=?",
                (token, now + lease_seconds, now, now, row["id"]),
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
                """INSERT INTO attempts(delivery_id,attempt_number,cycle_attempt,status_code,error,duration_ms,response_excerpt,started_at,finished_at,created_at)
                          VALUES(?,?,?,?,?,?,?,?,?,?)""",
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
                ),
            )
            db.execute(
                """UPDATE deliveries SET status=?,attempt_count=?,cycle_attempts=?,next_attempt_at=?,last_status_code=?,last_error=?,last_duration_ms=?,
                          last_response_excerpt=?,updated_at=?,lease_token=NULL,lease_expires_at=NULL,lease_started_at=NULL WHERE id=?""",
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
            if timed_out and status != "delivered":
                db.execute(
                    """UPDATE endpoints SET
                    cooldown_until=CASE WHEN consecutive_timeouts>=2 THEN ? ELSE cooldown_until END,
                    consecutive_timeouts=MIN(consecutive_timeouts+1,3) WHERE id=?""",
                    (now + 30, row["endpoint_id"]),
                )
            else:
                db.execute(
                    "UPDATE endpoints SET consecutive_timeouts=0,cooldown_until=0 WHERE id=?",
                    (row["endpoint_id"],),
                )
            return True

    def replay_delivery(self, project_id, delivery_id):
        with self._db(True) as db:
            now = time.time()
            return (
                db.execute(
                    """UPDATE deliveries SET status='pending',cycle_attempts=0,next_attempt_at=?,updated_at=?,lease_token=NULL,lease_expires_at=NULL,lease_started_at=NULL,
                last_status_code=NULL,last_error=NULL,last_duration_ms=NULL,last_response_excerpt=NULL WHERE id=? AND project_id=?
                AND (status!='processing' OR lease_expires_at<=?)""",
                    (now, now, delivery_id, project_id, now),
                ).rowcount
                == 1
            )
