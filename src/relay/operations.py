"""Bounded maintenance, aggregate metrics, and durable operator notifications."""

import hashlib
import json
import logging
import math
import secrets
import shutil
import threading
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from urllib.parse import urlsplit

from .delivery import deliver, sign_headers

logger = logging.getLogger(__name__)
WINDOW_SECONDS = 3600
MAX_NOTIFICATION_ATTEMPTS = 3
RETENTION_SCAN_LIMIT = 1000
CLEANUP_BURST_SECONDS = 0.2
CLEANUP_BURST_MAX_BATCHES = 50
CLEANUP_CATCHUP_SECONDS = 1
CLEANUP_BATCH_YIELD_SECONDS = 0.001
RESOURCE_REASONS = ("dns_global_capacity", "dns_project_capacity")
RESOURCE_PRESSURE_SECONDS = 60
OPERATIONS_SCHEMA = """
CREATE TABLE IF NOT EXISTS idempotency_tombstones (
 project_id TEXT NOT NULL REFERENCES projects(id), key_hash TEXT NOT NULL,
 event_id TEXT NOT NULL, request_hash TEXT NOT NULL, expires_at REAL NOT NULL,
 PRIMARY KEY(project_id,key_hash)
);
CREATE INDEX IF NOT EXISTS tombstones_expiry ON idempotency_tombstones(expires_at);
CREATE INDEX IF NOT EXISTS events_retention ON events(created_at,id);
CREATE INDEX IF NOT EXISTS attempts_finished ON attempts(finished_at);
CREATE INDEX IF NOT EXISTS deliveries_active_event ON deliveries(event_id)
 WHERE status IN ('pending','processing','retrying');
CREATE INDEX IF NOT EXISTS deliveries_retention_updated ON deliveries(event_id,updated_at);
CREATE TABLE IF NOT EXISTS operation_alerts (
 code TEXT PRIMARY KEY, severity TEXT NOT NULL, active INTEGER NOT NULL,
 message TEXT NOT NULL, first_seen_at REAL NOT NULL, updated_at REAL NOT NULL,
 resolved_at REAL, version INTEGER NOT NULL DEFAULT 1,
 notification_attempts INTEGER NOT NULL DEFAULT 0, next_notification_at REAL,
 lease_token TEXT, lease_expires_at REAL, last_notified_at REAL,
 last_notification_status TEXT, notification_cycle INTEGER NOT NULL DEFAULT 0,
 notification_gate_until REAL NOT NULL DEFAULT 0,
 notification_version INTEGER NOT NULL DEFAULT 0,
 healthy_since REAL, last_observed_at REAL
);
CREATE TABLE IF NOT EXISTS operation_leases (
 name TEXT PRIMARY KEY, token TEXT NOT NULL, expires_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS operation_cursors (
 name TEXT PRIMARY KEY, created_at REAL NOT NULL, event_id TEXT NOT NULL
);
"""


def initialize_operations(db):
    """Called inside Store.initialize's migration transaction."""
    for statement in OPERATIONS_SCHEMA.split(";"):
        if statement.strip():
            db.execute(statement)
    existing = {row["name"] for row in db.execute("PRAGMA table_info(operation_alerts)")}
    for name, definition in {
        "notification_gate_until": "REAL NOT NULL DEFAULT 0",
        "notification_version": "INTEGER NOT NULL DEFAULT 0",
        "healthy_since": "REAL",
        "last_observed_at": "REAL",
    }.items():
        if name not in existing:
            db.execute(f"ALTER TABLE operation_alerts ADD COLUMN {name} {definition}")


class IdempotencyExpired(RuntimeError):
    """A retained key identifies an event whose payload has been purged."""

    def __init__(self, event_id):
        self.event_id = event_id
        super().__init__(
            "This idempotency key belongs to an expired event; no new event was created"
        )


class CapacityExceeded(RuntimeError):
    """Configured capacity admission has rejected a new event."""


def _request_hash(event_type, raw):
    return hashlib.sha256(json.dumps([event_type, raw], ensure_ascii=False).encode()).hexdigest()


def check_tombstone(db, project_id, key, event_type, raw, now=None):
    """Check inside the event transaction, after checking the live event table."""
    if key is None:
        return
    now = time.time() if now is None else now
    key_hash = hashlib.sha256(key.encode()).hexdigest()
    previous = db.execute(
        "SELECT * FROM idempotency_tombstones WHERE project_id=? AND key_hash=?",
        (project_id, key_hash),
    ).fetchone()
    if previous is None:
        return
    if previous["expires_at"] <= now:
        db.execute(
            "DELETE FROM idempotency_tombstones WHERE project_id=? AND key_hash=?",
            (project_id, key_hash),
        )
        return
    if previous["request_hash"] != _request_hash(event_type, raw):
        from .store import IdempotencyConflict

        raise IdempotencyConflict("Idempotency key was already used for a different request")
    raise IdempotencyExpired(previous["event_id"])


@dataclass(frozen=True)
class OperationsPolicy:
    retention_days: int = 0
    idempotency_days: int = 90
    cleanup_batch_size: int = 100
    min_free_bytes: int = 0
    max_storage_bytes: int = 0
    storage_warning_percent: float = 90
    queue_warning_age_seconds: float = 300
    failure_warning_percent: float = 25
    failure_min_samples: int = 10
    alert_webhook_url: str | None = None
    alert_signing_secret: str | None = None
    alert_cooldown_seconds: float = 3600
    alert_rearm_seconds: float = 300
    interval_seconds: float = 30

    def __post_init__(self):
        integer_bounds = {
            "retention_days": (0, 36500),
            "idempotency_days": (1, 36500),
            "cleanup_batch_size": (1, 1000),
            "min_free_bytes": (0, 2**63 - 1),
            "max_storage_bytes": (0, 2**63 - 1),
            "failure_min_samples": (1, 1000000),
        }
        for name, (minimum, maximum) in integer_bounds.items():
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, int)
                or not minimum <= value <= maximum
            ):
                raise ValueError(f"{name} must be an integer between {minimum} and {maximum}")
        for name in ("storage_warning_percent", "failure_warning_percent"):
            value = getattr(self, name)
            if isinstance(value, bool) or not math.isfinite(value) or not 0 < value <= 100:
                raise ValueError(f"{name} must be greater than 0 and at most 100")
        for name in (
            "queue_warning_age_seconds",
            "alert_cooldown_seconds",
            "alert_rearm_seconds",
            "interval_seconds",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not math.isfinite(value) or value < 1:
                raise ValueError(f"{name} must be finite and at least 1 second")
        if bool(self.alert_webhook_url) != bool(self.alert_signing_secret):
            raise ValueError("Alert webhook URL and signing secret must be configured together")
        if self.alert_signing_secret:
            sign_headers(self.alert_signing_secret, "configuration-check", b"")
        if self.alert_webhook_url:
            url = self.alert_webhook_url
            if (
                not isinstance(url, str)
                or len(url) > 8192
                or any(ord(character) <= 32 or ord(character) == 127 for character in url)
                or "\\" in url
                or "#" in url
            ):
                raise ValueError("Invalid alert webhook URL")
            parts = urlsplit(url)
            if (
                parts.scheme not in ("http", "https")
                or not parts.hostname
                or parts.username is not None
                or parts.password is not None
                or "%" in parts.hostname
                or (parts.port is not None and not 1 <= parts.port <= 65535)
            ):
                raise ValueError(
                    "Alert webhook URL must be HTTP(S) without credentials or fragments"
                )


class Operations:
    def __init__(self, store, policy=None, allow_private=False):
        self.store = store
        self.policy = policy or OperationsPolicy()
        self.allow_private = allow_private
        # URL syntax is checked by policy; DNS/network policy is checked on each
        # notification. An unavailable alert receiver must not prevent startup.
        self.store.capacity_guard = self.check_capacity
        self._stopping = threading.Event()
        self._lifecycle = threading.Lock()
        self._threads = []
        self._maintenance_delay = self.policy.interval_seconds

    def storage(self, db=None):
        if db is None:
            with self.store._db() as connection:
                return self.storage(connection)
        page_size = db.execute("PRAGMA page_size").fetchone()[0]
        pages = db.execute("PRAGMA page_count").fetchone()[0]
        reusable_pages = db.execute("PRAGMA freelist_count").fetchone()[0]
        path = Path(self.store.path)
        try:
            wal_bytes = Path(str(path) + "-wal").stat().st_size
        except FileNotFoundError:
            wal_bytes = 0
        disk = shutil.disk_usage(path.parent)
        return {
            "database_bytes": pages * page_size,
            "wal_bytes": wal_bytes,
            "total_bytes": pages * page_size + wal_bytes,
            "used_bytes": (pages - reusable_pages) * page_size + wal_bytes,
            "reusable_bytes": reusable_pages * page_size,
            "free_bytes": disk.free,
            "disk_percent": round((disk.total - disk.free) * 100 / max(1, disk.total), 2),
        }

    def check_capacity(self, db, incoming_bytes=0, endpoint_count=1):
        if not (self.policy.min_free_bytes or self.policy.max_storage_bytes):
            return
        storage = self.storage(db)
        # Admission is conservative, not a hard filesystem quota. Index/WAL growth
        # depends on SQLite page splits and concurrent writers outside this service.
        estimate = max(4096, incoming_bytes + max(0, endpoint_count) * 1024 + 8192)
        if (
            self.policy.min_free_bytes
            and storage["free_bytes"] - estimate < self.policy.min_free_bytes
        ):
            raise CapacityExceeded("Storage free-space reserve reached; retry after cleanup")
        if (
            self.policy.max_storage_bytes
            and storage["used_bytes"] + estimate > self.policy.max_storage_bytes
        ):
            raise CapacityExceeded("Configured storage capacity reached; retry after cleanup")

    def summary(self, project_id=None, now=None):
        now = time.time() if now is None else now
        args = [project_id] if project_id else []
        scope = " WHERE project_id=?" if project_id else ""
        with self.store._db() as db:
            db.execute("BEGIN")
            states = {
                key: 0 for key in ("pending", "processing", "retrying", "delivered", "failed")
            }
            for row in db.execute(
                "SELECT status,COUNT(*) count FROM deliveries" + scope + " GROUP BY status", args
            ):
                states[row["status"]] = row["count"]
            oldest = db.execute(
                "SELECT MIN(COALESCE(cycle_started_at,created_at)) FROM deliveries WHERE status IN ('pending','processing','retrying')"
                + (" AND project_id=?" if project_id else ""),
                args,
            ).fetchone()[0]
            runnable = db.execute(
                """SELECT MIN(COALESCE(d.cycle_started_at,d.created_at)) FROM deliveries d JOIN endpoints ep ON ep.id=d.endpoint_id
                WHERE d.status IN ('pending','processing','retrying') AND ep.enabled=1"""
                + (" AND d.project_id=?" if project_id else ""),
                args,
            ).fetchone()[0]
            attempts = db.execute(
                """SELECT COUNT(*) total,
                COALESCE(SUM(CASE WHEN a.status_code BETWEEN 200 AND 299 THEN 0 ELSE 1 END),0) failed,
                AVG(a.duration_ms) latency FROM attempts a JOIN deliveries d ON d.id=a.delivery_id
                WHERE a.finished_at>=?"""
                + (" AND d.project_id=?" if project_id else ""),
                [now - WINDOW_SECONDS, *args],
            ).fetchone()
            endpoints = db.execute(
                """SELECT COALESCE(SUM(enabled),0) enabled,
                COALESCE(SUM(CASE WHEN enabled=0 THEN 1 ELSE 0 END),0) paused,
                COALESCE(SUM(CASE WHEN enabled=1 AND cooldown_until>? THEN 1 ELSE 0 END),0) cooling_down
                FROM endpoints"""
                + scope,
                [now, *args],
            ).fetchone()
            deferrals = {reason: {"count": 0, "last_seen_at": 0} for reason in RESOURCE_REASONS}
            for row in db.execute("SELECT reason,count,last_seen_at FROM delivery_deferrals"):
                if row["reason"] in deferrals:
                    deferrals[row["reason"]] = {
                        "count": row["count"],
                        "last_seen_at": row["last_seen_at"],
                    }
            storage = self.storage(db)
        policy = asdict(self.policy)
        policy.pop("alert_signing_secret")
        policy.pop("alert_webhook_url")
        policy["alert_webhook_configured"] = bool(self.policy.alert_webhook_url)
        return {
            "observed_at": now,
            "queue": {
                **states,
                "depth": sum(states[k] for k in ("pending", "processing", "retrying")),
                "oldest_age_seconds": max(0, now - oldest) if oldest is not None else 0,
                "runnable_oldest_age_seconds": max(0, now - runnable)
                if runnable is not None
                else 0,
            },
            "outcomes": {
                "window_seconds": WINDOW_SECONDS,
                "attempts": attempts["total"],
                "failed_attempts": attempts["failed"],
                "failure_percent": round(100 * attempts["failed"] / attempts["total"], 2)
                if attempts["total"]
                else 0,
                "avg_latency_ms": round(attempts["latency"] or 0, 2),
            },
            "endpoints": dict(endpoints),
            "storage": storage,
            "resources": {
                "scope": "instance",
                "delivery_deferrals": deferrals,
                "pressure_recent": any(
                    value["count"] and value["last_seen_at"] >= now - RESOURCE_PRESSURE_SECONDS
                    for value in deferrals.values()
                ),
            },
            "policy": policy,
            "alerts": self.list_alerts(),
        }

    def prometheus(self):
        snapshot = self.summary()
        lines = [
            "# HELP relay_deliveries Current retained deliveries by status.",
            "# TYPE relay_deliveries gauge",
        ]
        for status in ("pending", "processing", "retrying", "delivered", "failed"):
            lines.append(f'relay_deliveries{{status="{status}"}} {snapshot["queue"][status]}')
        metrics = {
            "relay_queue_oldest_age_seconds": (
                snapshot["queue"]["oldest_age_seconds"],
                "Oldest unfinished delivery age, including paused endpoints.",
            ),
            "relay_queue_runnable_oldest_age_seconds": (
                snapshot["queue"]["runnable_oldest_age_seconds"],
                "Oldest unfinished delivery age for enabled endpoints.",
            ),
            "relay_attempts_last_hour": (
                snapshot["outcomes"]["attempts"],
                "Delivery attempts completed in the last hour.",
            ),
            "relay_failed_attempts_last_hour": (
                snapshot["outcomes"]["failed_attempts"],
                "Failed delivery attempts completed in the last hour.",
            ),
            "relay_attempt_latency_mean_seconds": (
                snapshot["outcomes"]["avg_latency_ms"] / 1000,
                "Mean attempt latency over the last hour.",
            ),
            "relay_endpoints_cooling_down": (
                snapshot["endpoints"]["cooling_down"],
                "Enabled endpoints currently in timeout cooldown.",
            ),
            "relay_storage_bytes": (
                snapshot["storage"]["total_bytes"],
                "Database allocated pages plus WAL bytes.",
            ),
            "relay_storage_reusable_bytes": (
                snapshot["storage"]["reusable_bytes"],
                "SQLite free pages available for reuse.",
            ),
            "relay_disk_free_bytes": (
                snapshot["storage"]["free_bytes"],
                "Free bytes on the database filesystem.",
            ),
        }
        for name, (value, help_text) in metrics.items():
            lines.extend([f"# HELP {name} {help_text}", f"# TYPE {name} gauge", f"{name} {value}"])
        lines.extend(
            [
                "# HELP relay_delivery_deferrals_total Delivery deferrals caused by local DNS capacity, without consuming an attempt.",
                "# TYPE relay_delivery_deferrals_total counter",
            ]
        )
        for reason, values in snapshot["resources"]["delivery_deferrals"].items():
            lines.append(f'relay_delivery_deferrals_total{{reason="{reason}"}} {values["count"]}')
        lines.extend(
            [
                "# HELP relay_delivery_last_deferral_timestamp_seconds Unix timestamp of the last local DNS capacity deferral.",
                "# TYPE relay_delivery_last_deferral_timestamp_seconds gauge",
            ]
        )
        for reason, values in snapshot["resources"]["delivery_deferrals"].items():
            lines.append(
                f'relay_delivery_last_deferral_timestamp_seconds{{reason="{reason}"}} {values["last_seen_at"]}'
            )
        lines.extend(
            [
                "# HELP relay_alert_active Current operator alert state.",
                "# TYPE relay_alert_active gauge",
            ]
        )
        for alert in snapshot["alerts"]:
            lines.append(f'relay_alert_active{{code="{alert["code"]}"}} {int(alert["active"])}')
        return "\n".join(lines) + "\n"

    def cleanup(self, dry_run=True, now=None):
        if not isinstance(dry_run, bool):
            raise TypeError("dry_run must be a boolean")
        now = time.time() if now is None else now
        batch = self.policy.cleanup_batch_size
        result = {
            "enabled": bool(self.policy.retention_days),
            "dry_run": dry_run,
            "retention_days": self.policy.retention_days,
            "cutoff": None,
            "eligible_events": 0,
            "deleted_events": 0,
            "deleted_deliveries": 0,
            "deleted_attempts": 0,
            "deleted_tombstones": 0,
            "batch_size": batch,
            "scanned_events": 0,
            "scan_limit": RETENTION_SCAN_LIMIT,
            "has_more": False,
            "scan_wrapped": False,
        }
        with self.store._db(write=not dry_run) as db:
            if dry_run:
                db.execute("BEGIN")
            if not dry_run:
                result["deleted_tombstones"] = db.execute(
                    """DELETE FROM idempotency_tombstones WHERE (project_id,key_hash) IN
                    (SELECT project_id,key_hash FROM idempotency_tombstones WHERE expires_at<=?
                     ORDER BY expires_at LIMIT ?)""",
                    (now, batch),
                ).rowcount
                result["has_more"] = result["deleted_tombstones"] == batch
            if not self.policy.retention_days:
                return result
            cutoff = now - self.policy.retention_days * 86400
            result["cutoff"] = cutoff
            cursor = db.execute(
                "SELECT created_at,event_id FROM operation_cursors WHERE name='retention'"
            ).fetchone()
            cursor = tuple(cursor) if cursor else (-1e30, "")

            def scan(after):
                return db.execute(
                    """SELECT id,created_at FROM events INDEXED BY events_retention
                    WHERE created_at<? AND (created_at,id)>(?,?)
                    ORDER BY created_at,id LIMIT ?""",
                    (cutoff, *after, RETENTION_SCAN_LIMIT),
                ).fetchall()

            window = scan(cursor)
            if not window and cursor != (-1e30, ""):
                result["scan_wrapped"] = True
                cursor = (-1e30, "")
                window = scan(cursor)
            result["scanned_events"] = len(window)
            if not window:
                return result
            boundary = (window[-1]["created_at"], window[-1]["id"])
            if not dry_run:
                next_cursor = boundary if len(window) == RETENTION_SCAN_LIMIT else (-1e30, "")
                db.execute(
                    """INSERT INTO operation_cursors VALUES('retention',?,?)
                    ON CONFLICT(name) DO UPDATE SET created_at=excluded.created_at,event_id=excluded.event_id""",
                    next_cursor,
                )
            # Protect an entire fanout until every delivery is terminal and old.
            # Active recovery memberships are protected even before they are released.
            # The keyset window bounds selection too: a parked backlog cannot cause
            # an unbounded full-history scan while holding SQLite's writer lock.
            candidates = db.execute(
                """SELECT ev.id,ev.project_id,ev.idempotency_key FROM events ev INDEXED BY events_retention
                WHERE (ev.created_at,ev.id)>(?,?) AND (ev.created_at,ev.id)<=(?,?) AND NOT EXISTS
                (SELECT 1 FROM deliveries d INDEXED BY deliveries_active_event WHERE d.event_id=ev.id
                 AND d.status IN ('pending','processing','retrying'))
                AND NOT EXISTS (SELECT 1 FROM deliveries d WHERE d.event_id=ev.id AND d.updated_at>=?)
                AND NOT EXISTS (SELECT 1 FROM recovery_items ri INDEXED BY recovery_items_event_waiting
                 JOIN recovery_jobs rj ON rj.id=ri.job_id
                 WHERE ri.event_id=ev.id AND ri.state='queued' AND rj.status='running')
                ORDER BY ev.created_at,ev.id LIMIT ?""",
                (*cursor, *boundary, cutoff, batch + 1),
            ).fetchall()
            window_has_more = len(candidates) > batch
            result["has_more"] |= window_has_more or len(window) == RETENTION_SCAN_LIMIT
            candidates = candidates[:batch]
            result["eligible_events"] = len(candidates)
            if dry_run or not candidates:
                return result
            ids = [row["id"] for row in candidates]
            placeholders = ",".join("?" for _ in ids)
            # Select a bounded delivery window before filtering by remaining
            # attempts. Otherwise a large fanout where every delivery still has
            # attempts could be scanned in full to find removable deliveries.
            delivery_ids = [
                row[0]
                for row in db.execute(
                    f"SELECT id FROM deliveries WHERE event_id IN ({placeholders}) LIMIT ?",
                    [*ids, batch],
                )
            ]
            if delivery_ids:
                delivery_placeholders = ",".join("?" for _ in delivery_ids)
                result["deleted_attempts"] = db.execute(
                    f"""DELETE FROM attempts WHERE id IN (SELECT id FROM attempts
                    WHERE delivery_id IN ({delivery_placeholders}) LIMIT ?)""",
                    [*delivery_ids, batch],
                ).rowcount
                result["deleted_deliveries"] = db.execute(
                    f"""DELETE FROM deliveries WHERE id IN ({delivery_placeholders}) AND NOT EXISTS
                    (SELECT 1 FROM attempts a WHERE a.delivery_id=deliveries.id)""",
                    delivery_ids,
                ).rowcount
            for row in candidates:
                if db.execute(
                    "SELECT 1 FROM deliveries WHERE event_id=? LIMIT 1", (row["id"],)
                ).fetchone():
                    result["has_more"] = True
                    window_has_more = True
                    continue
                if row["idempotency_key"] is not None:
                    event = db.execute(
                        "SELECT event_type,payload_json FROM events WHERE id=?", (row["id"],)
                    ).fetchone()
                    db.execute(
                        """INSERT INTO idempotency_tombstones
                        (project_id,key_hash,event_id,request_hash,expires_at) VALUES(?,?,?,?,?)
                        ON CONFLICT(project_id,key_hash) DO UPDATE SET event_id=excluded.event_id,
                        request_hash=excluded.request_hash,expires_at=excluded.expires_at""",
                        (
                            row["project_id"],
                            hashlib.sha256(row["idempotency_key"].encode()).hexdigest(),
                            row["id"],
                            _request_hash(event["event_type"], event["payload_json"]),
                            now + self.policy.idempotency_days * 86400,
                        ),
                    )
                result["deleted_events"] += db.execute(
                    "DELETE FROM events WHERE id=?", (row["id"],)
                ).rowcount
            if window_has_more:
                # Finish this window before moving past it. Advancing after a
                # partial batch skips eligible rows and can mistake a parked
                # prefix on the next wrap for a completed scan. Persisting the
                # cursor also keeps catch-up correct across processes/bursts.
                db.execute(
                    "UPDATE operation_cursors SET created_at=?,event_id=? WHERE name='retention'",
                    cursor,
                )
        return result

    def cleanup_burst(self, now=None):
        """Catch up using short transactions, bounded time and a bounded pass count.

        The time budget is checked between batches; one SQLite operation may
        overrun it while waiting for a writer. Each batch retains its row limits.
        """
        now = time.time() if now is None else now
        started = time.monotonic()
        totals = {
            name: 0
            for name in (
                "deleted_events",
                "deleted_deliveries",
                "deleted_attempts",
                "deleted_tombstones",
                "scanned_events",
                "passes",
            )
        }
        catch_up = False
        for _ in range(CLEANUP_BURST_MAX_BATCHES):
            if self._stopping.is_set():
                break
            result = self.cleanup(dry_run=False, now=now)
            totals["passes"] += 1
            for name in totals:
                if name != "passes":
                    totals[name] += result[name]
            progressed = any(
                result[name]
                for name in (
                    "deleted_events",
                    "deleted_deliveries",
                    "deleted_attempts",
                    "deleted_tombstones",
                )
            )
            # A full scan of parked/ineligible history is not useful work to
            # repeat at catch-up speed. Resume ordinary polling after a wrap.
            catch_up = result["has_more"] and not (result["scan_wrapped"] and not progressed)
            if not catch_up or time.monotonic() - started >= CLEANUP_BURST_SECONDS:
                break
            # The preceding transaction is closed before yielding to ingress and
            # delivery writers. A process-wide maintenance lease bounds all peers.
            if self._stopping.wait(CLEANUP_BATCH_YIELD_SECONDS):
                break
        return {
            **totals,
            "catch_up": catch_up,
            "elapsed_ms": round((time.monotonic() - started) * 1000, 2),
        }

    def list_alerts(self):
        with self.store._db() as db:
            rows = db.execute(
                """SELECT code,severity,active,message,first_seen_at,updated_at,resolved_at,
                last_notified_at,notification_attempts,last_notification_status,
                notification_gate_until,next_notification_at,healthy_since,last_observed_at
                FROM operation_alerts ORDER BY active DESC,code"""
            ).fetchall()
        return [{**dict(row), "active": bool(row["active"])} for row in rows]

    def evaluate_alerts(self, snapshot, now=None):
        now = time.time() if now is None else now
        storage, queue, outcomes = snapshot["storage"], snapshot["queue"], snapshot["outcomes"]
        policy = self.policy
        storage_high = (
            storage["disk_percent"] >= policy.storage_warning_percent
            or (
                policy.max_storage_bytes
                and storage["used_bytes"]
                >= policy.max_storage_bytes * policy.storage_warning_percent / 100
            )
            or (policy.min_free_bytes and storage["free_bytes"] < policy.min_free_bytes * 2)
        )
        conditions = {
            "storage_pressure": (
                bool(storage_high),
                "warning",
                "Database filesystem is nearing its configured capacity. Review retention, free space, and admission limits.",
            ),
            "queue_delay": (
                queue["runnable_oldest_age_seconds"] >= policy.queue_warning_age_seconds,
                "warning",
                "An enabled destination has an old unfinished delivery. Check worker health, destination availability, and rate limits.",
            ),
            "delivery_failures": (
                outcomes["attempts"] >= policy.failure_min_samples
                and outcomes["failure_percent"] >= policy.failure_warning_percent,
                "warning",
                "Recent delivery failures exceed the configured threshold. Inspect failed attempts and destination responses.",
            ),
            "endpoint_cooldown": (
                snapshot["endpoints"]["cooling_down"] > 0,
                "warning",
                "A destination is in timeout cooldown. Check its connectivity and response time.",
            ),
            "resolver_capacity": (
                snapshot.get("resources", {}).get("pressure_recent", False),
                "warning",
                "Local DNS lookup capacity is exhausted. Deliveries are being deferred without consuming attempts; check resolver health and stalled destinations.",
            ),
        }
        with self.store._db(True) as db:
            for code, (active, severity, message) in conditions.items():
                previous = db.execute(
                    "SELECT * FROM operation_alerts WHERE code=?", (code,)
                ).fetchone()
                if previous is None:
                    if active:
                        db.execute(
                            """INSERT INTO operation_alerts(code,severity,active,message,first_seen_at,updated_at,
                            next_notification_at,last_observed_at) VALUES(?,?,1,?,?,?,?,?)""",
                            (code, severity, message, now, now, now, now),
                        )
                    continue
                observed = previous["last_observed_at"]
                if observed is not None and now < observed:
                    # A delayed snapshot must not undo a newer state or extend
                    # an apparently healthy period using out-of-order evidence.
                    continue
                continuous = observed is not None and now - observed <= 2 * policy.interval_seconds
                healthy_since = None
                if not active:
                    healthy_since = (
                        previous["healthy_since"]
                        if not previous["active"]
                        and continuous
                        and previous["healthy_since"] is not None
                        else now
                    )
                if bool(previous["active"]) != active:
                    rearmed = (
                        active
                        and continuous
                        and previous["healthy_since"] is not None
                        and observed - previous["healthy_since"] >= policy.alert_rearm_seconds
                    )
                    # Rapid transitions keep their gate. A newly active incident
                    # after observed stable health bypasses even a just-sent
                    # recovery notice; the next claim reserves a fresh cooldown.
                    gate = (
                        now
                        if rearmed
                        else max(
                            previous["notification_gate_until"],
                            (previous["last_notified_at"] or 0) + policy.alert_cooldown_seconds
                            if previous["last_notified_at"] is not None
                            else 0,
                        )
                    )
                    db.execute(
                        """UPDATE operation_alerts SET active=?,message=?,first_seen_at=?,updated_at=?,resolved_at=?,
                        version=version+1,notification_cycle=0,notification_attempts=0,next_notification_at=?,lease_token=NULL,
                        lease_expires_at=NULL,last_notification_status=NULL,notification_gate_until=?,
                        healthy_since=?,last_observed_at=? WHERE code=?""",
                        (
                            int(active),
                            message,
                            now if active else previous["first_seen_at"],
                            now,
                            None if active else now,
                            max(now, gate),
                            gate,
                            healthy_since,
                            now,
                            code,
                        ),
                    )
                else:
                    db.execute(
                        "UPDATE operation_alerts SET healthy_since=?,last_observed_at=? WHERE code=?",
                        (healthy_since, now, code),
                    )

    def notify_once(self, now=None):
        if not self.policy.alert_webhook_url:
            return False
        now = time.time() if now is None else now
        token = secrets.token_hex(16)
        with self.store._db(True) as db:
            row = db.execute(
                """SELECT * FROM operation_alerts WHERE next_notification_at<=?
                AND (lease_expires_at IS NULL OR lease_expires_at<=?)
                AND ((notification_version=version AND notification_attempts>0)
                     OR notification_gate_until<=?)
                ORDER BY next_notification_at,code LIMIT 1""",
                (now, now, now),
            ).fetchone()
            if row is None:
                return False
            row = dict(row)
            if row["notification_attempts"] >= MAX_NOTIFICATION_ATTEMPTS:
                # Count claims, not just completed requests: repeated process
                # crashes after sending must not create an unlimited retry cycle.
                gate = max(row["notification_gate_until"], now + self.policy.alert_cooldown_seconds)
                db.execute(
                    """UPDATE operation_alerts SET notification_attempts=0,
                    notification_gate_until=?,next_notification_at=?,notification_cycle=notification_cycle+1,
                    lease_token=NULL,lease_expires_at=NULL,last_notification_status='failed' WHERE code=?""",
                    (gate, gate if row["active"] else None, row["code"]),
                )
                return True
            retrying = (
                row["notification_version"] == row["version"] and row["notification_attempts"] > 0
            )
            gate = (
                row["notification_gate_until"]
                if retrying
                else now + self.policy.alert_cooldown_seconds
            )
            attempts = row["notification_attempts"] + 1
            db.execute(
                """UPDATE operation_alerts SET lease_token=?,lease_expires_at=?,
                notification_gate_until=?,notification_version=version,notification_attempts=? WHERE code=?""",
                (token, now + 15, gate, attempts, row["code"]),
            )
        event_id = f"alert_{row['code']}_{row['version']}_{row['notification_cycle']}"
        payload = {
            "id": event_id,
            "type": "relay.alert.active" if row["active"] else "relay.alert.resolved",
            "created_at": row["updated_at"],
            "data": {
                key: row[key]
                for key in ("code", "severity", "message", "first_seen_at", "resolved_at")
            },
        }
        body = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode()
        headers = sign_headers(self.policy.alert_signing_secret, event_id, body)
        headers["webhook-type"] = payload["type"]
        try:
            result = deliver(
                self.policy.alert_webhook_url,
                body,
                headers,
                timeout=5,
                allow_private=self.allow_private,
            )
            if result.deferred_reason in RESOURCE_REASONS:
                # No request was made. Undo this claim's retry charge and gate
                # reservation, including when a newer state replaced its version
                # while DNS was being checked. A later sender's claim is fenced.
                with self.store._db(True) as db:
                    db.execute(
                        """UPDATE operation_alerts SET
                        notification_attempts=CASE WHEN version=? THEN ? ELSE notification_attempts END,
                        notification_version=CASE WHEN version=? THEN ? ELSE notification_version END,
                        notification_gate_until=?,next_notification_at=CASE WHEN version=? THEN ? ELSE MAX(?,?) END,
                        lease_token=NULL,lease_expires_at=NULL,last_notification_status='deferred: DNS capacity'
                        WHERE code=? AND notification_version=? AND notification_gate_until=?
                        AND (lease_token=? OR version!=?)""",
                        (
                            row["version"],
                            row["notification_attempts"],
                            row["version"],
                            row["notification_version"],
                            row["notification_gate_until"],
                            row["version"],
                            now + 5,
                            now + 5,
                            row["notification_gate_until"],
                            row["code"],
                            row["version"],
                            gate,
                            token,
                            row["version"],
                        ),
                    )
                return True
            success = result.status_code is not None and 200 <= result.status_code < 300
            permanent = result.permanent or (
                result.status_code is not None
                and 400 <= result.status_code < 500
                and result.status_code not in (408, 429)
            )
            retry_after = min(3600, max(0, result.retry_after_seconds or 0))
        except Exception:
            logger.exception("Operator notification failed")
            success, permanent, retry_after = False, False, 0
        if success or permanent or attempts >= MAX_NOTIFICATION_ATTEMPTS:
            gate = max(gate, now + self.policy.alert_cooldown_seconds)
            next_at = gate if row["active"] else None
            next_attempts = 0
        else:
            next_at = now + max(30 * 2 ** (attempts - 1), retry_after)
            next_attempts = attempts
        with self.store._db(True) as db:
            db.execute(
                """UPDATE operation_alerts SET notification_attempts=?,next_notification_at=?,
                last_notification_status=?,last_notified_at=CASE WHEN ? THEN ? ELSE last_notified_at END,
                notification_cycle=notification_cycle+?,notification_gate_until=?,
                lease_token=NULL,lease_expires_at=NULL
                WHERE code=? AND version=? AND lease_token=?""",
                (
                    next_attempts,
                    next_at,
                    "delivered" if success else "failed",
                    int(success),
                    now,
                    int(next_attempts == 0),
                    gate,
                    row["code"],
                    row["version"],
                    token,
                ),
            )
        return True

    def run_once(self, now=None):
        now = time.time() if now is None else now
        started = time.monotonic()
        token = secrets.token_hex(16)
        self._maintenance_delay = self.policy.interval_seconds
        with self.store._db(True) as db:
            claimed = db.execute(
                """INSERT INTO operation_leases(name,token,expires_at) VALUES('maintenance',?,?)
                ON CONFLICT(name) DO UPDATE SET token=excluded.token,expires_at=excluded.expires_at
                WHERE operation_leases.expires_at<=?""",
                (token, now + 60, now),
            ).rowcount
            if claimed:
                monitor = db.execute(
                    """INSERT INTO operation_leases(name,token,expires_at) VALUES('monitoring',?,?)
                    ON CONFLICT(name) DO UPDATE SET token=excluded.token,expires_at=excluded.expires_at
                    WHERE operation_leases.expires_at<=?""",
                    (token, now + self.policy.interval_seconds, now),
                ).rowcount
            else:
                expiry = db.execute(
                    "SELECT expires_at FROM operation_leases WHERE name='maintenance'"
                ).fetchone()[0]
                self._maintenance_delay = max(0.1, min(self.policy.interval_seconds, expiry - now))
        if not claimed:
            return False
        try:
            result = self.cleanup_burst(now=now)
            if result["catch_up"]:
                self._maintenance_delay = min(self.policy.interval_seconds, CLEANUP_CATCHUP_SECONDS)
            if monitor:
                self.evaluate_alerts(self.summary(now=now), now=now)
        finally:
            with self.store._db(True) as db:
                db.execute(
                    "UPDATE operation_leases SET expires_at=? WHERE name='maintenance' AND token=?",
                    (now + max(0, time.monotonic() - started) + self._maintenance_delay, token),
                )
        return True

    def _run(self, notify=False):
        while not self._stopping.is_set():
            try:
                worked = self.notify_once() if notify else self.run_once()
            except Exception:
                logger.exception(
                    "Operator notification cycle failed" if notify else "Maintenance cycle failed"
                )
                worked = False
            self._stopping.wait(
                0.1 if notify and worked else (1 if notify else self._maintenance_delay)
            )

    def start(self):
        with self._lifecycle:
            if any(thread.is_alive() for thread in self._threads):
                return
            self._stopping.clear()
            self._threads = [
                threading.Thread(target=self._run, name="relay-maintenance", daemon=True)
            ]
            if self.policy.alert_webhook_url:
                self._threads.append(
                    threading.Thread(
                        target=self._run, kwargs={"notify": True}, name="relay-alerts", daemon=True
                    )
                )
            for thread in self._threads:
                thread.start()

    def stop(self):
        with self._lifecycle:
            self._stopping.set()
            for thread in self._threads:
                if thread is not threading.current_thread():
                    thread.join()
            self._threads = []
