"""Measure claim and healthy-project acceptance with a parked delivery backlog."""

import argparse
import json
import platform
import statistics
import tempfile
import time
from pathlib import Path

from relay.store import Store


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--backlog", type=int, default=200_000)
    parser.add_argument("--samples", type=int, default=20)
    parser.add_argument("--mode", choices=("paused", "rate-limited"), default="paused")
    args = parser.parse_args()
    if args.backlog < 0 or args.samples < 1:
        parser.error("backlog must be nonnegative and samples must be positive")
    with tempfile.TemporaryDirectory(prefix="relay-queue-benchmark-") as folder:
        store = Store(Path(folder) / "queue.db")
        store.initialize()
        parked = store.create_project("Parked")
        healthy = store.create_project("Healthy")
        endpoint = store.create_endpoint(parked["id"], "https://parked.example/hook")
        store.create_endpoint(healthy["id"], "https://healthy.example/hook", rate_limit=1000)
        stamp = time.time() - 60
        # Bulk fixture setup is outside the measured operations. Each delivery
        # references a distinct event, as it would after ordinary ingestion.
        with store._db(True) as db:
            db.executemany(
                "INSERT INTO events VALUES(?,?,?,?,?,?)",
                (
                    (f"evt_fixture_{i}", parked["id"], "fixture", "{}", None, stamp)
                    for i in range(args.backlog)
                ),
            )
            db.executemany(
                """INSERT INTO deliveries
                (id,project_id,event_id,endpoint_id,status,next_attempt_at,created_at,updated_at)
                VALUES(?,?,?,?,'pending',?,?,?)""",
                (
                    (
                        f"dlv_fixture_{i}",
                        parked["id"],
                        f"evt_fixture_{i}",
                        endpoint["id"],
                        stamp,
                        stamp,
                        stamp,
                    )
                    for i in range(args.backlog)
                ),
            )
            if args.mode == "paused":
                db.execute("UPDATE endpoints SET enabled=0 WHERE id=?", (endpoint["id"],))
            else:
                db.execute(
                    "UPDATE endpoints SET next_available_at=? WHERE id=?",
                    (time.time() + 86400, endpoint["id"]),
                )
        idle, acceptance, active = [], [], []
        for _ in range(args.samples):
            start = time.perf_counter()
            assert store.claim_delivery() is None
            idle.append((time.perf_counter() - start) * 1000)
            start = time.perf_counter()
            store.create_event(healthy["id"], "healthy", {})
            acceptance.append((time.perf_counter() - start) * 1000)
            start = time.perf_counter()
            job = store.claim_delivery()
            active.append((time.perf_counter() - start) * 1000)
            assert job is not None and job["project_id"] == healthy["id"]
            assert store.complete_delivery(
                job["id"],
                job["lease_token"],
                status_code=204,
                error=None,
                duration_ms=0,
                response_excerpt="",
            )
            time.sleep(0.002)  # Keep endpoint rate limiting outside the next sample.
        print(
            json.dumps(
                {
                    "python": platform.python_version(),
                    "os": platform.system(),
                    "backlog": args.backlog,
                    "mode": args.mode,
                    "samples": args.samples,
                    "idle_claim_median_ms": round(statistics.median(idle), 2),
                    "healthy_claim_median_ms": round(statistics.median(active), 2),
                    "healthy_acceptance_median_ms": round(statistics.median(acceptance), 2),
                },
                indent=2,
            )
        )


if __name__ == "__main__":
    main()
