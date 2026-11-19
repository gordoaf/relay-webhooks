"""Measure local queue acceptance and real HTTP delivery; not an API or cloud benchmark."""

import argparse
import json
import platform
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import tempfile
import threading
import time
from pathlib import Path
from relay.store import Store
from relay.worker import Worker


class Receiver(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def do_POST(self):
        self.rfile.read(int(self.headers["Content-Length"]))
        self.send_response(204)
        self.send_header("Content-Length", "0")
        self.end_headers()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--events", type=int, default=200)
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args()
    if args.events < 1 or args.workers < 1:
        parser.error("events and workers must be positive")
    server = ThreadingHTTPServer(("127.0.0.1", 0), Receiver)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        with tempfile.TemporaryDirectory(prefix="relay-benchmark-") as folder:
            store = Store(Path(folder) / "benchmark.db")
            store.initialize()
            project = store.create_project("Benchmark")
            store.create_endpoint(
                project["id"], f"http://127.0.0.1:{server.server_port}/webhook", rate_limit=1000
            )
            start = time.perf_counter()
            for i in range(args.events):
                store.create_event(
                    project["id"], "benchmark.event", {"sequence": i, "message": "x" * 512}
                )
            acceptance = time.perf_counter() - start
            worker = Worker(store, allow_private=True, concurrency=args.workers, poll_interval=0.01)
            start = time.perf_counter()
            worker.start()
            try:
                while store.stats(project["id"])["delivered"] < args.events:
                    if time.perf_counter() - start > 120:
                        raise RuntimeError("Benchmark did not complete within 120 seconds")
                    time.sleep(0.02)
                elapsed = time.perf_counter() - start
            finally:
                worker.stop()
            with store._db() as db:
                latencies = sorted(r[0] for r in db.execute("SELECT duration_ms FROM attempts"))
            print(
                json.dumps(
                    {
                        "python": platform.python_version(),
                        "os": platform.system(),
                        "processor": platform.processor(),
                        "events": args.events,
                        "workers": args.workers,
                        "payload_data_bytes": 512,
                        "destinations": 1,
                        "durability": "SQLite WAL, synchronous FULL",
                        "acceptance_events_per_second": round(args.events / acceptance, 1),
                        "delivery_events_per_second": round(args.events / elapsed, 1),
                        "http_attempt_p95_ms": round(
                            latencies[min(len(latencies) - 1, int(len(latencies) * 0.95))], 1
                        ),
                        "all_delivered": store.stats(project["id"])["delivered"] == args.events,
                    },
                    indent=2,
                )
            )
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


if __name__ == "__main__":
    main()
