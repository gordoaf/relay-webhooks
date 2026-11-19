import argparse
import json
import secrets
import signal
import threading
from dataclasses import replace

import uvicorn

from .app import create_app, make_worker
from .config import Settings
from .operations import Operations
from .store import Store


def main():
    parser = argparse.ArgumentParser(prog="relay")
    commands = parser.add_subparsers(dest="command", required=True)
    serve = commands.add_parser("serve", help="Run the API and delivery worker")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8787)
    commands.add_parser("worker", help="Run a separate delivery worker")
    cleanup = commands.add_parser("cleanup", help="Preview or apply one bounded retention batch")
    cleanup.add_argument("--apply", action="store_true", help="Delete eligible retained history")
    commands.add_parser("status", help="Print queue health, storage use, and alerts as JSON")
    demo = commands.add_parser("demo", help="Run a local demonstration with a receiver")
    demo.add_argument("--port", type=int, default=8787)
    demo.add_argument("--receiver-port", type=int, default=8790)
    args = parser.parse_args()
    try:
        settings = Settings.from_env()
    except ValueError as exc:
        parser.error(str(exc))
    if args.command == "demo":
        settings = replace(
            settings,
            db_path="data/demo.db",
            admin_token=settings.admin_token or secrets.token_urlsafe(32),
            allow_private_networks=True,
            worker_enabled=True,
            demo_mode=True,
            port=args.port,
            demo_receiver_port=args.receiver_port,
            public_url="",
        )
        print(f"\nRelay demo: http://127.0.0.1:{settings.port}")
        print(f"Admin token: {settings.admin_token}")
        print("Local lab: private destinations are enabled for this process.\n", flush=True)
    elif args.command in {"serve", "worker"} and len(settings.admin_token) < 24:
        parser.error("Set RELAY_ADMIN_TOKEN to at least 24 characters, or use relay demo.")
    if args.command in {"worker", "cleanup", "status"}:
        store = Store(settings.db_path)
        store.initialize()
        operations = Operations(
            store, settings.operations_policy(), settings.allow_private_networks
        )
        if args.command == "cleanup":
            print(json.dumps(operations.cleanup(dry_run=not args.apply), indent=2))
            return
        if args.command == "status":
            print(json.dumps(operations.summary(), indent=2))
            return
        worker = make_worker(store, settings)
        stopped = threading.Event()
        signal.signal(signal.SIGTERM, lambda *_: stopped.set())
        try:
            worker.start()
            if settings.maintenance_enabled:
                operations.start()
            stopped.wait()
        except KeyboardInterrupt:
            pass
        finally:
            operations.stop()
            worker.stop()
        return
    if args.command == "serve":
        settings = replace(settings, host=args.host, port=args.port)
    if not 1 <= settings.port <= 65535:
        parser.error("Port must be between 1 and 65535")
    if settings.demo_mode and (
        not 1 <= settings.demo_receiver_port <= 65535
        or settings.port == settings.demo_receiver_port
    ):
        parser.error("Receiver port must be valid and different from the API port")
    uvicorn.run(
        create_app(settings),
        host=settings.host,
        port=settings.port,
        proxy_headers=False,
        access_log=False,
    )


if __name__ == "__main__":
    main()
