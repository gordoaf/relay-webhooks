"""Local storefront: a transactional outbox and two signed webhook consumers."""

import argparse
import asyncio
import base64
import json
import os
import secrets
import sqlite3
import threading
import time
from contextlib import asynccontextmanager, contextmanager
from pathlib import Path
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, build_opener
from urllib.request import Request as URLRequest

import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse

from relay.client import RelayClient, RelayError, RelayEventExpired, verify_signature

HERE = Path(__file__).resolve().parent
SERVICES = {"warehouse": "order.created", "accounting": "invoice.paid"}


class Storefront:
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        # Create private before SQLite opens it, rather than chmod after writing secrets.
        fd = os.open(self.path, os.O_CREAT | os.O_RDWR, 0o600)
        os.close(fd)
        if os.name != "nt":
            self.path.chmod(0o600)
        with self.db() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS configuration(key TEXT PRIMARY KEY, value TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS services(name TEXT PRIMARY KEY, online INTEGER NOT NULL);
                CREATE TABLE IF NOT EXISTS orders(
                    id TEXT PRIMARY KEY, created_at REAL NOT NULL, customer TEXT NOT NULL,
                    quantity INTEGER NOT NULL, total_cents INTEGER NOT NULL);
                CREATE TABLE IF NOT EXISTS outbox(
                    id INTEGER PRIMARY KEY, order_id TEXT NOT NULL, event_type TEXT NOT NULL,
                    payload TEXT NOT NULL, idempotency_key TEXT NOT NULL UNIQUE,
                    event_id TEXT, attempts INTEGER NOT NULL DEFAULT 0,
                    next_attempt_at REAL NOT NULL DEFAULT 0, last_error TEXT,
                    blocked INTEGER NOT NULL DEFAULT 0,
                    acceptance_expired INTEGER NOT NULL DEFAULT 0);
                CREATE TABLE IF NOT EXISTS receipts(
                    service TEXT NOT NULL, event_id TEXT NOT NULL, order_id TEXT NOT NULL,
                    received_at REAL NOT NULL, PRIMARY KEY(service,event_id));
                CREATE TABLE IF NOT EXISTS fulfillment(
                    service TEXT NOT NULL, order_id TEXT NOT NULL, event_id TEXT NOT NULL,
                    created_at REAL NOT NULL, PRIMARY KEY(service,order_id));
            """)
            for name in SERVICES:
                db.execute("INSERT OR IGNORE INTO services VALUES(?,1)", (name,))
            if "acceptance_expired" not in {
                row["name"] for row in db.execute("PRAGMA table_info(outbox)")
            }:
                db.execute(
                    "ALTER TABLE outbox ADD COLUMN acceptance_expired INTEGER NOT NULL DEFAULT 0"
                )

    @contextmanager
    def db(self):
        db = sqlite3.connect(self.path, timeout=5)
        db.row_factory = sqlite3.Row
        try:
            with db:
                yield db
        finally:
            db.close()

    def config(self):
        with self.db() as db:
            return {
                row["key"]: json.loads(row["value"])
                for row in db.execute("SELECT * FROM configuration")
            }

    def configure(self, config):
        with self.db() as db:
            db.executemany(
                "INSERT INTO configuration VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                [(key, json.dumps(value)) for key, value in config.items()],
            )

    def update_credentials(self, api_key=None, signing_secrets=None, resume_blocked=False):
        config = self.config()
        if not config:
            raise ValueError("Run setup first")
        changes = {}
        if api_key is not None:
            RelayClient(config["relay_url"], api_key)
            changes["api_key"] = api_key
        if signing_secrets:
            new_secrets = dict(config["secrets"])
            for service, secret in signing_secrets.items():
                if (
                    service not in SERVICES
                    or not isinstance(secret, str)
                    or not secret.startswith("whsec_")
                ):
                    raise ValueError("Invalid receiver signing secret")
                try:
                    if len(base64.b64decode(secret[6:], validate=True)) < 16:
                        raise ValueError()
                except (ValueError, UnicodeError):
                    raise ValueError("Invalid receiver signing secret") from None
                new_secrets[service] = secret
            changes["secrets"] = new_secrets
        if not changes:
            raise ValueError("Select at least one credential field to update")
        with self.db() as db:
            db.executemany(
                "UPDATE configuration SET value=? WHERE key=?",
                ((json.dumps(value), key) for key, value in changes.items()),
            )
            if resume_blocked:
                return self._resume_blocked(db)
        return 0

    @staticmethod
    def _resume_blocked(db):
        return db.execute(
            "UPDATE outbox SET blocked=0,next_attempt_at=0,last_error=NULL "
            "WHERE event_id IS NULL AND blocked=1"
        ).rowcount

    def resume_blocked(self):
        with self.db() as db:
            return self._resume_blocked(db)

    def order(self, customer, quantity):
        if not isinstance(customer, str) or not 1 <= len(customer.strip()) <= 80:
            raise ValueError("Customer name must contain 1-80 characters")
        try:
            customer.encode("utf-8")
        except UnicodeError:
            raise ValueError("Customer name must contain valid Unicode") from None
        if type(quantity) is not int or not 1 <= quantity <= 20:
            raise ValueError("Quantity must be between 1 and 20")
        order_id, now = "ord_" + secrets.token_hex(8), time.time()
        payload = {
            "order_id": order_id,
            "customer": customer.strip(),
            "sku": "RELAY-MUG",
            "quantity": quantity,
            "total_cents": quantity * 1800,
            "currency": "USD",
        }
        # The business operation and both pending events commit together.
        with self.db() as db:
            db.execute(
                "INSERT INTO orders VALUES(?,?,?,?,?)",
                (
                    order_id,
                    now,
                    customer.strip(),
                    quantity,
                    payload["total_cents"],
                ),
            )
            for event_type in SERVICES.values():
                db.execute(
                    "INSERT INTO outbox(order_id,event_type,payload,idempotency_key) VALUES(?,?,?,?)",
                    (order_id, event_type, json.dumps(payload), f"{order_id}:{event_type}"),
                )
        return order_id

    def send_pending(self, client=None, now=None, stop=None):
        clock = time.time if now is None else lambda: now
        with self.db() as db:
            rows = db.execute(
                "SELECT * FROM outbox WHERE event_id IS NULL AND blocked=0 AND next_attempt_at<=? "
                "ORDER BY next_attempt_at,id LIMIT 20",
                (clock(),),
            ).fetchall()
        for row in rows:
            if stop is not None and stop.is_set():
                break
            sending_config = self.config() if client is None else None
            sender = client or RelayClient(
                sending_config["relay_url"],
                sending_config["api_key"],
                timeout=3,
                max_retries=0,
            )
            try:
                receipt = sender.emit(
                    row["event_type"],
                    json.loads(row["payload"]),
                    idempotency_key=row["idempotency_key"],
                )
            except RelayEventExpired as exc:
                # This confirms prior acceptance only, never successful delivery.
                with self.db() as db:
                    db.execute(
                        "UPDATE outbox SET event_id=?,acceptance_expired=1,blocked=0,"
                        "attempts=attempts+1,last_error=NULL WHERE id=?",
                        (exc.event_id, row["id"]),
                    )
            except RelayError as exc:
                # Never replace a conflicting key: operators must inspect 4xx failures.
                with self.db() as db:
                    db.execute("BEGIN IMMEDIATE")
                    credentials_changed = (
                        sending_config is not None
                        and json.loads(
                            db.execute(
                                "SELECT value FROM configuration WHERE key='api_key'"
                            ).fetchone()[0]
                        )
                        != sending_config["api_key"]
                    )
                    db.execute(
                        "UPDATE outbox SET attempts=attempts+1,next_attempt_at=?,last_error=?,blocked=? "
                        "WHERE id=? AND event_id IS NULL",
                        (
                            clock() + min(30, 2 ** min(row["attempts"], 5)),
                            str(exc),
                            int(not exc.retryable and not credentials_changed),
                            row["id"],
                        ),
                    )
            else:
                with self.db() as db:
                    db.execute(
                        "UPDATE outbox SET event_id=?,acceptance_expired=0,blocked=0,"
                        "attempts=attempts+1,last_error=NULL WHERE id=?",
                        (receipt["event"]["id"], row["id"]),
                    )

    def receive(self, service, event):
        if service not in SERVICES or event.get("type") != SERVICES[service]:
            raise ValueError("Event type does not match this subscription")
        event_id = event.get("id")
        payload = event.get("data")
        if not isinstance(event_id, str) or not isinstance(payload, dict):
            raise TypeError("Invalid event envelope")
        order_id = payload.get("order_id")
        if not isinstance(order_id, str):
            raise TypeError("Event has no order ID")
        with self.db() as db:
            if db.execute("SELECT 1 FROM orders WHERE id=?", (order_id,)).fetchone() is None:
                raise ValueError("Order not found")
            inserted = db.execute(
                "INSERT OR IGNORE INTO receipts VALUES(?,?,?,?)",
                (
                    service,
                    event_id,
                    order_id,
                    time.time(),
                ),
            ).rowcount
            if inserted:
                # Deduplication and the sample business effect share one transaction.
                db.execute(
                    "INSERT OR IGNORE INTO fulfillment VALUES(?,?,?,?)",
                    (
                        service,
                        order_id,
                        event_id,
                        time.time(),
                    ),
                )
        return bool(inserted)

    def set_online(self, service, online):
        if service not in SERVICES or type(online) is not bool:
            raise ValueError("Invalid service or availability")
        with self.db() as db:
            db.execute("UPDATE services SET online=? WHERE name=?", (online, service))

    def is_online(self, service):
        with self.db() as db:
            row = db.execute("SELECT online FROM services WHERE name=?", (service,)).fetchone()
            return row is not None and bool(row[0])

    def state(self):
        config = self.config()
        with self.db() as db:
            orders = [
                dict(row)
                for row in db.execute("""
                SELECT o.*, EXISTS(SELECT 1 FROM fulfillment f WHERE f.order_id=o.id
                  AND f.service='warehouse') AS warehouse,
                  EXISTS(SELECT 1 FROM fulfillment f WHERE f.order_id=o.id
                  AND f.service='accounting') AS accounting,
                  (SELECT count(*) FROM outbox x WHERE x.order_id=o.id
                  AND x.event_id IS NULL) AS pending_events,
                  (SELECT count(*) FROM outbox x WHERE x.order_id=o.id
                  AND x.acceptance_expired=1) AS expired_events
                FROM orders o ORDER BY o.created_at DESC LIMIT 100
            """)
            ]
            services = {
                row["name"]: bool(row["online"]) for row in db.execute("SELECT * FROM services")
            }
            outbox = [
                dict(row)
                for row in db.execute(
                    "SELECT order_id,event_type,event_id,attempts,last_error,blocked FROM outbox "
                    "WHERE event_id IS NULL ORDER BY id LIMIT 20"
                )
            ]
            totals = dict(
                db.execute(
                    "SELECT count(*) AS orders,coalesce(sum(total_cents),0) AS total_cents FROM orders"
                ).fetchone()
            )
            blocked_count = db.execute(
                "SELECT count(*) FROM outbox WHERE event_id IS NULL AND blocked=1"
            ).fetchone()[0]
        return {
            "configured": bool(config),
            "project_id": config.get("project_id"),
            "relay_url": config.get("relay_url"),
            "services": services,
            "orders": orders,
            "outbox": outbox,
            "totals": totals,
            "blocked_count": blocked_count,
        }


def create_app(store, port=9000, run_sender=True):
    stop = threading.Event()
    config = store.config()

    def send_loop():
        while not stop.is_set():
            try:
                store.send_pending(stop=stop)
            except (sqlite3.Error, ValueError):
                # Keep the local example alive while a locked database recovers.
                pass
            stop.wait(0.5)

    @asynccontextmanager
    async def lifespan(app):
        sender = None
        if config and run_sender:
            sender = threading.Thread(target=send_loop, name="storefront-outbox", daemon=True)
            sender.start()
        yield
        stop.set()
        if sender:
            sender.join(timeout=5)

    app = FastAPI(
        title="Relay storefront example",
        lifespan=lifespan,
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )

    @app.middleware("http")
    async def local_boundary(request, call_next):
        host = request.headers.get("host", "")
        if host not in {f"127.0.0.1:{port}", f"localhost:{port}"}:
            return JSONResponse({"detail": "This example only accepts its local host"}, 403)
        if request.method not in {"GET", "HEAD"} and not request.url.path.startswith("/hooks/"):
            origin = request.headers.get("origin")
            if (origin is not None and origin != "http://" + host) or (
                request.headers.get("x-storefront-request") != "1"
            ):
                return JSONResponse({"detail": "Same-origin request required"}, 403)
        response = await call_next(request)
        response.headers["Cache-Control"] = "no-store"
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self'; "
            "base-uri 'none'; frame-ancestors 'none'; form-action 'self'"
        )
        return response

    async def body(request, limit=16384):
        raw = bytearray()
        async for chunk in request.stream():
            raw.extend(chunk)
            if len(raw) > limit:
                raise HTTPException(413, "Request body too large")
        return bytes(raw)

    async def json_body(request):
        try:
            value = json.loads(await body(request))
            if not isinstance(value, dict):
                raise TypeError()
            json.dumps(value, ensure_ascii=False, allow_nan=False).encode("utf-8")
            return value
        except (ValueError, TypeError, UnicodeError):
            raise HTTPException(422, "Expected a JSON object") from None

    @app.get("/")
    def index():
        return FileResponse(HERE / "index.html")

    @app.get("/app.js")
    def script():
        return FileResponse(HERE / "app.js", media_type="text/javascript")

    @app.get("/style.css")
    def style():
        return FileResponse(HERE / "style.css", media_type="text/css")

    @app.get("/api/state")
    def state():
        return store.state()

    @app.post("/api/orders", status_code=201)
    async def order(request: Request):
        if not config:
            raise HTTPException(503, "Run the setup command first")
        value = await json_body(request)
        try:
            order_id = await asyncio.to_thread(
                store.order, value.get("customer"), value.get("quantity")
            )
            return {"order_id": order_id}
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from None

    @app.post("/api/services/{service}")
    async def availability(service: str, request: Request):
        value = await json_body(request)
        try:
            await asyncio.to_thread(store.set_online, service, value.get("online"))
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from None
        return {"ok": True}

    @app.post("/api/outbox/resume")
    async def resume_outbox():
        return {"resumed": await asyncio.to_thread(store.resume_blocked)}

    @app.post("/hooks/{service}")
    async def receive(service: str, request: Request):
        if service not in SERVICES or not config:
            raise HTTPException(404, "Unknown receiver")
        raw = await body(request, 270000)
        current_config = await asyncio.to_thread(store.config)
        if not verify_signature(current_config["secrets"][service], raw, dict(request.headers)):
            raise HTTPException(401, "Invalid signature")
        if not await asyncio.to_thread(store.is_online, service):
            return JSONResponse({"detail": "Receiver is deliberately offline"}, 503)
        try:
            event = json.loads(raw)
            if not isinstance(event, dict) or event.get("id") != request.headers.get("webhook-id"):
                raise ValueError("Invalid envelope")
            await asyncio.to_thread(store.receive, service, event)
        except (ValueError, TypeError, UnicodeError):
            raise HTTPException(422, "Invalid event") from None
        return JSONResponse({"ok": True})

    return app


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def setup(store, relay_url, port):
    if store.config():
        raise ValueError("This database is already configured; reuse it or choose another --db")
    admin_token = os.environ.get("RELAY_ADMIN_TOKEN", "")
    if not admin_token:
        raise ValueError("Set RELAY_ADMIN_TOKEN to the local Relay administrator token")
    # The bootstrap token must only go to an explicitly local development service.
    target = urlsplit(relay_url)
    if (
        target.scheme != "http"
        or target.hostname not in {"127.0.0.1", "localhost"}
        or target.path not in {"", "/"}
        or target.query
        or target.fragment
        or target.username is not None
        or target.password is not None
    ):
        raise ValueError("Storefront setup requires a local HTTP Relay origin")
    relay_url = relay_url.rstrip("/")

    def admin_post(path, data):
        request = URLRequest(
            relay_url + path,
            json.dumps(data).encode(),
            {
                "Authorization": "Bearer " + admin_token,
                "Content-Type": "application/json",
            },
            method="POST",
        )
        with build_opener(ProxyHandler({}), NoRedirect()).open(request, timeout=10) as response:
            return json.load(response)

    project = admin_post("/api/projects", {"name": "Storefront example"})
    endpoint_ids, signing_secrets = {}, {}
    for service, event_type in SERVICES.items():
        endpoint = admin_post(
            f"/api/projects/{project['id']}/endpoints",
            {
                "url": f"http://127.0.0.1:{port}/hooks/{service}",
                "description": f"Storefront {service}",
                "rate_limit": 10,
                "subscriptions": [event_type],
            },
        )
        endpoint_ids[service], signing_secrets[service] = endpoint["id"], endpoint["signing_secret"]
    store.configure(
        {
            "relay_url": relay_url,
            "project_id": project["id"],
            "api_key": project["api_key"],
            "endpoint_ids": endpoint_ids,
            "secrets": signing_secrets,
            "port": port,
        }
    )
    print(
        "Created a project and two subscribed endpoints. Credentials saved to the local database."
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["setup", "serve", "credentials", "resume"])
    parser.add_argument("--db", default="data/storefront.db")
    parser.add_argument("--relay-url", default="http://127.0.0.1:8787")
    parser.add_argument("--port", type=int, default=9000)
    parser.add_argument("--resume-blocked", action="store_true")
    parser.add_argument(
        "--project-key-env", metavar="NAME", help="Read the new project API key from NAME"
    )
    for service in SERVICES:
        parser.add_argument(
            f"--{service}-secret-env",
            metavar="NAME",
            help=f"Read the new {service} signing secret from NAME",
        )
    args = parser.parse_args()
    if not 1 <= args.port <= 65535:
        parser.error("Port must be between 1 and 65535")
    selectors = {
        field: (name, label)
        for field, name, label in [
            ("api_key", args.project_key_env, "project API key"),
            *(
                (service, getattr(args, f"{service}_secret_env"), f"{service} signing secret")
                for service in SERVICES
            ),
        ]
        if name is not None
    }
    if args.command != "credentials" and (selectors or args.resume_blocked):
        parser.error("Credential selectors and --resume-blocked require the credentials command")
    credentials = {}
    if args.command == "credentials":
        if not selectors:
            parser.error("Select at least one credential field with a --*-env option")
        for field, (name, label) in selectors.items():
            value = os.environ.get(name)
            if not value:
                parser.error(
                    f"The selected environment variable for {label} must be set and nonempty"
                )
            credentials[field] = value
    store = Storefront(args.db)
    if args.command == "credentials":
        try:
            resumed = store.update_credentials(
                credentials.get("api_key"),
                {service: credentials[service] for service in SERVICES if service in credentials},
                args.resume_blocked,
            )
        except ValueError as exc:
            parser.error(str(exc))
        fields = ", ".join(label for _, label in selectors.values())
        print(
            f"Updated credential fields: {fields}. "
            f"Resumed {resumed} blocked submissions with their original keys."
        )
        return
    if args.command == "resume":
        print(f"Resumed {store.resume_blocked()} blocked submissions with their original keys.")
        return
    if args.command == "setup":
        try:
            setup(store, args.relay_url, args.port)
        except (ValueError, OSError) as exc:
            parser.error(str(exc))
        return
    config = store.config()
    if not config:
        parser.error("Run setup first")
    if args.port != config["port"]:
        parser.error("Use the port saved during setup so Relay can reach the receivers")
    print(f"Storefront: http://127.0.0.1:{args.port} (local example; no real payments)")
    uvicorn.run(
        create_app(store, args.port),
        host="127.0.0.1",
        port=args.port,
        proxy_headers=False,
        access_log=False,
    )


if __name__ == "__main__":
    main()
