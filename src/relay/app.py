"""Administrative console and project-scoped event ingestion."""

import asyncio
import hashlib
import hmac
import json
import secrets
import threading
import time
from collections import OrderedDict, deque
from contextlib import asynccontextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, ConfigDict, Field

from . import __version__
from .config import Settings
from .delivery import validate_endpoint
from .operations import CapacityExceeded, IdempotencyExpired, Operations
from .store import STATUSES, IdempotencyConflict, Store
from .worker import Worker

COOKIE = "relay_session"
SESSION_SECONDS = 8 * 60 * 60
STATIC = Path(__file__).parent / "static"


class Model(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Login(Model):
    token: str = Field(min_length=1, max_length=1024)


class ProjectInput(Model):
    name: str = Field(min_length=1, max_length=120)


class EndpointInput(Model):
    url: str = Field(min_length=8, max_length=2048)
    description: str = Field(default="", max_length=500)
    rate_limit: int = Field(default=10, ge=1, le=1000)
    subscriptions: list[str] = Field(default_factory=lambda: ["*"], max_length=100)


class EndpointUpdate(Model):
    enabled: bool | None = None
    url: str | None = Field(default=None, min_length=8, max_length=2048)
    description: str | None = Field(default=None, max_length=500)
    rate_limit: int | None = Field(default=None, ge=1, le=1000)
    subscriptions: list[str] | None = Field(default=None, max_length=100)


class RotationInput(Model):
    overlap_seconds: int = Field(default=3600, ge=0, le=86400, strict=True)


class RecoveryPreviewInput(Model):
    endpoint_id: str | None = Field(default=None, max_length=100)
    since: float | None = Field(default=None, ge=0, allow_inf_nan=False)
    until: float | None = Field(default=None, ge=0, allow_inf_nan=False)
    limit: int = Field(default=1000, ge=1, le=1000, strict=True)


class RecoveryInput(Model):
    preview_id: str = Field(min_length=1, max_length=100)
    pace_per_second: int = Field(default=10, ge=1, le=1000, strict=True)


class CleanupInput(Model):
    dry_run: bool = True


class EventInput(Model):
    type: str = Field(pattern=r"^[A-Za-z][A-Za-z0-9_.:-]{0,127}$")
    payload: dict


class ReceiverInput(Model):
    online: bool


class DemoSend(Model):
    project_id: str
    count: int = Field(default=1, ge=1, le=20)


class BodyLimit:
    """Bound actual request bytes, including requests without Content-Length."""

    def __init__(self, app, max_bytes):
        self.app, self.max_bytes = app, max_bytes

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or scope["method"] not in {"POST", "PUT", "PATCH", "DELETE"}:
            await self.app(scope, receive, send)
            return
        body = bytearray()
        while True:
            message = await receive()
            if message["type"] == "http.disconnect":
                return
            body.extend(message.get("body", b""))
            if len(body) > self.max_bytes:
                await JSONResponse({"detail": "Request body too large"}, 413)(scope, receive, send)
                return
            if not message.get("more_body", False):
                break
        content_type = (
            next((value for key, value in scope.get("headers", []) if key == b"content-type"), b"")
            .split(b";", 1)[0]
            .strip()
            .lower()
        )
        if body and (
            not content_type
            or content_type == b"application/json"
            or content_type.endswith(b"+json")
        ):
            try:
                # Reject invalid Unicode before validation errors can echo it in a response.
                json.dumps(json.loads(body), ensure_ascii=False, allow_nan=False).encode("utf-8")
            except (ValueError, UnicodeError, RecursionError):
                await JSONResponse(
                    {"detail": "Body must be valid UTF-8 JSON with finite numbers"}, 422
                )(scope, receive, send)
                return
        delivered = False

        async def buffered_receive():
            nonlocal delivered
            if not delivered:
                delivered = True
                return {"type": "http.request", "body": bytes(body), "more_body": False}
            return await receive()

        await self.app(scope, buffered_receive, send)


class DemoReceiver:
    def __init__(self, port, max_bytes):
        self.online, self.received = True, 0
        self.seen = set()
        self.lock = threading.Lock()
        state = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_args):
                pass

            def do_POST(self):
                code = 404
                if self.path == "/webhook":
                    try:
                        size = int(self.headers.get("Content-Length", "-1"))
                        if not 0 <= size <= max_bytes:
                            raise ValueError("Invalid request length")
                        self.connection.settimeout(5)
                        raw = self.rfile.read(size)
                        if len(raw) != size:
                            raise ValueError("Incomplete request")
                        json.loads(raw)
                        with state.lock:
                            code = 204 if state.online else 503
                            event_id = self.headers.get("webhook-id")
                            if state.online and event_id not in state.seen:
                                state.seen.add(event_id)
                                state.received += 1
                    except (ValueError, OSError):
                        code = 400
                self.send_response(code)
                self.send_header("Content-Length", "0")
                self.end_headers()
                self.close_connection = True

        self.server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
        self.server.daemon_threads = True
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def start(self):
        self.thread.start()

    def stop(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)

    def snapshot(self):
        with self.lock:
            return {"online": self.online, "received": self.received}


def make_worker(store, settings):
    return Worker(
        store,
        allow_private=settings.allow_private_networks,
        max_attempts=settings.max_attempts,
        retry_base=settings.retry_base,
    )


def create_app(settings=None):
    settings = settings or Settings.from_env()
    store = Store(settings.db_path)
    operations = Operations(store, settings.operations_policy(), settings.allow_private_networks)
    failed_attempts = OrderedDict()
    auth_lock = threading.Lock()
    demo_setup_lock = asyncio.Lock()

    @asynccontextmanager
    async def lifespan(app):
        if len(settings.admin_token) < 24:
            raise RuntimeError(
                "Set RELAY_ADMIN_TOKEN to at least 24 characters, or run relay demo."
            )
        await asyncio.to_thread(store.initialize)
        worker = make_worker(store, settings) if settings.worker_enabled else None
        receiver = None
        try:
            if settings.demo_mode:
                if not settings.allow_private_networks:
                    raise RuntimeError("Demo mode requires private destinations")
                receiver = DemoReceiver(settings.demo_receiver_port, settings.max_payload_bytes)
                receiver.start()
                app.state.demo_receiver = receiver
            if worker:
                worker.start()
            if settings.maintenance_enabled:
                operations.start()
            yield
        finally:
            await asyncio.to_thread(operations.stop)
            if worker:
                await asyncio.to_thread(worker.stop)
            if receiver:
                await asyncio.to_thread(receiver.stop)

    app = FastAPI(
        title="Relay",
        version=__version__,
        lifespan=lifespan,
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )
    app.state.store, app.state.settings = store, settings
    app.state.operations = operations
    app.state.demo_project = app.state.demo_receiver = None
    app.add_middleware(BodyLimit, max_bytes=settings.max_payload_bytes)

    @app.exception_handler(CapacityExceeded)
    async def capacity_error(_request, _exc):
        return JSONResponse(
            {"detail": "Storage capacity limit reached; event was not accepted"},
            503,
            headers={"Retry-After": "30"},
        )

    @app.exception_handler(IdempotencyExpired)
    async def expired_event(_request, exc):
        return JSONResponse(
            {
                "detail": "The original event was accepted but its retained data has expired. "
                "This idempotency key cannot create another event.",
                "event_id": exc.event_id,
            },
            410,
        )

    @app.exception_handler(RequestValidationError)
    async def validation_error(_request, exc):
        # Do not echo raw input: it may contain secrets, bytes or invalid Unicode.
        def safe_text(value):
            return value.encode("utf-8", errors="replace").decode("utf-8")

        detail = [
            {
                "type": safe_text(error["type"]),
                "loc": [
                    safe_text(part) if isinstance(part, str) else part for part in error["loc"]
                ],
                "msg": safe_text(error["msg"]),
            }
            for error in exc.errors()
        ]
        return JSONResponse({"detail": detail}, 422)

    @app.middleware("http")
    async def security_headers(request, call_next):
        response = await call_next(request)
        response.headers.update(
            {
                "X-Content-Type-Options": "nosniff",
                "X-Frame-Options": "DENY",
                "Referrer-Policy": "no-referrer",
                "Content-Security-Policy": "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; object-src 'none'; base-uri 'self'; frame-ancestors 'none'; form-action 'self'",
            }
        )
        if request.url.path.startswith(("/api/", "/v1/")) or request.url.path == "/metrics":
            response.headers["Cache-Control"] = "no-store"
        return response

    def check_origin(request, require_evidence=False):
        origin = request.headers.get("origin")
        expected = settings.public_url or str(request.base_url).rstrip("/")
        if origin is not None and origin != expected:
            raise HTTPException(403, "Origin is not allowed")
        if require_evidence and not (
            origin == expected or request.headers.get("x-relay-request") == "1"
        ):
            raise HTTPException(403, "Missing request origin or X-Relay-Request")

    def signature(value):
        return hmac.new(
            settings.admin_token.encode(), ("relay-session:" + value).encode(), hashlib.sha256
        ).hexdigest()

    def session_hash(value):
        return hashlib.sha256(value.encode()).hexdigest()

    def secure_cookie(request):
        return (
            settings.public_url.startswith("https://")
            if settings.public_url
            else request.url.scheme == "https"
        )

    def token_matches(value):
        try:
            return hmac.compare_digest(value.encode(), settings.admin_token.encode())
        except UnicodeError:
            return False

    def auth_failure(request, detail):
        address = request.client.host if request.client else "unknown"
        with auth_lock:
            now = time.monotonic()
            attempts = failed_attempts.setdefault(address, deque())
            failed_attempts.move_to_end(address)
            while attempts and attempts[0] <= now - 60:
                attempts.popleft()
            while len(failed_attempts) > 4096:
                failed_attempts.popitem(last=False)
            if len(attempts) >= 10:
                raise HTTPException(
                    429, "Too many authentication attempts", headers={"Retry-After": "60"}
                )
            attempts.append(now)
        raise HTTPException(401, detail)

    def valid_session(value):
        try:
            if len(value) > 256:
                return False
            stamp, nonce, supplied = value.split(".")
            now = int(time.time())
            return (
                len(nonce) == 32
                and now <= int(stamp) <= now + SESSION_SECONDS
                and hmac.compare_digest(signature(stamp + "." + nonce), supplied)
                and store.valid_session(session_hash(value))
            )
        except (ValueError, TypeError, UnicodeError):
            return False

    def bearer(request):
        scheme, _, value = request.headers.get("authorization", "").partition(" ")
        return value if scheme.lower() == "bearer" else ""

    def admin(request: Request):
        token = bearer(request)
        if token and token_matches(token):
            return
        cookie = request.cookies.get(COOKIE, "")
        if not valid_session(cookie):
            if token or cookie:
                auth_failure(request, "Authentication required")
            raise HTTPException(401, "Authentication required")
        if request.method not in {"GET", "HEAD", "OPTIONS"}:
            check_origin(request, True)

    protected = [Depends(admin)]

    def project(project_id):
        if not any(p["id"] == project_id for p in store.list_projects()):
            raise HTTPException(404, "Project not found")

    def delivery(project_id, delivery_id):
        project(project_id)
        result = store.delivery_detail(project_id, delivery_id)
        if not result:
            raise HTTPException(404, "Delivery not found")
        return result

    def require_demo():
        if not settings.demo_mode:
            raise HTTPException(404, "Demo is not enabled")

    @app.get("/healthz")
    def health():
        with store._db() as db:
            db.execute("SELECT 1 FROM projects LIMIT 1").fetchone()
        return {"status": "ok", "version": __version__}

    @app.get("/api/session", dependencies=protected)
    def session():
        return {"ok": True, "demo_enabled": settings.demo_mode}

    @app.post("/api/session")
    async def login(body: Login, request: Request, response: Response):
        check_origin(request)
        if not token_matches(body.token):
            auth_failure(request, "Invalid token")
        expires_at = int(time.time()) + SESSION_SECONDS
        value = f"{expires_at}.{secrets.token_hex(16)}"
        cookie = value + "." + signature(value)
        await asyncio.to_thread(store.create_session, session_hash(cookie), expires_at)
        response.set_cookie(
            COOKIE,
            cookie,
            max_age=SESSION_SECONDS,
            httponly=True,
            samesite="strict",
            secure=secure_cookie(request),
            path="/",
        )
        return {"ok": True}

    @app.delete("/api/session", dependencies=protected)
    def logout(request: Request, response: Response):
        cookie = request.cookies.get(COOKIE)
        if cookie:
            try:
                hashed = session_hash(cookie)
            except UnicodeError:
                pass
            else:
                store.revoke_session(hashed)
        response.delete_cookie(
            COOKIE, path="/", httponly=True, samesite="strict", secure=secure_cookie(request)
        )
        return {"ok": True}

    @app.get("/api/projects", dependencies=protected)
    def projects():
        return {"projects": store.list_projects()}

    @app.post("/api/projects", dependencies=protected, status_code=201)
    def create_project(body: ProjectInput):
        if not body.name.strip():
            raise HTTPException(422, "Project name cannot be blank")
        return store.create_project(body.name)

    @app.post("/api/projects/{project_id}/rotate-key", dependencies=protected)
    def rotate_key(project_id: str, body: RotationInput | None = None):
        project(project_id)
        return store.rotate_project_key(project_id, body.overlap_seconds if body else 0)

    @app.get("/api/projects/{project_id}/endpoints", dependencies=protected)
    def endpoints(project_id: str):
        project(project_id)
        return {"endpoints": store.list_endpoints(project_id)}

    @app.post("/api/projects/{project_id}/endpoints", dependencies=protected, status_code=201)
    async def create_endpoint(project_id: str, body: EndpointInput):
        await asyncio.to_thread(project, project_id)
        try:
            await asyncio.to_thread(validate_endpoint, body.url, settings.allow_private_networks)
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from None
        try:
            return await asyncio.to_thread(
                store.create_endpoint,
                project_id,
                body.url,
                body.description,
                body.rate_limit,
                body.subscriptions,
            )
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from None

    @app.patch("/api/projects/{project_id}/endpoints/{endpoint_id}", dependencies=protected)
    async def update_endpoint(project_id: str, endpoint_id: str, body: EndpointUpdate):
        await asyncio.to_thread(project, project_id)
        changes = body.model_dump(exclude_unset=True)
        if not changes or any(value is None for value in changes.values()):
            raise HTTPException(422, "Supply at least one non-null endpoint setting")
        try:
            if body.url is not None:
                await asyncio.to_thread(
                    validate_endpoint, body.url, settings.allow_private_networks
                )
            result = await asyncio.to_thread(
                store.update_endpoint, project_id, endpoint_id, **changes
            )
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from None
        if result is None:
            raise HTTPException(404, "Endpoint not found")
        return result

    @app.post(
        "/api/projects/{project_id}/endpoints/{endpoint_id}/rotate-secret", dependencies=protected
    )
    def rotate_secret(project_id: str, endpoint_id: str, body: RotationInput | None = None):
        project(project_id)
        result = store.rotate_endpoint_secret(
            project_id, endpoint_id, body.overlap_seconds if body else 3600
        )
        if result is None:
            raise HTTPException(404, "Endpoint not found")
        return result

    @app.get("/api/operations", dependencies=protected)
    def operations_summary():
        return operations.summary()

    @app.get("/api/projects/{project_id}/metrics", dependencies=protected)
    def project_metrics(project_id: str):
        project(project_id)
        return operations.summary(project_id)

    @app.get("/metrics", dependencies=protected, response_class=PlainTextResponse)
    def prometheus():
        return PlainTextResponse(operations.prometheus(), media_type="text/plain; version=0.0.4")

    @app.post("/api/maintenance/cleanup", dependencies=protected)
    def cleanup(body: CleanupInput):
        return operations.cleanup(dry_run=body.dry_run)

    @app.post("/api/projects/{project_id}/recoveries/preview", dependencies=protected)
    def preview_recovery(project_id: str, body: RecoveryPreviewInput):
        project(project_id)
        try:
            return store.preview_recovery(project_id, **body.model_dump())
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from None

    @app.get("/api/projects/{project_id}/recoveries", dependencies=protected)
    def recoveries(project_id: str):
        project(project_id)
        return {"recoveries": store.list_recoveries(project_id)}

    @app.post("/api/projects/{project_id}/recoveries", dependencies=protected, status_code=201)
    def start_recovery(project_id: str, body: RecoveryInput):
        project(project_id)
        try:
            return store.create_recovery(project_id, body.preview_id, body.pace_per_second)
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from None

    @app.get("/api/projects/{project_id}/recoveries/{recovery_id}", dependencies=protected)
    def recovery_detail(project_id: str, recovery_id: str):
        project(project_id)
        result = store.get_recovery(project_id, recovery_id)
        if result is None:
            raise HTTPException(404, "Recovery not found")
        return result

    @app.post("/api/projects/{project_id}/recoveries/{recovery_id}/cancel", dependencies=protected)
    def cancel_recovery(project_id: str, recovery_id: str):
        project(project_id)
        result = store.cancel_recovery(project_id, recovery_id)
        if result is None:
            raise HTTPException(404, "Recovery not found")
        return result

    @app.get("/api/projects/{project_id}/deliveries", dependencies=protected)
    def deliveries(
        project_id: str,
        status: str | None = None,
        search: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ):
        project(project_id)
        if (
            not 1 <= limit <= 200
            or not 0 <= offset <= 2**63 - 1
            or len(search or "") > 200
            or (status and status not in STATUSES)
        ):
            raise HTTPException(422, "Invalid status, pagination or search")
        return store.list_deliveries(project_id, status, search, limit, offset)

    @app.get("/api/projects/{project_id}/deliveries/{delivery_id}", dependencies=protected)
    def detail(project_id: str, delivery_id: str):
        return delivery(project_id, delivery_id)

    @app.post("/api/projects/{project_id}/deliveries/{delivery_id}/replay", dependencies=protected)
    def replay(project_id: str, delivery_id: str):
        delivery(project_id, delivery_id)
        if not store.replay_delivery(project_id, delivery_id):
            raise HTTPException(409, "Delivery is currently processing")
        return {"ok": True}

    @app.get("/api/projects/{project_id}/stats", dependencies=protected)
    def stats(project_id: str):
        project(project_id)
        return store.stats(project_id)

    @app.post("/api/projects/{project_id}/events", dependencies=protected)
    def console_event(project_id: str, body: EventInput):
        project(project_id)
        try:
            event, _ = store.create_event(project_id, body.type, body.payload)
        except ValueError:
            raise HTTPException(422, "Payload must be valid JSON with finite numbers") from None
        return JSONResponse({"event": event, "created": True}, 202)

    @app.post("/v1/events")
    def ingest(body: EventInput, request: Request):
        owner = store.authenticate_project(bearer(request))
        if not owner:
            raise HTTPException(401, "Invalid project API key")
        key = request.headers.get("idempotency-key")
        if key is not None and (
            not 1 <= len(key) <= 200 or any(ord(c) < 33 or ord(c) > 126 for c in key)
        ):
            raise HTTPException(422, "Invalid Idempotency-Key")
        try:
            json.dumps(body.payload, ensure_ascii=False, allow_nan=False).encode("utf-8")
        except (ValueError, TypeError):
            raise HTTPException(422, "Payload must be valid JSON with finite numbers") from None
        try:
            event, created = store.create_event(owner["id"], body.type, body.payload, key)
        except IdempotencyConflict:
            raise HTTPException(409, "Idempotency key was used for another event") from None
        except (ValueError, TypeError):
            raise HTTPException(422, "Payload must be valid JSON with finite numbers") from None
        return JSONResponse({"event": event, "created": created}, 202 if created else 200)

    @app.get("/api/openapi.json", dependencies=protected)
    def openapi():
        return app.openapi()

    @app.post("/api/demo/setup", dependencies=protected)
    async def demo_setup():
        require_demo()

        def create_fixture():
            created = store.create_project("Demo shop")
            endpoint = store.create_endpoint(
                created["id"],
                f"http://127.0.0.1:{settings.demo_receiver_port}/webhook",
                "Demo warehouse",
                10,
            )
            return {
                "project": {k: v for k, v in created.items() if k != "api_key"},
                "api_key": created["api_key"],
                "endpoint": endpoint,
            }

        async with demo_setup_lock:
            if app.state.demo_project is None:
                app.state.demo_project = await asyncio.to_thread(create_fixture)
            return app.state.demo_project

    @app.get("/api/demo/receiver", dependencies=protected)
    def receiver_state():
        require_demo()
        return app.state.demo_receiver.snapshot()

    @app.post("/api/demo/receiver", dependencies=protected)
    def receiver_toggle(body: ReceiverInput):
        require_demo()
        receiver = app.state.demo_receiver
        with receiver.lock:
            receiver.online = body.online
        return receiver.snapshot()

    @app.post("/api/demo/send", dependencies=protected)
    def demo_send(body: DemoSend):
        require_demo()
        fixture = app.state.demo_project
        if fixture is None or body.project_id != fixture["project"]["id"]:
            raise HTTPException(422, "Set up and select the demo project first")
        for _ in range(body.count):
            store.create_event(
                body.project_id,
                "order.created",
                {
                    "order_id": "order_" + secrets.token_hex(4),
                    "item": "Workshop notebook",
                    "quantity": 1,
                    "amount": 2400,
                    "currency": "USD",
                },
            )
        return {"created": body.count}

    app.mount("/assets", StaticFiles(directory=STATIC), name="assets")

    @app.get("/", include_in_schema=False)
    def index():
        return FileResponse(STATIC / "index.html")

    return app
