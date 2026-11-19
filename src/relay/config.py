import ipaddress
import os
import re
from dataclasses import dataclass
from urllib.parse import urlsplit


def public_origin(value):
    """Validate and normalize the externally visible console origin."""
    if not value:
        return ""
    if (
        any(
            character.isspace() or ord(character) < 32 or ord(character) == 127
            for character in value
        )
        or "\\" in value
    ):
        raise ValueError("RELAY_PUBLIC_URL must be an HTTP(S) origin without whitespace")
    try:
        parsed = urlsplit(value)
        port = parsed.port
        host = parsed.hostname
        if (
            parsed.scheme not in {"http", "https"}
            or not host
            or parsed.username is not None
            or parsed.password is not None
            or parsed.path not in {"", "/"}
            or parsed.query
            or parsed.fragment
            or "?" in value
            or "#" in value
            or "%" in host
            or port == 0
        ):
            raise ValueError
        host = host.encode("idna").decode("ascii").lower()
        if ":" in host:
            host = f"[{ipaddress.IPv6Address(host).compressed}]"
        elif not all(
            re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", label)
            for label in host.rstrip(".").split(".")
        ):
            raise ValueError
        default_port = 443 if parsed.scheme == "https" else 80
        authority = host if port is None or port == default_port else f"{host}:{port}"
        return f"{parsed.scheme}://{authority}"
    except (ValueError, UnicodeError):
        raise ValueError(
            "RELAY_PUBLIC_URL must be an HTTP(S) origin, such as https://relay.example"
        ) from None


def env_bool(name, default):
    value = os.getenv(name)
    if value is None:
        return default
    value = value.strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be true or false")


@dataclass(frozen=True, slots=True)
class Settings:
    db_path: str = "data/relay.db"
    admin_token: str = ""
    allow_private_networks: bool = False
    worker_enabled: bool = True
    host: str = "127.0.0.1"
    port: int = 8787
    max_payload_bytes: int = 262144
    max_attempts: int = 6
    retry_base: float = 2.0
    demo_mode: bool = False
    demo_receiver_port: int = 8790
    public_url: str = ""
    retention_days: int = 0
    idempotency_days: int = 90
    cleanup_batch_size: int = 100
    min_free_bytes: int = 0
    max_storage_bytes: int = 0
    storage_warning_percent: float = 90
    queue_warning_age_seconds: int = 300
    failure_warning_percent: float = 25
    failure_min_samples: int = 10
    alert_webhook_url: str = ""
    alert_signing_secret: str = ""
    alert_cooldown_seconds: int = 3600
    maintenance_interval_seconds: float = 30
    maintenance_enabled: bool = True

    def __post_init__(self):
        object.__setattr__(self, "public_url", public_origin(self.public_url))
        self.operations_policy()

    def operations_policy(self):
        from .operations import OperationsPolicy

        return OperationsPolicy(
            retention_days=self.retention_days,
            idempotency_days=self.idempotency_days,
            cleanup_batch_size=self.cleanup_batch_size,
            min_free_bytes=self.min_free_bytes,
            max_storage_bytes=self.max_storage_bytes,
            storage_warning_percent=self.storage_warning_percent,
            queue_warning_age_seconds=self.queue_warning_age_seconds,
            failure_warning_percent=self.failure_warning_percent,
            failure_min_samples=self.failure_min_samples,
            alert_webhook_url=self.alert_webhook_url or None,
            alert_signing_secret=self.alert_signing_secret or None,
            alert_cooldown_seconds=self.alert_cooldown_seconds,
            interval_seconds=self.maintenance_interval_seconds,
        )

    @classmethod
    def from_env(cls):
        return cls(
            db_path=os.getenv("RELAY_DB_PATH", "data/relay.db"),
            admin_token=os.getenv("RELAY_ADMIN_TOKEN", ""),
            allow_private_networks=env_bool("RELAY_ALLOW_PRIVATE_NETWORKS", False),
            worker_enabled=env_bool("RELAY_WORKER_ENABLED", True),
            public_url=os.getenv("RELAY_PUBLIC_URL", ""),
            retention_days=int(os.getenv("RELAY_RETENTION_DAYS", "0")),
            idempotency_days=int(os.getenv("RELAY_IDEMPOTENCY_DAYS", "90")),
            cleanup_batch_size=int(os.getenv("RELAY_CLEANUP_BATCH_SIZE", "100")),
            min_free_bytes=int(os.getenv("RELAY_MIN_FREE_BYTES", "0")),
            max_storage_bytes=int(os.getenv("RELAY_MAX_STORAGE_BYTES", "0")),
            storage_warning_percent=float(os.getenv("RELAY_STORAGE_WARNING_PERCENT", "90")),
            queue_warning_age_seconds=int(os.getenv("RELAY_QUEUE_WARNING_AGE_SECONDS", "300")),
            failure_warning_percent=float(os.getenv("RELAY_FAILURE_WARNING_PERCENT", "25")),
            failure_min_samples=int(os.getenv("RELAY_FAILURE_MIN_SAMPLES", "10")),
            alert_webhook_url=os.getenv("RELAY_ALERT_WEBHOOK_URL", ""),
            alert_signing_secret=os.getenv("RELAY_ALERT_SIGNING_SECRET", ""),
            alert_cooldown_seconds=int(os.getenv("RELAY_ALERT_COOLDOWN_SECONDS", "3600")),
            maintenance_interval_seconds=float(
                os.getenv("RELAY_MAINTENANCE_INTERVAL_SECONDS", "30")
            ),
            maintenance_enabled=env_bool("RELAY_MAINTENANCE_ENABLED", True),
        )
