import os
import ipaddress
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

    def __post_init__(self):
        object.__setattr__(self, "public_url", public_origin(self.public_url))

    @classmethod
    def from_env(cls):
        return cls(
            db_path=os.getenv("RELAY_DB_PATH", "data/relay.db"),
            admin_token=os.getenv("RELAY_ADMIN_TOKEN", ""),
            allow_private_networks=env_bool("RELAY_ALLOW_PRIVATE_NETWORKS", False),
            worker_enabled=env_bool("RELAY_WORKER_ENABLED", True),
            public_url=os.getenv("RELAY_PUBLIC_URL", ""),
        )
