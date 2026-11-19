import os
from dataclasses import dataclass


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

    @classmethod
    def from_env(cls):
        return cls(
            db_path=os.getenv("RELAY_DB_PATH", "data/relay.db"),
            admin_token=os.getenv("RELAY_ADMIN_TOKEN", ""),
            allow_private_networks=env_bool("RELAY_ALLOW_PRIVATE_NETWORKS", False),
            worker_enabled=env_bool("RELAY_WORKER_ENABLED", True),
        )
