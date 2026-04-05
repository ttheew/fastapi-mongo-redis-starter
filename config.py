"""
Central application configuration.

Reads values from environment variables and `.env` files using Pydantic Settings.
"""

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

_BASE_DIR = Path(__file__).resolve().parent
_PROJECT_ROOT = _BASE_DIR.parent

_COMMON_CONFIG: dict[str, object] = {
    "env_file": (
        str(_PROJECT_ROOT / ".env"),
        str(_BASE_DIR / ".env"),
        ".env",
    ),
    "env_file_encoding": "utf-8",
    "case_sensitive": False,
    "extra": "ignore",
}


class App(BaseSettings):
    app_name: str = Field(default="app", validation_alias="APP_NAME")
    env: str = Field(default="dev", validation_alias="ENV")

    model_config = SettingsConfigDict(**_COMMON_CONFIG)


class Mongo(BaseSettings):
    uri: str = "mongodb://localhost:27017/"
    db: str = "app"
    tls: bool = False
    tls_ca_file: str | None = None
    connect_timeout_ms: int = 5_000
    socket_timeout_ms: int = 30_000
    server_selection_timeout_ms: int = 5_000
    max_pool_size: int = 100
    min_pool_size: int = 5
    max_idle_time_ms: int = 60_000
    retry_writes: bool = True
    retry_reads: bool = True
    write_concern_w: str = "majority"
    journal: bool = True
    wtimeout_ms: int = 10_000

    model_config = SettingsConfigDict(env_prefix="MONGO_", **_COMMON_CONFIG)


class Redis(BaseSettings):
    uri: str = "redis://localhost:6379/0"
    max_connections: int = 50
    tls_ca_file: str | None = None
    socket_connect_timeout: int = 5
    socket_timeout: int = 10
    health_check_interval: int = 30
    retry_attempts: int = 3
    retry_backoff_base: float = 0.25
    retry_backoff_cap: float = 8.0

    model_config = SettingsConfigDict(env_prefix="REDIS_", **_COMMON_CONFIG)


class Http(BaseSettings):
    base_url: str = ""
    ssl: bool = True
    connector_limit: int = 100
    timeout_total: float = 30.0
    timeout_connect: float = 5.0
    timeout_sock_read: float = 25.0
    retry_attempts: int = 3
    retry_backoff_base: float = 0.5
    retry_backoff_max: float = 16.0
    demo_base_url: str = "https://httpbin.org"
    demo_sse_url: str | None = None

    model_config = SettingsConfigDict(env_prefix="HTTP_", **_COMMON_CONFIG)


class Settings:
    def __init__(self) -> None:
        self.app = App()
        self.mongo = Mongo()
        self.redis = Redis()
        self.http = Http()


settings = Settings()
