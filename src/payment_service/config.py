"""Environment-based validated configuration."""

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql+asyncpg://payments:payments@localhost:5432/payments"
    rabbitmq_url: str = "amqp://payments:payments@localhost:5672/"
    api_key: SecretStr = Field(min_length=16)
    webhook_allowed_hosts: list[str] = Field(default_factory=list)
    webhook_signing_secret: SecretStr | None = None
    webhook_timeout: float = Field(default=10.0, gt=0, le=30)
    outbox_poll_interval: float = Field(default=0.5, gt=0)
    publish_timeout: float = Field(default=5.0, gt=0)
    log_level: str = "INFO"
