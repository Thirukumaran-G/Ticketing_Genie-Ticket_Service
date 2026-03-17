from pydantic_settings import BaseSettings, SettingsConfigDict
from pathlib import Path


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=Path(__file__).resolve().parent.parent.parent / ".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

    LOG_LEVEL:              str
    PORT:                   int
    SERVICE_NAME:           str
    DATABASE_URL:           str
    MONGO_URL:              str
    MONGO_DB_NAME:          str
    REDIS_URL:              str
    CELERY_BROKER_URL:      str
    CELERY_RESULT_BACKEND:  str
    AUTH_SERVICE_URL:       str
    GROQ_API_KEY:           str
    ALLOWED_ORIGINS:        str
    TRUSTED_HOST:           str
    GROQ_MODEL:             str
    SMTP_HOST:              str
    SMTP_PORT:              int
    SMTP_USERNAME:          str
    SMTP_PASSWORD:          str
    SMTP_FROM_EMAIL:        str
    SMTP_FROM_NAME:         str
    EMAIL_FROM:             str = ""
    PORTAL_URL:             str = "https://support.ticketinggenie.com"

    # IMAP host/port/mailbox stay in env.
    # IMAP_USER and IMAP_PASSWORD live in ticket.email_config table only.
    IMAP_HOST:    str | None = None
    IMAP_PORT:    int        = 993
    IMAP_MAILBOX: str        = "INBOX"

    # JWT
    JWT_SECRET_KEY: str = ""
    JWT_ALGORITHM:  str = "HS256"

    SLA_CHECK_INTERVAL_SECONDS: int = 60

    @property
    def allowed_origin_list(self) -> list[str]:
        return [o.strip() for o in self.ALLOWED_ORIGINS.split(",")]

    @property
    def trusted_host_list(self) -> list[str]:
        return [h.strip() for h in self.TRUSTED_HOST.split(",")]


settings = Settings()  # type: ignore[call-arg]