from fastapi import FastAPI
from fastapi.middleware.trustedhost import TrustedHostMiddleware

from src.config.settings import settings


def setup_trusted_hosts(app: FastAPI) -> None:
    app.add_middleware(
        TrustedHostMiddleware,
        allowed_hosts=settings.trusted_host_list,
    )
