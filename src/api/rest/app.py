# ticket service
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator

from fastapi import FastAPI
from fastapi.openapi.utils import get_openapi
from sqlalchemy import text

from src.api.middleware.error_handler import setup_error_handlers
from src.api.middleware.cors import setup_cors
from src.api.middleware.trustedhost import setup_trusted_hosts
from src.api.rest.routes.admin_routes import router as admin_router
from src.api.rest.routes.customer_routes import router as customer_router
from src.api.rest.routes.agent_routes import router as agent_router
from src.api.rest.routes.team_lead_routes import router as team_lead_routes
from src.api.rest.routes.notification_routes import router as notification_router
from src.api.rest.routes.agent_conversation_routes import router as agent_conversation_router
from src.api.rest.routes.customer_conversation_routes import router as customer_conversation_router
from src.api.rest.routes.health import router as health_router
from src.data.clients.postgres_client import get_db_session, engine
from src.data.models.postgres.models import Base
from src.scripts.keyword_seeder import seed_keyword_rules
from src.scripts.team_and_member_seeder import seed_teams
from src.scripts.priority_seviority_seeder import seed_pri_sev_map
from src.scripts.notification_templates import seed
from src.scripts.sla_rule_seeder import seed_sla_rules
from src.config.settings import settings
from src.observability.logging.logger import configure_logging, get_logger


logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Run startup tasks before the app begins serving requests."""
    logger.info("ticket_service_starting")

    async with engine.begin() as conn:
        await conn.execute(text("CREATE SCHEMA IF NOT EXISTS ticket"))
        await conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))  # ← added
        await conn.run_sync(Base.metadata.create_all)
    logger.info("database_tables_created")

    async for session in get_db_session():
        await seed_keyword_rules()
        await seed_pri_sev_map()
        await seed_sla_rules()
        await seed_teams()
        await seed()
        logger.info("keyword_rules_seeded")
        logger.info("priority_severity_map_seeded")
        logger.info("sla_rules_seeded")
        logger.info("teams_seeded")
        break

    logger.info("ticket_service_ready")
    yield
    logger.info("ticket_service_shutdown")


def create_app() -> FastAPI:
    configure_logging()

    app = FastAPI(
        title="Ticketing Genie — Ticket Service",
        description="Ticket lifecycle management microservice.",
        version="1.0.0",
        docs_url="/docs",
        redoc_url=None,
        lifespan=lifespan,
    )

    def custom_openapi() -> dict[str, Any]:
        if app.openapi_schema:
            return app.openapi_schema

        openapi_schema = get_openapi(
            title=app.title,
            version=app.version,
            description=app.description,
            routes=app.routes,
        )

        openapi_schema.setdefault("components", {}).setdefault("securitySchemes", {})
        openapi_schema["components"]["securitySchemes"]["HTTPBearer"] = {
            "type": "http",
            "scheme": "bearer",
            "bearerFormat": "JWT",
            "description": "JWT access token from /auth/login or /auth/login-json",
        }

        http_methods = {"get", "post", "put", "patch", "delete", "options", "head"}

        for _path, path_item in openapi_schema.get("paths", {}).items():
            if not isinstance(path_item, dict):
                continue
            for method, operation in list(path_item.items()):
                if method.lower() not in http_methods:
                    continue
                if not isinstance(operation, dict):
                    continue
                tags = operation.get("tags", [])
                if tags and tags[0] == "Authentication":
                    continue
                if "security" in operation:
                    existing = operation["security"]
                    if not any("HTTPBearer" in sec for sec in existing):
                        existing.append({"HTTPBearer": []})
                    operation["security"] = existing
                else:
                    operation["security"] = [{"HTTPBearer": []}]

        app.openapi_schema = openapi_schema
        return app.openapi_schema

    app.openapi = custom_openapi  # type: ignore[method-assign]

    setup_trusted_hosts(app)
    setup_cors(app)
    setup_error_handlers(app)

    prefix = "/api/v1/ticket"
    app.include_router(health_router)
    app.include_router(admin_router, prefix=prefix)
    app.include_router(customer_router, prefix=prefix)
    app.include_router(agent_router, prefix=prefix)
    app.include_router(team_lead_routes, prefix=prefix)
    app.include_router(notification_router, prefix=prefix)
    app.include_router(agent_conversation_router, prefix=prefix)
    app.include_router(customer_conversation_router, prefix=prefix)
    return app