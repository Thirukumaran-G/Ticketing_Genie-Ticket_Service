# ticket service
from fastapi import FastAPI
from typing import Any
from fastapi.openapi.utils import get_openapi
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
from src.config.settings import settings
from src.observability.logging.logger import configure_logging



def create_app() -> FastAPI:
    configure_logging()

    app = FastAPI(
        title="Ticketing Genie — Ticket Service",
        description="Ticket lifecycle management microservice.",
        version="1.0.0",
        docs_url="/docs",
        redoc_url=None,
    )
    
    def custom_openapi() -> dict[str, Any]:
        """Customize OpenAPI schema - HTTPBearer only for non-Authentication-tagged operations."""
        if app.openapi_schema:
            return app.openapi_schema

        openapi_schema = get_openapi(
            title=app.title,
            version=app.version,
            description=app.description,
            routes=app.routes,
        )

        # Define the security scheme
        openapi_schema.setdefault("components", {}).setdefault("securitySchemes", {})
        openapi_schema["components"]["securitySchemes"]["HTTPBearer"] = {
            "type": "http",
            "scheme": "bearer",
            "bearerFormat": "JWT",
            "description": "JWT access token from /auth/login or /auth/login-json"
        }

        # Only consider standard HTTP methods present in path item objects
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

                # Skip operations explicitly tagged as Authentication
                if tags and tags[0] == "Authentication":
                    continue

                # If operation already has security, skip or merge.
                if "security" in operation:
                    # Option A (skip if present): continue #
                    # continue
                    # Option B (merge): ensure our scheme present
                    existing = operation["security"]
                    if not any("HTTPBearer" in sec for sec in existing):
                        existing.append({"HTTPBearer": []})
                    operation["security"] = existing
                else:
                    operation["security"] = [{"HTTPBearer": []}]

        app.openapi_schema = openapi_schema
        return app.openapi_schema

    app.openapi = custom_openapi
    
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
