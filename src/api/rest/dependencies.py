"""
FastAPI dependencies for ticket-service.

JWT is validated via auth-service internal endpoint.
Role names mirror auth-service seeded roles:
  "customer" | "support_agent" | "support_team_lead" | "admin"

Usage:
  actor: CurrentActor = Depends(require_role(ROLE_AGENT, ROLE_TEAM_LEAD))
  actor: CurrentActor = Depends(get_current_actor)   # any authenticated user
"""

from __future__ import annotations

from collections.abc import Callable

from fastapi import Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from src.handlers.http_clients.auth_client import AuthHttpClient
from src.observability.logging.logger import get_logger

logger = get_logger(__name__)

_bearer = HTTPBearer()

# ── Role constants ────────────────────────────────────────────────────────────

ROLE_CUSTOMER  = "customer"
ROLE_AGENT     = "agent"
ROLE_TEAM_LEAD = "support_team_lead"
ROLE_ADMIN     = "admin"

_INTERNAL_ROLES = {ROLE_AGENT, ROLE_TEAM_LEAD, ROLE_ADMIN}


# ── CurrentActor ──────────────────────────────────────────────────────────────

class CurrentActor:
    """Parsed, validated token claims for the current request actor."""

    def __init__(
        self,
        actor_id: str,
        role_name: str,
        customer_tier: str | None = None,
        customer_id: str | None = None,
        email: str | None = None,
    ) -> None:
        self.actor_id      = actor_id
        self.role_name     = role_name
        self.customer_tier = customer_tier
        self.customer_id   = customer_id
        self.email         = email

    # ── Convenience role checks ───────────────────────────────────────────────

    @property
    def is_customer(self) -> bool:
        return self.role_name == ROLE_CUSTOMER

    @property
    def is_agent(self) -> bool:
        return self.role_name == ROLE_AGENT

    @property
    def is_team_lead(self) -> bool:
        return self.role_name == ROLE_TEAM_LEAD

    @property
    def is_admin(self) -> bool:
        return self.role_name == ROLE_ADMIN

    @property
    def is_internal(self) -> bool:
        return self.role_name in _INTERNAL_ROLES

    def __repr__(self) -> str:
        return f"<CurrentActor id={self.actor_id} role={self.role_name} email={self.email}>"


# ── Core dependency ───────────────────────────────────────────────────────────

async def get_current_actor(
    credentials: HTTPAuthorizationCredentials = Depends(_bearer),
) -> CurrentActor:
    """Validate Bearer JWT via auth-service and hydrate CurrentActor.

    Raises 401 on invalid / expired token.
    """
    token = credentials.credentials
    auth_client = AuthHttpClient()
    result = await auth_client.validate_token(token)

    if not result or not result.get("valid"):
        raise HTTPException(status_code=401, detail="Invalid or expired token.")

    return CurrentActor(
        actor_id=result["actor_id"],
        role_name=result["role"],           # ← auth-service returns "role" not "role_name"
        customer_tier=result.get("customer_tier"),
        customer_id=result.get("customer_id"),
        email=result.get("email"),
    )


# ── Role guard ────────────────────────────────────────────────────────────────

def require_role(*allowed_roles: str) -> Callable:
    """Dependency factory — actor must have one of the supplied role names.

    Usage:
        actor: CurrentActor = Depends(require_role(ROLE_AGENT, ROLE_TEAM_LEAD))
    """

    async def _check(actor: CurrentActor = Depends(get_current_actor)) -> CurrentActor:
        if actor.role_name not in allowed_roles:
            raise HTTPException(
                status_code=403,
                detail=f"Role '{actor.role_name}' not permitted. Required: {list(allowed_roles)}",
            )
        return actor

    return _check


# ── Composite shorthand dependencies ─────────────────────────────────────────

def require_internal() -> Callable:
    """Any internal staff role (agent / team_lead / admin)."""
    return require_role(ROLE_AGENT, ROLE_TEAM_LEAD, ROLE_ADMIN)


def require_team_lead_or_admin() -> Callable:
    return require_role(ROLE_TEAM_LEAD, ROLE_ADMIN)


def require_admin() -> Callable:
    return require_role(ROLE_ADMIN)


def require_customer() -> Callable:
    return require_role(ROLE_CUSTOMER)