"""
FastAPI dependencies for ticket-service.

JWT is validated via auth-service internal endpoint.
Role names mirror auth-service seeded roles:
  "customer" | "agent" | "team_lead" | "admin"
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, Dict, Optional

from fastapi import Depends, HTTPException, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from src.handlers.http_clients.auth_client import AuthHttpClient
from src.observability.logging.logger import get_logger

logger = get_logger(__name__)

# auto_error=False so we can fall back to cookie without a hard 403
_bearer = HTTPBearer(auto_error=False)

# ── Role constants ────────────────────────────────────────────────────────────

ROLE_CUSTOMER  = "customer"
ROLE_AGENT     = "agent"
ROLE_TEAM_LEAD = "team_lead"
ROLE_ADMIN     = "admin"

_INTERNAL_ROLES = {ROLE_AGENT, ROLE_TEAM_LEAD, ROLE_ADMIN}


# ── CurrentActor ──────────────────────────────────────────────────────────────

class CurrentActor:
    """Parsed, validated token claims for the current request actor."""

    def __init__(
        self,
        actor_id:      str,
        role_name:     str,
        email:         str | None            = None,
        company_id:    str | None            = None,
        product_tiers: Dict[str, Any] | None = None,
    ) -> None:
        self.actor_id      = actor_id
        self.role_name     = role_name
        self.email         = email
        self.company_id    = company_id
        self.product_tiers: Dict[str, Any] = product_tiers or {}

    # ── Per-product tier lookup ───────────────────────────────────────────────

    def get_tier_name_for_product(self, product_id: str) -> str | None:
        entry = self.product_tiers.get(str(product_id))
        if not entry:
            return None
        return entry.get("tier_name")

    def get_tier_id_for_product(self, product_id: str) -> str | None:
        entry = self.product_tiers.get(str(product_id))
        if not entry:
            return None
        return entry.get("tier_id")

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
        return (
            f"<CurrentActor id={self.actor_id} role={self.role_name} "
            f"email={self.email} company_id={self.company_id}>"
        )


# ── Token extraction helper ───────────────────────────────────────────────────

def _extract_token(
    request:     Request,
    credentials: Optional[HTTPAuthorizationCredentials],
) -> str:
    """
    Extract bearer token from:
    1. Authorization: Bearer <token>  header
    2. access_token httpOnly cookie (set by auth-service on login/refresh)
    """
    if credentials and credentials.credentials:
        return credentials.credentials

    cookie_token = request.cookies.get("access_token")
    if cookie_token:
        return cookie_token

    raise HTTPException(status_code=401, detail="Missing authorization token.")


# ── Core dependency ───────────────────────────────────────────────────────────

async def get_current_actor(
    request:     Request,
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(_bearer),
) -> CurrentActor:
    """Validate Bearer JWT via auth-service and hydrate CurrentActor."""
    token = _extract_token(request, credentials)

    auth_client = AuthHttpClient()
    result = await auth_client.validate_token(token)

    if not result or not result.get("valid"):
        raise HTTPException(status_code=401, detail="Invalid or expired token.")

    return CurrentActor(
        actor_id=result["actor_id"],
        role_name=result["role"],
        email=result.get("email"),
        company_id=result.get("company_id"),
        product_tiers=result.get("product_tiers") or {},
    )


# ── Role guard ────────────────────────────────────────────────────────────────

def require_role(*allowed_roles: str) -> Callable:
    async def _check(actor: CurrentActor = Depends(get_current_actor)) -> CurrentActor:
        if actor.role_name not in allowed_roles:
            raise HTTPException(
                status_code=403,
                detail=f"Role '{actor.role_name}' not permitted. Required: {list(allowed_roles)}",
            )
        return actor
    return _check


async def get_current_actor_from_token(token: str) -> CurrentActor:
    """Used by SSE endpoint — token comes as query param, no cookie needed."""
    auth_client = AuthHttpClient()
    result = await auth_client.validate_token(token)

    if not result or not result.get("valid"):
        raise HTTPException(status_code=401, detail="Invalid or expired token.")

    return CurrentActor(
        actor_id=result["actor_id"],
        role_name=result["role"],
        email=result.get("email"),
        company_id=result.get("company_id"),
        product_tiers=result.get("product_tiers") or {},
    )


# ── Composite shorthand dependencies ─────────────────────────────────────────

def require_internal() -> Callable:
    return require_role(ROLE_AGENT, ROLE_TEAM_LEAD, ROLE_ADMIN)

def require_team_lead_or_admin() -> Callable:
    return require_role(ROLE_TEAM_LEAD, ROLE_ADMIN)

def require_admin() -> Callable:
    return require_role(ROLE_ADMIN)

def require_customer() -> Callable:
    return require_role(ROLE_CUSTOMER)