from __future__ import annotations

from collections.abc import Callable
from typing import Any, Dict, Optional

from fastapi import Depends, HTTPException, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from src.observability.logging.logger import get_logger

from src.utils.jwt import decode_token

logger = get_logger(__name__)
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
    

def _extract_token(
    request:     Request,
    credentials: Optional[HTTPAuthorizationCredentials],
) -> str:
    if credentials and credentials.credentials:
        return credentials.credentials

    cookie_token = request.cookies.get("access_token")
    if cookie_token:
        return cookie_token

    raise HTTPException(status_code=401, detail="Missing authorization token.")


def _validate_token_local(token: str) -> CurrentActor:
    """
    Decode and validate the JWT locally using the shared secret/public key.
    Raises 401 if the token is invalid, expired, or not an access token.
    """
    try:
        payload = decode_token(token)
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid or expired token.")

    if payload.get("type") != "access":
        raise HTTPException(status_code=401, detail="Access token required.")

    return CurrentActor(
        actor_id=str(payload["sub"]),
        role_name=payload["role"],
        email=payload.get("email"),
        company_id=payload.get("company_id"),
        product_tiers=payload.get("product_tiers") or {},
    )


async def get_current_actor(
    request:     Request,
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(_bearer),
) -> CurrentActor:
    token = _extract_token(request, credentials)
    return _validate_token_local(token)


async def get_current_actor_from_token(token: str) -> CurrentActor:
    """Used by SSE endpoint — token comes as query param, no cookie needed."""
    return _validate_token_local(token)


# ── Role guards ───────────────────────────────────────────────────────────────

def require_role(*allowed_roles: str) -> Callable:
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
    return require_role(ROLE_AGENT, ROLE_TEAM_LEAD, ROLE_ADMIN)

def require_team_lead_or_admin() -> Callable:
    return require_role(ROLE_TEAM_LEAD, ROLE_ADMIN)

def require_admin() -> Callable:
    return require_role(ROLE_ADMIN)

def require_customer() -> Callable:
    return require_role(ROLE_CUSTOMER)