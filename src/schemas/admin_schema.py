"""
Pydantic schemas for admin endpoints.
Kept in admin_schema.py — separate from ticket_schema.py to avoid coupling.
"""

from __future__ import annotations

import uuid
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field


# ── Email Config ──────────────────────────────────────────────────────────────

class EmailConfigUpdateRequest(BaseModel):
    """
    Admin-supplied keys only.
    System-level keys (IMAP_HOST, IMAP_PORT, IMAP_MAILBOX) are fixed in .env —
    only IMAP_USER and IMAP_PASSWORD are configurable via this endpoint.
    """
    key: str = Field(..., examples=["IMAP_USER", "IMAP_PASSWORD"])
    value: str
    is_secret: bool = False


class EmailConfigResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    key: str
    # value is masked for secrets
    value: Optional[str]
    is_secret: bool
    is_active: bool


# ── SLA Rules ─────────────────────────────────────────────────────────────────

class SLARuleCreateRequest(BaseModel):
    tier_id: uuid.UUID
    priority: str = Field(..., examples=["P0", "P1", "P2", "P3"])
    response_time_min: int = Field(..., gt=0)
    resolution_time_min: int = Field(..., gt=0)


class SLARuleResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    tier_id: uuid.UUID
    priority: str
    response_time_min: int
    resolution_time_min: int
    is_active: bool


# ── Severity / Priority Map ───────────────────────────────────────────────────

class SeverityPriorityMapCreateRequest(BaseModel):
    severity: str = Field(..., examples=["critical", "high", "medium", "low"])
    tier_id: uuid.UUID
    derived_priority: str = Field(..., examples=["P0", "P1", "P2", "P3"])


class SeverityPriorityMapResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    severity: str
    tier_id: uuid.UUID
    derived_priority: str


# ── Keyword Rules ─────────────────────────────────────────────────────────────

class KeywordRuleCreateRequest(BaseModel):
    keyword: str = Field(..., max_length=100)
    severity: str = Field(..., examples=["critical", "high", "medium", "low"])


class KeywordRuleUpdateRequest(BaseModel):
    keyword: Optional[str] = Field(None, max_length=100)
    severity: Optional[str] = None
    is_active: Optional[bool] = None


class KeywordRuleResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    keyword: str
    severity: str
    is_active: bool


# ── Product Config ────────────────────────────────────────────────────────────

class ProductConfigUpsertRequest(BaseModel):
    """
    Admin sets per-product ticket behaviour.
    product_id comes from the URL path — not in the body.
    Both fields are optional so admin can update just one at a time.
    """
    min_severity: Optional[str] = Field(
        None,
        examples=["critical", "high", "medium", "low"],
        description="Minimum severity level required to open a ticket for this product.",
    )
    default_escalate: bool = Field(
        False,
        description="When True, all new tickets for this product are auto-escalated.",
    )


class ProductConfigResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    product_id: uuid.UUID
    min_severity: Optional[str]
    default_escalate: bool
    is_active: bool

class TeamCreateRequest(BaseModel):
    name:            str            = Field(..., max_length=255)
    product_id:      uuid.UUID
    team_lead_id:    Optional[uuid.UUID] = None


class TeamResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id:           uuid.UUID
    name:         str
    product_id:   uuid.UUID
    team_lead_id: Optional[uuid.UUID]
    is_active:    bool


class TeamMemberAddRequest(BaseModel):
    user_id:    uuid.UUID
    experience: Optional[int] = Field(None, ge=0)
    skill_text: Optional[str] = Field(
        None,
        description="Skill description paragraph — will be embedded automatically.",
    )


class TeamMemberResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id:         uuid.UUID
    team_id:    uuid.UUID
    user_id:    uuid.UUID
    experience: Optional[int]
    skills:     Optional[dict]
    is_active:  bool