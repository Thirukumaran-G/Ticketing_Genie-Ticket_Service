from __future__ import annotations

import uuid
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field


# ── Email Config ──────────────────────────────────────────────────────────────

class EmailConfigUpdateRequest(BaseModel):
    key:       str  = Field(..., examples=["IMAP_USER", "IMAP_PASSWORD"])
    value:     str
    is_secret: bool = False


class EmailConfigResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id:         uuid.UUID
    key:        str
    value:      Optional[str]   # masked "***" for secrets
    is_secret:  bool
    is_active:  bool


# ── SLA Rules ─────────────────────────────────────────────────────────────────

class SLARuleCreateRequest(BaseModel):
    tier_id:             uuid.UUID
    priority:            str = Field(..., examples=["P0", "P1", "P2", "P3"])
    response_time_min:   int = Field(..., gt=0)
    resolution_time_min: int = Field(..., gt=0)


class SLARuleResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id:                  uuid.UUID
    tier_id:             uuid.UUID
    priority:            str
    response_time_min:   int
    resolution_time_min: int
    is_active:           bool


# ── Severity / Priority Map ───────────────────────────────────────────────────

class SeverityPriorityMapCreateRequest(BaseModel):
    severity:         str       = Field(..., examples=["critical", "high", "medium", "low"])
    tier_id:          uuid.UUID
    derived_priority: str       = Field(..., examples=["P0", "P1", "P2", "P3"])


class SeverityPriorityMapResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id:               uuid.UUID
    severity:         str
    tier_id:          uuid.UUID
    derived_priority: str


# ── Keyword Rules ─────────────────────────────────────────────────────────────

class KeywordRuleCreateRequest(BaseModel):
    keyword:  str = Field(..., max_length=100)
    severity: str = Field(..., examples=["critical", "high", "medium", "low"])


class KeywordRuleUpdateRequest(BaseModel):
    keyword:   Optional[str]  = Field(None, max_length=100)
    severity:  Optional[str]  = None
    is_active: Optional[bool] = None


class KeywordRuleResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id:        uuid.UUID
    keyword:   str
    severity:  str
    is_active: bool


# ── Product Config ────────────────────────────────────────────────────────────

class ProductConfigUpsertRequest(BaseModel):
    min_severity:     Optional[str]  = Field(
        None, examples=["critical", "high", "medium", "low"]
    )
    default_escalate: bool = Field(False)


class ProductConfigResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id:               uuid.UUID
    product_id:       uuid.UUID
    min_severity:     Optional[str]
    default_escalate: bool
    is_active:        bool


# ── Teams ─────────────────────────────────────────────────────────────────────

class TeamCreateRequest(BaseModel):
    name:         str                  = Field(..., max_length=255)
    product_id:   uuid.UUID
    team_lead_id: Optional[uuid.UUID]  = None


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
        description="Free-text skill description — auto-embedded.",
    )


class TeamMemberResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id:         uuid.UUID
    team_id:    uuid.UUID
    user_id:    uuid.UUID
    experience: Optional[int]
    skills:     Optional[dict]
    is_active:  bool


# ── Reports ───────────────────────────────────────────────────────────────────

class OpenTicketsByPriorityResponse(BaseModel):
    open_tickets_by_priority: list[dict]


class SLABreachesByDayResponse(BaseModel):
    sla_breaches_by_day: list[dict]


class FirstResponseTimeResponse(BaseModel):
    average_first_response_time_min: float
    median_first_response_time_min:  float


class TicketsByProductResponse(BaseModel):
    tickets_by_product: list[dict]