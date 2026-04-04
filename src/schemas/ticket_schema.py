from __future__ import annotations

import uuid
from datetime import datetime
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field
import uuid

# ── Customer — Create Ticket ──────────────────────────────────────────────────

class TicketCreateRequest(BaseModel):
    title:             str           = Field(..., min_length=5, max_length=500)
    description:       str           = Field(..., min_length=10)
    product_id:        uuid.UUID
    customer_severity: str           = Field(
        ...,
        description="Customer-perceived severity: critical | high | medium | low",
        pattern="^(critical|high|medium|low)$",
    )
    environment:       Optional[str] = Field(
        None,
        pattern="^(production|staging|development|other)$",
    )
    source:            Optional[str] = Field(default="web")


class TicketCreateResponse(BaseModel):
    ticket_id:     uuid.UUID
    ticket_number: str
    status:        str
    message:       str = "Your ticket has been received. Our team will reach out shortly."


# ── Customer — View My Tickets ────────────────────────────────────────────────

class CustomerTicketListItem(BaseModel):
    """Lightweight row returned in the customer's ticket list."""
    model_config = ConfigDict(from_attributes=True)

    id:                uuid.UUID
    ticket_number:     str
    title:             Optional[str]
    status:            str
    customer_severity: Optional[str]   = Field(None, alias="customer_priority")
    priority:          Optional[str]
    severity:          Optional[str]
    product_id:        Optional[uuid.UUID]
    environment:       Optional[str]
    source:            Optional[str]
    sla_response_due:  Optional[datetime]
    sla_resolve_due:   Optional[datetime]
    created_at:        datetime
    resolved_at:       Optional[datetime]
    closed_at:         Optional[datetime]


class CustomerTicketDetailResponse(CustomerTicketListItem):
    """Full detail view for a single customer ticket."""
    assigned_to:              Optional[uuid.UUID] 
    description:              Optional[str]
    priority_overridden:      bool
    override_reason:          Optional[str]
    tier_snapshot:            Optional[str]
    first_response_at:        Optional[datetime]
    reopen_count:             int
    sla_breached_at:          Optional[datetime]
    response_sla_breached_at: Optional[datetime]


# ── Agent / TL — Ticket view ──────────────────────────────────────────────────

class TicketQueueItem(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id:               uuid.UUID
    ticket_number:    str
    title:            Optional[str]
    status:           str
    severity:         Optional[str]
    priority:         Optional[str]
    product_id:       Optional[uuid.UUID]
    team_id:          Optional[uuid.UUID]
    assigned_to:      Optional[uuid.UUID]
    tier_snapshot:    Optional[str]
    sla_response_due: Optional[datetime]
    sla_resolve_due:  Optional[datetime]
    created_at:       datetime


class TicketDetailResponse(BaseModel):
    id:                      uuid.UUID
    ticket_number:           str
    title:                   Optional[str]         = None
    description:             Optional[str]         = None
    product_id: Optional[uuid.UUID]
    status:                  str
    severity:                Optional[str]         = None
    priority:                Optional[str]         = None
    source:                  Optional[str]         = None
    environment:             Optional[str]         = None
    customer_id:             uuid.UUID
    assigned_to:             Optional[uuid.UUID]   = None
    team_id:                 Optional[uuid.UUID]   = None
    tier_snapshot:           Optional[str]         = None
    customer_priority:       Optional[str]         = None
    priority_overridden:     bool                  = False
    override_reason:         Optional[str]         = None
    ai_draft:                Optional[str]         = None
    reopen_count:            int                   = 0
    first_response_at:       Optional[datetime]    = None
    resolved_at:             Optional[datetime]    = None
    closed_at:               Optional[datetime]    = None
    sla_response_due:        Optional[datetime]    = None
    sla_resolve_due:         Optional[datetime]    = None
    response_sla_breached_at: Optional[datetime]  = None
    sla_breached_at:         Optional[datetime]    = None
    # on-hold SLA fields
    on_hold_started_at:              Optional[datetime] = None
    on_hold_duration_accumulated:    int                = 0
    created_at:              datetime
    updated_at:              datetime

    model_config = {"from_attributes": True}


class UnassignRequest(BaseModel):
    justification: str


# ── Notification ──────────────────────────────────────────────────────────────

class NotificationResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id:          uuid.UUID
    type:        Optional[str]
    title:       Optional[str]
    message:     Optional[str]
    ticket_id:   Optional[uuid.UUID]
    is_read:     bool
    is_internal: bool
    created_at:  datetime


class StatusUpdateRequest(BaseModel):
    status: str
    reason: Optional[str] = None


# ── SLA Breach Justification ──────────────────────────────────────────────────

class BreachJustificationRequest(BaseModel):
    """
    Agent submits this when response or resolution SLA has been breached.
    breach_type: "response" | "resolution"
    justification: min 30 chars — forces meaningful explanation
    """
    breach_type:   str = Field(
        ...,
        pattern="^(response|resolution)$",
        description="Which SLA was breached: response or resolution",
    )
    justification: str = Field(
        ...,
        min_length=30,
        description="Minimum 30 characters — explain what caused the breach",
    )


class BreachJustificationResponse(BaseModel):
    """
    Single breach justification entry returned to TL.
    Derived from Conversation rows prefixed with [BREACH_JUSTIFICATION].
    """
    conversation_id: uuid.UUID
    ticket_id:       uuid.UUID
    agent_id:        uuid.UUID
    breach_type:     str
    justification:   str
    submitted_at:    datetime

class EnhanceRequest(BaseModel):
    draft: str = Field(..., min_length=1, description="The agent's draft text to enhance")
    mode:  str = Field("reply", pattern="^(reply|internal)$", description="'reply' or 'internal'")
 
 
class EnhanceResponse(BaseModel):
    enhanced_text:   str
    changes_summary: str

class ChildReplyRequest(BaseModel):
    message: str