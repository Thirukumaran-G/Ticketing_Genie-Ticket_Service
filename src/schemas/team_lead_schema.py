"""
Team Lead schemas.
src/schemas/team_lead_schema.py
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator


# ── Ticket manual assign ──────────────────────────────────────────────────────

class ManualAssignRequest(BaseModel):
    agent_user_id: uuid.UUID


# ── Ticket status update ──────────────────────────────────────────────────────

class TicketStatusUpdateRequest(BaseModel):
    status: str


# ── Team overview ─────────────────────────────────────────────────────────────

class AgentWorkloadItem(BaseModel):
    user_id:      uuid.UUID
    full_name:    Optional[str] = None   # resolved from auth-service
    experience:   Optional[int] = None
    open_tickets: int
    skills:       Optional[dict] = None


class TeamOverviewResponse(BaseModel):
    team_id:          uuid.UUID
    team_name:        str
    product_id:       uuid.UUID
    unassigned_count: int
    agents:           list[AgentWorkloadItem]


# ── Ticket detail (TL view) ───────────────────────────────────────────────────

class TLTicketDetailResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id:                           uuid.UUID
    ticket_number:                str
    title:                        Optional[str]
    description:                  Optional[str]
    status:                       str
    severity:                     Optional[str]
    priority:                     Optional[str]
    product_id:                   Optional[uuid.UUID]
    team_id:                      Optional[uuid.UUID]
    assigned_to:                  Optional[uuid.UUID]
    customer_id:                  uuid.UUID
    company_id:                   Optional[uuid.UUID]
    tier_snapshot:                Optional[str]
    source:                       Optional[str]
    environment:                  Optional[str]
    customer_priority:            Optional[str]
    priority_overridden:          bool
    override_reason:              Optional[str]
    ai_draft:                     Optional[str]
    sla_response_due:             Optional[datetime]
    sla_resolve_due:              Optional[datetime]
    first_response_at:            Optional[datetime]
    response_sla_breached_at:     Optional[datetime]
    sla_breached_at:              Optional[datetime]
    on_hold_started_at:           Optional[datetime]
    on_hold_duration_accumulated: int = 0
    reopen_count:                 int
    resolved_at:                  Optional[datetime]
    closed_at:                    Optional[datetime]
    created_at:                   datetime
    updated_at:                   datetime


# ── Thread schemas ────────────────────────────────────────────────────────────

class ConversationItem(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id:          str
    ticket_id:   str
    author_id:   str
    author_type: str
    content:     str
    is_internal: bool
    created_at:  datetime
    updated_at:  datetime

    @model_validator(mode="before")
    @classmethod
    def coerce_uuids(cls, values: Any) -> Any:
        if not isinstance(values, dict):
            return {
                "id":          str(values.id),
                "ticket_id":   str(values.ticket_id),
                "author_id":   str(values.author_id),
                "author_type": values.author_type,
                "content":     values.content,
                "is_internal": values.is_internal,
                "created_at":  values.created_at,
                "updated_at":  values.updated_at,
            }
        for field in ("id", "ticket_id", "author_id"):
            if field in values and values[field] is not None:
                values[field] = str(values[field])
        return values


class AttachmentItem(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id:         str
    ticket_id:  str
    file_name:  str
    file_size:  Optional[int] = None
    mime_type:  Optional[str] = None
    created_at: datetime

    @model_validator(mode="before")
    @classmethod
    def coerce_uuids(cls, values: Any) -> Any:
        if not isinstance(values, dict):
            return {
                "id":         str(values.id),
                "ticket_id":  str(values.ticket_id),
                "file_name":  values.file_name,
                "file_size":  values.file_size,
                "mime_type":  values.mime_type,
                "created_at": values.created_at,
            }
        for field in ("id", "ticket_id"):
            if field in values and values[field] is not None:
                values[field] = str(values[field])
        return values


class TLTicketThreadResponse(BaseModel):
    conversations: list[ConversationItem]
    attachments:   list[AttachmentItem]


class TLInternalNoteRequest(BaseModel):
    content: str


# ── Notification Templates ────────────────────────────────────────────────────

class NotificationTemplateResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id:         uuid.UUID
    key:        str
    name:       str
    subject:    str
    body:       str
    variables:  list[str]
    is_active:  bool
    updated_by: Optional[uuid.UUID]
    updated_at: datetime
    created_at: datetime


class NotificationTemplateUpdateRequest(BaseModel):
    name:      Optional[str]  = Field(None, min_length=3, max_length=255)
    subject:   Optional[str]  = Field(None, min_length=5)
    body:      Optional[str]  = Field(None, min_length=10)
    is_active: Optional[bool] = None


# ── Send Apology ──────────────────────────────────────────────────────────────

class SendApologyRequest(BaseModel):
    template_id:    uuid.UUID
    custom_message: Optional[str] = Field(
        None,
        max_length=1000,
        description="Custom message injected into {custom_message} variable",
    )
    commit_time:    Optional[str] = Field(
        None,
        max_length=100,
        description="Commitment time string e.g. 'within 2 hours'",
    )


class SendApologyResponse(BaseModel):
    sent:         bool
    channel:      str
    ticket_id:    uuid.UUID
    template_key: str
    message:      str


# ── Similar Ticket Groups ─────────────────────────────────────────────────────

class SimilarTicketMemberResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    ticket_id:        uuid.UUID
    ticket_number:    str
    title:            Optional[str]
    status:           str
    priority:         Optional[str]
    severity:         Optional[str]
    similarity_score: float
    added_at:         datetime


class SimilarTicketGroupResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id:                uuid.UUID
    name:              Optional[str]
    confirmed_by_lead: bool
    confirmed_at:      Optional[datetime]
    confirmed_by:      Optional[uuid.UUID]
    member_count:      int
    members:           list[SimilarTicketMemberResponse]
    created_at:        datetime
    updated_at:        datetime


class ConfirmGroupRequest(BaseModel):
    name: Optional[str] = Field(
        None,
        max_length=255,
        description="Optional label for the group e.g. 'Payment gateway outage'",
    )


class AddGroupMemberRequest(BaseModel):
    ticket_id:        uuid.UUID
    similarity_score: float = Field(default=0.0, ge=0.0, le=1.0)


class BulkAssignRequest(BaseModel):
    agent_user_id:    uuid.UUID
    internal_message: str = Field(
        ...,
        min_length=10,
        description="Message posted as internal note on every assigned ticket",
    )


class BulkResolveRequest(BaseModel):
    resolution_message: str = Field(
        ...,
        min_length=10,
        description="Message sent to every affected customer",
    )


class BulkActionResponse(BaseModel):
    assigned:       Optional[int] = None
    resolved:       Optional[int] = None
    skipped:        int
    ticket_numbers: list[str]

class RerouteTicketRequest(BaseModel):
    target_team_id: uuid.UUID

class UpdateAgentSkillRequest(BaseModel):
    skill_text: str

class SetParentRequest(BaseModel):
    parent_ticket_id: str

class AssignParentRequest(BaseModel):
    agent_id: str
    note: str

class BroadcastRequest(BaseModel):
    message: str

class GroupResolveRequest(BaseModel):
    resolution_message: str