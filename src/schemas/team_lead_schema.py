"""
Team Lead schemas.
src/schemas/team_lead_schema.py
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Optional

from pydantic import BaseModel, ConfigDict


# ── Ticket manual assign ──────────────────────────────────────────────────────

class ManualAssignRequest(BaseModel):
    agent_user_id: uuid.UUID


# ── Ticket status update ──────────────────────────────────────────────────────

class TicketStatusUpdateRequest(BaseModel):
    status: str   # open | in_progress | resolved | closed


# ── Team overview ─────────────────────────────────────────────────────────────

class AgentWorkloadItem(BaseModel):
    user_id:      uuid.UUID
    experience:   Optional[int]
    open_tickets: int
    skills:       Optional[dict]


class TeamOverviewResponse(BaseModel):
    team_id:           uuid.UUID
    team_name:         str
    product_id:        uuid.UUID
    unassigned_count:  int
    agents:            list[AgentWorkloadItem]


# ── Ticket detail (TL view — same as agent but always full) ───────────────────

class TLTicketDetailResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id:                    uuid.UUID
    ticket_number:         str
    title:                 Optional[str]
    description:           Optional[str]
    status:                str
    severity:              Optional[str]
    priority:              Optional[str]
    product_id:            Optional[uuid.UUID]
    team_id:               Optional[uuid.UUID]
    assigned_to:           Optional[uuid.UUID]
    customer_id:           uuid.UUID
    company_id:            Optional[uuid.UUID]
    tier_snapshot:         Optional[str]
    source:                Optional[str]
    environment:           Optional[str]
    customer_priority:     Optional[str]
    priority_overridden:   bool
    override_reason:       Optional[str]
    ai_draft:              Optional[str]
    sla_response_due:      Optional[datetime]
    sla_resolve_due:       Optional[datetime]
    first_response_at:     Optional[datetime]
    response_sla_breached_at: Optional[datetime]
    sla_breached_at:       Optional[datetime]
    reopen_count:          int
    resolved_at:           Optional[datetime]
    closed_at:             Optional[datetime]
    created_at:            datetime
    updated_at:            datetime

class ConversationItem(BaseModel):
    id: str
    ticket_id: str
    author_id: str
    author_type: str
    content: str
    is_internal: bool
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class AttachmentItem(BaseModel):
    id: str
    ticket_id: str
    file_name: str
    file_size: Optional[int] = None
    mime_type: Optional[str] = None
    created_at: datetime

    model_config = {"from_attributes": True}


class TLTicketThreadResponse(BaseModel):
    conversations: list[ConversationItem]
    attachments: list[AttachmentItem]


class TLInternalNoteRequest(BaseModel):
    content: str