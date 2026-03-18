"""
Schemas for conversations and attachments.
src/schemas/conversation_schema.py

Add these to your existing ticket_schema.py or keep as a separate module.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Optional

from pydantic import BaseModel, ConfigDict


# ── Conversation ───────────────────────────────────────────────────────────────

class ConversationItem(BaseModel):
    """A single message in the ticket thread."""

    model_config = ConfigDict(from_attributes=True)

    id:         uuid.UUID
    ticket_id:  uuid.UUID
    author_id:  uuid.UUID
    author_type: str          # "customer" | "agent"
    content:    str
    is_internal: bool
    is_ai_draft: bool
    created_at: datetime
    updated_at: datetime


class CustomerReplyRequest(BaseModel):
    message: str              # min length validated at route level


class AgentReplyRequest(BaseModel):
    content:     str
    is_internal: bool = False  # True → internal note, hidden from customer


# ── Attachment ─────────────────────────────────────────────────────────────────

class AttachmentItem(BaseModel):
    """Metadata for a single uploaded attachment."""

    model_config = ConfigDict(from_attributes=True)

    id:          uuid.UUID
    ticket_id:   uuid.UUID
    file_name:   str
    file_size:   Optional[int]
    mime_type:   Optional[str]
    uploaded_by: Optional[uuid.UUID]
    created_at:  datetime


# ── Combined thread response ───────────────────────────────────────────────────

class TicketThreadResponse(BaseModel):
    """Full thread + attachments returned together in one response."""

    conversations: list[ConversationItem]
    attachments:   list[AttachmentItem]