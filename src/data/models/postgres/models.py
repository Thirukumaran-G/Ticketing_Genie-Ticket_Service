from __future__ import annotations

import uuid
from datetime import datetime
from typing import ClassVar, Optional

import uuid6
from sqlalchemy import (
    Boolean, DateTime, ForeignKey, Index, Integer,
    String, Text, UniqueConstraint, func
)
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from pgvector.sqlalchemy import Vector

from src.constants import TicketStatus, NotificationStatus, EmailProcessingStatus


class Base(DeclarativeBase):
    pass


class ProductConfig(Base):
    __tablename__ = "product_config"
    __table_args__: ClassVar[dict] = {"schema": "ticket"}

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid6.uuid7
    )
    product_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), nullable=False, unique=True, index=True
    )
    default_escalate: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False
    )
    min_severity: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    updated_by: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True), nullable=True
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )


class SeverityPriorityMap(Base):
    __tablename__ = "severity_priority_map"
    __table_args__: ClassVar[tuple] = (
        UniqueConstraint("severity", "tier_id", name="uq_severity_tier"),
        {"schema": "ticket"},
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid6.uuid7
    )
    severity: Mapped[str] = mapped_column(String(20), nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    tier_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), nullable=False
    )
    derived_priority: Mapped[str] = mapped_column(String(10), nullable=False)


class SLARule(Base):
    __tablename__ = "sla_rule"
    __table_args__: ClassVar[tuple] = (
        UniqueConstraint("tier_id", "priority", name="uq_sla_tier_priority"),
        {"schema": "ticket"},
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid6.uuid7
    )
    tier_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), nullable=False
    )
    priority: Mapped[str] = mapped_column(String(10), nullable=False)
    response_time_min: Mapped[int] = mapped_column(Integer, nullable=False)
    resolution_time_min: Mapped[int] = mapped_column(Integer, nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_by: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )


class KeywordRule(Base):
    __tablename__ = "keyword_rule"
    __table_args__: ClassVar[tuple] = (
        UniqueConstraint("keyword", "product_id", name="uq_keyword_product"),
        Index("ix_keyword_rule_product_id", "product_id"),
        {"schema": "ticket"},
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid6.uuid7
    )
    keyword: Mapped[str] = mapped_column(String(100), nullable=False)
    severity: Mapped[str] = mapped_column(String(20), nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_by: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    # index=True removed — declared explicitly above in __table_args__
    product_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True), nullable=True
    )


class EmailConfig(Base):
    __tablename__ = "email_config"
    __table_args__: ClassVar[dict] = {"schema": "ticket"}

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid6.uuid7
    )
    key: Mapped[str] = mapped_column(String(100), nullable=False, unique=True)
    value: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    is_secret: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    updated_by: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True), nullable=True
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )


class NotificationTemplate(Base):
    __tablename__ = "notification_template"
    __table_args__: ClassVar[dict] = {"schema": "ticket"}

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid6.uuid7
    )
    # unique=True on the column already creates a unique index — no need for
    # a separate Index() in __table_args__
    key: Mapped[str] = mapped_column(
        String(100), nullable=False, unique=True, index=True,
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    subject: Mapped[str] = mapped_column(Text, nullable=False)
    body: Mapped[str] = mapped_column(Text, nullable=False)
    variables: Mapped[list] = mapped_column(JSONB, nullable=False, default=list)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    updated_by: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class Team(Base):
    __tablename__ = "team"
    __table_args__: ClassVar[tuple] = (
        # One active team per name per product.
        # Partial — deactivated teams never block name reuse.
        Index(
            "uq_team_name_product_active",
            "name", "product_id",
            unique=True,
            postgresql_where="is_active = TRUE",
        ),
        # General lookup indexes — declared here, NOT via index=True on columns
        Index("ix_team_product_id",    "product_id"),
        Index("ix_team_team_lead_id",  "team_lead_id"),
        Index("ix_team_is_active",     "is_active"),
        {"schema": "ticket"},
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid6.uuid7
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    # index=False — covered by ix_team_product_id above
    product_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), nullable=False
    )
    team_lead_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True), nullable=True
    )
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    members: Mapped[list["TeamMember"]] = relationship(
        "TeamMember", back_populates="team"
    )


class TeamMember(Base):
    __tablename__ = "team_member"
    __table_args__: ClassVar[tuple] = (
        # Prevents duplicate (team_id, user_id) — was causing the 500 error
        UniqueConstraint("team_id", "user_id", name="uq_team_member_team_user"),
        # General lookup indexes — declared here, NOT via index=True on columns
        Index("ix_team_member_team_id",     "team_id"),
        Index("ix_team_member_user_id",     "user_id"),
        Index("ix_team_member_team_active", "team_id", "is_active"),
        {"schema": "ticket"},
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid6.uuid7
    )
    # index=False — covered by ix_team_member_team_id above
    team_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("ticket.team.id", ondelete="CASCADE"),
        nullable=False
    )
    # index=False — covered by ix_team_member_user_id above
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), nullable=False
    )
    skills: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    skill_embedding: Mapped[Optional[list[float]]] = mapped_column(
        Vector(384), nullable=True
    )
    experience: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    team: Mapped["Team"] = relationship("Team", back_populates="members")


class Ticket(Base):
    __tablename__ = "ticket"
    __table_args__: ClassVar[dict] = {"schema": "ticket"}

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid6.uuid7
    )
    ticket_number: Mapped[str] = mapped_column(
        String(50), nullable=False, unique=True, index=True
    )
    title: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(
        String(20), nullable=False, default=TicketStatus.NEW.value
    )
    severity: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    priority: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)
    source: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    environment: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    product_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True), nullable=True, index=True
    )
    team_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("ticket.team.id", ondelete="SET NULL"),
        nullable=True
    )
    customer_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), nullable=False, index=True
    )
    company_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True), nullable=True, index=True
    )
    assigned_to: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True), nullable=True
    )
    tier_snapshot: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    email_message_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    customer_priority: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)
    priority_overridden: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    override_reason: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    ai_draft: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    ticket_embedding: Mapped[Optional[list[float]]] = mapped_column(Vector(384), nullable=True)
    similar_ticket_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("ticket.ticket.id", ondelete="SET NULL"),
        nullable=True
    )
    sla_response_due: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    sla_resolve_due: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    first_response_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    response_sla_breached_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    sla_breached_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    on_hold_started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    on_hold_duration_accumulated: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    response_sla_warned_pct: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    resolution_sla_warned_pct: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    escalation_notified_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    reopen_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    resolved_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    resolved_by: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), nullable=True)
    closed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    closed_by: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    team: Mapped[Optional["Team"]] = relationship("Team")
    conversations: Mapped[list["Conversation"]] = relationship(
        "Conversation", back_populates="ticket", cascade="all, delete-orphan"
    )
    attachments: Mapped[list["Attachment"]] = relationship(
        "Attachment", back_populates="ticket", cascade="all, delete-orphan"
    )
    notifications: Mapped[list["Notification"]] = relationship(
        "Notification", back_populates="ticket", cascade="all, delete-orphan"
    )
    email_processings: Mapped[list["EmailProcessing"]] = relationship(
        "EmailProcessing", back_populates="ticket"
    )


class Conversation(Base):
    __tablename__ = "conversation"
    __table_args__: ClassVar[dict] = {"schema": "ticket"}

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid6.uuid7)
    ticket_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("ticket.ticket.id", ondelete="CASCADE"),
        nullable=False, index=True
    )
    author_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    author_type: Mapped[str] = mapped_column(String(20), nullable=False)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    is_internal: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    is_ai_draft: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    parent_comment_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("ticket.conversation.id", ondelete="SET NULL"),
        nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )
    ticket: Mapped["Ticket"] = relationship("Ticket", back_populates="conversations")


class Attachment(Base):
    __tablename__ = "attachment"
    __table_args__: ClassVar[dict] = {"schema": "ticket"}

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid6.uuid7)
    ticket_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("ticket.ticket.id", ondelete="CASCADE"),
        nullable=False, index=True
    )
    file_name: Mapped[str] = mapped_column(String(255), nullable=False)
    file_path: Mapped[str] = mapped_column(Text, nullable=False)
    file_size: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    mime_type: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    uploaded_by: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    ticket: Mapped["Ticket"] = relationship("Ticket", back_populates="attachments")


class Notification(Base):
    __tablename__ = "notification"
    __table_args__: ClassVar[tuple] = (
        Index("ix_notification_email_worker", "email_sent", "channel",
              postgresql_where="email_sent = FALSE"),
        Index("ix_notification_status_channel", "status", "channel"),
        Index("ix_notification_recipient_unread", "recipient_id", "is_read",
              postgresql_where="is_read = FALSE"),
        {"schema": "ticket"},
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid6.uuid7)
    recipient_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False, index=True)
    ticket_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("ticket.ticket.id", ondelete="CASCADE"),
        nullable=True
    )
    channel: Mapped[str] = mapped_column(String(20), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default=NotificationStatus.PENDING.value)
    type: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    title: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    is_read: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    read_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    is_internal: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    email_sent: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    email_sent_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    email_error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    ticket: Mapped[Optional["Ticket"]] = relationship("Ticket", back_populates="notifications")


class EmailProcessing(Base):
    __tablename__ = "email_processing"
    __table_args__: ClassVar[dict] = {"schema": "ticket"}

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid6.uuid7)
    message_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True, unique=True, index=True)
    in_reply_to: Mapped[Optional[str]] = mapped_column(String(255), nullable=True, index=True)
    references: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    from_email: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    subject: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    received_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    processed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default=EmailProcessingStatus.PENDING.value)
    ticket_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("ticket.ticket.id", ondelete="SET NULL"),
        nullable=True
    )
    is_thread_reply: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    failure_reason: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    retry_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    admin_notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    pass_to: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), nullable=True)
    resolved_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    resolved_by: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    ticket: Mapped[Optional["Ticket"]] = relationship("Ticket", back_populates="email_processings")


class SimilarTicketGroup(Base):
    __tablename__ = "similar_ticket_group"
    __table_args__: ClassVar[dict] = {"schema": "ticket"}

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid6.uuid7)
    name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    confirmed_by_lead: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    confirmed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    confirmed_by: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )
    members: Mapped[list["SimilarTicketGroupMember"]] = relationship(
        "SimilarTicketGroupMember", back_populates="group", cascade="all, delete-orphan",
    )


class SimilarTicketGroupMember(Base):
    __tablename__ = "similar_ticket_group_member"
    __table_args__: ClassVar[tuple] = (
        UniqueConstraint("group_id", "ticket_id", name="uq_group_ticket"),
        {"schema": "ticket"},
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid6.uuid7)
    group_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("ticket.similar_ticket_group.id", ondelete="CASCADE"),
        nullable=False, index=True
    )
    ticket_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("ticket.ticket.id", ondelete="CASCADE"),
        nullable=False, index=True
    )
    similarity_score: Mapped[float] = mapped_column(nullable=False, default=0.0)
    added_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    group: Mapped["SimilarTicketGroup"] = relationship("SimilarTicketGroup", back_populates="members")
    ticket: Mapped["Ticket"] = relationship("Ticket")