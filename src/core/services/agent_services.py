"""
Agent service.
src/core/services/agent_services.py
"""
from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timezone, timedelta
from typing import Optional

from fastapi import BackgroundTasks
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

        # Matches full status machine — no 'open', includes 'assigned' and 'reopened'
from src.data.models.postgres.models import Conversation, Notification, Team, Ticket
from src.data.repositories.ticket_repository import TicketRepository
from src.core.services.notification_service import NotificationService
from src.observability.logging.logger import get_logger
from src.schemas.ticket_schema import BreachJustificationResponse

logger = get_logger(__name__)

# ── Status machine ─────────────────────────────────────────────────────────────

VALID_TRANSITIONS: dict[str, set[str]] = {
    "new":          {"acknowledged"},
    "acknowledged": {"assigned"},
    "assigned":     {"in_progress", "on_hold"},
    "in_progress":  {"on_hold", "resolved"},
    "on_hold":      {"in_progress", "resolved"},
    "resolved":     {"closed", "reopened"},
    "closed":       {"reopened"},
    "reopened":     {"in_progress", "on_hold"},
}

AGENT_ALLOWED_STATUSES = {"in_progress", "on_hold", "resolved"}

_BREACH_PREFIX = "[BREACH_JUSTIFICATION:"


class AgentService:

    def __init__(
        self,
        session:          AsyncSession,
        background_tasks: Optional[BackgroundTasks] = None,
    ) -> None:
        self._session          = session
        self._repo             = TicketRepository(session)
        self._notif_svc        = NotificationService(session, background_tasks)

    # ── Queue ──────────────────────────────────────────────────────────────────

    async def get_agent_queue(self, agent_id: str) -> list[Ticket]:
        try:
            tickets = await self._repo.get_agent_queue(agent_id)
        except Exception as exc:
            logger.error("agent_queue_fetch_failed", agent_id=agent_id, error=str(exc))
            raise
        logger.info("agent_queue_fetched", agent_id=agent_id, count=len(tickets))
        return tickets

    async def get_team_lead_queue(self, team_id: str) -> list[Ticket]:
        try:
            tickets = await self._repo.get_team_lead_queue(team_id)
        except Exception as exc:
            logger.error("team_lead_queue_fetch_failed", team_id=team_id, error=str(exc))
            raise
        logger.info("team_lead_queue_fetched", team_id=team_id, count=len(tickets))
        return tickets

    # ── Single ticket fetch ────────────────────────────────────────────────────

    async def get_ticket_by_id(self, agent_id: str, ticket_id: str) -> Ticket:
        try:
            ticket = await self._repo.get_by_id_and_agent(ticket_id, agent_id)
        except Exception as exc:
            logger.error(
                "agent_ticket_fetch_failed",
                agent_id=agent_id,
                ticket_id=ticket_id,
                error=str(exc),
            )
            raise
        if ticket is None:
            raise ValueError(f"Ticket {ticket_id} not found for agent {agent_id}")
        logger.info("agent_ticket_fetched", agent_id=agent_id, ticket_id=ticket_id)
        return ticket

    # ── Set in_progress ────────────────────────────────────────────────────────

    async def set_in_progress(self, agent_id: str, ticket_id: str) -> None:
        ticket = await self.get_ticket_by_id(agent_id, ticket_id)
        if ticket.status != "assigned":
            return
        await self._repo.update_fields(ticket_id, {"status": "in_progress"})
        await self._session.commit()
        logger.info("ticket_set_in_progress", ticket_id=ticket_id, agent_id=agent_id)

    # ── Status update ──────────────────────────────────────────────────────────

    async def update_ticket_status(
        self,
        agent_id:   str,
        ticket_id:  str,
        new_status: str,
        reason:     Optional[str] = None,
    ) -> Ticket:
        if new_status not in AGENT_ALLOWED_STATUSES:
            raise ValueError(
                f"Agents can only set: {', '.join(sorted(AGENT_ALLOWED_STATUSES))}. "
                f"'{new_status}' is not permitted."
            )

        ticket  = await self.get_ticket_by_id(agent_id, ticket_id)
        current = ticket.status
        allowed = VALID_TRANSITIONS.get(current, set())

        if new_status not in allowed:
            raise ValueError(
                f"Cannot transition from '{current}' to '{new_status}'. "
                f"Allowed: {sorted(allowed) or 'none'}"
            )

        now    = datetime.now(timezone.utc)
        fields: dict = {"status": new_status}

        if new_status == "on_hold":
            fields["on_hold_started_at"] = now

        elif new_status == "in_progress" and current == "on_hold":
            fields = self._resume_from_hold(ticket, now, fields)

        elif new_status == "resolved":
            fields["resolved_at"] = now
            fields["resolved_by"] = agent_id
            if current == "on_hold" and ticket.on_hold_started_at:
                held_mins = int((now - ticket.on_hold_started_at).total_seconds() / 60)
                fields["on_hold_duration_accumulated"] = (
                    (ticket.on_hold_duration_accumulated or 0) + held_mins
                )
                fields["on_hold_started_at"] = None

        await self._repo.update_fields(ticket_id, fields)
        await self._session.commit()
        ticket = await self._repo.get_by_id(ticket_id)

        logger.info(
            "ticket_status_updated",
            ticket_id=ticket_id,
            agent_id=agent_id,
            old_status=current,
            new_status=new_status,
        )

        await self._notify_customer_status_change(
            ticket=ticket,
            old_status=current,
            new_status=new_status,
            reason=reason,
        )
        return ticket

    # ── SLA resume helper ──────────────────────────────────────────────────────

    def _resume_from_hold(self, ticket: Ticket, now: datetime, fields: dict) -> dict:
        if ticket.on_hold_started_at:
            held_mins = int((now - ticket.on_hold_started_at).total_seconds() / 60)
            new_accumulated = (ticket.on_hold_duration_accumulated or 0) + held_mins
            fields["on_hold_duration_accumulated"] = new_accumulated
            fields["on_hold_started_at"]           = None
            if ticket.sla_resolve_due:
                new_resolve_due = ticket.sla_resolve_due + timedelta(minutes=held_mins)
                fields["sla_resolve_due"] = new_resolve_due
                logger.info(
                    "ticket_sla_resumed",
                    ticket_id=str(ticket.id),
                    held_mins=held_mins,
                    new_resolve_due=new_resolve_due.isoformat(),
                )
        else:
            fields["on_hold_started_at"] = None
        return fields

    # ── Unassign ───────────────────────────────────────────────────────────────

    async def unassign_ticket(
        self,
        agent_id:      str,
        ticket_id:     str,
        justification: str,
    ) -> Ticket:
        ticket = await self.get_ticket_by_id(agent_id, ticket_id)

        await self._repo.update_fields(ticket_id, {
            "assigned_to": None,
            "status":      "acknowledged",
        })

        try:
            note = Conversation(
                ticket_id=ticket.id,
                author_id=uuid.UUID(agent_id),
                author_type="agent",
                content=f"[Unassigned] {justification}",
                is_internal=True,
            )
            self._session.add(note)
        except Exception as exc:
            logger.warning(
                "unassign_internal_note_failed",
                ticket_id=ticket_id,
                error=str(exc),
            )

        await self._session.commit()
        ticket = await self._repo.get_by_id(ticket_id)

        logger.info(
            "ticket_unassigned_by_agent",
            ticket_id=ticket_id,
            agent_id=agent_id,
        )

        self._push_unassign_sse_to_team(ticket, agent_id, justification)
        return ticket

    def _push_unassign_sse_to_team(
        self,
        ticket:        Ticket,
        agent_id:      str,
        justification: str,
    ) -> None:
        try:
            if not ticket.team_id:
                return
            from src.core.sse.sse_manager import sse_manager

            async def _push():
                r = await self._session.execute(
                    select(Team).where(Team.id == ticket.team_id)
                )
                team = r.scalar_one_or_none()
                if not team or not team.team_lead_id:
                    return
                await sse_manager.push(
                    str(team.team_lead_id),
                    {
                        "event": "queue_update",
                        "data": {
                            "ticket_id":     str(ticket.id),
                            "ticket_number": ticket.ticket_number,
                            "title":         ticket.title or "",
                            "priority":      ticket.priority or "",
                            "severity":      ticket.severity or "",
                            "status":        ticket.status,
                            "reason":        "agent_unassigned",
                            "justification": justification,
                            "unassigned_by": agent_id,
                        },
                    },
                )
            asyncio.create_task(_push())
        except Exception as exc:
            logger.warning("unassign_sse_push_failed", ticket_id=str(ticket.id), error=str(exc))

    # ── SLA Breach Justification — submit ──────────────────────────────────────

    async def submit_breach_justification(
        self,
        agent_id:      str,
        ticket_id:     str,
        breach_type:   str,
        justification: str,
    ) -> BreachJustificationResponse:
        ticket = await self.get_ticket_by_id(agent_id, ticket_id)

        if breach_type == "response" and not ticket.response_sla_breached_at:
            raise ValueError("Response SLA has not been breached on this ticket.")
        if breach_type == "resolution" and not ticket.sla_breached_at:
            raise ValueError("Resolution SLA has not been breached on this ticket.")

        prefix   = f"{_BREACH_PREFIX}{breach_type}]"
        existing = await self._session.execute(
            select(Conversation).where(
                Conversation.ticket_id == ticket.id,
                Conversation.is_internal.is_(True),
                Conversation.content.like(f"{prefix}%"),
            )
        )
        if existing.scalar_one_or_none():
            raise ValueError(
                f"A {breach_type} breach justification has already been submitted."
            )

        note_content = f"{_BREACH_PREFIX}{breach_type}] {justification}"
        note = Conversation(
            ticket_id=ticket.id,
            author_id=uuid.UUID(agent_id),
            author_type="agent",
            content=note_content,
            is_internal=True,
            is_ai_draft=False,
        )
        self._session.add(note)
        await self._session.flush()

        await self._notify_tl_breach_justification(
            ticket=ticket,
            agent_id=agent_id,
            breach_type=breach_type,
            justification=justification,
        )

        await self._session.commit()

        logger.info(
            "breach_justification_submitted",
            ticket_id=ticket_id,
            agent_id=agent_id,
            breach_type=breach_type,
        )

        return BreachJustificationResponse(
            conversation_id=note.id,
            ticket_id=ticket.id,
            agent_id=uuid.UUID(agent_id),
            breach_type=breach_type,
            justification=justification,
            submitted_at=note.created_at,
        )

    # ── SLA Breach Justification — list ───────────────────────────────────────

    async def get_breach_justifications(
        self,
        ticket_id: str,
    ) -> list[BreachJustificationResponse]:
        result = await self._session.execute(
            select(Conversation).where(
                Conversation.ticket_id == uuid.UUID(ticket_id),
                Conversation.is_internal.is_(True),
                Conversation.content.like(f"{_BREACH_PREFIX}%"),
            ).order_by(Conversation.created_at.asc())
        )
        notes = result.scalars().all()

        justifications: list[BreachJustificationResponse] = []
        for note in notes:
            breach_type, justification_text = _parse_breach_note(note.content)
            if not breach_type:
                continue
            justifications.append(
                BreachJustificationResponse(
                    conversation_id=note.id,
                    ticket_id=note.ticket_id,
                    agent_id=note.author_id,
                    breach_type=breach_type,
                    justification=justification_text,
                    submitted_at=note.created_at,
                )
            )
        return justifications

    # ── Notify TL of breach justification ─────────────────────────────────────

    async def _notify_tl_breach_justification(
        self,
        ticket:        Ticket,
        agent_id:      str,
        breach_type:   str,
        justification: str,
    ) -> None:
        try:
            r = await self._session.execute(
                select(Team).where(Team.id == ticket.team_id)
            )
            team = r.scalar_one_or_none()
            if not team or not team.team_lead_id:
                return

            tl_id   = str(team.team_lead_id)
            title   = f"Breach justification submitted: {ticket.ticket_number}"
            message = (
                f"Agent submitted a {breach_type} SLA breach justification.\n\n"
                f"Ticket: {ticket.ticket_number}\n"
                f"Breach type: {breach_type.title()}\n"
                f"Justification: {justification}"
            )

            await self._notif_svc.notify(
                recipient_id=tl_id,
                ticket=ticket,
                notif_type="sla_breach_justification",
                title=title,
                message=message,
                is_internal=True,
                email_subject=f"[{ticket.ticket_number}] SLA Breach Justification Submitted",
                email_body=(
                    f"A {breach_type} SLA breach justification has been submitted "
                    f"for ticket {ticket.ticket_number}.\n\n"
                    f"Justification: {justification}\n\n"
                    f"— Ticketing Genie"
                ),
            )
        except Exception as exc:
            logger.error(
                "notify_tl_breach_justification_failed",
                ticket_id=str(ticket.id),
                error=str(exc),
            )

    # ── Customer status change notification ───────────────────────────────────

    async def _notify_customer_status_change(
        self,
        ticket:     Ticket,
        old_status: str,
        new_status: str,
        reason:     Optional[str],
    ) -> None:
        try:
            status_label = new_status.replace("_", " ").title()
            title        = f"Ticket {ticket.ticket_number} — {status_label}"
            message      = (
                f"Your ticket {ticket.ticket_number} status has been updated to {status_label}."
            )
            if reason:
                message += f"\n\nNote: {reason}"

            email_body_lines = [
                "Hi,\n",
                "Your support ticket has been updated.\n",
                f"Ticket:     {ticket.ticket_number}",
                f"Title:      {ticket.title or '(no title)'}",
                f"New Status: {status_label}",
            ]
            if reason:
                email_body_lines.append(f"Note:       {reason}")
            email_body_lines.append("\nBest regards,\nTicketing Genie Support Team")

            await self._notif_svc.notify(
                recipient_id=str(ticket.customer_id),
                ticket=ticket,
                notif_type="status_update",
                title=title,
                message=message,
                is_internal=False,
                email_subject=f"[{ticket.ticket_number}] Status updated — {status_label}",
                email_body="\n".join(email_body_lines),
            )
        except Exception as exc:
            logger.error(
                "customer_status_notification_failed",
                ticket_id=str(ticket.id),
                error=str(exc),
            )

    # ── Customer info ──────────────────────────────────────────────────────────

    async def get_customer_info(self, customer_id: str) -> dict | None:
        try:
            from src.handlers.http_clients.auth_client import AuthHttpClient
            user = await AuthHttpClient().get_user_by_id(customer_id)
            if not user:
                return None
            return {
                "full_name": user.get("full_name") or user.get("email", "Customer"),
                "email":     user.get("email", ""),
            }
        except Exception as exc:
            logger.error("get_customer_info_failed", customer_id=customer_id, error=str(exc))
            return None


# ── Parse breach note helper ───────────────────────────────────────────────────

def _parse_breach_note(content: str) -> tuple[str | None, str]:
    if not content.startswith(_BREACH_PREFIX):
        return None, ""
    try:
        end_bracket   = content.index("]", len(_BREACH_PREFIX))
        breach_type   = content[len(_BREACH_PREFIX):end_bracket].strip()
        justification = content[end_bracket + 1:].strip()
        if breach_type not in ("response", "resolution"):
            return None, ""
        return breach_type, justification
    except (ValueError, IndexError):
        return None, ""