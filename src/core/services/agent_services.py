from __future__ import annotations

import uuid
from datetime import datetime, timezone, timedelta
from typing import Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.data.models.postgres.models import Conversation, Notification, Team, Ticket
from src.data.repositories.ticket_repository import TicketRepository
from src.observability.logging.logger import get_logger
from src.schemas.ticket_schema import BreachJustificationResponse

logger = get_logger(__name__)

VALID_TRANSITIONS: dict[str, set[str]] = {
    "new":          {"acknowledged", "open", "in_progress", "on_hold", "closed"},
    "acknowledged": {"open", "in_progress", "on_hold", "closed"},
    "open":         {"in_progress", "on_hold", "closed"},
    "in_progress":  {"resolved", "on_hold", "closed"},
    "on_hold":      {"in_progress", "resolved", "closed"},
    "resolved":     {"closed", "reopened"},
    "closed":       {"reopened"},
    "reopened":     {"open", "in_progress", "on_hold", "closed"},
}

_BREACH_PREFIX = "[BREACH_JUSTIFICATION:"


class AgentService:

    def __init__(self, session: AsyncSession) -> None:
        self._session = session
        self._repo    = TicketRepository(session)

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

    # ── Single ticket fetch — auto-sets open if new ────────────────────────────

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
            logger.warning(
                "agent_ticket_not_found",
                agent_id=agent_id,
                ticket_id=ticket_id,
            )
            raise ValueError(f"Ticket {ticket_id} not found for agent {agent_id}")

        # Auto-set status to open when agent first opens the ticket
        if ticket.status == "new":
            await self._repo.update_fields(ticket_id, {"status": "open"})
            await self._session.commit()
            ticket = await self._repo.get_by_id(ticket_id)
            logger.info(
                "ticket_auto_opened",
                ticket_id=ticket_id,
                agent_id=agent_id,
            )

        logger.info("agent_ticket_fetched", agent_id=agent_id, ticket_id=ticket_id)
        return ticket

    # ── Status update with SLA pause/resume ───────────────────────────────────

    async def update_ticket_status(
        self,
        agent_id:   str,
        ticket_id:  str,
        new_status: str,
        reason:     Optional[str] = None,
    ) -> Ticket:
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
            logger.info("ticket_sla_paused", ticket_id=ticket_id)

        elif new_status == "in_progress" and current == "on_hold":
            fields = self._resume_from_hold(ticket, now, fields)

        elif new_status == "resolved":
            fields["resolved_at"] = now
            fields["resolved_by"] = agent_id
            if current == "on_hold" and ticket.on_hold_started_at:
                held_mins = int(
                    (now - ticket.on_hold_started_at).total_seconds() / 60
                )
                fields["on_hold_duration_accumulated"] = (
                    (ticket.on_hold_duration_accumulated or 0) + held_mins
                )
                fields["on_hold_started_at"] = None

        elif new_status == "closed":
            fields["closed_at"] = now
            fields["closed_by"] = agent_id
            if current == "on_hold" and ticket.on_hold_started_at:
                held_mins = int(
                    (now - ticket.on_hold_started_at).total_seconds() / 60
                )
                fields["on_hold_duration_accumulated"] = (
                    (ticket.on_hold_duration_accumulated or 0) + held_mins
                )
                fields["on_hold_started_at"] = None

        elif new_status == "reopened":
            fields["reopen_count"] = (ticket.reopen_count or 0) + 1
            fields["resolved_at"]  = None
            fields["resolved_by"]  = None

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
            held_mins = int(
                (now - ticket.on_hold_started_at).total_seconds() / 60
            )
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
                    new_accumulated=new_accumulated,
                    new_resolve_due=new_resolve_due.isoformat(),
                )
        else:
            fields["on_hold_started_at"] = None
            logger.warning(
                "ticket_on_hold_started_at_missing",
                ticket_id=str(ticket.id),
            )
        return fields

    # ── Agent self-unassign — sets status to open ─────────────────────────────

    async def unassign_ticket(
        self,
        agent_id:      str,
        ticket_id:     str,
        justification: str,
    ) -> Ticket:
        ticket = await self.get_ticket_by_id(agent_id, ticket_id)

        # Clear assignment and set status to open (unassigned but acknowledged)
        await self._repo.update_fields(ticket_id, {
            "assigned_to": None,
            "status":      "open",
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
                agent_id=agent_id,
                error=str(exc),
            )

        await self._session.commit()
        ticket = await self._repo.get_by_id(ticket_id)

        logger.info(
            "ticket_unassigned_by_agent",
            ticket_id=ticket_id,
            agent_id=agent_id,
            justification=justification,
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
            import asyncio

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
            logger.warning(
                "unassign_sse_push_failed",
                ticket_id=str(ticket.id),
                agent_id=agent_id,
                error=str(exc),
            )

    # ── SLA Breach Justification — submit ─────────────────────────────────────

    async def submit_breach_justification(
        self,
        agent_id:      str,
        ticket_id:     str,
        breach_type:   str,
        justification: str,
    ) -> BreachJustificationResponse:
        ticket = await self.get_ticket_by_id(agent_id, ticket_id)

        if breach_type == "response" and not ticket.response_sla_breached_at:
            raise ValueError(
                "Cannot submit response breach justification — "
                "response SLA has not been breached on this ticket."
            )
        if breach_type == "resolution" and not ticket.sla_breached_at:
            raise ValueError(
                "Cannot submit resolution breach justification — "
                "resolution SLA has not been breached on this ticket."
            )

        prefix = f"{_BREACH_PREFIX}{breach_type}]"
        existing = await self._session.execute(
            select(Conversation).where(
                Conversation.ticket_id == ticket.id,
                Conversation.is_internal.is_(True),
                Conversation.content.like(f"{prefix}%"),
            )
        )
        if existing.scalar_one_or_none():
            raise ValueError(
                f"A {breach_type} breach justification has already been submitted "
                f"for this ticket."
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
                logger.warning(
                    "breach_justification_tl_not_found",
                    ticket_id=str(ticket.id),
                )
                return

            import asyncio
            from src.core.sse.sse_manager import sse_manager

            notif_title   = f"Breach justification submitted: {ticket.ticket_number}"
            notif_message = (
                f"Agent submitted a {breach_type} SLA breach justification.\n\n"
                f"Ticket: {ticket.ticket_number}\n"
                f"Breach type: {breach_type.title()}\n"
                f"Justification: {justification}"
            )

            tl_notif = Notification(
                channel="in_app",
                recipient_id=team.team_lead_id,
                ticket_id=ticket.id,
                is_internal=True,
                type="sla_breach_justification",
                title=notif_title,
                message=notif_message,
            )
            self._session.add(tl_notif)
            await self._session.flush()

            tl_id = str(team.team_lead_id)

            async def _push():
                try:
                    await sse_manager.push(
                        tl_id,
                        {
                            "event": "notification",
                            "data": {
                                "type":          "sla_breach_justification",
                                "title":         notif_title,
                                "message":       notif_message,
                                "ticket_number": ticket.ticket_number,
                                "ticket_id":     str(ticket.id),
                            },
                        },
                    )
                except Exception as exc:
                    logger.warning(
                        "breach_justification_sse_push_failed",
                        tl_id=tl_id,
                        error=str(exc),
                    )

            asyncio.create_task(_push())

            logger.info(
                "tl_breach_justification_notified",
                tl_id=tl_id,
                ticket_id=str(ticket.id),
                breach_type=breach_type,
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
            from src.handlers.http_clients.auth_client import AuthHttpClient
            from src.handlers.http_clients.email_client import EmailClient

            auth  = AuthHttpClient()
            email = EmailClient()

            customer_email = await auth.get_user_email(str(ticket.customer_id))
            if not customer_email:
                logger.warning(
                    "customer_email_not_found_for_notification",
                    customer_id=str(ticket.customer_id),
                    ticket_id=str(ticket.id),
                )
                return

            status_label = new_status.replace("_", " ").title()
            subject      = f"[{ticket.ticket_number}] Status updated — {status_label}"

            body_lines = [
                "Hi,\n",
                "Your support ticket has been updated.\n",
                f"Ticket:     {ticket.ticket_number}",
                f"Title:      {ticket.title or '(no title)'}",
                f"New Status: {status_label}",
            ]
            if reason:
                body_lines.append(f"Note:       {reason}")

            body_lines += [
                "\nBest regards,\nTicketing Genie Support Team",
            ]

            await email.send_generic(
                to_email=customer_email,
                subject=subject,
                body="\n".join(body_lines),
            )

        except Exception as exc:
            logger.error(
                "customer_status_notification_failed",
                ticket_id=str(ticket.id),
                error=str(exc),
            )

    # ── Customer name lookup ───────────────────────────────────────────────────

    async def get_customer_info(self, customer_id: str) -> dict | None:
        try:
            from src.handlers.http_clients.auth_client import AuthHttpClient
            auth = AuthHttpClient()
            user = await auth.get_user_by_id(customer_id)
            if not user:
                return None
            return {
                "full_name": user.get("full_name") or user.get("email", "Customer"),
                "email":     user.get("email", ""),
            }
        except Exception as exc:
            logger.error(
                "get_customer_info_failed",
                customer_id=customer_id,
                error=str(exc),
            )
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