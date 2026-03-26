"""
TeamLeadService.
src/core/services/team_lead_service.py
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone, timedelta
from typing import Optional

from fastapi import BackgroundTasks
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.exceptions.base import NotFoundException
from src.core.services.audit_service import audit_service
from src.core.services.agent_services import VALID_TRANSITIONS
from src.core.services.notification_service import NotificationService
from src.data.models.postgres.models import Notification, Ticket
from src.data.repositories.ticket_repository import NotificationRepository, TicketRepository
from src.data.repositories.team_lead_repository import TeamLeadRepository
from src.observability.logging.logger import get_logger
from src.schemas.team_lead_schema import (
    AgentWorkloadItem,
    ManualAssignRequest,
    TeamOverviewResponse,
    TicketStatusUpdateRequest,
)

logger = get_logger(__name__)

CUSTOMER_SUPPORT_TEAM_NAME = "Customer Support Team"

TL_ALLOWED_STATUSES = {"in_progress", "on_hold", "resolved", "closed"}

TL_VALID_TRANSITIONS: dict[str, set[str]] = {
    "new":          {"acknowledged", "assigned", "in_progress", "closed"},
    "acknowledged": {"assigned", "in_progress", "closed"},
    "assigned":     {"in_progress", "on_hold", "closed"},
    "in_progress":  {"on_hold", "resolved", "closed"},
    "on_hold":      {"in_progress", "resolved", "closed"},
    "resolved":     {"closed"},
    "closed":       set(),
    "reopened":     {"in_progress", "on_hold", "closed"},
}


class TeamLeadService:

    def __init__(
        self,
        session:          AsyncSession,
        background_tasks: Optional[BackgroundTasks] = None,
    ) -> None:
        self._session          = session
        self._repo             = TeamLeadRepository(session)
        self._ticket_repo      = TicketRepository(session)
        self._notif_repo       = NotificationRepository(session)
        self._notif_svc        = NotificationService(session, background_tasks)

    # ── Resolve TL's teams ────────────────────────────────────────────────────

    async def _get_team_id(self, lead_user_id: str) -> str:
        team = await self._repo.get_team_by_lead(lead_user_id)
        if not team:
            raise NotFoundException("No active team found for this team lead.")
        return str(team.id)

    async def _get_all_team_ids(self, lead_user_id: str) -> list[str]:
        ids = await self._repo.get_all_team_ids_by_lead(lead_user_id)
        if not ids:
            raise NotFoundException("No active teams found for this team lead.")
        return [str(i) for i in ids]

    # ── Queue ─────────────────────────────────────────────────────────────────

    async def get_unassigned_queue(self, lead_user_id: str) -> list[Ticket]:
        team_ids = await self._get_all_team_ids(lead_user_id)
        tickets  = await self._repo.get_unassigned_tickets_multi(team_ids)
        logger.info("tl_unassigned_queue_fetched", count=len(tickets))
        return tickets

    async def get_all_team_tickets(
        self,
        lead_user_id: str,
        status: str | None = None,
    ) -> list[Ticket]:
        team_ids = await self._get_all_team_ids(lead_user_id)
        tickets  = await self._repo.get_all_team_tickets_multi(team_ids, status=status)
        logger.info("tl_all_tickets_fetched", count=len(tickets))
        return tickets

    # ── Single ticket ─────────────────────────────────────────────────────────

    async def get_ticket(self, ticket_id: str, lead_user_id: str) -> Ticket:
        team_ids = await self._get_all_team_ids(lead_user_id)
        ticket   = await self._repo.get_ticket_by_any_team(ticket_id, team_ids)
        if not ticket:
            raise NotFoundException(f"Ticket {ticket_id} not found in your team.")
        return ticket

    # ── Manual assign ─────────────────────────────────────────────────────────

    async def manual_assign(
        self,
        ticket_id:    str,
        payload:      ManualAssignRequest,
        lead_user_id: str,
    ) -> Ticket:
        team_ids        = await self._get_all_team_ids(lead_user_id)
        ticket          = await self._repo.get_ticket_by_any_team(ticket_id, team_ids)
        if not ticket:
            raise NotFoundException(f"Ticket {ticket_id} not found in your team.")

        old_assigned_to = ticket.assigned_to

        await self._repo.assign_ticket(ticket_id, str(payload.agent_user_id))
        await self._ticket_repo.update_fields(ticket_id, {"status": "assigned"})

        await audit_service.log(
            entity_type="ticket",
            entity_id=ticket.id,
            action="ticket_manually_assigned",
            actor_id=uuid.UUID(lead_user_id),
            actor_type="team_lead",
            old_value={"assigned_to": str(old_assigned_to) if old_assigned_to else None},
            new_value={"assigned_to": str(payload.agent_user_id), "status": "assigned"},
            changed_fields=["assigned_to", "status"],
            reason="Manual assignment by team lead",
            ticket_id=ticket.id,
        )

        await self._session.commit()
        ticket = await self._ticket_repo.get_by_id(ticket_id)

        # Notify agent of assignment per their preference
        await self._notif_svc.notify(
            recipient_id=str(payload.agent_user_id),
            ticket=ticket,
            notif_type="ticket_assigned",
            title=f"Ticket {ticket.ticket_number} assigned to you",
            message=(
                f"Ticket {ticket.ticket_number} manually assigned by team lead.\n"
                f"Priority: {ticket.priority} | Severity: {ticket.severity}"
            ),
            is_internal=True,
            email_subject=f"[Ticketing Genie] New ticket assigned: {ticket.ticket_number}",
            email_body=(
                f"Hi,\n\nA ticket has been manually assigned to you by a team lead.\n\n"
                f"Ticket:   {ticket.ticket_number}\n"
                f"Title:    {ticket.title}\n"
                f"Priority: {ticket.priority}\n"
                f"Severity: {ticket.severity}\n\n"
                f"Please respond within your SLA window.\n\n"
                f"— Ticketing Genie"
            ),
        )

        self._push_sse_to_agent(str(payload.agent_user_id), ticket)

        logger.info(
            "ticket_manually_assigned",
            ticket_id=ticket_id,
            agent_user_id=str(payload.agent_user_id),
            lead_user_id=lead_user_id,
        )
        return ticket

    # ── Reroute ticket (Customer Support TL only) ─────────────────────────────

    async def reroute_ticket(
        self,
        ticket_id:      str,
        target_team_id: str,
        lead_user_id:   str,
    ) -> Ticket:
        """
        Reroute a ticket to another team.
        Only the team lead of the Customer Support Team is allowed to do this.
        Clears assigned_to, updates team_id, resets status to acknowledged,
        and notifies the target team's lead.
        """
        from sqlalchemy import select
        from src.data.models.postgres.models import Team

        # ── Verify caller is TL of Customer Support Team ──────────────────
        caller_team = await self._repo.get_team_by_lead(lead_user_id)
        if not caller_team or caller_team.name != CUSTOMER_SUPPORT_TEAM_NAME:
            raise PermissionError(
                "Only the team lead of the Customer Support Team can re-route tickets."
            )

        # ── Verify the ticket belongs to the caller's team ─────────────────
        team_ids = await self._get_all_team_ids(lead_user_id)
        ticket   = await self._repo.get_ticket_by_any_team(ticket_id, team_ids)
        if not ticket:
            raise NotFoundException(f"Ticket {ticket_id} not found in your team.")

        # ── Fetch target team ──────────────────────────────────────────────
        result      = await self._session.execute(
            select(Team).where(
                Team.id == uuid.UUID(target_team_id),
                Team.is_active.is_(True),
            )
        )
        target_team = result.scalar_one_or_none()
        if not target_team:
            raise ValueError(f"Target team {target_team_id} not found or inactive.")

        if str(target_team.id) == str(caller_team.id):
            raise ValueError("Cannot re-route a ticket to the same team.")

        old_team_id = ticket.team_id

        # ── Update ticket ──────────────────────────────────────────────────
        await self._ticket_repo.update_fields(ticket_id, {
            "team_id":     target_team.id,
            "assigned_to": None,
            "status":      "acknowledged",
        })

        await audit_service.log(
            entity_type="ticket",
            entity_id=ticket.id,
            action="ticket_rerouted_by_cs_tl",
            actor_id=uuid.UUID(lead_user_id),
            actor_type="team_lead",
            old_value={
                "team_id":     str(old_team_id) if old_team_id else None,
                "assigned_to": str(ticket.assigned_to) if ticket.assigned_to else None,
            },
            new_value={
                "team_id":     str(target_team.id),
                "assigned_to": None,
                "status":      "acknowledged",
            },
            changed_fields=["team_id", "assigned_to", "status"],
            reason=f"Re-routed by Customer Support Team lead to {target_team.name}",
            ticket_id=ticket.id,
        )

        await self._session.commit()
        ticket = await self._ticket_repo.get_by_id(ticket_id)

        # ── Notify target team's lead ──────────────────────────────────────
        if target_team.team_lead_id:
            await self._notif_svc.notify(
                recipient_id=str(target_team.team_lead_id),
                ticket=ticket,
                notif_type="ticket_rerouted",
                title=(
                    f"Ticket {ticket.ticket_number} has been routed to your team"
                ),
                message=(
                    f"The Customer Support Team lead has routed ticket "
                    f"{ticket.ticket_number} to your team ({target_team.name}).\n\n"
                    f"Ticket Number : {ticket.ticket_number}\n"
                    f"Title         : {ticket.title or 'N/A'}\n"
                    f"Priority      : {ticket.priority or 'N/A'}\n"
                    f"Severity      : {ticket.severity or 'N/A'}\n\n"
                    f"Please assign this ticket to an agent in your team."
                ),
                is_internal=True,
                email_subject=(
                    f"[{ticket.ticket_number}] Ticket routed to your team — {target_team.name}"
                ),
                email_body=(
                    f"Hi,\n\n"
                    f"The Customer Support Team lead has routed a ticket to your team.\n\n"
                    f"Ticket Number : {ticket.ticket_number}\n"
                    f"Title         : {ticket.title or 'N/A'}\n"
                    f"Priority      : {ticket.priority or 'N/A'}\n"
                    f"Severity      : {ticket.severity or 'N/A'}\n\n"
                    f"Please log in to the portal and assign this ticket to a member of "
                    f"your team at your earliest convenience.\n\n"
                    f"— Ticketing Genie"
                ),
            )
        else:
            logger.warning(
                "reroute_target_team_no_lead",
                ticket_id=ticket_id,
                target_team_id=target_team_id,
            )

        logger.info(
            "ticket_rerouted",
            ticket_id=ticket_id,
            from_team=str(old_team_id),
            to_team=str(target_team.id),
            lead_user_id=lead_user_id,
        )
        return ticket

    # ── Get all active teams (for reroute dropdown) ───────────────────────────

    # Service
    async def get_all_teams(self, product_id: str | None = None) -> list[dict]:
        from sqlalchemy import select
        from src.data.models.postgres.models import Team

        query = select(Team).where(Team.is_active.is_(True))

        if product_id:
            query = query.where(Team.product_id == uuid.UUID(product_id))

        result = await self._session.execute(query.order_by(Team.name))
        return [{"id": str(t.id), "name": t.name} for t in result.scalars().all()]

    # ── Status update ─────────────────────────────────────────────────────────

    async def update_ticket_status(
        self,
        ticket_id:    str,
        payload:      TicketStatusUpdateRequest,
        lead_user_id: str,
    ) -> Ticket:
        if payload.status not in TL_ALLOWED_STATUSES:
            raise ValueError(
                f"Team leads can only set: {', '.join(sorted(TL_ALLOWED_STATUSES))}. "
                f"'{payload.status}' is not permitted."
            )

        team_ids = await self._get_all_team_ids(lead_user_id)
        ticket   = await self._repo.get_ticket_by_any_team(ticket_id, team_ids)
        if not ticket:
            raise NotFoundException(f"Ticket {ticket_id} not found in your team.")

        current = ticket.status
        allowed = TL_VALID_TRANSITIONS.get(current, set())

        if payload.status not in allowed:
            raise ValueError(
                f"Cannot transition from '{current}' to '{payload.status}'. "
                f"Allowed: {sorted(allowed) or 'none'}"
            )

        old_status    = current
        now           = datetime.now(timezone.utc)
        extra_fields: dict = {"status": payload.status}

        if payload.status == "on_hold":
            extra_fields["on_hold_started_at"] = now

        elif payload.status == "in_progress" and current == "on_hold":
            if ticket.on_hold_started_at:
                held_mins = int((now - ticket.on_hold_started_at).total_seconds() / 60)
                extra_fields["on_hold_duration_accumulated"] = (
                    (ticket.on_hold_duration_accumulated or 0) + held_mins
                )
                extra_fields["on_hold_started_at"] = None
                if ticket.sla_resolve_due:
                    extra_fields["sla_resolve_due"] = (
                        ticket.sla_resolve_due + timedelta(minutes=held_mins)
                    )
            else:
                extra_fields["on_hold_started_at"] = None

        elif payload.status == "resolved":
            extra_fields["resolved_at"] = now
            extra_fields["resolved_by"] = lead_user_id
            if current == "on_hold" and ticket.on_hold_started_at:
                held_mins = int((now - ticket.on_hold_started_at).total_seconds() / 60)
                extra_fields["on_hold_duration_accumulated"] = (
                    (ticket.on_hold_duration_accumulated or 0) + held_mins
                )
                extra_fields["on_hold_started_at"] = None

        elif payload.status == "closed":
            extra_fields["closed_at"] = now
            extra_fields["closed_by"] = lead_user_id
            if current == "on_hold" and ticket.on_hold_started_at:
                held_mins = int((now - ticket.on_hold_started_at).total_seconds() / 60)
                extra_fields["on_hold_duration_accumulated"] = (
                    (ticket.on_hold_duration_accumulated or 0) + held_mins
                )
                extra_fields["on_hold_started_at"] = None

        await self._ticket_repo.update_fields(ticket_id, extra_fields)

        await audit_service.log(
            entity_type="ticket",
            entity_id=ticket.id,
            action="ticket_status_updated_by_tl",
            actor_id=uuid.UUID(lead_user_id),
            actor_type="team_lead",
            old_value={"status": old_status},
            new_value={"status": payload.status},
            changed_fields=["status"],
            ticket_id=ticket.id,
        )

        await self._session.commit()
        ticket = await self._ticket_repo.get_by_id(ticket_id)

        # Notify customer of status change per their preference
        status_label = payload.status.replace("_", " ").title()
        await self._notif_svc.notify(
            recipient_id=str(ticket.customer_id),
            ticket=ticket,
            notif_type="status_update",
            title=f"Ticket {ticket.ticket_number} — {status_label}",
            message=(
                f"Your ticket {ticket.ticket_number} has been updated to {status_label}."
            ),
            is_internal=False,
            email_subject=f"[{ticket.ticket_number}] Status updated — {status_label}",
            email_body=(
                f"Hi,\n\nYour support ticket has been updated.\n\n"
                f"Ticket:     {ticket.ticket_number}\n"
                f"Title:      {ticket.title or '(no title)'}\n"
                f"New Status: {status_label}\n\n"
                f"Best regards,\nTicketing Genie Support Team"
            ),
        )

        logger.info(
            "ticket_status_updated_by_tl",
            ticket_id=ticket_id,
            old_status=old_status,
            new_status=payload.status,
        )
        return ticket

    # ── Team overview ─────────────────────────────────────────────────────────

    async def get_team_overview(self, lead_user_id: str) -> TeamOverviewResponse:
        team     = await self._repo.get_team_by_lead(lead_user_id)
        team_ids = await self._get_all_team_ids(lead_user_id)
        if not team:
            raise NotFoundException("No active team found for this team lead.")

        workloads        = await self._repo.get_agent_workloads_multi(team_ids)
        unassigned_count = await self._repo.unassigned_count_multi(team_ids)

        from src.handlers.http_clients.auth_client import AuthHttpClient
        auth = AuthHttpClient()

        agents: list[AgentWorkloadItem] = []
        for member, count in workloads:
            full_name: str | None = None
            try:
                user = await auth.get_user_by_id(str(member.user_id))
                if user:
                    full_name = user.get("full_name") or user.get("email")
            except Exception as exc:
                logger.warning("team_overview_agent_name_failed", error=str(exc))
            agents.append(
                AgentWorkloadItem(
                    user_id=member.user_id,
                    full_name=full_name,
                    experience=member.experience,
                    open_tickets=count,
                    skills=member.skills,
                )
            )

        return TeamOverviewResponse(
            team_id=team.id,
            team_name=team.name,
            product_id=team.product_id,
            unassigned_count=unassigned_count,
            agents=agents,
        )

    # ── Thread ────────────────────────────────────────────────────────────────

    async def get_ticket_thread(self, ticket_id: str, lead_user_id: str) -> dict:
        team_ids = await self._get_all_team_ids(lead_user_id)
        try:
            conversations, attachments = await self._repo.get_ticket_conversations(
                ticket_id, team_ids
            )
        except ValueError as exc:
            raise NotFoundException(str(exc))
        return {"conversations": conversations, "attachments": attachments}

    # ── Internal note ─────────────────────────────────────────────────────────

    async def add_internal_note(
        self,
        ticket_id:    str,
        content:      str,
        lead_user_id: str,
    ) -> "Conversation":
        team_ids = await self._get_all_team_ids(lead_user_id)
        try:
            note = await self._repo.add_internal_note(
                ticket_id=ticket_id,
                team_ids=team_ids,
                author_id=lead_user_id,
                content=content,
            )
        except ValueError as exc:
            raise NotFoundException(str(exc))

        await self._session.commit()
        return note

    # ── SSE push to agent ─────────────────────────────────────────────────────

    def _push_sse_to_agent(self, agent_user_id: str, ticket: Ticket) -> None:
        try:
            from src.config.settings import settings
            from src.core.sse.redis_subscriber import publish_queue_update, publish_notification
            publish_queue_update(
                settings.CELERY_BROKER_URL,
                agent_user_id,
                {
                    "ticket_id":     str(ticket.id),
                    "ticket_number": ticket.ticket_number,
                    "title":         ticket.title or "",
                    "priority":      ticket.priority or "",
                    "severity":      ticket.severity or "",
                    "status":        ticket.status,
                    "reason":        "manually_assigned",
                },
            )
        except Exception as exc:
            logger.warning("tl_sse_push_failed", error=str(exc))

    # ── Notification Templates ────────────────────────────────────────────────

    async def list_templates(self) -> list:
        from src.data.repositories.template_repository import NotificationTemplateRepository
        return await NotificationTemplateRepository(self._session).get_all(active_only=False)

    async def get_template(self, template_id: str):
        from src.data.repositories.template_repository import NotificationTemplateRepository
        tpl = await NotificationTemplateRepository(self._session).get_by_id(template_id)
        if not tpl:
            raise ValueError(f"Template {template_id} not found.")
        return tpl

    async def update_template(
        self,
        template_id:  str,
        lead_user_id: str,
        name:         str | None,
        subject:      str | None,
        body:         str | None,
        is_active:    bool | None,
    ):
        from src.data.repositories.template_repository import NotificationTemplateRepository
        tpl = await NotificationTemplateRepository(self._session).update(
            template_id=template_id,
            updated_by=lead_user_id,
            name=name,
            subject=subject,
            body=body,
            is_active=is_active,
        )
        if not tpl:
            raise ValueError(f"Template {template_id} not found.")
        await self._session.commit()
        return tpl

    # ── Send Apology ──────────────────────────────────────────────────────────

    async def send_apology(
        self,
        ticket_id:      str,
        lead_user_id:   str,
        template_id:    str,
        custom_message: str | None,
        commit_time:    str | None,
    ) -> dict:
        from src.data.repositories.template_repository import NotificationTemplateRepository
        from src.data.models.postgres.models import Conversation
        import uuid as _uuid

        ticket = await self._ticket_repo.get_by_id(ticket_id)
        if not ticket:
            raise ValueError(f"Ticket {ticket_id} not found.")

        tpl = await NotificationTemplateRepository(self._session).get_by_id(template_id)
        if not tpl:
            raise ValueError(f"Template {template_id} not found.")

        customer_name = "Customer"
        try:
            from src.handlers.http_clients.auth_client import AuthHttpClient
            user = await AuthHttpClient().get_user_by_id(str(ticket.customer_id))
            if user:
                customer_name = user.get("full_name") or user.get("email", "Customer")
        except Exception:
            pass

        tl_name = "Support Team Lead"
        try:
            from src.handlers.http_clients.auth_client import AuthHttpClient
            tl_user = await AuthHttpClient().get_user_by_id(lead_user_id)
            if tl_user:
                tl_name = tl_user.get("full_name") or tl_user.get("email", tl_name)
        except Exception:
            pass

        variables = {
            "customer_name":      customer_name,
            "ticket_number":      ticket.ticket_number,
            "ticket_title":       ticket.title or "(no title)",
            "commit_time":        commit_time or "as soon as possible",
            "custom_message":     custom_message or "",
            "team_lead_name":     tl_name,
            "hold_reason":        "pending investigation",
            "resume_date":        "shortly",
            "resolution_summary": "Your issue has been resolved.",
            "agent_name":         "the assigned agent",
            "breach_type":        "SLA",
            "breach_time":        "recently",
            "priority":           ticket.priority or "standard",
            "required_action":    "Please action this ticket immediately.",
            "deadline":           "4",
        }

        subject = _substitute_variables(tpl.subject, variables)
        body    = _substitute_variables(tpl.body,    variables)

        # Send to customer via their preference
        await self._notif_svc.notify(
            recipient_id=str(ticket.customer_id),
            ticket=ticket,
            notif_type="apology_message",
            title=subject,
            message=body,
            is_internal=False,
            email_subject=subject,
            email_body=body,
        )

        # Always save internal note
        note_content = (
            f"[APOLOGY_SENT] Template: {tpl.key}\n\n"
            f"Subject: {subject}\n\n{body}"
        )
        note = Conversation(
            ticket_id=ticket.id,
            author_id=_uuid.UUID(lead_user_id),
            author_type="team_lead",
            content=note_content,
            is_internal=True,
            is_ai_draft=False,
        )
        self._session.add(note)
        await self._session.commit()

        logger.info("apology_sent", ticket_id=ticket_id, template_key=tpl.key)

        return {
            "sent":         True,
            "channel":      "preference_based",
            "ticket_id":    ticket.id,
            "template_key": tpl.key,
            "message":      body[:300],
        }


def _substitute_variables(template_str: str, variables: dict) -> str:
    result = template_str
    for key, value in variables.items():
        result = result.replace(f"{{{key}}}", str(value) if value else "")
    return result