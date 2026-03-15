"""
TeamLeadService — all actions a team lead can perform on tickets.
src/core/services/team_lead_service.py
"""

from __future__ import annotations

import uuid

from sqlalchemy.ext.asyncio import AsyncSession

from src.core.exceptions.base import NotFoundException
from src.core.services.audit_service import audit_service
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


class TeamLeadService:

    def __init__(self, session: AsyncSession) -> None:
        self._session     = session
        self._repo        = TeamLeadRepository(session)
        self._ticket_repo = TicketRepository(session)   # ← for re-fetch after commit
        self._notif_repo  = NotificationRepository(session)

    # ── Resolve TL's teams ────────────────────────────────────────────────────

    async def _get_team_id(self, lead_user_id: str) -> str:
        try:
            team = await self._repo.get_team_by_lead(lead_user_id)
        except Exception as exc:
            logger.error(
                "team_lead_team_lookup_failed",
                lead_user_id=lead_user_id,
                error=str(exc),
            )
            raise
        if not team:
            raise NotFoundException(
                "No active team found for this team lead. "
                "Ensure you are assigned as team_lead in the team record."
            )
        return str(team.id)

    async def _get_all_team_ids(self, lead_user_id: str) -> list[str]:
        try:
            ids = await self._repo.get_all_team_ids_by_lead(lead_user_id)
        except Exception as exc:
            logger.error(
                "team_lead_all_teams_lookup_failed",
                lead_user_id=lead_user_id,
                error=str(exc),
            )
            raise
        if not ids:
            raise NotFoundException("No active teams found for this team lead.")
        return [str(i) for i in ids]

    # ── Queue ─────────────────────────────────────────────────────────────────

    async def get_unassigned_queue(self, lead_user_id: str) -> list[Ticket]:
        team_ids = await self._get_all_team_ids(lead_user_id)
        try:
            tickets = await self._repo.get_unassigned_tickets_multi(team_ids)
        except Exception as exc:
            logger.error(
                "tl_unassigned_queue_failed",
                lead_user_id=lead_user_id,
                team_ids=team_ids,
                error=str(exc),
            )
            raise
        logger.info(
            "tl_unassigned_queue_fetched",
            lead_user_id=lead_user_id,
            team_ids=team_ids,
            count=len(tickets),
        )
        return tickets

    async def get_all_team_tickets(
        self,
        lead_user_id: str,
        status: str | None = None,
    ) -> list[Ticket]:
        team_ids = await self._get_all_team_ids(lead_user_id)
        try:
            tickets = await self._repo.get_all_team_tickets_multi(team_ids, status=status)
        except Exception as exc:
            logger.error(
                "tl_all_tickets_failed",
                lead_user_id=lead_user_id,
                team_ids=team_ids,
                status=status,
                error=str(exc),
            )
            raise
        logger.info(
            "tl_all_tickets_fetched",
            lead_user_id=lead_user_id,
            team_ids=team_ids,
            status=status,
            count=len(tickets),
        )
        return tickets

    # ── Single ticket ─────────────────────────────────────────────────────────

    async def get_ticket(self, ticket_id: str, lead_user_id: str) -> Ticket:
        team_ids = await self._get_all_team_ids(lead_user_id)
        try:
            ticket = await self._repo.get_ticket_by_any_team(ticket_id, team_ids)
        except Exception as exc:
            logger.error(
                "tl_get_ticket_failed",
                ticket_id=ticket_id,
                lead_user_id=lead_user_id,
                error=str(exc),
            )
            raise
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
        team_ids = await self._get_all_team_ids(lead_user_id)
        ticket   = await self._repo.get_ticket_by_any_team(ticket_id, team_ids)
        if not ticket:
            raise NotFoundException(f"Ticket {ticket_id} not found in your team.")

        old_assigned_to = ticket.assigned_to

        try:
            await self._repo.assign_ticket(ticket_id, str(payload.agent_user_id))
        except Exception as exc:
            logger.error(
                "tl_manual_assign_failed",
                ticket_id=ticket_id,
                agent_user_id=str(payload.agent_user_id),
                lead_user_id=lead_user_id,
                error=str(exc),
            )
            raise

        await audit_service.log(
            entity_type="ticket",
            entity_id=ticket.id,
            action="ticket_manually_assigned",
            actor_id=uuid.UUID(lead_user_id),
            actor_type="team_lead",
            old_value={"assigned_to": str(old_assigned_to) if old_assigned_to else None},
            new_value={"assigned_to": str(payload.agent_user_id)},
            changed_fields=["assigned_to"],
            reason="Manual assignment by team lead",
            ticket_id=ticket.id,
        )

        try:
            notif = Notification(
                channel="in_app",
                recipient_id=payload.agent_user_id,
                ticket_id=ticket.id,
                is_internal=True,
                type="ticket_assigned",
                title=f"Ticket {ticket.ticket_number} assigned to you",
                message=(
                    f"Ticket {ticket.ticket_number} manually assigned by team lead.\n"
                    f"Priority: {ticket.priority} | Severity: {ticket.severity}"
                ),
            )
            await self._notif_repo.create(notif)
        except Exception as exc:
            logger.warning(
                "tl_manual_assign_notif_failed",
                ticket_id=ticket_id,
                agent_user_id=str(payload.agent_user_id),
                error=str(exc),
            )

        await self._session.commit()

        # Re-fetch via TicketRepository after commit so all columns
        # (including updated_at) are loaded — avoids MissingGreenlet on serialization
        ticket = await self._ticket_repo.get_by_id(ticket_id)

        self._push_sse_to_agent(str(payload.agent_user_id), ticket)

        logger.info(
            "ticket_manually_assigned",
            ticket_id=ticket_id,
            agent_user_id=str(payload.agent_user_id),
            lead_user_id=lead_user_id,
            old_assigned_to=str(old_assigned_to) if old_assigned_to else None,
        )
        return ticket

    # ── Status update ─────────────────────────────────────────────────────────

    async def update_ticket_status(
        self,
        ticket_id:    str,
        payload:      TicketStatusUpdateRequest,
        lead_user_id: str,
    ) -> Ticket:
        team_ids = await self._get_all_team_ids(lead_user_id)
        ticket   = await self._repo.get_ticket_by_any_team(ticket_id, team_ids)
        if not ticket:
            raise NotFoundException(f"Ticket {ticket_id} not found in your team.")

        old_status = ticket.status

        try:
            await self._repo.update_ticket_status(ticket_id, payload.status)
        except Exception as exc:
            logger.error(
                "tl_status_update_failed",
                ticket_id=ticket_id,
                status=payload.status,
                lead_user_id=lead_user_id,
                error=str(exc),
            )
            raise

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
        logger.info(
            "ticket_status_updated_by_tl",
            ticket_id=ticket_id,
            old_status=old_status,
            new_status=payload.status,
            lead_user_id=lead_user_id,
        )
        return ticket

    # ── Team overview — resolves agent names from auth-service ────────────────

    async def get_team_overview(self, lead_user_id: str) -> TeamOverviewResponse:
        try:
            team     = await self._repo.get_team_by_lead(lead_user_id)
            team_ids = await self._get_all_team_ids(lead_user_id)
        except Exception as exc:
            logger.error(
                "tl_team_overview_failed",
                lead_user_id=lead_user_id,
                error=str(exc),
            )
            raise
        if not team:
            raise NotFoundException("No active team found for this team lead.")

        try:
            workloads        = await self._repo.get_agent_workloads_multi(team_ids)
            unassigned_count = await self._repo.unassigned_count_multi(team_ids)
        except Exception as exc:
            logger.error(
                "tl_team_overview_stats_failed",
                lead_user_id=lead_user_id,
                team_ids=team_ids,
                error=str(exc),
            )
            raise

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
                logger.warning(
                    "team_overview_agent_name_failed",
                    user_id=str(member.user_id),
                    error=str(exc),
                )
            agents.append(
                AgentWorkloadItem(
                    user_id=member.user_id,
                    full_name=full_name,
                    experience=member.experience,
                    open_tickets=count,
                    skills=member.skills,
                )
            )

        logger.info(
            "tl_team_overview_fetched",
            lead_user_id=lead_user_id,
            team_ids=team_ids,
            agent_count=len(agents),
            unassigned_count=unassigned_count,
        )

        return TeamOverviewResponse(
            team_id=team.id,
            team_name=team.name,
            product_id=team.product_id,
            unassigned_count=unassigned_count,
            agents=agents,
        )

    # ── Thread ────────────────────────────────────────────────────────────────

    async def get_ticket_thread(
        self,
        ticket_id:    str,
        lead_user_id: str,
    ) -> dict:
        team_ids = await self._get_all_team_ids(lead_user_id)
        try:
            conversations, attachments = await self._repo.get_ticket_conversations(
                ticket_id, team_ids
            )
        except ValueError as exc:
            raise NotFoundException(str(exc))
        except Exception as exc:
            logger.error(
                "tl_get_thread_failed",
                ticket_id=ticket_id,
                lead_user_id=lead_user_id,
                error=str(exc),
            )
            raise

        logger.info(
            "tl_ticket_thread_fetched",
            ticket_id=ticket_id,
            lead_user_id=lead_user_id,
            conversation_count=len(conversations),
            attachment_count=len(attachments),
        )
        return {
            "conversations": conversations,
            "attachments":   attachments,
        }

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
        except Exception as exc:
            logger.error(
                "tl_add_note_failed",
                ticket_id=ticket_id,
                lead_user_id=lead_user_id,
                error=str(exc),
            )
            raise

        await self._session.commit()

        logger.info(
            "tl_internal_note_added",
            ticket_id=ticket_id,
            lead_user_id=lead_user_id,
        )
        return note

    # ── SSE helper ────────────────────────────────────────────────────────────

    def _push_sse_to_agent(self, agent_user_id: str, ticket: Ticket) -> None:
        try:
            from src.config.settings import settings
            from src.core.sse.redis_subscriber import (
                publish_notification,
                publish_queue_update,
            )
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
            publish_notification(
                settings.CELERY_BROKER_URL,
                agent_user_id,
                {
                    "type":          "ticket_assigned",
                    "title":         f"Ticket {ticket.ticket_number} assigned to you",
                    "message":       (
                        f"Manually assigned by team lead. "
                        f"Priority: {ticket.priority} | Severity: {ticket.severity}"
                    ),
                    "ticket_number": ticket.ticket_number,
                },
            )
        except Exception as exc:
            logger.warning(
                "tl_sse_push_failed",
                agent_user_id=agent_user_id,
                ticket_id=str(ticket.id),
                error=str(exc),
            )

    # ── Notification Templates ────────────────────────────────────────────────

    async def list_templates(self) -> list:
        from src.data.repositories.template_repository import NotificationTemplateRepository
        repo = NotificationTemplateRepository(self._session)
        return await repo.get_all(active_only=False)

    async def get_template(self, template_id: str):
        from src.data.repositories.template_repository import NotificationTemplateRepository
        repo = NotificationTemplateRepository(self._session)
        tpl  = await repo.get_by_id(template_id)
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
        repo = NotificationTemplateRepository(self._session)
        tpl  = await repo.update(
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
        from src.data.repositories.notification_preference_repository import NotificationPreferenceRepository
        from src.data.models.postgres.models import Conversation, Notification
        import uuid as _uuid

        tpl_repo   = NotificationTemplateRepository(self._session)
        pref_repo  = NotificationPreferenceRepository(self._session)
        notif_repo = NotificationRepository(self._session)

        ticket = await self._ticket_repo.get_by_id(ticket_id)
        if not ticket:
            raise ValueError(f"Ticket {ticket_id} not found.")

        tpl = await tpl_repo.get_by_id(template_id)
        if not tpl:
            raise ValueError(f"Template {template_id} not found.")

        customer_name  = "Customer"
        customer_email = None
        try:
            from src.handlers.http_clients.auth_client import AuthHttpClient
            auth = AuthHttpClient()
            user = await auth.get_user_by_id(str(ticket.customer_id))
            if user:
                customer_name  = user.get("full_name") or user.get("email", "Customer")
                customer_email = user.get("email")
        except Exception as exc:
            logger.warning("send_apology_customer_lookup_failed", ticket_id=ticket_id, error=str(exc))

        tl_name = "Support Team Lead"
        try:
            from src.handlers.http_clients.auth_client import AuthHttpClient
            auth    = AuthHttpClient()
            tl_user = await auth.get_user_by_id(lead_user_id)
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

        channel = "in_app"
        try:
            channel = await pref_repo.get_preferred_contact(str(ticket.customer_id))
        except Exception:
            pass

        sent = False
        if channel == "in_app":
            try:
                notif = Notification(
                    channel="in_app",
                    recipient_id=ticket.customer_id,
                    ticket_id=ticket.id,
                    is_internal=False,
                    type="apology_message",
                    title=subject,
                    message=body,
                )
                self._session.add(notif)
                sent = True

                from src.core.sse.sse_manager import sse_manager
                import asyncio
                asyncio.create_task(
                    sse_manager.push(
                        str(ticket.customer_id),
                        {
                            "event": "notification",
                            "data": {
                                "type":          "apology_message",
                                "title":         subject,
                                "message":       body,
                                "ticket_number": ticket.ticket_number,
                            },
                        },
                    )
                )
            except Exception as exc:
                logger.error("send_apology_in_app_failed", ticket_id=ticket_id, error=str(exc))
        else:
            if customer_email:
                try:
                    from src.handlers.http_clients.email_client import EmailClient
                    await EmailClient().send_generic(
                        to_email=customer_email,
                        subject=subject,
                        body=body,
                    )
                    sent = True
                except Exception as exc:
                    logger.error("send_apology_email_failed", ticket_id=ticket_id, error=str(exc))

        note_content = (
            f"[APOLOGY_SENT] Template: {tpl.key} | Channel: {channel} | "
            f"Sent: {sent}\n\nSubject: {subject}\n\n{body}"
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

        logger.info(
            "apology_sent",
            ticket_id=ticket_id,
            lead_user_id=lead_user_id,
            template_key=tpl.key,
            channel=channel,
            sent=sent,
        )

        return {
            "sent":         sent,
            "channel":      channel,
            "ticket_id":    ticket.id,
            "template_key": tpl.key,
            "message":      body[:300],
        }


# ── Variable substitution helper ──────────────────────────────────────────────

def _substitute_variables(template_str: str, variables: dict) -> str:
    result = template_str
    for key, value in variables.items():
        result = result.replace(f"{{{key}}}", str(value) if value else "")
    return result