"""
TeamLeadService — all actions a team lead can perform on tickets.
src/core/services/team_lead_service.py
"""

from __future__ import annotations

import uuid

from sqlalchemy.ext.asyncio import AsyncSession

from src.core.exceptions.base import ForbiddenException, NotFoundException
from src.core.services.audit_service import audit_service
from src.data.models.postgres.models import Notification, Ticket
from src.data.repositories.ticket_repository import NotificationRepository
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
        self._session    = session
        self._repo       = TeamLeadRepository(session)
        self._notif_repo = NotificationRepository(session)

    # ── Resolve TL's teams ────────────────────────────────────────────────────

    async def _get_team_id(self, lead_user_id: str) -> str:
        """
        Returns first active team_id for this TL.
        Used for single-ticket ops where ticket already has a team_id.
        """
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
        """
        Returns ALL active team_ids for this TL across all products.
        Used for member/ticket/queue aggregation.
        """
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
        """Unassigned tickets across all product teams."""
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
        """All tickets across all product teams — TL sees all their members' tickets."""
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
        """
        Fetch single ticket — checks across all product teams this TL leads.
        """
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
        ticket_id: str,
        payload: ManualAssignRequest,
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
        ticket_id: str,
        payload: TicketStatusUpdateRequest,
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

    # ── Team overview ─────────────────────────────────────────────────────────

    async def get_team_overview(self, lead_user_id: str) -> TeamOverviewResponse:
        """
        Overview aggregated across all product teams.
        Members are deduplicated by user_id — same agent appears once.
        """
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

        agents = [
            AgentWorkloadItem(
                user_id=member.user_id,
                experience=member.experience,
                open_tickets=count,
                skills=member.skills,
            )
            for member, count in workloads
        ]

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
        ticket_id: str,
        lead_user_id: str,
    ) -> dict:
        """Fetch all conversations and attachments for a ticket in any of TL's teams."""
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
        ticket_id: str,
        content: str,
        lead_user_id: str,
    ) -> "Conversation":
        """TL posts internal note — always is_internal=True, never customer-visible."""
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