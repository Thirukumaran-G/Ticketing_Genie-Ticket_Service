"""
AgentService additions — status update, customer name resolution.
src/core/services/agent_services.py  (add these methods to the existing class)

Full updated class included below — replace your existing file.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from sqlalchemy.ext.asyncio import AsyncSession

from src.data.models.postgres.models import Ticket
from src.data.repositories.ticket_repository import TicketRepository
from src.observability.logging.logger import get_logger

logger = get_logger(__name__)
VALID_TRANSITIONS: dict[str, set[str]] = {
    "new":         {"acknowledged", "in_progress", "on_hold", "closed"},
    "acknowledged":{"in_progress", "on_hold", "closed"},
    "in_progress": {"resolved", "on_hold", "closed"},
    "on_hold":     {"in_progress", "resolved", "closed"},
    "resolved":    {"closed", "reopened"},
    "closed":      {"reopened"},
    "reopened":    {"acknowledged", "in_progress", "on_hold", "closed"},
}


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

    # ── Single ticket fetch ────────────────────────────────────────────────────

    async def get_ticket_by_id(self, agent_id: str, ticket_id: str) -> Ticket:
        try:
            ticket = await self._repo.get_by_id_and_agent(ticket_id, agent_id)
        except Exception as exc:
            logger.error("agent_ticket_fetch_failed", agent_id=agent_id, ticket_id=ticket_id, error=str(exc))
            raise
        if ticket is None:
            logger.warning("agent_ticket_not_found", agent_id=agent_id, ticket_id=ticket_id)
            raise ValueError(f"Ticket {ticket_id} not found for agent {agent_id}")
        logger.info("agent_ticket_fetched", agent_id=agent_id, ticket_id=ticket_id)
        return ticket

    # ── Status update ──────────────────────────────────────────────────────────

    async def update_ticket_status(
        self,
        agent_id:  str,
        ticket_id: str,
        new_status: str,
        reason: Optional[str] = None,
    ) -> Ticket:
        """
        Update ticket status, stamp timestamps, notify customer via email.

        Side effects:
          - resolved_at / closed_at stamped when entering those states
          - reopen_count incremented on "reopened"
          - Customer receives a status-change notification email (non-blocking)
        """
        ticket = await self.get_ticket_by_id(agent_id, ticket_id)

        current = ticket.status
        allowed = VALID_TRANSITIONS.get(current, set())
        if new_status not in allowed:
            raise ValueError(
                f"Cannot transition from '{current}' to '{new_status}'. "
                f"Allowed: {sorted(allowed) or 'none'}"
            )

        now = datetime.now(timezone.utc)
        fields: dict = {"status": new_status}

        if new_status == "resolved":
            fields["resolved_at"] = now
            fields["resolved_by"] = agent_id
        elif new_status == "closed":
            fields["closed_at"] = now
            fields["closed_by"] = agent_id
        elif new_status == "reopened":
            fields["reopen_count"] = (ticket.reopen_count or 0) + 1
            fields["resolved_at"]  = None
            fields["resolved_by"]  = None

        await self._repo.update_fields(ticket_id, fields)
        await self._session.commit()

        # Reload for fresh state
        ticket = await self._repo.get_by_id(ticket_id)

        logger.info(
            "ticket_status_updated",
            ticket_id=ticket_id,
            agent_id=agent_id,
            old_status=current,
            new_status=new_status,
        )

        # Fire customer email notification — non-blocking, never raises
        await self._notify_customer_status_change(
            ticket=ticket,
            old_status=current,
            new_status=new_status,
            reason=reason,
        )

        return ticket

    async def _notify_customer_status_change(
        self,
        ticket: Ticket,
        old_status: str,
        new_status: str,
        reason: Optional[str],
    ) -> None:
        """
        Resolve customer email from auth-service and send a status change email.
        Failures are logged but never propagated — never block the main flow.
        """
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
            subject = f"[{ticket.ticket_number}] Status updated — {status_label}"

            body_lines = [
                f"Hi,\n",
                f"Your support ticket has been updated.\n",
                f"Ticket:     {ticket.ticket_number}",
                f"Title:      {ticket.title or '(no title)'}",
                f"New Status: {status_label}",
            ]
            if reason:
                body_lines.append(f"Note:       {reason}")

            body_lines += [
                f"\nView your ticket at: https://app.ticketinggenie.com/tickets/{ticket.id}",
                f"\nBest regards,\nTicketing Genie Support Team",
            ]

            await email.send_generic(
                to_email=customer_email,
                subject=subject,
                body="\n".join(body_lines),
            )

            logger.info(
                "customer_status_notification_sent",
                ticket_id=str(ticket.id),
                customer_email=customer_email,
                new_status=new_status,
            )

        except Exception as exc:
            logger.error(
                "customer_status_notification_failed",
                ticket_id=str(ticket.id),
                error=str(exc),
            )

    # ── Customer name lookup ───────────────────────────────────────────────────

    async def get_customer_info(self, customer_id: str) -> dict | None:
        """
        Fetch customer name + email from auth-service.
        Returns {"full_name": str, "email": str} or None on failure.
        Cached result is not stored — caller should cache if needed.
        """
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
            logger.error("get_customer_info_failed", customer_id=customer_id, error=str(exc))
            return None