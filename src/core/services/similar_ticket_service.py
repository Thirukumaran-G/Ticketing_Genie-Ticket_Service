"""
SimilarTicketService — detects similar tickets, manages groups, bulk actions.
src/core/services/similar_ticket_service.py
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy.ext.asyncio import AsyncSession

from src.data.models.postgres.models import (
    Notification,
    SimilarTicketGroup,
    SimilarTicketGroupMember,
    Ticket,
)
from src.data.repositories.similar_ticket_repository import SimilarTicketRepository
from src.data.repositories.ticket_repository import (
    NotificationRepository,
    TicketRepository,
)
from src.observability.logging.logger import get_logger

logger = get_logger(__name__)

# Cosine similarity threshold — tickets above this are considered similar
SIMILARITY_THRESHOLD = 0.82


class SimilarTicketService:

    def __init__(self, session: AsyncSession) -> None:
        self._session     = session
        self._repo        = SimilarTicketRepository(session)
        self._ticket_repo = TicketRepository(session)
        self._notif_repo  = NotificationRepository(session)

    # ── Detection (called from ai_worker after classification) ─────────────────

    async def detect_and_group(
        self,
        ticket_id:    str,
        embedding:    list[float],
        team_lead_id: Optional[str],
    ) -> Optional[SimilarTicketGroup]:
        """
        Find similar tickets for the given embedding.
        If matches found:
          - Create or extend a SimilarTicketGroup
          - Add source ticket + all matches as members
          - Notify TL via in_app + SSE

        Returns the group if created/extended, None if no matches.
        """
        similar = await self._repo.find_similar_tickets(
            ticket_id=ticket_id,
            embedding=embedding,
            threshold=SIMILARITY_THRESHOLD,
        )

        if not similar:
            logger.info(
                "no_similar_tickets_found",
                ticket_id=ticket_id,
                threshold=SIMILARITY_THRESHOLD,
            )
            return None

        logger.info(
            "similar_tickets_found",
            ticket_id=ticket_id,
            count=len(similar),
            top_score=round(similar[0][1], 3) if similar else 0,
        )

        # Check if any matched ticket already belongs to a group
        existing_group: Optional[SimilarTicketGroup] = None
        for matched_ticket, _ in similar:
            groups = await self._repo.get_groups_for_ticket(str(matched_ticket.id))
            if groups:
                unconfirmed = [g for g in groups if not g.confirmed_by_lead]
                existing_group = unconfirmed[0] if unconfirmed else groups[0]
                break

        if existing_group:
            group = existing_group
            logger.info(
                "extending_existing_group",
                group_id=str(group.id),
                ticket_id=ticket_id,
            )
        else:
            group = await self._repo.create_group()

        # Add source ticket to the group
        source_ticket = await self._ticket_repo.get_by_id(ticket_id)
        if source_ticket and source_ticket.ticket_embedding:
            await self._repo.add_member(
                group_id=str(group.id),
                ticket_id=ticket_id,
                similarity_score=1.0,
            )

        # Add all similar tickets to the group
        for matched_ticket, score in similar:
            await self._repo.add_member(
                group_id=str(group.id),
                ticket_id=str(matched_ticket.id),
                similarity_score=score,
            )

        await self._session.commit()

        # Notify TL
        if team_lead_id:
            await self._notify_tl_similar_detected(
                group=group,
                source_ticket=source_ticket,
                similar_tickets=similar,
                team_lead_id=team_lead_id,
            )

        return group

    # ── Group management ───────────────────────────────────────────────────────

    async def list_groups(
        self,
        confirmed_only: bool = False,
    ) -> list[SimilarTicketGroup]:
        return await self._repo.list_groups(confirmed_only=confirmed_only)

    async def get_group(self, group_id: str) -> SimilarTicketGroup:
        group = await self._repo.get_group_by_id(group_id)
        if not group:
            raise ValueError(f"Group {group_id} not found.")
        return group

    async def confirm_group(
        self,
        group_id:     str,
        lead_user_id: str,
        name:         Optional[str] = None,
    ) -> SimilarTicketGroup:
        group = await self._repo.confirm_group(
            group_id=group_id,
            lead_user_id=lead_user_id,
            name=name,
        )
        if not group:
            raise ValueError(f"Group {group_id} not found.")
        await self._session.commit()
        logger.info(
            "group_confirmed_by_lead",
            group_id=group_id,
            lead_user_id=lead_user_id,
        )
        return group

    async def add_ticket_to_group(
        self,
        group_id:         str,
        ticket_id:        str,
        similarity_score: float = 0.0,
    ) -> SimilarTicketGroupMember:
        group = await self._repo.get_group_by_id(group_id)
        if not group:
            raise ValueError(f"Group {group_id} not found.")

        ticket = await self._ticket_repo.get_by_id(ticket_id)
        if not ticket:
            raise ValueError(f"Ticket {ticket_id} not found.")

        member = await self._repo.add_member(
            group_id=group_id,
            ticket_id=ticket_id,
            similarity_score=similarity_score,
        )
        await self._session.commit()
        return member

    async def remove_ticket_from_group(
        self,
        group_id:  str,
        ticket_id: str,
    ) -> bool:
        removed = await self._repo.remove_member(
            group_id=group_id,
            ticket_id=ticket_id,
        )
        if removed:
            await self._session.commit()
        return removed

    async def get_groups_for_ticket(
        self,
        ticket_id: str,
    ) -> list[SimilarTicketGroup]:
        return await self._repo.get_groups_for_ticket(ticket_id)

    # ── Bulk assign ────────────────────────────────────────────────────────────

    async def bulk_assign(
        self,
        group_id:         str,
        agent_user_id:    str,
        lead_user_id:     str,
        internal_message: str,
    ) -> dict:
        """
        Assign all open tickets in the group to a single agent.

        Returns summary: { assigned: int, skipped: int, ticket_numbers: list }
        """
        from src.core.reddis.assignment_lock import assignment_lock, AssignmentLockError
        from src.data.models.postgres.models import Conversation
        import asyncio

        group = await self.get_group(group_id)

        assigned_count  = 0
        skipped_count   = 0
        ticket_numbers: list[str] = []

        for member in group.members:
            ticket = member.ticket
            if not ticket:
                skipped_count += 1
                continue

            if ticket.status in ("resolved", "closed"):
                skipped_count += 1
                continue

            try:
                async with assignment_lock(str(ticket.id)):
                    fresh = await self._ticket_repo.get_by_id(str(ticket.id))
                    if not fresh:
                        skipped_count += 1
                        continue

                    await self._ticket_repo.update_fields(str(ticket.id), {
                        "assigned_to": uuid.UUID(agent_user_id),
                        "team_id":     fresh.team_id,
                    })

                    note = Conversation(
                        ticket_id=ticket.id,
                        author_id=uuid.UUID(lead_user_id),
                        author_type="team_lead",
                        content=(
                            f"[BULK_ASSIGNED] Group: {group.name or str(group.id)}\n"
                            f"Assigned to agent by team lead.\n\n"
                            f"{internal_message}"
                        ),
                        is_internal=True,
                        is_ai_draft=False,
                    )
                    self._session.add(note)

                    assigned_count += 1
                    ticket_numbers.append(fresh.ticket_number)

            except AssignmentLockError:
                logger.warning(
                    "bulk_assign_lock_failed",
                    ticket_id=str(ticket.id),
                )
                skipped_count += 1
                continue

        await self._session.commit()

        if assigned_count > 0:
            await self._notify_agent_bulk_assign(
                agent_user_id=agent_user_id,
                ticket_numbers=ticket_numbers,
                group_name=group.name,
                internal_message=internal_message,
                group=group,
            )

        logger.info(
            "bulk_assign_complete",
            group_id=group_id,
            assigned=assigned_count,
            skipped=skipped_count,
            agent_user_id=agent_user_id,
        )

        return {
            "assigned":       assigned_count,
            "skipped":        skipped_count,
            "ticket_numbers": ticket_numbers,
        }

    # ── Bulk resolve ───────────────────────────────────────────────────────────

    async def bulk_resolve(
        self,
        group_id:           str,
        lead_user_id:       str,
        resolution_message: str,
    ) -> dict:
        """
        Resolve all open tickets in the group.
        Returns summary: { resolved: int, skipped: int, ticket_numbers: list }
        """
        from src.data.models.postgres.models import Conversation
        from src.data.repositories.notification_preference_repository import (
            NotificationPreferenceRepository,
        )

        group     = await self.get_group(group_id)
        pref_repo = NotificationPreferenceRepository(self._session)

        now            = datetime.now(timezone.utc)
        resolved_count = 0
        skipped_count  = 0
        ticket_numbers: list[str] = []

        for member in group.members:
            ticket = member.ticket
            if not ticket:
                skipped_count += 1
                continue

            if ticket.status in ("resolved", "closed"):
                skipped_count += 1
                continue

            await self._ticket_repo.update_fields(str(ticket.id), {
                "status":      "resolved",
                "resolved_at": now,
                "resolved_by": lead_user_id,
            })

            note = Conversation(
                ticket_id=ticket.id,
                author_id=uuid.UUID(lead_user_id),
                author_type="team_lead",
                content=(
                    f"[BULK_RESOLVED] Group: {group.name or str(group.id)}\n"
                    f"Resolved by team lead as part of bulk resolution.\n\n"
                    f"Message sent to customer:\n{resolution_message}"
                ),
                is_internal=True,
                is_ai_draft=False,
            )
            self._session.add(note)

            await self._notify_customer_bulk_resolve(
                ticket=ticket,
                resolution_message=resolution_message,
                pref_repo=pref_repo,
            )

            resolved_count += 1
            ticket_numbers.append(ticket.ticket_number)

        await self._session.commit()

        logger.info(
            "bulk_resolve_complete",
            group_id=group_id,
            resolved=resolved_count,
            skipped=skipped_count,
        )

        return {
            "resolved":       resolved_count,
            "skipped":        skipped_count,
            "ticket_numbers": ticket_numbers,
        }

    # ── TL notification — similar tickets detected ─────────────────────────────

    async def _notify_tl_similar_detected(
        self,
        group:           SimilarTicketGroup,
        source_ticket:   Optional[Ticket],
        similar_tickets: list[tuple[Ticket, float]],
        team_lead_id:    str,
    ) -> None:
        try:
            from src.core.sse.sse_manager import sse_manager
            import asyncio as _asyncio

            count         = len(similar_tickets)
            source_number = source_ticket.ticket_number if source_ticket else "unknown"
            top_numbers   = ", ".join(
                t.ticket_number for t, _ in similar_tickets[:3]
            )

            notif_title   = f"Similar tickets detected — {source_number}"
            notif_message = (
                f"{count} ticket{'s' if count != 1 else ''} found similar to "
                f"{source_number}.\n"
                f"Top matches: {top_numbers}\n\n"
                f"Review the group and confirm if these share a root cause."
            )

            notif = Notification(
                channel="in_app",
                recipient_id=uuid.UUID(team_lead_id),
                ticket_id=source_ticket.id if source_ticket else None,
                is_internal=True,
                type="similar_tickets_detected",
                title=notif_title,
                message=notif_message,
            )
            self._session.add(notif)
            await self._session.flush()

            async def _push():
                try:
                    await sse_manager.push(
                        team_lead_id,
                        {
                            "event": "notification",
                            "data": {
                                "type":          "similar_tickets_detected",
                                "title":         notif_title,
                                "message":       notif_message,
                                "ticket_number": source_number,
                                "group_id":      str(group.id),
                                "count":         count,
                            },
                        },
                    )
                except Exception as exc:
                    logger.warning(
                        "similar_tickets_sse_push_failed",
                        error=str(exc),
                    )

            _asyncio.create_task(_push())

        except Exception as exc:
            logger.error(
                "notify_tl_similar_tickets_failed",
                group_id=str(group.id),
                error=str(exc),
            )

    # ── Agent notification — bulk assign ──────────────────────────────────────

    async def _notify_agent_bulk_assign(
        self,
        agent_user_id:    str,
        ticket_numbers:   list[str],
        group_name:       Optional[str],
        internal_message: str,
        group:            SimilarTicketGroup,
    ) -> None:
        try:
            from src.core.sse.sse_manager import sse_manager
            import asyncio as _asyncio

            numbers_str   = ", ".join(ticket_numbers[:5])
            extra         = f" (+{len(ticket_numbers) - 5} more)" if len(ticket_numbers) > 5 else ""
            notif_title   = f"{len(ticket_numbers)} tickets assigned to you"
            notif_message = (
                f"Team lead has assigned a group of related tickets to you.\n\n"
                f"Group: {group_name or str(group.id)}\n"
                f"Tickets: {numbers_str}{extra}\n\n"
                f"Team lead note:\n{internal_message}"
            )

            notif = Notification(
                channel="in_app",
                recipient_id=uuid.UUID(agent_user_id),
                ticket_id=None,
                is_internal=True,
                type="bulk_assigned",
                title=notif_title,
                message=notif_message,
            )
            self._session.add(notif)
            await self._session.flush()

            async def _push():
                try:
                    await sse_manager.push(
                        agent_user_id,
                        {
                            "event": "notification",
                            "data": {
                                "type":           "bulk_assigned",
                                "title":          notif_title,
                                "message":        notif_message,
                                "ticket_numbers": ticket_numbers,
                                "group_id":       str(group.id),
                            },
                        },
                    )
                except Exception as exc:
                    logger.warning(
                        "bulk_assign_sse_push_failed",
                        agent_user_id=agent_user_id,
                        error=str(exc),
                    )

            _asyncio.create_task(_push())

        except Exception as exc:
            logger.error(
                "notify_agent_bulk_assign_failed",
                agent_user_id=agent_user_id,
                error=str(exc),
            )

    # ── Customer notification — bulk resolve ───────────────────────────────────

    async def _notify_customer_bulk_resolve(
        self,
        ticket:             Ticket,
        resolution_message: str,
        pref_repo,
    ) -> None:
        try:
            channel = await pref_repo.get_preferred_contact(str(ticket.customer_id))

            if channel == "in_app":
                notif = Notification(
                    channel="in_app",
                    recipient_id=ticket.customer_id,
                    ticket_id=ticket.id,
                    is_internal=False,
                    type="ticket_resolved",
                    title=f"Your ticket {ticket.ticket_number} has been resolved",
                    message=resolution_message,
                )
                self._session.add(notif)

                from src.core.sse.sse_manager import sse_manager
                import asyncio as _asyncio

                async def _push():
                    try:
                        await sse_manager.push(
                            str(ticket.customer_id),
                            {
                                "event": "notification",
                                "data": {
                                    "type":          "ticket_resolved",
                                    "title":         f"Your ticket {ticket.ticket_number} has been resolved",
                                    "message":       resolution_message,
                                    "ticket_number": ticket.ticket_number,
                                },
                            },
                        )
                    except Exception:
                        pass

                _asyncio.create_task(_push())

            else:
                try:
                    from src.handlers.http_clients.auth_client import AuthHttpClient
                    from src.handlers.http_clients.email_client import EmailClient

                    auth  = AuthHttpClient()
                    user  = await auth.get_user_by_id(str(ticket.customer_id))
                    email = user.get("email") if user else None
                    if email:
                        await EmailClient().send_generic(
                            to_email=email,
                            subject=f"[{ticket.ticket_number}] Your ticket has been resolved",
                            body=resolution_message,
                        )
                except Exception as exc:
                    logger.warning(
                        "bulk_resolve_customer_email_failed",
                        ticket_id=str(ticket.id),
                        error=str(exc),
                    )

        except Exception as exc:
            logger.warning(
                "notify_customer_bulk_resolve_failed",
                ticket_id=str(ticket.id),
                error=str(exc),
            )