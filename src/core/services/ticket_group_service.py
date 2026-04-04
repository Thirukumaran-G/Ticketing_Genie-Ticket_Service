from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Optional

from fastapi import BackgroundTasks
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.services.notification_service import NotificationService
from src.data.models.postgres.models import Conversation, Ticket
from src.data.repositories.ticket_group_repository import TicketGroupRepository
from src.data.repositories.ticket_repository import TicketRepository
from src.observability.logging.logger import get_logger

logger = get_logger(__name__)


class TicketGroupService:

    def __init__(
        self,
        session: AsyncSession,
        background_tasks: Optional[BackgroundTasks] = None,
    ) -> None:
        self._session = session
        self._repo = TicketGroupRepository(session)
        self._ticket_repo = TicketRepository(session)
        self._notif_svc = NotificationService(session, background_tasks)

    # ── Confirm group ─────────────────────────────────────────────────────────

    async def confirm_group(
        self,
        group_id: str,
        name: Optional[str],
        lead_id: str,
    ) -> dict:
        group = await self._repo.confirm_group(group_id, name, lead_id)
        if not group:
            raise ValueError(f"Group {group_id} not found.")
        await self._session.commit()
        logger.info("group_confirmed", group_id=group_id, lead_id=lead_id)
        return {"id": str(group.id), "confirmed": True}

    # ── Set parent ticket ─────────────────────────────────────────────────────

    async def set_parent(
        self,
        group_id: str,
        parent_ticket_id: str,
        lead_id: str,
    ) -> dict:
        group = await self._repo.get_group_with_members(group_id)
        if not group:
            raise ValueError(f"Group {group_id} not found.")
        if not group.confirmed_by_lead:
            raise ValueError("Group must be confirmed before setting a parent.")

        # Mark parent ticket
        await self._ticket_repo.update_fields(parent_ticket_id, {"is_parent": True})

        # Freeze all child tickets to in_progress
        child_ids = [
            str(m.ticket_id)
            for m in group.members
            if str(m.ticket_id) != parent_ticket_id
        ]
        for child_id in child_ids:
            await self._ticket_repo.update_fields(child_id, {
                "status": "in_progress",
                "parent_ticket_id": uuid.UUID(parent_ticket_id),
            })

        # Set parent on group
        await self._repo.set_parent_ticket(group_id, parent_ticket_id)
        await self._session.commit()

        logger.info(
            "group_parent_set",
            group_id=group_id,
            parent_ticket_id=parent_ticket_id,
            children_frozen=len(child_ids),
        )
        return {
            "group_id": group_id,
            "parent_ticket_id": parent_ticket_id,
            "children_frozen": len(child_ids),
        }

    # ── Assign parent ticket to agent ─────────────────────────────────────────

    async def assign_parent(
        self,
        group_id: str,
        agent_id: str,
        note: str,
        lead_id: str,
    ) -> dict:
        group = await self._repo.get_group_with_members(group_id)
        if not group:
            raise ValueError(f"Group {group_id} not found.")
        if not group.parent_ticket_id:
            raise ValueError("Set a parent ticket before assigning.")

        parent_ticket_id = str(group.parent_ticket_id)
        child_count = len([
            m for m in group.members
            if str(m.ticket_id) != parent_ticket_id
        ])

        # Assign parent ticket to agent
        await self._ticket_repo.update_fields(parent_ticket_id, {
            "assigned_to": uuid.UUID(agent_id),
            "status": "assigned",
        })

        # Save internal note from lead on parent ticket
        note_content = (
            f"[GROUP_ASSIGNMENT] This ticket represents {child_count + 1} "
            f"customers reporting the same issue.\n\n"
            f"Resolving this ticket will automatically resolve all related customer tickets.\n\n"
            f"Lead note: {note}"
        )
        conv = Conversation(
            ticket_id=uuid.UUID(parent_ticket_id),
            author_id=uuid.UUID(lead_id),
            author_type="team_lead",
            content=note_content,
            is_internal=True,
        )
        self._session.add(conv)
        await self._session.flush()

        await self._session.commit()

        # Notify agent
        parent_ticket = await self._ticket_repo.get_by_id(parent_ticket_id)
        if parent_ticket:
            group_name = group.name or f"Group {group_id[:8]}"
            await self._notif_svc.notify(
                recipient_id=agent_id,
                ticket=parent_ticket,
                notif_type="grouped_ticket_assigned",
                title=f"[Grouped] {parent_ticket.ticket_number} assigned — {child_count + 1} customers",
                message=(
                    f"You have been assigned a grouped ticket.\n\n"
                    f"Ticket       : {parent_ticket.ticket_number}\n"
                    f"Group        : {group_name}\n"
                    f"Customers    : {child_count + 1} affected\n\n"
                    f"Resolving this ticket will automatically resolve all "
                    f"related customer tickets.\n\n"
                    f"Note from lead:\n{note}"
                ),
                is_internal=True,
                email_subject=(
                    f"[{parent_ticket.ticket_number}] Grouped ticket assigned — "
                    f"{child_count + 1} customers affected"
                ),
                email_body=(
                    f"Hi,\n\n"
                    f"A grouped ticket has been assigned to you.\n\n"
                    f"Ticket       : {parent_ticket.ticket_number}\n"
                    f"Group        : {group_name}\n"
                    f"Customers    : {child_count + 1} affected\n\n"
                    f"Resolving this ticket will automatically resolve all "
                    f"related customer tickets.\n\n"
                    f"Note from lead:\n{note}\n\n"
                    f"— Ticketing Genie"
                ),
            )

        logger.info(
            "group_parent_assigned",
            group_id=group_id,
            agent_id=agent_id,
            parent_ticket_id=parent_ticket_id,
        )
        return {"assigned_to": agent_id, "parent_ticket_id": parent_ticket_id}

    # ── Broadcast message to all customers in group ───────────────────────────

    async def broadcast_to_group(
        self,
        group_id: str,
        message: str,
        lead_id: str,
    ) -> dict:
        group = await self._repo.get_group_with_members(group_id)
        if not group:
            raise ValueError(f"Group {group_id} not found.")

        ticket_ids = [str(m.ticket_id) for m in group.members]
        notified = 0

        for ticket_id in ticket_ids:
            ticket = await self._ticket_repo.get_by_id(ticket_id)
            if not ticket:
                continue
            try:
                await self._notif_svc.notify(
                    recipient_id=str(ticket.customer_id),
                    ticket=ticket,
                    notif_type="group_broadcast",
                    title=f"Update on your ticket {ticket.ticket_number}",
                    message=(
                        f"Our support team has a status update on your ticket "
                        f"{ticket.ticket_number}:\n\n{message}"
                    ),
                    is_internal=False,
                    email_subject=f"[{ticket.ticket_number}] Update from support team",
                    email_body=(
                        f"Dear Customer,\n\n"
                        f"Our support team has a status update on your ticket "
                        f"{ticket.ticket_number}:\n\n"
                        f"{message}\n\n"
                        f"— Ticketing Genie Support Team"
                    ),
                )
                notified += 1
            except Exception as exc:
                logger.warning(
                    "broadcast_notify_failed",
                    ticket_id=ticket_id,
                    error=str(exc),
                )

        logger.info(
            "group_broadcast_sent",
            group_id=group_id,
            notified=notified,
            lead_id=lead_id,
        )
        return {"notified": notified, "total": len(ticket_ids)}

    # ── Cascade resolve — called when agent resolves parent ───────────────────

    async def cascade_resolve(
        self,
        parent_ticket_id: str,
        resolution_message: str,
        resolver_id: str,
        resolver_type: str = "agent",
    ) -> dict:
        now = datetime.now(timezone.utc)
        children = await self._repo.get_children_of_parent(parent_ticket_id)

        resolved_numbers = []
        for child in children:
            try:
                await self._ticket_repo.update_fields(str(child.id), {
                    "status": "resolved",
                    "resolved_at": now,
                    "resolved_by": uuid.UUID(resolver_id),
                })
                resolved_numbers.append(child.ticket_number)

                await self._notif_svc.notify(
                    recipient_id=str(child.customer_id),
                    ticket=child,
                    notif_type="ticket_resolved_via_group",
                    title=f"Your ticket {child.ticket_number} has been resolved",
                    message=(
                        f"Great news! Your ticket {child.ticket_number} has been resolved.\n\n"
                        f"{resolution_message}\n\n"
                        f"If you have further issues, please raise a new ticket."
                    ),
                    is_internal=False,
                    email_subject=f"[{child.ticket_number}] Your ticket has been resolved",
                    email_body=(
                        f"Dear Customer,\n\n"
                        f"Your support ticket {child.ticket_number} has been resolved.\n\n"
                        f"{resolution_message}\n\n"
                        f"If you experience further issues, please raise a new ticket "
                        f"through the portal.\n\n"
                        f"— Ticketing Genie Support Team"
                    ),
                )
            except Exception as exc:
                logger.warning(
                    "cascade_resolve_child_failed",
                    child_ticket_id=str(child.id),
                    error=str(exc),
                )

        await self._session.commit()

        # Notify team lead
        parent_ticket = await self._ticket_repo.get_by_id(parent_ticket_id)
        if parent_ticket and parent_ticket.team_id:
            from sqlalchemy import select
            from src.data.models.postgres.models import Team
            r = await self._session.execute(
                select(Team).where(Team.id == parent_ticket.team_id)
            )
            team = r.scalar_one_or_none()
            if team and team.team_lead_id:
                await self._notif_svc.notify(
                    recipient_id=str(team.team_lead_id),
                    ticket=parent_ticket,
                    notif_type="group_cascade_resolved",
                    title=(
                        f"{parent_ticket.ticket_number} resolved — "
                        f"{len(resolved_numbers)} tickets auto-closed"
                    ),
                    message=(
                        f"Parent ticket {parent_ticket.ticket_number} was resolved.\n\n"
                        f"The following tickets were automatically resolved:\n"
                        + "\n".join(f"• {n}" for n in resolved_numbers)
                    ),
                    is_internal=True,
                    email_subject=(
                        f"[{parent_ticket.ticket_number}] Group resolved — "
                        f"{len(resolved_numbers)} tickets auto-closed"
                    ),
                    email_body=(
                        f"Hi,\n\n"
                        f"Parent ticket {parent_ticket.ticket_number} was resolved "
                        f"by the assigned agent.\n\n"
                        f"The following child tickets were automatically resolved:\n"
                        + "\n".join(f"  • {n}" for n in resolved_numbers)
                        + f"\n\n— Ticketing Genie"
                    ),
                )

        logger.info(
            "cascade_resolve_complete",
            parent_ticket_id=parent_ticket_id,
            resolved_count=len(resolved_numbers),
        )
        return {
            "resolved": len(resolved_numbers),
            "ticket_numbers": resolved_numbers,
        }

    # ── Get grouped ticket detail for agent ───────────────────────────────────

    async def get_grouped_ticket_children_with_conversations(
        self,
        parent_ticket_id: str,
        agent_id: str,
    ) -> list[dict]:
        from sqlalchemy import select
        from src.data.models.postgres.models import Conversation

        # Verify agent owns parent ticket
        parent = await self._ticket_repo.get_by_id(parent_ticket_id)
        if not parent:
            raise ValueError("Parent ticket not found.")
        if str(parent.assigned_to) != agent_id:
            raise ValueError("You are not assigned to this ticket.")

        children = await self._repo.get_children_of_parent(parent_ticket_id)
        result = []

        for child in children:
            conv_r = await self._session.execute(
                select(Conversation)
                .where(Conversation.ticket_id == child.id)
                .order_by(Conversation.created_at.asc())
            )
            conversations = list(conv_r.scalars().all())

            result.append({
                "ticket_id": str(child.id),
                "ticket_number": child.ticket_number,
                "title": child.title,
                "status": child.status,
                "customer_id": str(child.customer_id),
                "priority": child.priority,
                "severity": child.severity,
                "created_at": child.created_at,
                "conversations": [
                    {
                        "id": str(c.id),
                        "author_id": str(c.author_id),
                        "author_type": c.author_type,
                        "content": c.content,
                        "is_internal": c.is_internal,
                        "created_at": c.created_at,
                    }
                    for c in conversations
                ],
            })

        return result

    # ── Agent replies to a specific child ticket customer ─────────────────────

    async def reply_to_child(
        self,
        parent_ticket_id: str,
        child_ticket_id: str,
        agent_id: str,
        message: str,
    ) -> dict:
        # Verify agent owns parent
        parent = await self._ticket_repo.get_by_id(parent_ticket_id)
        if not parent or str(parent.assigned_to) != agent_id:
            raise ValueError("Not authorized.")

        # Verify child belongs to this parent
        child = await self._ticket_repo.get_by_id(child_ticket_id)
        if not child or str(child.parent_ticket_id) != parent_ticket_id:
            raise ValueError("Child ticket not found under this parent.")

        # Save conversation on child ticket
        conv = Conversation(
            ticket_id=uuid.UUID(child_ticket_id),
            author_id=uuid.UUID(agent_id),
            author_type="agent",
            content=message,
            is_internal=False,
        )
        self._session.add(conv)
        await self._session.flush()

        # Update first_response_at if not set
        if not child.first_response_at:
            await self._ticket_repo.update_fields(child_ticket_id, {
                "first_response_at": datetime.now(timezone.utc)
            })

        await self._session.commit()

        # Notify child customer
        await self._notif_svc.notify(
            recipient_id=str(child.customer_id),
            ticket=child,
            notif_type="agent_reply",
            title=f"Reply on your ticket {child.ticket_number}",
            message=message,
            is_internal=False,
            email_subject=f"[{child.ticket_number}] Reply from support",
            email_body=(
                f"Dear Customer,\n\n"
                f"Our support agent has replied to your ticket "
                f"{child.ticket_number}:\n\n"
                f"{message}\n\n"
                f"— Ticketing Genie Support Team"
            ),
        )

        logger.info(
            "agent_replied_to_child",
            parent_ticket_id=parent_ticket_id,
            child_ticket_id=child_ticket_id,
            agent_id=agent_id,
        )
        return {"conversation_id": str(conv.id), "child_ticket_id": child_ticket_id}

    # ── Find and group similar tickets (called from Celery) ───────────────────

    async def find_and_group_similar(
    self,
    ticket_id: str,
    embedding: list[float],
    team_id: str,
) -> dict:
        similar = await self._repo.find_similar_tickets(
            embedding=embedding,
            threshold=0.88,
            limit=10,
            exclude_ticket_id=ticket_id,
            team_id=team_id,
        )

        if len(similar) < 1:
            return {"grouped": False, "reason": "no_similar_found"}

        similar_ticket_ids = [str(t.id) for t, _ in similar]
        all_ids = [ticket_id] + similar_ticket_ids

        # Check both confirmed AND unconfirmed groups
        existing_group = await self._repo.find_existing_group_for_tickets(all_ids)

        current_ticket = await self._ticket_repo.get_by_id(ticket_id)
        if not current_ticket:
            return {"grouped": False, "reason": "ticket_not_found"}

        if existing_group:
            # ── Add to existing group ─────────────────────────────────────
            await self._s_refresh_group(existing_group)

            # Add the new ticket
            await self._repo.add_member(
                group_id=str(existing_group.id),
                ticket_id=ticket_id,
                similarity_score=1.0,
            )

            # Add any other similar tickets not yet in this group
            for sim_ticket, score in similar:
                await self._repo.add_member(
                    group_id=str(existing_group.id),
                    ticket_id=str(sim_ticket.id),
                    similarity_score=score,
                )

            await self._session.commit()

            # Get updated member count
            member_count = await self._repo.get_member_count(str(existing_group.id))
            group_name   = existing_group.name or f"Group {str(existing_group.id)[:8]}"
            is_confirmed = existing_group.confirmed_by_lead

            # Notify lead — different message for confirmed vs unconfirmed
            await self._notify_lead_ticket_added_to_group(
                team_id=team_id,
                current_ticket=current_ticket,
                group_id=str(existing_group.id),
                group_name=group_name,
                member_count=member_count,
                is_confirmed=is_confirmed,
            )

            return {
                "grouped":        True,
                "group_id":       str(existing_group.id),
                "is_new_group":   False,
                "is_confirmed":   is_confirmed,
                "member_count":   member_count,
            }

        else:
            # ── Create new group ──────────────────────────────────────────
            id_score_pairs = [(ticket_id, 1.0)] + [
                (str(t.id), score) for t, score in similar
            ]
            group = await self._repo.create_group_with_members(id_score_pairs)
            await self._session.commit()

            member_count = len(id_score_pairs)
            similar_numbers = [t.ticket_number for t, _ in similar]
            all_numbers     = [current_ticket.ticket_number] + similar_numbers

            # Notify lead — new group detected
            await self._notify_lead_new_group(
                team_id=team_id,
                current_ticket=current_ticket,
                group_id=str(group.id),
                all_ticket_numbers=all_numbers,
            )

            return {
                "grouped":      True,
                "group_id":     str(group.id),
                "is_new_group": True,
                "is_confirmed": False,
                "member_count": member_count,
            }
        
    async def _notify_lead_new_group(
    self,
    team_id: str,
    current_ticket,
    group_id: str,
    all_ticket_numbers: list[str],
) -> None:
        try:
            from sqlalchemy import select
            from src.data.models.postgres.models import Team

            r = await self._session.execute(
                select(Team).where(Team.id == current_ticket.team_id)
            )
            team = r.scalar_one_or_none()
            if not team or not team.team_lead_id:
                return

            count = len(all_ticket_numbers)

            await self._notif_svc.notify(
                recipient_id=str(team.team_lead_id),
                ticket=current_ticket,
                notif_type="similar_tickets_detected",
                title=f"{count} tickets may share the same issue",
                message=(
                    f"{count} tickets appear to be reporting the same issue "
                    f"and have been grouped automatically.\n\n"
                    f"Tickets: {', '.join(all_ticket_numbers)}\n\n"
                    f"Review and confirm the group to enable bulk assign "
                    f"and bulk resolve."
                ),
                is_internal=True,
                email_subject=(
                    f"[Action needed] {count} tickets may share the same issue"
                ),
                email_body=(
                    f"Hi,\n\n"
                    f"{count} tickets appear to be reporting the same issue.\n\n"
                    f"Tickets: {', '.join(all_ticket_numbers)}\n\n"
                    f"Please log in to the portal, review the group, and confirm "
                    f"it to enable bulk assign and bulk resolve.\n\n"
                    f"— Ticketing Genie"
                ),
            )
        except Exception as exc:
            logger.warning(
                "notify_lead_new_group_failed",
                ticket_id=str(current_ticket.id),
                error=str(exc),
            )

    async def _s_refresh_group(self, group) -> None:
        try:
            await self._session.refresh(group, ["members"])
        except Exception:
            pass
    
    async def _notify_lead_ticket_added_to_group(
    self,
    team_id: str,
    current_ticket,
    group_id: str,
    group_name: str,
    member_count: int,
    is_confirmed: bool,
) -> None:
        try:
            from sqlalchemy import select
            from src.data.models.postgres.models import Team

            r = await self._session.execute(
                select(Team).where(Team.id == current_ticket.team_id)
            )
            team = r.scalar_one_or_none()
            if not team or not team.team_lead_id:
                return

            status_note = (
                "This group is already confirmed — the new ticket has been "
                "added and is ready for bulk actions."
                if is_confirmed else
                "This group is not yet confirmed — review and confirm it "
                "to enable bulk assign and bulk resolve."
            )

            await self._notif_svc.notify(
                recipient_id=str(team.team_lead_id),
                ticket=current_ticket,
                notif_type="ticket_added_to_existing_group",
                title=(
                    f"New ticket added to group '{group_name}' "
                    f"— now {member_count} tickets"
                ),
                message=(
                    f"Ticket {current_ticket.ticket_number} has been automatically "
                    f"added to an existing group based on similarity.\n\n"
                    f"Group        : {group_name}\n"
                    f"Total tickets: {member_count}\n"
                    f"New ticket   : {current_ticket.ticket_number}\n"
                    f"Title        : {current_ticket.title or 'N/A'}\n\n"
                    f"{status_note}"
                ),
                is_internal=True,
                email_subject=(
                    f"[{current_ticket.ticket_number}] Added to group "
                    f"'{group_name}' — {member_count} tickets total"
                ),
                email_body=(
                    f"Hi,\n\n"
                    f"A new ticket has been automatically added to an existing group.\n\n"
                    f"Group        : {group_name}\n"
                    f"Total tickets: {member_count}\n"
                    f"New ticket   : {current_ticket.ticket_number}\n"
                    f"Title        : {current_ticket.title or 'N/A'}\n\n"
                    f"{status_note}\n\n"
                    f"— Ticketing Genie"
                ),
            )
        except Exception as exc:
            logger.warning(
                "notify_lead_ticket_added_to_group_failed",
                ticket_id=str(current_ticket.id),
                error=str(exc),
            )