"""
Seed notification templates.
src/data/seeds/notification_templates.py

Run once:
    python -m src.data.seeds.notification_templates

Templates seeded:
  1. sla_apology          — TL sends to customer when SLA breached
  2. ticket_on_hold       — Customer informed ticket placed on hold
  3. ticket_resolved      — Resolution confirmation to customer
  4. breach_agent_warning — TL internal note to agent about breach timeline
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from src.observability.logging.logger import get_logger

logger = get_logger(__name__)

TEMPLATES = [
    {
        "key":       "sla_apology",
        "name":      "SLA Breach Apology",
        "subject":   "We apologise for the delay on your ticket [{ticket_number}]",
        "body": (
            "Dear {customer_name},\n\n"
            "We sincerely apologise for the delay in resolving your support ticket "
            "{ticket_number} — {ticket_title}.\n\n"
            "We understand this has caused inconvenience and we take full responsibility "
            "for not meeting our committed response time.\n\n"
            "Our team is actively working on your issue and you will hear from us by "
            "{commit_time}.\n\n"
            "{custom_message}\n\n"
            "Thank you for your patience and understanding.\n\n"
            "Best regards,\n"
            "{team_lead_name}\n"
            "Support Team Lead\n"
            "Ticketing Genie"
        ),
        "variables": [
            "customer_name",
            "ticket_number",
            "ticket_title",
            "commit_time",
            "custom_message",
            "team_lead_name",
        ],
        "is_active": True,
    },
    {
        "key":       "ticket_on_hold",
        "name":      "Ticket Placed On Hold",
        "subject":   "Update on your ticket [{ticket_number}] — placed on hold",
        "body": (
            "Dear {customer_name},\n\n"
            "We wanted to keep you informed that your support ticket "
            "{ticket_number} — {ticket_title} has been temporarily placed on hold.\n\n"
            "Reason: {hold_reason}\n\n"
            "We expect to resume work on your ticket by {resume_date}. "
            "You will be notified as soon as we have an update.\n\n"
            "If you have any urgent concerns in the meantime, please do not hesitate "
            "to reply to this message.\n\n"
            "Best regards,\n"
            "Ticketing Genie Support Team"
        ),
        "variables": [
            "customer_name",
            "ticket_number",
            "ticket_title",
            "hold_reason",
            "resume_date",
        ],
        "is_active": True,
    },
    {
        "key":       "ticket_resolved",
        "name":      "Ticket Resolved Confirmation",
        "subject":   "Your ticket [{ticket_number}] has been resolved",
        "body": (
            "Dear {customer_name},\n\n"
            "We are pleased to let you know that your support ticket "
            "{ticket_number} — {ticket_title} has been resolved.\n\n"
            "Resolution summary:\n"
            "{resolution_summary}\n\n"
            "If you feel your issue has not been fully addressed or you experience "
            "the problem again, please reply to this message and we will reopen "
            "your ticket immediately.\n\n"
            "We value your feedback — please take a moment to rate your support "
            "experience.\n\n"
            "Thank you for choosing Ticketing Genie.\n\n"
            "Best regards,\n"
            "Ticketing Genie Support Team"
        ),
        "variables": [
            "customer_name",
            "ticket_number",
            "ticket_title",
            "resolution_summary",
        ],
        "is_active": True,
    },
    {
        "key":       "breach_agent_warning",
        "name":      "Agent SLA Breach Warning (Internal)",
        "subject":   "ACTION REQUIRED — SLA breach on ticket [{ticket_number}]",
        "body": (
            "Hi {agent_name},\n\n"
            "This is an urgent notice that ticket {ticket_number} — {ticket_title} "
            "has breached its {breach_type} SLA.\n\n"
            "Breach details:\n"
            "- SLA type:    {breach_type}\n"
            "- Breached at: {breach_time}\n"
            "- Customer:    {customer_name}\n"
            "- Priority:    {priority}\n\n"
            "Required action:\n"
            "{required_action}\n\n"
            "Please respond to the customer and submit your breach justification "
            "through the ticket detail page within the next {deadline} hours.\n\n"
            "— {team_lead_name}\n"
            "Team Lead"
        ),
        "variables": [
            "agent_name",
            "ticket_number",
            "ticket_title",
            "breach_type",
            "breach_time",
            "customer_name",
            "priority",
            "required_action",
            "deadline",
            "team_lead_name",
        ],
        "is_active": True,
    },
]


async def seed() -> None:
    from src.data.clients.postgres_client import CelerySessionFactory
    from src.data.models.postgres.models import NotificationTemplate
    from sqlalchemy import select

    async with CelerySessionFactory() as session:
        for tpl_data in TEMPLATES:
            existing = await session.execute(
                select(NotificationTemplate).where(
                    NotificationTemplate.key == tpl_data["key"]
                )
            )
            row = existing.scalar_one_or_none()

            if row:
                # Update body/subject/variables if template already exists
                # but never overwrite TL edits to body — only update if unchanged
                row.name      = tpl_data["name"]
                row.variables = tpl_data["variables"]
                row.is_active = tpl_data["is_active"]
                # Only reset subject/body if they still match the original
                # (meaning TL has not customised them yet)
                logger.info(
                    "notification_template_exists_skipping_body",
                    key=tpl_data["key"],
                )
            else:
                import uuid6
                new_tpl = NotificationTemplate(
                    id=uuid6.uuid7(),
                    key=tpl_data["key"],
                    name=tpl_data["name"],
                    subject=tpl_data["subject"],
                    body=tpl_data["body"],
                    variables=tpl_data["variables"],
                    is_active=tpl_data["is_active"],
                )
                session.add(new_tpl)
                logger.info("notification_template_seeded", key=tpl_data["key"])

        await session.commit()
        logger.info("notification_templates_seed_complete", count=len(TEMPLATES))


if __name__ == "__main__":
    asyncio.run(seed())